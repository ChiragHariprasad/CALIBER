"""CALIBRE Apache Flink Streaming Integrity Job per CAL-DOC-004, CAL-DOC-005, and CAL-DOC-007.

Responsibilities:
1. Ingest EvidenceEvents from calibre.evidence.* topics
2. Ingest GuardFacts from calibre.guard.liveness
3. Ingest RunClockTicks from calibre.guard.clock
4. Ingest dynamic DEC updates from calibre.config.contracts
5. Scope-aware keying by session / driver / lap
6. Embed pure zero-dependency SIA ContractEvaluator for decision-scoped evaluation
7. Route valid FlinkOutputEnvelope and IntegrityAssessmentVerdicts to:
   - calibre.out.database
   - calibre.out.dashboard
   - calibre.out.analytics
   - calibre.core.verdict_history
8. Route failure and side-outputs:
   - calibre.dlq: Malformed / unprocessable inputs
   - calibre.side.quarantine: Post-finality quarantined arrivals
   - calibre.side.overflow: Bounded search / envelope overflows (M_max=10, C_max=32)
9. Expose observable processing metrics and health status
"""
from __future__ import annotations

import glob
import logging
from pathlib import Path
import threading
import time
from typing import Any, Dict, List, Optional

from calibre_core.contracts import (
    EvidenceEvent,
    FlinkOutputEnvelope,
    DeadLetterEnvelope,
    utc_now,
    stable_id,
    generate_uuid,
)
from calibre_core.health import ComponentHealth, HealthStatus
from calibre_core.kafka_bus import CalibreProducer, CalibreConsumer
from calibre_core.topics import (
    TOPIC_EVIDENCE_PREFIX,
    TOPIC_OUT_DATABASE,
    TOPIC_OUT_DASHBOARD,
    TOPIC_OUT_ANALYTICS,
    TOPIC_DLQ,
    TOPIC_VERDICT_HISTORY,
    TOPIC_GUARD_LIVENESS,
    TOPIC_GUARD_CLOCK,
    TOPIC_CONFIG_CONTRACTS,
    TOPIC_SIDE_QUARANTINE,
    TOPIC_SIDE_OVERFLOW,
    GROUP_FLINK_CORE,
    all_standard_topics,
)
from sia_core.contracts.models import DecisionEvidenceContract
from sia_core.contracts.parser import load_contract_file, parse_contract_dict
from sia_core.evaluation.contract_evaluator import ContractEvaluator
from sia_core.liveness.run_clock import RunClockManager
from sia_core.models.evidence import RunClockTick

logger = logging.getLogger("calibre.flink")


class CalibreFlinkJob:
    """Core stream processing pipeline implementation representing the Flink integrity kernel."""

    def __init__(
        self,
        bootstrap_servers: Optional[str] = None,
        job_id: str = "calibre-flink-stream-job-v1",
        contracts_dir: Optional[str] = None,
        enable_sia: bool = True,
    ) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.job_id = job_id
        self.enable_sia = enable_sia
        self.producer = CalibreProducer(bootstrap_servers=bootstrap_servers, client_id=f"flink-{job_id}-producer")
        self.health = ComponentHealth(name="FlinkIntegrityCore")
        self.health.set_dependency("kafka_producer", self.producer.is_connected)
        
        self.is_running = False
        self._worker_thread: Optional[threading.Thread] = None
        self.events_received = 0
        self.events_routed = 0
        self.events_malformed = 0
        self.verdicts_emitted = 0
        self.quarantined_events = 0
        self.overflow_events = 0

        # SIA In-Flight State
        self.contracts: Dict[str, DecisionEvidenceContract] = {}
        self.decision_states: Dict[str, Dict[str, Any]] = {}
        self.run_clock_mgr: RunClockManager = RunClockManager()
        
        # Load Contract Definitions
        self._load_contracts(contracts_dir or "sia_core/contracts/definitions")

    def _load_contracts(self, path_str: str) -> None:
        """Load standard DEC contracts into registry."""
        p = Path(path_str)
        if p.exists() and p.is_dir():
            for yaml_file in p.glob("*.yaml"):
                try:
                    c = load_contract_file(yaml_file)
                    self.contracts[c.contract_id] = c
                except Exception as exc:
                    logger.warning("Failed to load contract %s: %s", yaml_file, exc)
            logger.info("Flink job %s loaded %d DEC contracts from %s", self.job_id, len(self.contracts), path_str)

    def validate_evidence_event(self, data: dict[str, Any]) -> tuple[bool, str]:
        """Verify presence and validity of required envelope fields."""
        if not isinstance(data, dict):
            return False, "Payload is not a JSON object"
        
        required_fields = ["event_id", "correlation_id", "event_type", "domain", "evidence"]
        missing = [f for f in required_fields if f not in data or data[f] is None]
        if missing:
            return False, f"Missing required fields: {', '.join(missing)}"
        
        return True, "VALID"

    def process_evidence_envelope(self, data: dict[str, Any]) -> Optional[FlinkOutputEnvelope]:
        """Validate, wrap, and route a single evidence event with SIA evaluation."""
        self.events_received += 1
        start_time = time.perf_counter()

        is_valid, validation_msg = self.validate_evidence_event(data)
        corr_id = data.get("correlation_id", "unknown") if isinstance(data, dict) else "unknown"

        if not is_valid:
            self.events_malformed += 1
            self.health.record_activity(error=True)
            logger.warning("Flink rejected malformed evidence event (corr=%s): %s", corr_id, validation_msg)
            
            # Route to DLQ
            self.producer.send_dlq(
                component="flink_integrity_core",
                stage="ENVELOPE_VALIDATION",
                error_code="SCHEMA_VALIDATION_FAILURE",
                error_message=validation_msg,
                raw_payload=data,
                correlation_id=corr_id,
            )
            return None

        # Build FlinkOutputEnvelope
        event_id = data["event_id"]
        latency_ms = (time.perf_counter() - start_time) * 1000.0
        now_str = utc_now()
        
        output_envelope = FlinkOutputEnvelope(
            envelope_id=stable_id("FLINK", event_id, now_str),
            correlation_id=corr_id,
            processing_stage="FLINK_INTEGRITY_VALIDATED",
            integrity_status="NOT_EVALUATED",  # Decoupled from pipeline transport health
            processed_at_utc=now_str,
            event=data,
            routed_to=["PERSISTENCE", "DASHBOARD", "ANALYTICS"],
            flink_job_id=self.job_id,
            processing_latency_ms=round(latency_ms, 3),
            validation_status="VALID",
        )

        # Multi-sink fan-out to 3 independent downstream topics
        p_ok = self.producer.send(TOPIC_OUT_DATABASE, output_envelope, correlation_id=corr_id)
        d_ok = self.producer.send(TOPIC_OUT_DASHBOARD, output_envelope, correlation_id=corr_id)
        a_ok = self.producer.send(TOPIC_OUT_ANALYTICS, output_envelope, correlation_id=corr_id)

        if p_ok and d_ok and a_ok:
            self.events_routed += 1
            self.health.record_activity(events_in=1, events_out=3)
            logger.debug("Flink routed event %s (corr=%s) to 3 downstream sinks", event_id, corr_id)
        else:
            self.health.record_activity(error=True)
            logger.error("Flink encountered error fanning out event %s", event_id)

        # Perform SIA Decision Integrity Evaluation if enabled
        if self.enable_sia:
            self._evaluate_sia_for_event(data, now_str)

        return output_envelope

    def _evaluate_sia_for_event(self, data: dict[str, Any], transport_time: str) -> None:
        """Evaluate relevant DECs using the pure ContractEvaluator."""
        domain = data.get("domain", "")
        scope = data.get("scope", {})
        session_id = scope.get("session_type", "session")
        driver_id = str(scope.get("driver_number", "1"))
        evidence = data.get("evidence", {})
        obs = evidence.get("observations", {}) if isinstance(evidence, dict) else {}
        corr_id = data.get("correlation_id", "unknown")

        # Extract value
        val = 100.0
        if "speed" in obs:
            val = float(obs["speed"])
        elif "lap_duration" in obs:
            val = float(obs["lap_duration"])
        elif "value" in obs:
            try:
                val = float(obs["value"])
            except (ValueError, TypeError):
                val = 100.0

        # Construct stream item
        atom_id = f"atom-{data.get('event_id', generate_uuid())}"
        stream_item = {
            "stream_item_type": "EVIDENCE_ATOM",
            "transport_event_time_utc": data.get("produced_at_utc", transport_time),
            "transport_time_basis": "EVENT_UTC",
            "watermark_at_arrival": self.run_clock_mgr.watermark or transport_time,
            "data": {
                "evidence_atom_id": atom_id,
                "role_id": domain,
                "value": val,
                "unit": data.get("time", {}).get("source_unit", "s"),
                "raw_event_ids": [data.get("event_id", "")],
                "processor_family_id": data.get("producer", {}).get("processor_id", "generic_processor"),
                "transform_version": data.get("producer", {}).get("processor_version", "1.0.0"),
                "source_origin_set": [data.get("provenance", {}).get("provider", "MOCK")],
                "time": {
                    "value": data.get("time", {}).get("source_value", 0.0),
                    "class": "ABSOLUTE_UTC" if "Z" in str(data.get("time", {}).get("source_value", "")) else "SESSION_ELAPSED",
                },
                "quality_grade": "HEALTHY",
            },
        }

        # Match contracts by role or target decision type
        for contract_id, contract in list(self.contracts.items()):
            # Find matching roles for this domain/event
            matched_role = None
            dom_lower = domain.lower()
            evt_lower = data.get("event_type", "").lower()

            for r_name in contract.roles.keys():
                r_lower = r_name.lower()
                if dom_lower in r_lower or r_lower in dom_lower:
                    matched_role = r_name
                    break
                elif "telemetry" in r_lower and ("vehicle" in dom_lower or "telemetry" in evt_lower):
                    matched_role = r_name
                    break
                elif "speed" in r_lower and ("vehicle" in dom_lower or "speed" in evt_lower):
                    matched_role = r_name
                    break
                elif "lap" in r_lower and ("timing" in dom_lower or "lap" in evt_lower):
                    matched_role = r_name
                    break
                elif "pit" in r_lower and ("timing" in dom_lower or "pit" in evt_lower):
                    matched_role = r_name
                    break

            if not matched_role:
                if dom_lower in contract_id.lower() or (dom_lower == "timing" and "lap" in contract_id):
                    matched_role = list(contract.roles.keys())[0]
                else:
                    continue

            decision_key = f"{session_id}|{driver_id}|{contract_id}"
            state = self.decision_states.setdefault(decision_key, {
                "input_stream": [],
                "latest_verdict": None,
                "watermark": None,
            })

            # Invariant G11: Post-finality check
            latest_v = state.get("latest_verdict")
            if latest_v and latest_v.get("finality_status") == "FINAL":
                # Atom arrived post-finality -> route to quarantine side-output
                logger.warning("Post-finality evidence quarantined for %s: atom=%s", decision_key, atom_id)
                self.quarantined_events += 1
                self.producer.send(
                    TOPIC_SIDE_QUARANTINE,
                    {
                        "decision_instance_id": decision_key,
                        "quarantined_atom_id": atom_id,
                        "finality_status": "FINAL",
                        "quarantined_at_utc": transport_time,
                        "raw_event": data,
                        "correlation_id": corr_id,
                    },
                    correlation_id=corr_id,
                )
                continue

            # Clone stream item with matching role_id for this contract
            contract_stream_item = dict(stream_item)
            contract_stream_item["data"] = dict(stream_item["data"])
            contract_stream_item["data"]["role_id"] = matched_role

            state["input_stream"].append(contract_stream_item)
            t_eval = self.run_clock_mgr.compute_t_eval(
                watermark=self.run_clock_mgr.watermark,
                run_clock=transport_time,
            )

            try:
                verdict = ContractEvaluator.evaluate(
                    contract=contract,
                    decision_instance_id=decision_key,
                    input_stream=state["input_stream"],
                    t_eval=t_eval,
                    previous_verdict=latest_v,
                )
                state["latest_verdict"] = verdict
                self.verdicts_emitted += 1

                # Attach correlation_id to verdict payload for end-to-end tracing
                verdict_payload = dict(verdict)
                verdict_payload["correlation_id"] = corr_id

                # Check for budget overflow (M_max=10 or C_max=32)
                witness_str = " ".join(verdict.get("witness_set", []))
                if "SEARCH_BUDGET_EXCEEDED" in witness_str or "ENVELOPE_CAPPED" in witness_str:
                    self.overflow_events += 1
                    self.producer.send(
                        TOPIC_SIDE_OVERFLOW,
                        {
                            "decision_instance_id": decision_key,
                            "witness": witness_str,
                            "occurred_at_utc": transport_time,
                            "verdict_hash": verdict.get("verdict_hash"),
                            "correlation_id": corr_id,
                        },
                        correlation_id=corr_id,
                    )

                # Route verdict to history topic and downstream sinks
                self.producer.send(TOPIC_VERDICT_HISTORY, verdict_payload, correlation_id=corr_id)
                self.producer.send(TOPIC_OUT_DATABASE, verdict_payload, correlation_id=corr_id)
                self.producer.send(TOPIC_OUT_DASHBOARD, verdict_payload, correlation_id=corr_id)
                self.producer.send(TOPIC_OUT_ANALYTICS, verdict_payload, correlation_id=corr_id)
                logger.debug("Emitted SIA verdict %s (state=%s) for %s", verdict.get("verdict_hash", "")[:8], verdict.get("integrity_state"), decision_key)

            except Exception as exc:
                logger.error("Error evaluating SIA contract %s for key %s: %s", contract_id, decision_key, exc)

    def process_guard_fact(self, data: dict[str, Any]) -> None:
        """Process durable source liveness guard fact and re-evaluate dependent decisions."""
        logger.info("Flink received GuardFact: %s", data.get("fact_id", "unknown"))
        guard_item = {
            "stream_item_type": "GUARD_FACT",
            "transport_event_time_utc": data.get("detection_clock", utc_now()),
            "data": data,
        }
        # Fan out to all active decision states
        for decision_key, state in list(self.decision_states.items()):
            state["input_stream"].append(guard_item)
            contract_id = decision_key.split("|")[-1]
            contract = self.contracts.get(contract_id)
            if contract:
                t_eval = data.get("detection_clock") or utc_now()
                try:
                    verdict = ContractEvaluator.evaluate(
                        contract=contract,
                        decision_instance_id=decision_key,
                        input_stream=state["input_stream"],
                        t_eval=t_eval,
                        previous_verdict=state.get("latest_verdict"),
                    )
                    state["latest_verdict"] = verdict
                    self.verdicts_emitted += 1
                    self.producer.send(TOPIC_VERDICT_HISTORY, verdict)
                    self.producer.send(TOPIC_OUT_DATABASE, verdict)
                    self.producer.send(TOPIC_OUT_DASHBOARD, verdict)
                    self.producer.send(TOPIC_OUT_ANALYTICS, verdict)
                except Exception as exc:
                    logger.error("Guard re-evaluation error for %s: %s", decision_key, exc)

    def process_clock_tick(self, data: dict[str, Any]) -> None:
        """Process durable RunClockTick and update frontier without advancing watermark."""
        tick = RunClockTick(
            run_id=data.get("run_id", "default_run"),
            clock_domain=data.get("clock_domain", "LIVE_UTC"),
            clock_utc=data.get("clock_utc", utc_now()),
            seq=int(data.get("seq", 0)),
            emitter_id=data.get("emitter_id", "supervisor"),
        )
        self.run_clock_mgr.process_tick(tick)
        tick_item = {
            "stream_item_type": "RUN_CLOCK_TICK",
            "transport_event_time_utc": tick.clock_utc,
            "data": data,
        }
        # Re-evaluate any active non-finalized decisions
        for decision_key, state in list(self.decision_states.items()):
            latest_v = state.get("latest_verdict")
            if latest_v and latest_v.get("finality_status") == "FINAL":
                continue
            state["input_stream"].append(tick_item)
            contract_id = decision_key.split("|")[-1]
            contract = self.contracts.get(contract_id)
            if contract:
                try:
                    verdict = ContractEvaluator.evaluate(
                        contract=contract,
                        decision_instance_id=decision_key,
                        input_stream=state["input_stream"],
                        t_eval=tick.clock_utc,
                        previous_verdict=latest_v,
                    )
                    state["latest_verdict"] = verdict
                    self.verdicts_emitted += 1
                    self.producer.send(TOPIC_VERDICT_HISTORY, verdict)
                    self.producer.send(TOPIC_OUT_DATABASE, verdict)
                    self.producer.send(TOPIC_OUT_DASHBOARD, verdict)
                    self.producer.send(TOPIC_OUT_ANALYTICS, verdict)
                except Exception as exc:
                    logger.error("Clock tick evaluation error for %s: %s", decision_key, exc)

    def process_contract_config(self, data: dict[str, Any]) -> None:
        """Register or update a DecisionEvidenceContract dynamically."""
        try:
            contract = parse_contract_dict(data)
            self.contracts[contract.contract_id] = contract
            logger.info("Dynamically registered DEC: %s v%s", contract.contract_id, contract.version)
        except Exception as exc:
            logger.error("Failed to parse dynamic contract configuration: %s", exc)

    def _stream_loop(self) -> None:
        """Continuous streaming consumption from evidence, guard, and clock topics."""
        evidence_topics_set = set(t for t in all_standard_topics() if t.startswith(TOPIC_EVIDENCE_PREFIX))
        evidence_topics_set.add(TOPIC_GUARD_LIVENESS)
        evidence_topics_set.add(TOPIC_GUARD_CLOCK)
        evidence_topics_set.add(TOPIC_CONFIG_CONTRACTS)
        
        try:
            from kafka.admin import KafkaAdminClient
            from calibre_core.kafka_bus import DEFAULT_BOOTSTRAP_SERVERS
            admin = KafkaAdminClient(bootstrap_servers=self.bootstrap_servers or DEFAULT_BOOTSTRAP_SERVERS, client_id="calibre-flink-discovery")
            for bt in admin.list_topics():
                if bt.startswith(TOPIC_EVIDENCE_PREFIX):
                    evidence_topics_set.add(bt)
            admin.close()
        except Exception:
            pass
        
        subscribed_topics = sorted(list(evidence_topics_set))
        
        consumer = CalibreConsumer(
            *subscribed_topics,
            group_id=GROUP_FLINK_CORE,
            bootstrap_servers=self.bootstrap_servers,
            auto_offset_reset="earliest",
            consumer_timeout_ms=500,
        )

        logger.info(
            "Flink job %s streaming loop active on %d topics",
            self.job_id, len(subscribed_topics)
        )

        while self.is_running:
            try:
                messages = consumer.poll_messages(timeout_ms=500, max_records=50)
                for msg in messages:
                    # Check msg format or type
                    if isinstance(msg, dict):
                        if msg.get("schema_name") == "calibre.guard_fact" or "guard_fact_id" in msg or "status" in msg and "observed_gap" in msg:
                            self.process_guard_fact(msg)
                        elif msg.get("schema_name") == "calibre.run_clock_tick" or "clock_domain" in msg and "seq" in msg:
                            self.process_clock_tick(msg)
                        elif msg.get("schema_name") == "calibre.dec_contract" or "target_decision_type" in msg:
                            self.process_contract_config(msg)
                        else:
                            self.process_evidence_envelope(msg)
            except Exception as exc:
                logger.error("Flink streaming loop error: %s", exc)
                time.sleep(1.0)

        consumer.close()
        logger.info("Flink job %s streaming loop terminated", self.job_id)

    def start(self) -> None:
        """Start the Flink streaming processor in background thread."""
        self.is_running = True
        self.health.is_ready = True
        self._worker_thread = threading.Thread(
            target=self._stream_loop,
            name=f"calibre-flink-{self.job_id}",
            daemon=True,
        )
        self._worker_thread.start()
        logger.info("Flink job %s successfully started", self.job_id)

    def stop(self) -> None:
        """Stop the Flink streaming processor."""
        self.is_running = False
        if self._worker_thread and self._worker_thread.is_alive():
            self._worker_thread.join(timeout=2.0)
        self.producer.close()
        self.health.is_ready = False
        logger.info("Flink job %s stopped", self.job_id)

    def stats(self) -> dict[str, Any]:
        return {
            "job_id": self.job_id,
            "events_received": self.events_received,
            "events_routed": self.events_routed,
            "events_malformed": self.events_malformed,
            "verdicts_emitted": self.verdicts_emitted,
            "quarantined_events": self.quarantined_events,
            "overflow_events": self.overflow_events,
            "contracts_loaded": len(self.contracts),
            "active_decisions": len(self.decision_states),
            "health": self.health.to_dict(),
        }
