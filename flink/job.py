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
import hashlib
import json
import logging
from collections import OrderedDict
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
from calibre_core.run_scope import belongs_to_run
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
from sia_core.contracts.scope import DecisionScopeError, decision_scope_key
from sia_core.evaluation.contract_evaluator import ContractEvaluator
from sia_core.liveness.run_clock import RunClockManager
from sia_core.models.evidence import RunClockTick

logger = logging.getLogger("calibre.flink")
_VOLATILE_EVIDENCE_TIMESTAMPS = {
    "created_at", "created_at_utc", "produced_at_utc", "updated_at_utc",
    "received_at_utc", "ingested_at_utc", "processed_at_utc",
}


def _without_volatile_timestamps(value: Any) -> Any:
    """Copy evidence content while excluding generated lifecycle timestamps."""
    if isinstance(value, dict):
        return {
            key: _without_volatile_timestamps(item)
            for key, item in value.items()
            if key not in _VOLATILE_EVIDENCE_TIMESTAMPS
        }
    if isinstance(value, list):
        return [_without_volatile_timestamps(item) for item in value]
    return value


class CalibreFlinkJob:
    """Core stream processing pipeline implementation representing the Flink integrity kernel."""

    def __init__(
        self,
        bootstrap_servers: Optional[str] = None,
        job_id: str = "calibre-flink-stream-job-v1",
        contracts_dir: Optional[str] = None,
        enable_sia: bool = True,
        run_id: Optional[str] = None,
        consumer_group_id: Optional[str] = None,
    ) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.job_id = job_id
        self.run_id = run_id
        self.consumer_group_id = consumer_group_id
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
        self.run_metrics: Dict[str, Dict[str, int]] = {}
        self.seen_evidence_fingerprints: OrderedDict[str, str] = OrderedDict()
        self.dedupe_cache_limit = 100_000
        self.identity_conflicts = 0
        self.clock_frontiers: Dict[str, tuple[str, str, str]] = {}
        self.duplicate_clock_ticks = 0

        # SIA In-Flight State
        self.contracts: Dict[str, DecisionEvidenceContract] = {}
        self.decision_states: Dict[str, Dict[str, Any]] = {}
        self.run_clock_mgr: RunClockManager = RunClockManager()
        self.run_clock_managers: Dict[str, RunClockManager] = {}
        
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

    def _record_run_metric(self, run_id: Optional[str], metric: str, amount: int = 1) -> None:
        if not run_id:
            return
        run_stats = self.run_metrics.setdefault(run_id, {})
        run_stats[metric] = run_stats.get(metric, 0) + amount

    def process_evidence_envelope(self, data: dict[str, Any]) -> Optional[FlinkOutputEnvelope]:
        """Validate, wrap, and route a single evidence event with SIA evaluation."""
        self.events_received += 1
        start_time = time.perf_counter()

        run_id = data.get("provenance", {}).get("run_id") if isinstance(data, dict) else None
        self._record_run_metric(run_id, "events_received")

        is_valid, validation_msg = self.validate_evidence_event(data)
        corr_id = data.get("correlation_id", "unknown") if isinstance(data, dict) else "unknown"

        if not is_valid:
            self.events_malformed += 1
            self._record_run_metric(run_id, "events_malformed")
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

        # Replays may legitimately redeliver an identical evidence event. Keep
        # the transport envelope flowing to all sinks so their idempotency can
        # be observed, while preventing duplicate DEC evaluation and revisions.
        semantic_fields = (
            "event_id", "correlation_id", "event_type", "domain", "source_refs",
            "scope", "time", "value_origin", "evidence", "findings", "limitations",
            "provenance", "session",
        )
        semantic_event = _without_volatile_timestamps(
            {key: data.get(key) for key in semantic_fields if key in data}
        )
        event_fingerprint = hashlib.sha256(
            json.dumps(semantic_event, sort_keys=True, ensure_ascii=False, separators=(",", ":"), default=str).encode("utf-8")
        ).hexdigest()
        previous_fingerprint = self.seen_evidence_fingerprints.get(str(data["event_id"]))
        is_duplicate = previous_fingerprint == event_fingerprint
        if previous_fingerprint is not None and not is_duplicate:
            self.identity_conflicts += 1
            self.quarantined_events += 1
            self._record_run_metric(run_id, "identity_conflicts")
            self.health.record_activity(error=True)
            self.producer.send(
                TOPIC_SIDE_QUARANTINE,
                {
                    "event_id": data["event_id"],
                    "correlation_id": corr_id,
                    "reason": "EVENT_ID_REUSED_WITH_DIFFERENT_SEMANTIC_CONTENT",
                    "previous_fingerprint": previous_fingerprint,
                    "incoming_fingerprint": event_fingerprint,
                    "observed_at_utc": utc_now(),
                },
                correlation_id=corr_id,
            )
            return None
        if is_duplicate:
            self._record_run_metric(run_id, "duplicate_evidence_events")
            self.seen_evidence_fingerprints.move_to_end(str(data["event_id"]))
        else:
            self.seen_evidence_fingerprints[str(data["event_id"])] = event_fingerprint
            if len(self.seen_evidence_fingerprints) > self.dedupe_cache_limit:
                self.seen_evidence_fingerprints.popitem(last=False)

        # Build FlinkOutputEnvelope
        event_id = data["event_id"]
        latency_ms = (time.perf_counter() - start_time) * 1000.0
        now_str = utc_now()
        
        output_envelope = FlinkOutputEnvelope(
            envelope_id=stable_id("FLINK", event_id),
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
            self._record_run_metric(run_id, "events_routed")
            self.health.record_activity(events_in=1, events_out=3)
            logger.debug("Flink routed event %s (corr=%s) to 3 downstream sinks", event_id, corr_id)
        else:
            self.health.record_activity(error=True)
            logger.error("Flink encountered error fanning out event %s", event_id)

        # Perform SIA Decision Integrity Evaluation if enabled
        if self.enable_sia and not is_duplicate:
            self._evaluate_sia_for_event(data, now_str)

        return output_envelope

    def _evaluate_sia_for_event(self, data: dict[str, Any], transport_time: str) -> None:
        """Evaluate relevant DECs using the pure ContractEvaluator."""
        # A weather envelope contains two independent measurements. Route each
        # to its explicitly selected DEC role rather than choosing one by field
        # ordering or matching both by a broad domain substring.
        forced_role = data.get("_sia_role")
        if not forced_role:
            ev = data.get("evidence", {})
            observations = ev.get("observations", {}) if isinstance(ev, dict) else {}
            feed = str(ev.get("feed", data.get("feed", data.get("provenance", {}).get("feed", "")))).lower() if isinstance(ev, dict) else ""
            if feed == "weather" and all(observations.get(k) is not None for k in ("track_temperature_c", "air_temperature_c")):
                for role_name in ("track_weather", "air_weather"):
                    routed = dict(data)
                    routed["_sia_role"] = role_name
                    self._evaluate_sia_for_event(routed, transport_time)
                return
        domain = data.get("domain", "")
        scope = data.get("scope", {})
        provenance = data.get("provenance", {})
        session_id = scope.get("session_id") or scope.get("session_type", "session")
        run_id = provenance.get("run_id") or data.get("run_id") or scope.get("run_id")
        session_instance_id = scope.get("session_instance_id") or (f"{session_id}#{run_id}" if run_id else session_id)
        evidence = data.get("evidence", {})
        obs = evidence.get("observations", {}) if isinstance(evidence, dict) else {}
        driver_id = str(scope.get("driver_number") or scope.get("driver_id") or evidence.get("driver_number") or evidence.get("driver_code") or "session")
        corr_id = data.get("correlation_id", "unknown")
        clock_domain = data.get("clock_domain") or ("REPLAY_VIRTUAL" if "#" in session_instance_id else "LIVE_UTC")
        clock_mgr = self.run_clock_managers.setdefault(
            str(run_id), RunClockManager(clock_domain=clock_domain)
        ) if run_id else self.run_clock_mgr

        # Extract value from observations or direct evidence fields
        val = 100.0
        val_source = obs if obs else (evidence if isinstance(evidence, dict) else {})
        for field in ("lap_duration_s", "lap_duration", "speed", "air_temperature_c", "track_temperature_c", "lane_duration", "duration", "value"):
            if field in val_source and val_source[field] is not None:
                try:
                    val = float(val_source[field])
                    break
                except (ValueError, TypeError):
                    pass
        if forced_role:
            contract_for_role = next((c for c in self.contracts.values() if forced_role in c.roles), None)
            selector = contract_for_role.roles[forced_role].input_selector if contract_for_role else None
            selected = val_source.get(selector.value_field) if selector and selector.value_field else None
            if selected is not None:
                try:
                    val = float(selected)
                except (TypeError, ValueError):
                    return

        # Construct stream item
        atom_id = f"atom-{data.get('event_id', generate_uuid())}" + (f"-{forced_role}" if forced_role else "")
        raw_source_val = data.get("time", {}).get("source_value")
        if raw_source_val is None:
            raw_source_val = data.get("produced_at_utc", transport_time)

        source_semantics = data.get("time", {}).get("source_semantics", "UNKNOWN")
        is_utc = source_semantics == "ABSOLUTE_UTC"
        source_event_time = data.get("time", {}).get("event_time_utc")
        if not source_event_time and is_utc and isinstance(raw_source_val, str):
            source_event_time = raw_source_val
        event_time_for_evaluation = source_event_time or transport_time
        if run_id and clock_domain == "REPLAY_VIRTUAL":
            clock_mgr.advance_to(run_id, clock_domain, event_time_for_evaluation)
        provider_name = provenance.get("provider") or data.get("session", {}).get("provider") or "PITWALL"

        stream_item = {
            "stream_item_type": "EVIDENCE_ATOM",
            "transport_event_time_utc": event_time_for_evaluation,
            "transport_time_basis": "EVENT_UTC",
            "watermark_at_arrival": clock_mgr.watermark or event_time_for_evaluation,
            "data": {
                "evidence_atom_id": atom_id,
                "role_id": domain,
                "value": val,
                "unit": data.get("time", {}).get("source_unit", "s"),
                "raw_event_ids": [data.get("event_id", "")],
                "processor_family_id": data.get("producer", {}).get("processor_id", "generic_processor"),
                "transform_version": data.get("producer", {}).get("processor_version", "1.0.0"),
                "source_origin_set": [provider_name],
                "time": {
                    "value": raw_source_val,
                    "class": "ABSOLUTE_UTC" if is_utc else "SESSION_ELAPSED",
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

            for r_name, role_spec in contract.roles.items():
                if forced_role and r_name != forced_role:
                    continue
                selector = role_spec.input_selector
                if selector:
                    feed = str(evidence.get("feed", data.get("feed", provenance.get("feed", "")))).lower()
                    event_type = str(data.get("event_type", "")).lower()
                    if selector.feeds and feed not in {x.lower() for x in selector.feeds}:
                        continue
                    if selector.event_types and event_type not in {x.lower() for x in selector.event_types}:
                        continue
                    if selector.value_field and val_source.get(selector.value_field) is None:
                        continue
                    matched_role = r_name
                    break
                r_lower = r_name.lower()
                if r_lower == "feed_telemetry":
                    feed = str(evidence.get("feed", data.get("feed", provenance.get("feed", "")))).lower()
                    heartbeat_event = any(token in evt_lower for token in ("heartbeat", "packet_gap", "feed_liveness"))
                    if feed not in {"heartbeat", "liveness", "network"} and not heartbeat_event:
                        continue
                if dom_lower in r_lower or (r_lower in dom_lower and r_lower not in {"pit_entry", "pit_exit"}):
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
                elif "weather" in r_lower and ("environment" in dom_lower or "weather" in evt_lower):
                    matched_role = r_name
                    break
                elif "incident" in r_lower and ("incident" in dom_lower or "rcm" in evt_lower or "race_control" in evt_lower):
                    matched_role = r_name
                    break
                elif "pit" in r_lower and ("pit" in evt_lower or str(evidence.get("feed", "")).lower() == "pit"):
                    matched_role = r_name
                    break

            if not matched_role and not forced_role and not any(
                role.input_selector is not None for role in contract.roles.values()
            ):
                if dom_lower in contract_id.lower() or (dom_lower == "timing" and "lap" in contract_id):
                    matched_role = list(contract.roles.keys())[0]
            if not matched_role:
                continue

            decision_scope = {
                **scope,
                "session_id": session_id,
                "driver_id": driver_id,
                "driver_number": scope.get("driver_number") or evidence.get("driver_number"),
                "lap_number": scope.get("lap_number") or evidence.get("lap_number") or evidence.get("lap"),
                "sector_number": scope.get("sector_number") or evidence.get("sector_number") or evidence.get("sector"),
            }
            try:
                scoped_key = decision_scope_key(contract, decision_scope)
            except DecisionScopeError as exc:
                self.quarantined_events += 1
                self.producer.send(
                    TOPIC_SIDE_QUARANTINE,
                    {
                        "schema_name": "calibre.quarantined_evidence",
                        "run_id": run_id,
                        "session_instance_id": session_instance_id,
                        "component": "python_integrity_worker",
                        "processing_stage": "DECISION_SCOPE_RESOLUTION",
                        "reason_code": str(exc).split(":", 1)[0],
                        "reason": str(exc),
                        "contract_id": contract_id,
                        "source_event_id": data.get("event_id"),
                        "raw_event_ids": list(data.get("source_refs", [])),
                        "correlation_id": corr_id,
                    },
                    correlation_id=corr_id,
                )
                continue
            decision_key = f"{session_instance_id}|{scoped_key}|{contract_id}"
            state = self.decision_states.setdefault(decision_key, {
                "input_stream": [],
                "latest_verdict": None,
                "watermark": None,
                "run_id": run_id,
                "clock_domain": clock_domain,
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
            role_selector = contract.roles[matched_role].input_selector
            if role_selector and role_selector.value_field:
                contract_stream_item["data"]["value"] = val_source[role_selector.value_field]
                contract_stream_item["data"]["unit"] = role_selector.unit or contract_stream_item["data"].get("unit")

            state["input_stream"].append(contract_stream_item)
            t_eval = clock_mgr.compute_t_eval(
                watermark=clock_mgr.watermark,
                run_clock=event_time_for_evaluation,
            )

            try:
                self._record_run_metric(run_id, "dec_evaluation_invocations")
                verdict = ContractEvaluator.evaluate(
                    contract=contract,
                    decision_instance_id=decision_key,
                    input_stream=state["input_stream"],
                    t_eval=t_eval,
                    previous_verdict=latest_v,
                    clock_domain=clock_domain,
                )
                state["latest_verdict"] = verdict
                self.verdicts_emitted += 1
                self._record_run_metric(run_id, "verdicts_emitted")

                # Attach correlation_id to verdict payload for end-to-end tracing
                verdict_payload = dict(verdict)
                verdict_payload["correlation_id"] = corr_id

                # Check for budget overflow (M_max=10 or C_max=32)
                witness_str = " ".join(verdict.get("witness_set", []))
                if "SEARCH_BUDGET_EXCEEDED" in witness_str or "ENVELOPE_CAPPED" in witness_str:
                    self.overflow_events += 1
                    reason_code = "SEARCH_BUDGET_EXCEEDED" if "SEARCH_BUDGET_EXCEEDED" in witness_str else "ENVELOPE_CAPPED"
                    self.producer.send(
                        TOPIC_SIDE_OVERFLOW,
                        {
                            "schema_name": "calibre.sia_overflow.v1",
                            "run_id": run_id,
                            "session_instance_id": session_instance_id,
                            "decision_instance_id": decision_key,
                            "contract_id": contract_id,
                            "component": "sia_leaf_evaluation",
                            "processing_stage": "BOUNDED_SUPPORT_SEARCH",
                            "reason_code": reason_code,
                            "configured_capacity": 10 if reason_code == "SEARCH_BUDGET_EXCEEDED" else 32,
                            "observed_count": int(witness_str.split("count=", 1)[1].split(",", 1)[0]) if "count=" in witness_str else None,
                            "action": "FAIL_CLOSED_UNKNOWN",
                            "source_event_id": data.get("event_id"),
                            "raw_event_ids": list(data.get("source_refs", [])),
                            "evidence_atom_ids": [item.get("data", {}).get("evidence_atom_id") for item in state["input_stream"] if item.get("stream_item_type") == "EVIDENCE_ATOM"],
                            "source_topic": data.get("_kafka_metadata", {}).get("topic"),
                            "source_partition": data.get("_kafka_metadata", {}).get("partition"),
                            "source_offset": data.get("_kafka_metadata", {}).get("offset"),
                            "witness": witness_str,
                            "occurred_at_utc": transport_time if clock_domain != "REPLAY_VIRTUAL" else None,
                            "occurred_at_replay_virtual": transport_time if clock_domain == "REPLAY_VIRTUAL" else None,
                            "clock_domain": clock_domain,
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
                clock_domain = data.get("clock_domain", "LIVE_UTC")
                try:
                    verdict = ContractEvaluator.evaluate(
                        contract=contract,
                        decision_instance_id=decision_key,
                        input_stream=state["input_stream"],
                        t_eval=t_eval,
                        previous_verdict=state.get("latest_verdict"),
                        clock_domain=clock_domain,
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
        """Process a run-clock tick and its optional explicit event-time watermark."""
        tick = RunClockTick(
            run_id=data.get("run_id", "default_run"),
            clock_domain=data.get("clock_domain", "LIVE_UTC"),
            clock_utc=data.get("clock_value") or data.get("clock_utc", utc_now()),
            seq=int(data.get("seq", 0)),
            emitter_id=data.get("emitter_id", "supervisor"),
        )
        run_id = tick.run_id
        is_completion = data.get("schema_name") == "calibre.replay_completion"
        if is_completion:
            self._record_run_metric(run_id, "replay_completions")
        clock_mgr = (
            self.run_clock_mgr
            if run_id == "default_run" or tick.clock_domain == "LIVE_UTC"
            else self.run_clock_managers.setdefault(
                run_id, RunClockManager(clock_domain=tick.clock_domain)
            )
        )
        clock_mgr.process_tick(tick)
        watermark = data.get("watermark_utc")
        if data.get("schema_name") == "calibre.replay_completion":
            # A finite replay may close its observed source frontier only after
            # upstream processing has drained. This is a completion fact, not a
            # watermark inferred from virtual clock passage.
            watermark = data.get("source_max_event_time_utc")
        if watermark:
            clock_mgr.update_watermark(str(watermark))
        current_frontier = (tick.clock_domain, str(tick.clock_utc), str(watermark or ""))
        if self.clock_frontiers.get(run_id) == current_frontier:
            self.duplicate_clock_ticks += 1
            self._record_run_metric(run_id, "clock_ticks")
            self._record_run_metric(run_id, "duplicate_clock_ticks")
            return
        self.clock_frontiers[run_id] = current_frontier
        tick_item = {
            "stream_item_type": "REPLAY_COMPLETION" if is_completion else "RUN_CLOCK_TICK",
            "watermark_at_arrival": watermark,
            "clock_domain": tick.clock_domain,
            "data": data,
        }
        # Re-evaluate any active non-finalized decisions
        for decision_key, state in list(self.decision_states.items()):
            if state.get("run_id") != run_id and not (
                run_id == "default_run" and state.get("run_id") is None
            ):
                continue
            latest_v = state.get("latest_verdict")
            if latest_v and latest_v.get("finality_status") == "FINAL":
                continue
            state["input_stream"].append(tick_item)
            contract_id = decision_key.split("|")[-1]
            contract = self.contracts.get(contract_id)
            if contract:
                try:
                    t_eval = clock_mgr.compute_t_eval(watermark, tick.clock_utc)
                    self._record_run_metric(run_id, "dec_evaluation_invocations")
                    verdict = ContractEvaluator.evaluate(
                        contract=contract,
                        decision_instance_id=decision_key,
                        input_stream=state["input_stream"],
                        t_eval=t_eval,
                        previous_verdict=latest_v,
                        clock_domain=tick.clock_domain,
                    )
                    state["latest_verdict"] = verdict
                    self.verdicts_emitted += 1
                    self._record_run_metric(run_id, "verdicts_emitted")
                    self.producer.send(TOPIC_VERDICT_HISTORY, verdict)
                    self.producer.send(TOPIC_OUT_DATABASE, verdict)
                    self.producer.send(TOPIC_OUT_DASHBOARD, verdict)
                    self.producer.send(TOPIC_OUT_ANALYTICS, verdict)
                except Exception as exc:
                    logger.error("Clock tick evaluation error for %s: %s", decision_key, exc)
        # This is a completion counter: the replay runner may wait on it before
        # snapshotting downstream topic offsets and persisted verdict hashes.
        self._record_run_metric(run_id, "clock_ticks")

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
            group_id=(
                self.consumer_group_id
                or (f"{GROUP_FLINK_CORE}-{self.run_id}" if self.run_id else GROUP_FLINK_CORE)
            ),
            bootstrap_servers=self.bootstrap_servers,
            auto_offset_reset="latest" if self.run_id or self.consumer_group_id else "earliest",
            consumer_timeout_ms=500,
        )

        logger.info(
            "Flink job %s streaming loop active on %d topics",
            self.job_id, len(subscribed_topics)
        )

        while self.is_running:
            try:
                records = consumer.poll_records(timeout_ms=500, max_records=50)
                for msg, kafka_metadata in records:
                    # Check msg format or type
                    if isinstance(msg, dict):
                        is_guard_fact = (
                            msg.get("schema_name") == "calibre.guard_fact"
                            or "guard_fact_id" in msg
                            or "status" in msg and "observed_gap" in msg
                        )
                        is_clock_fact = (
                            msg.get("schema_name") in {
                                "calibre.run_clock_tick", "calibre.replay_completion"
                            }
                            or "clock_domain" in msg and "seq" in msg
                        )
                        is_contract_config = (
                            msg.get("schema_name") == "calibre.dec_contract"
                            or "target_decision_type" in msg
                        )
                        if (
                            self.run_id
                            and (is_guard_fact or is_clock_fact)
                            and msg.get("run_id") not in (None, self.run_id)
                        ):
                            continue
                        if (
                            not (is_guard_fact or is_clock_fact or is_contract_config)
                            and not belongs_to_run(msg, self.run_id)
                        ):
                            continue
                        msg["_kafka_metadata"] = kafka_metadata
                        if is_guard_fact:
                            self.process_guard_fact(msg)
                        elif is_clock_fact:
                            self.process_clock_tick(msg)
                        elif is_contract_config:
                            self.process_contract_config(msg)
                        else:
                            self.process_evidence_envelope(msg)
                            msg.pop("_kafka_metadata", None)
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
            "identity_conflicts": self.identity_conflicts,
            "duplicate_clock_ticks": self.duplicate_clock_ticks,
            "dedupe_cache_entries": len(self.seen_evidence_fingerprints),
            "contracts_loaded": len(self.contracts),
            "active_decisions": len(self.decision_states),
            "run_metrics": {k: dict(v) for k, v in self.run_metrics.items()},
            "health": self.health.to_dict(),
        }
