"""CALIBRE Apache Flink Streaming Integrity Job.

Responsibilities per CAL-DOC-004 & CAL-DOC-005:
1. Ingest EvidenceEvents from calibre.evidence.* topics
2. Deserialize and validate basic envelope structure and required fields
3. Preserve original event, lineage, and correlation_id
4. Attach Flink processing metadata with explicit status "NOT_EVALUATED"
5. Route valid events to downstream outputs:
   - calibre.out.database
   - calibre.out.dashboard
   - calibre.out.analytics
6. Route malformed/unprocessable events to calibre.dlq without crashing
7. Expose observable processing metrics and counters
"""
from __future__ import annotations

import logging
import threading
import time
from typing import Any, Optional

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
    GROUP_FLINK_CORE,
    all_standard_topics,
)

logger = logging.getLogger("calibre.flink")


class CalibreFlinkJob:
    """Core stream processing pipeline implementation representing the Flink integrity kernel."""

    def __init__(
        self,
        bootstrap_servers: Optional[str] = None,
        job_id: str = "calibre-flink-stream-job-v1",
    ) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.job_id = job_id
        self.producer = CalibreProducer(bootstrap_servers=bootstrap_servers, client_id=f"flink-{job_id}-producer")
        self.health = ComponentHealth(name="FlinkIntegrityCore")
        self.health.set_dependency("kafka_producer", self.producer.is_connected)
        
        self.is_running = False
        self._worker_thread: Optional[threading.Thread] = None
        self.events_received = 0
        self.events_routed = 0
        self.events_malformed = 0

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
        """Validate, wrap, and route a single evidence event."""
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
            integrity_status="NOT_EVALUATED",  # Deliberately decoupled from pipeline health
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

        return output_envelope

    def _stream_loop(self) -> None:
        """Continuous streaming consumption from calibre.evidence.* topics."""
        evidence_topics = [t for t in all_standard_topics() if t.startswith(TOPIC_EVIDENCE_PREFIX)]
        
        consumer = CalibreConsumer(
            *evidence_topics,
            group_id=GROUP_FLINK_CORE,
            bootstrap_servers=self.bootstrap_servers,
            auto_offset_reset="earliest",
            consumer_timeout_ms=500,
        )

        logger.info(
            "Flink job %s streaming loop active on %d evidence topics",
            self.job_id, len(evidence_topics)
        )

        while self.is_running:
            try:
                messages = consumer.poll_messages(timeout_ms=500, max_records=50)
                for msg in messages:
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
            "health": self.health.to_dict(),
        }
