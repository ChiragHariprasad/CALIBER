"""CALIBRE Apache Flink Streaming Verification Test Suite.

Verifies:
1. Apache Flink JobManager cluster operational health and slot allocation.
2. Direct consumption of EvidenceEvents across all 5 specialist domains:
   - calibre.evidence.vehicle
   - calibre.evidence.incident
   - calibre.evidence.environment
   - calibre.evidence.timing
   - calibre.evidence.system
3. Flink validation, lineage stamping, and triple-sink fan-out:
   - calibre.out.database
   - calibre.out.dashboard
   - calibre.out.analytics
4. Failure-path DLQ routing:
   - Corrupt/malformed inputs routed to calibre.dlq without crashing the stream.
5. Invariant checking:
   - integrity_status == "NOT_EVALUATED" (contract semantic separation)
   - processing_stage == "FLINK_INTEGRITY_VALIDATED"
   - correlation_id and lineage preservation.
"""
from __future__ import annotations

import json
import logging
import time
import unittest
import uuid
from typing import Any, Optional

import requests
from kafka import KafkaConsumer, TopicPartition

from calibre_core.contracts import (
    EvidenceEvent,
    FlinkOutputEnvelope,
    DeadLetterEnvelope,
    utc_now,
    stable_id,
)
from calibre_core.kafka_bus import CalibreProducer
from calibre_core.topics import (
    evidence_topic,
    TOPIC_OUT_DATABASE,
    TOPIC_OUT_DASHBOARD,
    TOPIC_OUT_ANALYTICS,
    TOPIC_DLQ,
)
from flink.job import CalibreFlinkJob

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("calibre.tests.flink")

BOOTSTRAP_SERVERS = "127.0.0.1:9092"
FLINK_REST_URL = "http://127.0.0.1:8081"


def poll_matching_message(
    topic: str,
    correlation_id: str,
    timeout_sec: float = 6.0,
    bootstrap_servers: str = BOOTSTRAP_SERVERS,
) -> Optional[dict[str, Any]]:
    """Directly assign partition 0 to read topic without consumer group rebalance overhead."""
    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap_servers.split(","),
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        consumer_timeout_ms=500,
    )
    tp = TopicPartition(topic, 0)
    consumer.assign([tp])
    consumer.seek_to_beginning(tp)

    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        records = consumer.poll(timeout_ms=400)
        for part, msgs in records.items():
            for m in msgs:
                val = m.value
                if isinstance(val, dict) and val.get("correlation_id") == correlation_id:
                    consumer.close()
                    return val
        time.sleep(0.1)

    consumer.close()
    return None


class FlinkStreamingIntegrationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.producer = CalibreProducer(bootstrap_servers=BOOTSTRAP_SERVERS, client_id="test-flink-producer")
        cls.flink_job = CalibreFlinkJob(bootstrap_servers=BOOTSTRAP_SERVERS, job_id="test-flink-suite")
        cls.flink_job.start()
        time.sleep(1.5)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.flink_job.stop()
        cls.producer.close()

    def test_01_flink_jobmanager_cluster_health(self) -> None:
        """Verify Apache Flink cluster JobManager is running and responsive."""
        logger.info("Checking Apache Flink JobManager at %s/overview", FLINK_REST_URL)
        resp = requests.get(f"{FLINK_REST_URL}/overview", timeout=3.0)
        self.assertEqual(resp.status_code, 200, "Flink JobManager should return HTTP 200")
        
        data = resp.json()
        self.assertIn("flink-version", data)
        self.assertGreaterEqual(data.get("taskmanagers", 0), 1, "At least 1 TaskManager required")
        self.assertGreaterEqual(data.get("slots-total", 0), 1, "At least 1 execution slot required")
        logger.info(
            "Flink Cluster Verified: Version=%s, TaskManagers=%d, Total Slots=%d, Available Slots=%d",
            data.get("flink-version"),
            data.get("taskmanagers"),
            data.get("slots-total"),
            data.get("slots-available"),
        )

    def test_02_evidence_input_to_triple_topic_fanout(self) -> None:
        """Verify that an EvidenceEvent on calibre.evidence.vehicle produces outputs on all 3 downstream topics."""
        test_corr_id = f"trace-flink-fanout-{uuid.uuid4().hex[:8]}"
        now_str = utc_now()

        # 1. Create a valid EvidenceEvent
        ev = EvidenceEvent(
            schema_name="calibre.evidence_event",
            schema_version="1.0",
            event_id=stable_id("EV-TEST", test_corr_id, now_str),
            correlation_id=test_corr_id,
            event_type="vehicle_telemetry_sample_assessed",
            domain="vehicle",
            producer={
                "processor_id": "test_vehicle_twin_agent",
                "processor_version": "1.0.0",
                "component_role": "AGENT_PROCESSOR",
            },
            produced_at_utc=now_str,
            source_refs=[f"raw-{test_corr_id}"],
            scope={
                "season": 2026,
                "session_type": "Race",
                "driver_number": 1,
            },
            time={
                "source_value": 150.25,
                "source_semantics": "SESSION_ELAPSED_SECONDS",
                "source_unit": "seconds",
            },
            value_origin="SOURCE_REPORTED",
            evidence={
                "observations": {"speed": 312.4, "rpm": 11800, "throttle": 100},
                "assumptions": ["Sensor calibration nominal"],
                "limitations": [],
                "confidence_score": 0.98,
            },
            findings=[],
            provenance={
                "raw_event_id": f"raw-{test_corr_id}",
                "provider": "MOCK",
                "feed": "telemetry",
                "ingested_at_utc": now_str,
            },
        )

        # 2. Publish to calibre.evidence.vehicle
        ev_topic = evidence_topic("vehicle")
        logger.info("Publishing EvidenceEvent (corr=%s) to %s", test_corr_id, ev_topic)
        sent = self.producer.send(ev_topic, ev, correlation_id=test_corr_id)
        self.assertTrue(sent, f"Failed to send test event to {ev_topic}")

        # 3. Verify delivery to calibre.out.database
        logger.info("Probing downstream topic: %s", TOPIC_OUT_DATABASE)
        db_msg = poll_matching_message(TOPIC_OUT_DATABASE, test_corr_id)
        self.assertIsNotNone(db_msg, f"Event {test_corr_id} did not reach {TOPIC_OUT_DATABASE}")
        self.assertEqual(db_msg.get("correlation_id"), test_corr_id)
        self.assertEqual(db_msg.get("processing_stage"), "FLINK_INTEGRITY_VALIDATED")
        self.assertEqual(db_msg.get("integrity_status"), "NOT_EVALUATED")
        self.assertEqual(db_msg.get("validation_status"), "VALID")
        self.assertIn("PERSISTENCE", db_msg.get("routed_to", []))
        self.assertIn("processing_latency_ms", db_msg)

        # 4. Verify delivery to calibre.out.dashboard
        logger.info("Probing downstream topic: %s", TOPIC_OUT_DASHBOARD)
        dash_msg = poll_matching_message(TOPIC_OUT_DASHBOARD, test_corr_id)
        self.assertIsNotNone(dash_msg, f"Event {test_corr_id} did not reach {TOPIC_OUT_DASHBOARD}")
        self.assertEqual(dash_msg.get("correlation_id"), test_corr_id)
        self.assertIn("DASHBOARD", dash_msg.get("routed_to", []))

        # 5. Verify delivery to calibre.out.analytics
        logger.info("Probing downstream topic: %s", TOPIC_OUT_ANALYTICS)
        analytics_msg = poll_matching_message(TOPIC_OUT_ANALYTICS, test_corr_id)
        self.assertIsNotNone(analytics_msg, f"Event {test_corr_id} did not reach {TOPIC_OUT_ANALYTICS}")
        self.assertEqual(analytics_msg.get("correlation_id"), test_corr_id)
        self.assertIn("ANALYTICS", analytics_msg.get("routed_to", []))

        logger.info(
            "Test 02 PASSED: Flink received evidence and successfully fanned out to database, dashboard, and analytics."
        )

    def test_03_all_five_domain_streams_routed_correctly(self) -> None:
        """Verify Flink processes and routes EvidenceEvents across all 5 specialist domains."""
        domains = [
            ("vehicle", "vehicle_telemetry_sample_assessed"),
            ("incident", "track_state_or_flag"),
            ("environment", "environmental_sample_assessed"),
            ("timing", "timing_evidence_sample"),
            ("system", "telemetry_continuity_assessed"),
        ]

        now_str = utc_now()
        for domain, ev_type in domains:
            domain_corr_id = f"trace-flink-dom-{domain}-{uuid.uuid4().hex[:6]}"
            ev = EvidenceEvent(
                schema_name="calibre.evidence_event",
                schema_version="1.0",
                event_id=stable_id("EV", domain, domain_corr_id),
                correlation_id=domain_corr_id,
                event_type=ev_type,
                domain=domain,
                producer={
                    "processor_id": f"{domain}_test_agent",
                    "processor_version": "1.0.0",
                    "component_role": "AGENT_PROCESSOR",
                },
                produced_at_utc=now_str,
                source_refs=[f"raw-{domain_corr_id}"],
                scope={"season": 2026, "session_type": "Race"},
                time={
                    "source_value": 100.0,
                    "source_semantics": "SESSION_ELAPSED_SECONDS",
                    "source_unit": "seconds",
                },
                value_origin="SOURCE_REPORTED",
                evidence={"status": "OK", "domain": domain},
                findings=[],
                provenance={
                    "raw_event_id": f"raw-{domain_corr_id}",
                    "provider": "MOCK",
                    "feed": domain,
                    "ingested_at_utc": now_str,
                },
            )

            topic = evidence_topic(domain)
            self.producer.send(topic, ev, correlation_id=domain_corr_id)

            # Probe calibre.out.database for this domain event
            routed_msg = poll_matching_message(TOPIC_OUT_DATABASE, domain_corr_id, timeout_sec=5.0)
            self.assertIsNotNone(routed_msg, f"Domain {domain} evidence was not routed by Flink")
            self.assertEqual(routed_msg.get("correlation_id"), domain_corr_id)
            self.assertEqual(routed_msg.get("event", {}).get("domain"), domain)
            logger.info("Domain stream verified: %s -> Flink -> calibre.out.database [OK]", domain)

        logger.info("Test 03 PASSED: All 5 specialist evidence domains verified through Flink.")

    def test_04_malformed_evidence_dlq_routing(self) -> None:
        """Verify that malformed evidence is rejected by Flink and routed to calibre.dlq."""
        malformed_corr_id = f"trace-flink-bad-{uuid.uuid4().hex[:8]}"

        # Corrupt event missing required fields (no domain, no evidence, invalid schema)
        corrupt_event = {
            "invalid_key": "corrupt_data",
            "correlation_id": malformed_corr_id,
        }

        # Send corrupt event directly to an evidence topic
        self.producer.send(evidence_topic("incident"), corrupt_event, correlation_id=malformed_corr_id)

        # 1. Verify event reached calibre.dlq
        logger.info("Probing DLQ topic: %s for correlation_id=%s", TOPIC_DLQ, malformed_corr_id)
        dlq_msg = poll_matching_message(TOPIC_DLQ, malformed_corr_id, timeout_sec=6.0)
        self.assertIsNotNone(dlq_msg, f"Malformed event {malformed_corr_id} was not routed to DLQ")
        self.assertEqual(dlq_msg.get("correlation_id"), malformed_corr_id)
        self.assertEqual(dlq_msg.get("component"), "flink_integrity_core")
        self.assertEqual(dlq_msg.get("stage"), "ENVELOPE_VALIDATION")
        self.assertEqual(dlq_msg.get("error_code"), "SCHEMA_VALIDATION_FAILURE")

        # 2. Verify event was NOT routed to valid downstream topic calibre.out.database
        out_msg = poll_matching_message(TOPIC_OUT_DATABASE, malformed_corr_id, timeout_sec=1.5)
        self.assertIsNone(out_msg, "Corrupt event must NOT be published to calibre.out.database")

        logger.info("Test 04 PASSED: Malformed input cleanly isolated in DLQ without pipeline disruption.")


if __name__ == "__main__":
    unittest.main()
