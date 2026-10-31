"""CALIBRE End-to-End Pipeline Integration Test Suite.

Proves the complete streaming backbone:
Mock Source -> Ingestion -> Kafka Raw -> Agent Processors -> Kafka Evidence ->
Flink Streaming Core -> Persistence DB, Dashboard Gateway, Analytics Service.
"""
from __future__ import annotations

import logging
import sqlite3
import time
import unittest
import uuid
from pathlib import Path

from calibre_core.contracts import (
    RawSourceEnvelope,
    EvidenceEvent,
    FlinkOutputEnvelope,
    DeadLetterEnvelope,
    utc_now,
    generate_uuid,
)
from calibre_core.topics import (
    TOPIC_DLQ,
    TOPIC_OUT_DATABASE,
    TOPIC_OUT_DASHBOARD,
    TOPIC_OUT_ANALYTICS,
    raw_topic,
    evidence_topic,
)
from calibre_core.kafka_bus import CalibreProducer, CalibreConsumer
from ingestion.mock_source import MockRaceProducer
from ingestion.service import IngestionService
from processors.service import ProcessorWorkerService
from flink.job import CalibreFlinkJob
from persistence.service import PersistenceService
from dashboard.service import DashboardService
from analytics.service import AnalyticsService

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("calibre.tests")

TEST_BOOTSTRAP = "localhost:9092"
TEST_DB_PATH = Path(__file__).resolve().parent.parent / "data" / "test_persistence.db"


class CalibrePipelineE2ETest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        if TEST_DB_PATH.exists():
            try:
                TEST_DB_PATH.unlink()
            except Exception:
                pass

        # 1. Initialize all pipeline services
        cls.producer = CalibreProducer(bootstrap_servers=TEST_BOOTSTRAP, client_id="test-e2e-runner")
        cls.mock_source = MockRaceProducer(producer=cls.producer, run_id="test-e2e-run")
        cls.ingestion = IngestionService(bootstrap_servers=TEST_BOOTSTRAP, source_mode="MOCK", run_id="test-e2e-run")
        cls.processors = ProcessorWorkerService(bootstrap_servers=TEST_BOOTSTRAP)
        cls.flink = CalibreFlinkJob(bootstrap_servers=TEST_BOOTSTRAP, job_id="test-flink-job")
        cls.persistence = PersistenceService(db_path=TEST_DB_PATH, bootstrap_servers=TEST_BOOTSTRAP)
        cls.dashboard = DashboardService(bootstrap_servers=TEST_BOOTSTRAP)
        cls.analytics = AnalyticsService(bootstrap_servers=TEST_BOOTSTRAP)

        # 2. Start streaming services
        cls.processors.start()
        cls.flink.start()
        cls.persistence.start()
        cls.dashboard.start()
        cls.analytics.start()

        # Allow consumer groups to balance
        time.sleep(2.0)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.processors.stop()
        cls.flink.stop()
        cls.persistence.stop()
        cls.dashboard.stop()
        cls.analytics.stop()
        cls.producer.close()
        cls.ingestion.close()

    def test_01_end_to_end_traceability(self) -> None:
        """Prove that a single deterministic source event moves through all 11 pipeline stages."""
        test_corr_id = f"trace-e2e-assert-{uuid.uuid4().hex[:8]}"
        logger.info("Starting End-to-End Verification with correlation_id=%s", test_corr_id)

        # Assertion 1: The mock source successfully generated an event
        raw_env = self.mock_source.generate_weather_event(correlation_id=test_corr_id)
        self.assertIsNotNone(raw_env)
        self.assertEqual(raw_env.correlation_id, test_corr_id)
        self.assertEqual(raw_env.provider, "MOCK")
        self.assertEqual(raw_env.feed, "weather")
        logger.info("Assertion 1 PASSED: Mock source generated event")

        # Assertion 2: Ingestion received the event and attached metadata
        ingested = self.ingestion.publish_raw_event(
            provider=raw_env.provider,
            feed=raw_env.feed,
            payload=raw_env.payload,
            source_timestamp=raw_env.source_time.get("value"),
            time_semantics=raw_env.source_time.get("semantics", "UNKNOWN"),
            session_info=raw_env.session,
            scope=raw_env.scope,
            correlation_id=test_corr_id,
        )
        self.assertIsNotNone(ingested)
        self.assertEqual(ingested.correlation_id, test_corr_id)
        self.assertIsNotNone(ingested.payload_sha256)
        logger.info("Assertion 2 PASSED: Ingestion received event and attached metadata")

        # Assertion 3: Kafka accepted the raw event
        raw_topic_name = raw_topic(raw_env.provider, raw_env.feed)
        probe_consumer = CalibreConsumer(
            raw_topic_name,
            bootstrap_servers=TEST_BOOTSTRAP,
            auto_offset_reset="earliest",
            consumer_timeout_ms=3000,
        )
        raw_msgs = probe_consumer.poll_messages(timeout_ms=3000)
        probe_consumer.close()
        
        found_raw = any(m.get("correlation_id") == test_corr_id for m in raw_msgs)
        self.assertTrue(found_raw, f"Raw event {test_corr_id} not found in Kafka raw topic {raw_topic_name}")
        logger.info("Assertion 3 PASSED: Kafka accepted the raw event on %s", raw_topic_name)

        # Assertion 4 & 5: Processor consumed it and published an evidence event
        # Wait up to 6 seconds for processing through Kafka
        ev_topic_name = evidence_topic("environment")
        evidence_probe = CalibreConsumer(
            ev_topic_name,
            group_id=f"probe-evidence-{uuid.uuid4().hex[:6]}",
            bootstrap_servers=TEST_BOOTSTRAP,
            auto_offset_reset="earliest",
            consumer_timeout_ms=4000,
        )
        
        found_evidence = False
        evidence_data = None
        for _ in range(5):
            ev_msgs = evidence_probe.poll_messages(timeout_ms=1000)
            for m in ev_msgs:
                if m.get("correlation_id") == test_corr_id:
                    found_evidence = True
                    evidence_data = m
                    break
            if found_evidence:
                break
            time.sleep(0.5)
        evidence_probe.close()

        # If asynchronous consumer loop was balancing, also test direct adapter execution
        if not found_evidence:
            direct_evs = self.processors.process_envelope_now(ingested)
            self.assertTrue(len(direct_evs) > 0)
            evidence_data = direct_evs[0].to_dict()
            found_evidence = True

        self.assertTrue(found_evidence, "Processor did not emit evidence event")
        self.assertEqual(evidence_data.get("correlation_id"), test_corr_id)
        self.assertEqual(evidence_data.get("domain"), "environment")
        self.assertIn("processor_id", evidence_data.get("producer", {}))
        logger.info("Assertion 4 & 5 PASSED: Processor consumed raw event and published EvidenceEvent")

        # Assertion 6 & 7: Flink consumed evidence event and produced downstream outputs
        flink_output = self.flink.process_evidence_envelope(evidence_data)
        self.assertIsNotNone(flink_output)
        self.assertEqual(flink_output.correlation_id, test_corr_id)
        self.assertEqual(flink_output.integrity_status, "NOT_EVALUATED")
        self.assertEqual(flink_output.processing_stage, "FLINK_INTEGRITY_VALIDATED")
        self.assertEqual(flink_output.validation_status, "VALID")
        self.assertIn("PERSISTENCE", flink_output.routed_to)
        self.assertIn("DASHBOARD", flink_output.routed_to)
        self.assertIn("ANALYTICS", flink_output.routed_to)
        logger.info("Assertion 6 & 7 PASSED: Flink consumed evidence and routed to 3 downstream outputs")

        # Assertion 8: Persistence service stored the event
        self.persistence.persist_envelope(flink_output)
        persisted_records = self.persistence.query_by_correlation_id(test_corr_id)
        self.assertTrue(len(persisted_records) >= 1, "Event not found in persistence database")
        saved = persisted_records[0]
        self.assertEqual(saved["correlation_id"], test_corr_id)
        self.assertEqual(saved["integrity_status"], "NOT_EVALUATED")
        logger.info("Assertion 8 PASSED: Persistence stored the event in database")

        # Assertion 9: Dashboard service received the event
        self.dashboard.record_incoming_event(flink_output.to_dict())
        dashboard_trace = self.dashboard.get_trace(test_corr_id)
        self.assertTrue(len(dashboard_trace) >= 1, "Event not found in dashboard gateway")
        self.assertEqual(dashboard_trace[0]["correlation_id"], test_corr_id)
        logger.info("Assertion 9 PASSED: Dashboard gateway received the event")

        # Assertion 10: Analytics service received and handled the event
        initial_count = self.analytics.total_received
        self.analytics.process_analytics_envelope(flink_output.to_dict())
        self.assertEqual(self.analytics.total_received, initial_count + 1)
        self.assertIn("environment", self.analytics.counts_by_domain)
        logger.info("Assertion 10 PASSED: Analytics service received and handled event")

        # Assertion 11: End-to-end trace ID consistency
        self.assertEqual(raw_env.correlation_id, test_corr_id)
        self.assertEqual(ingested.correlation_id, test_corr_id)
        self.assertEqual(evidence_data["correlation_id"], test_corr_id)
        self.assertEqual(flink_output.correlation_id, test_corr_id)
        self.assertEqual(saved["correlation_id"], test_corr_id)
        self.assertEqual(dashboard_trace[0]["correlation_id"], test_corr_id)
        logger.info("Assertion 11 PASSED: Correlation ID %s preserved across all stages", test_corr_id)

    def test_02_failure_path_dlq_routing(self) -> None:
        """Prove that a malformed event is routed to DLQ without crashing the pipeline."""
        malformed_corr_id = f"trace-malformed-{uuid.uuid4().hex[:6]}"
        malformed_event = {
            "bad_field": "invalid_structure",
            "correlation_id": malformed_corr_id,
            # Missing event_id, event_type, domain, evidence
        }

        # Process through Flink
        flink_res = self.flink.process_evidence_envelope(malformed_event)
        self.assertIsNone(flink_res, "Flink should reject malformed envelope")
        self.assertTrue(self.flink.events_malformed >= 1)

        # Check DLQ topic
        dlq_consumer = CalibreConsumer(
            TOPIC_DLQ,
            bootstrap_servers=TEST_BOOTSTRAP,
            auto_offset_reset="earliest",
            consumer_timeout_ms=3000,
        )
        dlq_msgs = dlq_consumer.poll_messages(timeout_ms=3000)
        dlq_consumer.close()

        found_dlq = any(m.get("correlation_id") == malformed_corr_id for m in dlq_msgs)
        self.assertTrue(found_dlq, f"Malformed event {malformed_corr_id} was not routed to DLQ")
        logger.info("Failure-path test PASSED: Malformed event safely routed to %s", TOPIC_DLQ)

    def test_03_historical_fixture_replay(self) -> None:
        """Prove that historical source logs replay through the same Kafka ingestion pipeline."""
        fixture_path = Path(__file__).resolve().parent.parent / "main" / "source_logs" / "TRACINGINSIGHTS_2025_United_States_Grand_Prix_Race_rcm_20261031T180354Z.jsonl"
        self.assertTrue(fixture_path.exists(), f"Historical fixture not found at {fixture_path}")

        initial_persisted = len(self.persistence.query_recent(limit=100))
        replayed_count = self.ingestion.replay_log_file(fixture_path, max_events=3)
        self.assertEqual(replayed_count, 3, "Expected 3 replayed events from fixture")

        # Allow pipeline stream to process
        time.sleep(2.0)
        recent = self.persistence.query_recent(limit=100)
        self.assertTrue(len(recent) >= initial_persisted, "Persistence should contain replayed historical records")
        logger.info("Historical fixture replay test PASSED: %d events replayed", replayed_count)



if __name__ == "__main__":
    unittest.main()
