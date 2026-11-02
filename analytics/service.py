"""CALIBRE Analytics Downstream Service.

Consumes from calibre.out.analytics and performs pipeline-level telemetry calculations:
- Event totals and rates
- Distributions per source provider and processor
- Processing throughput
- Publishes operational metrics to calibre.analytics.metrics
- Publishes high-severity finding alerts to calibre.alerts
"""
from __future__ import annotations

import collections
import json
import logging
import threading
import time
from typing import Any, Optional

from calibre_core.contracts import FlinkOutputEnvelope, utc_now, generate_uuid
from calibre_core.health import ComponentHealth, HealthStatus
from calibre_core.kafka_bus import CalibreProducer, CalibreConsumer
from calibre_core.run_scope import belongs_to_run
from calibre_core.topics import (
    TOPIC_OUT_ANALYTICS,
    TOPIC_ANALYTICS_METRICS,
    TOPIC_ALERTS,
    GROUP_ANALYTICS,
)

logger = logging.getLogger("calibre.analytics")


class AnalyticsService:
    """Computes operational stream analytics without sporting decision logic."""

    def __init__(
        self,
        bootstrap_servers: Optional[str] = None,
        run_id: Optional[str] = None,
    ) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.run_id = run_id
        self.producer = CalibreProducer(bootstrap_servers=bootstrap_servers, client_id="calibre-analytics-producer")
        self.health = ComponentHealth(name="AnalyticsService")
        self.health.set_dependency("kafka_producer", self.producer.is_connected)
        
        self.is_running = False
        self._worker_thread: Optional[threading.Thread] = None
        
        # Operational Metrics Counters
        self.total_received = 0
        self.counts_by_provider: collections.Counter[str] = collections.Counter()
        self.counts_by_processor: collections.Counter[str] = collections.Counter()
        self.counts_by_domain: collections.Counter[str] = collections.Counter()
        self.findings_count = 0
        self._timestamps: collections.deque[float] = collections.deque(maxlen=100)

    def process_analytics_envelope(self, data: dict[str, Any]) -> None:
        """Process incoming Flink-validated output event."""
        if not belongs_to_run(data, self.run_id):
            return
        self.total_received += 1
        now = time.time()
        self._timestamps.append(now)

        ev = data.get("event", {})
        prov = ev.get("provenance", {})
        producer = ev.get("producer", {})

        provider_name = prov.get("provider", "UNKNOWN")
        processor_id = producer.get("processor_id", "UNKNOWN")
        domain = ev.get("domain", "UNKNOWN")

        self.counts_by_provider[provider_name] += 1
        self.counts_by_processor[processor_id] += 1
        self.counts_by_domain[domain] += 1

        findings = ev.get("findings", [])
        if findings:
            self.findings_count += len(findings)
            for finding in findings:
                sev = finding.get("severity", "INFO")
                if sev in ("HIGH", "CRITICAL"):
                    # Emit alert notification stub per contract
                    self._emit_alert(ev, finding)

        self.health.record_activity(events_in=1, events_out=1)
        
        # Periodically publish metrics summary to calibre.analytics.metrics
        if self.total_received % 5 == 0:
            self._publish_metrics()

    def _calculate_throughput(self) -> float:
        """Calculate events per second over recent sliding window."""
        if len(self._timestamps) < 2:
            return 0.0
        duration = self._timestamps[-1] - self._timestamps[0]
        if duration <= 0:
            return 0.0
        return round(len(self._timestamps) / duration, 2)

    def _publish_metrics(self) -> None:
        """Emit pipeline metrics record to Kafka."""
        metric_event = {
            "schema_name": "calibre.analytics_metric",
            "schema_version": "1.0",
            "metric_id": generate_uuid(),
            "metric_name": "pipeline_throughput",
            "value": self._calculate_throughput(),
            "unit": "events_per_sec",
            "observed_at_utc": utc_now(),
            "total_events": self.total_received,
            "counts_by_provider": dict(self.counts_by_provider),
            "counts_by_processor": dict(self.counts_by_processor),
            "counts_by_domain": dict(self.counts_by_domain),
            "findings_detected": self.findings_count,
        }
        self.producer.send(TOPIC_ANALYTICS_METRICS, metric_event)

    def _emit_alert(self, event_data: dict[str, Any], finding: dict[str, Any]) -> None:
        """Publish human-review alert stub."""
        alert_event = {
            "schema_name": "calibre.analytics_alert",
            "schema_version": "1.0",
            "alert_id": generate_uuid(),
            "created_at_utc": utc_now(),
            "status": "OPEN",
            "severity": finding.get("severity", "HIGH"),
            "category": finding.get("category", "ANOMALY"),
            "title": f"Review Alert: {finding.get('code', 'UNKNOWN')}",
            "message": finding.get("message", "High severity finding detected"),
            "correlation_id": event_data.get("correlation_id"),
            "evidence_refs": [event_data.get("event_id")],
        }
        self.producer.send(TOPIC_ALERTS, alert_event, correlation_id=event_data.get("correlation_id"))
        logger.warning("Analytics emitted alert: %s", alert_event["title"])

    def _stream_loop(self) -> None:
        """Continuous consumer loop for calibre.out.analytics."""
        consumer = CalibreConsumer(
            TOPIC_OUT_ANALYTICS,
            group_id=f"{GROUP_ANALYTICS}-{self.run_id}" if self.run_id else GROUP_ANALYTICS,
            bootstrap_servers=self.bootstrap_servers,
            auto_offset_reset="latest" if self.run_id else "earliest",
            consumer_timeout_ms=500,
        )

        logger.info("Analytics service consuming from %s", TOPIC_OUT_ANALYTICS)
        while self.is_running:
            try:
                messages = consumer.poll_messages(timeout_ms=500, max_records=20)
                for msg in messages:
                    self.process_analytics_envelope(msg)
            except Exception as exc:
                logger.error("Analytics consumer loop error: %s", exc)
                time.sleep(1.0)

        consumer.close()

    def start(self) -> None:
        self.is_running = True
        self.health.is_ready = True
        self._worker_thread = threading.Thread(
            target=self._stream_loop,
            name="calibre-analytics-consumer",
            daemon=True,
        )
        self._worker_thread.start()
        logger.info("AnalyticsService started")

    def stop(self) -> None:
        self.is_running = False
        if self._worker_thread and self._worker_thread.is_alive():
            self._worker_thread.join(timeout=2.0)
        self.producer.close()
        self.health.is_ready = False
        logger.info("AnalyticsService stopped")

    def summary(self) -> dict[str, Any]:
        return {
            "total_received": self.total_received,
            "throughput_eps": self._calculate_throughput(),
            "providers": dict(self.counts_by_provider),
            "processors": dict(self.counts_by_processor),
            "domains": dict(self.counts_by_domain),
            "findings_count": self.findings_count,
            "health": self.health.to_dict(),
        }
