"""
CALIBRE – Stream Processor
============================
Lightweight Python-native stream processor.

This replaces a heavy Flink deployment for the initial scaffold while
preserving the same logical pipeline:

    raw event → rule monitor → uncertainty → integrity → verdict → store
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Optional

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from config.settings import settings
from rules.pit_lane_speed import PitLaneSpeedMonitor
from uncertainty.engine import UncertaintyEngine
from integrity.detector import CUSUMDetector
from verdict.engine import VerdictEngine
from schemas.telemetry import TelemetryEvent
from schemas.timing import TimingEvent

logger = logging.getLogger(__name__)

# ── Observability counters (simple in-memory; exposed via /metrics) ──────
_metrics: dict[str, int | float] = {
    "events_received": 0,
    "events_processed": 0,
    "verdicts_created": 0,
    "breaches": 0,
    "clears": 0,
    "defers": 0,
    "integrity_alerts": 0,
    "processing_latency_ms": 0.0,
}


def get_metrics() -> dict[str, int | float]:
    """Return a snapshot of current processing metrics."""
    return dict(_metrics)


class StreamProcessor:
    """
    Orchestrates the full processing pipeline.

    In production this would be a Flink job; here it is an asyncio loop
    reading from Kafka, applying rules, and writing results back.
    """

    def __init__(self) -> None:
        self.rule_monitors = [PitLaneSpeedMonitor()]
        self.uncertainty_engine = UncertaintyEngine()
        self.integrity_detector = CUSUMDetector()
        self.verdict_engine = VerdictEngine(
            uncertainty_engine=self.uncertainty_engine,
            integrity_detector=self.integrity_detector,
        )
        self._producer: Optional[AIOKafkaProducer] = None

    async def _get_producer(self) -> AIOKafkaProducer:
        if self._producer is None:
            self._producer = AIOKafkaProducer(
                bootstrap_servers=settings.kafka_bootstrap_servers,
                value_serializer=lambda v: json.dumps(v, default=str).encode(),
            )
            await self._producer.start()
        return self._producer

    async def process_event(self, raw: dict, topic: str) -> None:
        """Route a single raw event through the pipeline."""
        t0 = time.monotonic()
        _metrics["events_received"] += 1

        try:
            if topic == "f1.raw.telemetry":
                event = TelemetryEvent(**raw)
                await self._evaluate_telemetry(event)
            elif topic == "f1.raw.timing":
                timing = TimingEvent(**raw)
                self.integrity_detector.update(
                    sector=timing.sector,
                    timing_speed=timing.timing_speed,
                    oncar_speed=raw.get("reference_speed", timing.timing_speed),
                )
            else:
                logger.debug("Unhandled topic: %s", topic)

            _metrics["events_processed"] += 1
        except Exception:
            logger.exception("Error processing event on topic=%s", topic)

        elapsed = (time.monotonic() - t0) * 1000
        _metrics["processing_latency_ms"] = elapsed

    async def _evaluate_telemetry(self, event: TelemetryEvent) -> None:
        """Run all rule monitors against a telemetry event."""
        for monitor in self.rule_monitors:
            result = monitor.evaluate(event)
            if result is None:
                continue  # rule does not apply to this event

            verdict = self.verdict_engine.decide(
                rule_result=result,
                event=event,
            )
            # Publish verdict
            producer = await self._get_producer()
            await producer.send_and_wait(
                "f1.verdicts",
                value=verdict.model_dump(mode="json"),
                key=str(event.driver_number).encode(),
            )

            # Update metrics
            _metrics["verdicts_created"] += 1
            _metrics[verdict.verdict.value.lower() + "s"] += 1

            logger.info(
                "Verdict: driver=%s rule=%s verdict=%s measured=%.1f threshold=%.1f",
                event.driver_number,
                result.rule_id,
                verdict.verdict.value,
                result.measured_value,
                result.threshold,
            )

    async def run(self) -> None:
        """Main consumer loop – blocks forever."""
        topics = ["f1.raw.telemetry", "f1.raw.timing", "f1.raw.race_control"]
        consumer = AIOKafkaConsumer(
            *topics,
            bootstrap_servers=settings.kafka_bootstrap_servers,
            group_id="calibre-processor",
            auto_offset_reset="earliest",
            value_deserializer=lambda v: json.loads(v.decode()),
            key_deserializer=lambda k: k.decode() if k else None,
        )
        await consumer.start()
        logger.info("StreamProcessor consuming %s", topics)
        try:
            async for msg in consumer:
                await self.process_event(msg.value, msg.topic)
        finally:
            await consumer.stop()
            if self._producer:
                await self._producer.stop()


# Allow `python -m streaming.processor`
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(StreamProcessor().run())
