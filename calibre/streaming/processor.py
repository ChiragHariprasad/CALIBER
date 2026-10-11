"""
CALIBRE – Stream Processor (§5 FR-KAFKA-001, FR-KAFKA-003)
============================================================
Lightweight Python-native stream processor consuming from Kafka topics,
applying real rule monitors, uncertainty evaluation, sensor integrity detection,
and broadcasting results to API/WebSocket and output topics.
"""

from __future__ import annotations

import asyncio
import json
import logging
import sys
import time
from pathlib import Path
from typing import Optional, Dict, Any

_ROOT = Path(__file__).resolve().parent.parent
_WORKSPACE = _ROOT.parent
if str(_WORKSPACE) not in sys.path:
    sys.path.insert(0, str(_WORKSPACE))
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

try:
    from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
except ImportError:
    AIOKafkaConsumer = None  # type: ignore
    AIOKafkaProducer = None  # type: ignore

try:
    from calibre.config.settings import settings
    from calibre.rules.pit_lane_speed import PitLaneSpeedMonitor
    from calibre.uncertainty.engine import UncertaintyEngine
    from calibre.integrity.detector import CUSUMDetector
    from calibre.verdict.engine import VerdictEngine
    from calibre.schemas.telemetry import TelemetryEvent
    from calibre.schemas.timing import TimingEvent
except ImportError:
    from config.settings import settings
    from rules.pit_lane_speed import PitLaneSpeedMonitor
    from uncertainty.engine import UncertaintyEngine
    from integrity.detector import CUSUMDetector
    from verdict.engine import VerdictEngine
    from schemas.telemetry import TelemetryEvent
    from schemas.timing import TimingEvent

logger = logging.getLogger(__name__)

# Observability counters
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
    Orchestrates the real Kafka stream processing pipeline.
    Reads from Kafka topics, applies rules & integrity checks, and writes results back.
    """

    CONSUMER_TOPICS = [
        "calibre.raw.telemetry",
        "calibre.normalized.telemetry",
        "calibre.race.events",
        "f1.raw.telemetry",
        "f1.raw.timing",
        "f1.raw.race_control",
    ]

    def __init__(self, bootstrap_servers: Optional[str] = None) -> None:
        self.bootstrap_servers = bootstrap_servers or settings.kafka_bootstrap_servers
        self.rule_monitors = [PitLaneSpeedMonitor()]
        self.uncertainty_engine = UncertaintyEngine()
        self.integrity_detector = CUSUMDetector()
        self.verdict_engine = VerdictEngine(
            uncertainty_engine=self.uncertainty_engine,
            integrity_detector=self.integrity_detector,
        )
        self._producer: Optional[AIOKafkaProducer] = None
        self._consumer: Optional[AIOKafkaConsumer] = None
        self._running = False

    async def _get_producer(self) -> Optional[AIOKafkaProducer]:
        if AIOKafkaProducer is None:
            return None
        if self._producer is None:
            try:
                self._producer = AIOKafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
                )
                await self._producer.start()
            except Exception as e:
                logger.warning("StreamProcessor could not start producer: %s", e)
                return None
        return self._producer

    async def process_event(self, raw: dict, topic: str) -> None:
        """Route a single event from Kafka through the analytical pipeline."""
        t0 = time.monotonic()
        _metrics["events_received"] += 1

        try:
            # Unpack canonical envelope if present
            payload = raw.get("payload", raw)
            event_type = raw.get("type", "unknown").lower()

            if "telemetry" in topic or "telemetry" in event_type:
                # Normalise fields
                drv_num = payload.get("driver_number", 1 if payload.get("driver") == "VER" else 4)
                telem_dict = {
                    "timestamp": payload.get("timestamp") or raw.get("event_time", "2024-07-07T14:00:00Z"),
                    "session_key": payload.get("session_key", 11731),
                    "driver_number": drv_num,
                    "speed": float(payload.get("speed_kph", payload.get("speed", 0.0))),
                    "throttle": float(payload.get("throttle_pct", payload.get("throttle", 0.0))),
                    "brake": float(payload.get("brake_pct", payload.get("brake", 0.0))),
                    "gear": int(payload.get("gear", 3)),
                    "rpm": float(payload.get("rpm", 6000.0)),
                    "source": raw.get("source", "kafka"),
                    "zone": payload.get("zone", "pit_lane" if payload.get("in_pit") else None),
                }
                event = TelemetryEvent(**telem_dict)
                await self._evaluate_telemetry(event)

            elif "timing" in topic:
                timing = TimingEvent(**payload)
                alert = self.integrity_detector.update(
                    sector=timing.sector,
                    timing_speed=timing.timing_speed,
                    oncar_speed=payload.get("reference_speed", timing.timing_speed),
                )
                if alert:
                    _metrics["integrity_alerts"] += 1

            _metrics["events_processed"] += 1
        except Exception:
            logger.exception("Error processing event on topic=%s", topic)

        elapsed = (time.monotonic() - t0) * 1000
        _metrics["processing_latency_ms"] = round(elapsed, 2)

    async def _evaluate_telemetry(self, event: TelemetryEvent) -> None:
        """Run rule monitors and verdict engine against telemetry."""
        for monitor in self.rule_monitors:
            result = monitor.evaluate(event)
            if result is None:
                continue

            verdict = self.verdict_engine.decide(
                rule_result=result,
                event=event,
            )

            # Publish verdict to output topics
            prod = await self._get_producer()
            if prod:
                verdict_json = verdict.model_dump(mode="json")
                try:
                    await prod.send_and_wait(
                        "f1.verdicts",
                        value=verdict_json,
                        key=str(event.driver_number).encode(),
                    )
                    await prod.send(
                        "calibre.agent.decisions",
                        value=verdict_json,
                        key=str(event.driver_number).encode(),
                    )
                except Exception as ex:
                    logger.debug("Could not publish verdict: %s", ex)

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
        """Main consumer loop – consumes continuously from configured Kafka topics."""
        if AIOKafkaConsumer is None:
            logger.error("aiokafka is not installed; StreamProcessor cannot run.")
            return

        self._running = True
        consumer = AIOKafkaConsumer(
            *self.CONSUMER_TOPICS,
            bootstrap_servers=self.bootstrap_servers,
            group_id="calibre-processor-group",
            auto_offset_reset="earliest",
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            key_deserializer=lambda k: k.decode("utf-8") if k else None,
        )
        self._consumer = consumer
        try:
            await consumer.start()
            logger.info("StreamProcessor consuming from %s on broker %s", self.CONSUMER_TOPICS, self.bootstrap_servers)
            async for msg in consumer:
                if not self._running:
                    break
                await self.process_event(msg.value, msg.topic)
        except Exception as e:
            logger.warning("StreamProcessor consumer encountered error: %s", e)
        finally:
            await consumer.stop()
            if self._producer:
                await self._producer.stop()
            logger.info("StreamProcessor stopped cleanly.")

    async def stop(self) -> None:
        self._running = False
        if self._consumer:
            await self._consumer.stop()
        if self._producer:
            await self._producer.stop()


# Allow `python -m calibre.streaming.processor`
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(StreamProcessor().run())
