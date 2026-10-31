"""CALIBRE Deterministic Mock / Replay Source Producer."""
from __future__ import annotations

import logging
import time
from typing import Any, Optional

from calibre_core.contracts import RawSourceEnvelope, utc_now, generate_uuid, stable_id
from calibre_core.kafka_bus import CalibreProducer
from calibre_core.topics import raw_topic

logger = logging.getLogger("calibre.mock_source")


class MockRaceProducer:
    """Deterministic generator of synthetic race telemetry, timing, weather, and race-control events."""

    def __init__(self, producer: Optional[CalibreProducer] = None, run_id: str = "mock-run-01") -> None:
        self.producer = producer or CalibreProducer(client_id="calibre-mock-source")
        self.run_id = run_id
        self.event_counter = 0

    def generate_weather_event(self, correlation_id: Optional[str] = None) -> RawSourceEnvelope:
        """Generate representative weather event."""
        self.event_counter += 1
        corr_id = correlation_id or f"trace-mock-weather-{self.event_counter:04d}"
        now_str = utc_now()
        payload = {
            "wT": 124.5,
            "wAT": 24.5,
            "wTT": 32.8,
            "wH": 55.0,
            "wP": 1013.2,
            "wR": False,
            "wWS": 2.1,
            "wWD": 180,
        }
        envelope = RawSourceEnvelope(
            event_id=stable_id("MOCK", "weather", corr_id, self.event_counter),
            correlation_id=corr_id,
            provider="MOCK",
            feed="weather",
            source_mode="LIVE",
            run_id=self.run_id,
            received_at_utc=now_str,
            source_time={"value": 124.5, "semantics": "SESSION_ELAPSED_SECONDS", "unit": "seconds"},
            payload_format="JSON",
            payload=payload,
            session={"season": 2026, "meeting": "Bahrain Grand Prix", "session_name": "Race"},
            scope={"session": "Race"},
        )
        return envelope

    def generate_telemetry_event(self, correlation_id: Optional[str] = None, driver: str = "VER") -> RawSourceEnvelope:
        """Generate representative telemetry sample."""
        self.event_counter += 1
        corr_id = correlation_id or f"trace-mock-tel-{self.event_counter:04d}"
        now_str = utc_now()
        payload = {
            "time": [0.0, 0.1, 0.2],
            "speed": [285, 288, 292],
            "rpm": [11200, 11400, 11550],
            "gear": [7, 7, 8],
            "throttle": [100, 100, 100],
            "brake": [0, 0, 0],
            "drs": [1, 1, 1],
            "x": [1204.5, 1212.8, 1221.1],
            "y": [3405.2, 3410.9, 3416.7],
            "z": [12.0, 12.1, 12.1],
        }
        envelope = RawSourceEnvelope(
            event_id=stable_id("MOCK", "telemetry", corr_id, self.event_counter),
            correlation_id=corr_id,
            provider="MOCK",
            feed="telemetry",
            source_mode="LIVE",
            run_id=self.run_id,
            received_at_utc=now_str,
            source_time={"value": 0.2, "semantics": "LAP_RELATIVE_SECONDS", "unit": "seconds"},
            payload_format="JSON",
            payload=payload,
            session={"season": 2026, "meeting": "Bahrain Grand Prix", "session_name": "Race"},
            scope={"driver_code": driver, "lap_number": 15},
        )
        return envelope

    def generate_rcm_event(self, correlation_id: Optional[str] = None) -> RawSourceEnvelope:
        """Generate representative race control message (incident notice)."""
        self.event_counter += 1
        corr_id = correlation_id or f"trace-mock-rcm-{self.event_counter:04d}"
        now_str = utc_now()
        payload = {
            "Message": "YELLOW FLAG IN SECTOR 2",
            "Category": "Flag",
            "Flag": "YELLOW",
            "Sector": 2,
            "Scope": "Track",
        }
        envelope = RawSourceEnvelope(
            event_id=stable_id("MOCK", "rcm", corr_id, self.event_counter),
            correlation_id=corr_id,
            provider="MOCK",
            feed="rcm",
            source_mode="LIVE",
            run_id=self.run_id,
            received_at_utc=now_str,
            source_time={"value": now_str, "semantics": "ABSOLUTE_UTC", "unit": "ISO-8601"},
            payload_format="JSON",
            payload=payload,
            session={"season": 2026, "meeting": "Bahrain Grand Prix", "session_name": "Race"},
            scope={"sector": 2},
        )
        return envelope

    def generate_timing_event(self, correlation_id: Optional[str] = None, driver: str = "VER") -> RawSourceEnvelope:
        """Generate representative lap timing record."""
        self.event_counter += 1
        corr_id = correlation_id or f"trace-mock-timing-{self.event_counter:04d}"
        now_str = utc_now()
        payload = {
            "driver": driver,
            "lap": 15,
            "time": 92.456,
            "s1": 29.123,
            "s2": 38.450,
            "s3": 24.883,
            "compound": "HARD",
            "stint": 2,
            "sesT": 1420.5,
            "lST": 1328.044,
        }
        envelope = RawSourceEnvelope(
            event_id=stable_id("MOCK", "timing", corr_id, self.event_counter),
            correlation_id=corr_id,
            provider="MOCK",
            feed="timing",
            source_mode="LIVE",
            run_id=self.run_id,
            received_at_utc=now_str,
            source_time={"value": 92.456, "semantics": "LAP_DURATION_SECONDS", "unit": "seconds"},
            payload_format="JSON",
            payload=payload,
            session={"season": 2026, "meeting": "Bahrain Grand Prix", "session_name": "Race"},
            scope={"driver_code": driver, "lap_number": 15},
            session_time_s=1420.5,
        )
        return envelope

    def produce_single_test_event(self, event_type: str = "weather", correlation_id: Optional[str] = None) -> RawSourceEnvelope:
        """Generate and publish a single deterministic event to Kafka."""
        if event_type == "telemetry":
            envelope = self.generate_telemetry_event(correlation_id)
        elif event_type == "rcm":
            envelope = self.generate_rcm_event(correlation_id)
        elif event_type == "timing":
            envelope = self.generate_timing_event(correlation_id)
        else:
            envelope = self.generate_weather_event(correlation_id)

        topic = raw_topic(envelope.provider, envelope.feed)
        success = self.producer.send(topic=topic, value=envelope, correlation_id=envelope.correlation_id)
        logger.info(
            "MockProducer emitted %s event (corr=%s) to topic %s [success=%s]",
            event_type, envelope.correlation_id, topic, success
        )
        return envelope

    def produce_batch(self, count: int = 5, correlation_id_prefix: str = "trace-e2e-batch") -> list[RawSourceEnvelope]:
        """Produce a sequence of mixed events for pipeline testing."""
        emitted: list[RawSourceEnvelope] = []
        types = ["weather", "telemetry", "rcm", "timing"]
        for i in range(count):
            corr = f"{correlation_id_prefix}-{i+1:03d}"
            ev_type = types[i % len(types)]
            envelope = self.produce_single_test_event(event_type=ev_type, correlation_id=corr)
            emitted.append(envelope)
            time.sleep(0.05)
        return emitted
