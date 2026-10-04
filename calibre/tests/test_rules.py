"""
CALIBRE – Rule Monitor Tests
==============================
"""

import sys
import os
from datetime import datetime, timezone

# Ensure the calibre package root is on sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Override settings before import
os.environ.setdefault("DATABASE_URL", "sqlite+aiosqlite:///test.db")
os.environ.setdefault("DATABASE_URL_SYNC", "sqlite:///test.db")

from schemas.telemetry import TelemetryEvent
from rules.pit_lane_speed import PitLaneSpeedMonitor


def _make_event(speed: float, zone: str = "pit_lane") -> TelemetryEvent:
    return TelemetryEvent(
        timestamp=datetime(2024, 5, 26, 14, 30, 0, tzinfo=timezone.utc),
        session_key=9999,
        driver_number=1,
        speed=speed,
        zone=zone,
        source="simulation",
    )


class TestPitLaneSpeedMonitor:
    """Pit-lane speed rule tests."""

    def test_clear_well_below_limit(self):
        monitor = PitLaneSpeedMonitor(limit_kmh=60.0)
        result = monitor.evaluate(_make_event(speed=48.0))
        assert result is not None
        assert result.exceeded is False
        assert result.measured_value == 48.0

    def test_breach_well_above_limit(self):
        monitor = PitLaneSpeedMonitor(limit_kmh=60.0)
        result = monitor.evaluate(_make_event(speed=67.0))
        assert result is not None
        assert result.exceeded is True
        assert result.measured_value == 67.0

    def test_exactly_at_limit_not_exceeded(self):
        monitor = PitLaneSpeedMonitor(limit_kmh=60.0)
        result = monitor.evaluate(_make_event(speed=60.0))
        assert result is not None
        assert result.exceeded is False

    def test_returns_none_outside_pit_lane(self):
        monitor = PitLaneSpeedMonitor(limit_kmh=60.0)
        result = monitor.evaluate(_make_event(speed=300.0, zone="sector_1"))
        assert result is None

    def test_pit_entry_is_evaluated(self):
        monitor = PitLaneSpeedMonitor(limit_kmh=60.0)
        result = monitor.evaluate(_make_event(speed=55.0, zone="pit_entry"))
        assert result is not None

    def test_pit_exit_is_evaluated(self):
        monitor = PitLaneSpeedMonitor(limit_kmh=60.0)
        result = monitor.evaluate(_make_event(speed=55.0, zone="pit_exit"))
        assert result is not None
