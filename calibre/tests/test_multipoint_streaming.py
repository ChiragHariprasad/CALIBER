"""
CALIBRE — Tests for Multi-Point Live Data Streaming & Agent Integration
=======================================================================
"""
import asyncio
import time
import pytest
from datetime import datetime, timezone

from calibre.integrity.detector import CUSUMDetector
from calibre.live.data_sources.f1_insights_source import F1InsightsSource
from calibre.live.data_sources.jolpica_source import JolpicaSource
from calibre.live.data_sources.mock_source import MockDataSource
from calibre.live.data_sources.openf1_source import OpenF1Source
from calibre.live.driver_radio import DriverRadioEngine, RadioCategory, RadioPriority
from calibre.live.multipoint_aggregator import MultiPointAggregator
from calibre.live.operating_mode import OperatingMode, OperatingModeController
from calibre.live.pit_lane_monitor import PitLaneMonitor
from calibre.live.schemas.events import CalibreEvent, EventType, ValidationState
from calibre.live.schemas.validation import EventValidator
from calibre.schemas.telemetry import TelemetryEvent
from calibre.schemas.verdict import VerdictValue


class TestOperatingModeStateMachine:
    def test_initial_mode_and_transitions(self):
        controller = OperatingModeController(initial_mode=OperatingMode.SIMULATION)
        assert controller.mode == OperatingMode.SIMULATION

        controller.transition_to(OperatingMode.LIVE, reason="Live GP session started")
        assert controller.mode == OperatingMode.LIVE
        assert len(controller.state.transition_history) == 1

        controller.declare_degraded(reason="Upstream latency spike")
        assert controller.state.feed_status.value == "DEGRADED"

        controller.declare_healthy()
        assert controller.state.feed_status.value == "HEALTHY"


class TestCanonicalEventValidation:
    def test_validation_and_deduplication(self):
        validator = EventValidator()
        ev1 = CalibreEvent(
            event_type=EventType.TELEMETRY,
            source_id="openf1_live",
            operating_mode="LIVE",
            source_timestamp=time.time(),
            driver="VER",
            data={"speed_kph": 305.2, "throttle_pct": 100.0, "gear": 8},
        )
        res1 = validator.validate(ev1)
        assert res1.validation_state == ValidationState.VALID
        assert res1.quality_score == 1.0

        # Out-of-bounds speed triggers warning
        ev_fast = CalibreEvent(
            event_type=EventType.TELEMETRY,
            source_id="openf1_live",
            operating_mode="LIVE",
            source_timestamp=time.time() + 1.0,
            driver="VER",
            data={"speed_kph": 480.0},
        )
        res_fast = validator.validate(ev_fast)
        assert res_fast.validation_state == ValidationState.WARNING


class TestPitLaneMonitorSubsystem:
    def test_verdict_generation_and_defer_override(self):
        monitor = PitLaneMonitor(speed_limit_kmh=60.0)
        now = datetime.now(timezone.utc)

        # Clear at 54 km/h
        ev_clear = TelemetryEvent(
            timestamp=now,
            session_key=11731,
            driver_number=1,
            speed=54.0,
            zone="pit_lane",
        )
        v_clear = monitor.evaluate_telemetry(ev_clear)
        assert v_clear is not None
        assert v_clear.verdict == VerdictValue.CLEAR

        # Breach at 65 km/h
        ev_breach = TelemetryEvent(
            timestamp=now,
            session_key=11731,
            driver_number=1,
            speed=65.0,
            zone="pit_lane",
        )
        v_breach = monitor.evaluate_telemetry(ev_breach)
        assert v_breach is not None
        assert v_breach.verdict == VerdictValue.BREACH

        # Feed sensor residual bias to trip CUSUM
        for _ in range(12):
            monitor.feed_timing_cross_check("pit_lane", timing_speed=66.0, oncar_speed=56.0)

        assert monitor.integrity_detector.has_recent_alert("pit_lane")

        # Now 65 km/h must be downgraded to DEFER
        v_downgrade = monitor.evaluate_telemetry(ev_breach)
        assert v_downgrade is not None
        assert v_downgrade.verdict == VerdictValue.DEFER


class TestDriverRadioEngineSubsystem:
    def test_radio_generation(self):
        engine = DriverRadioEngine(target_driver="VER")

        box_msg = engine.generate_pit_call(lap_number=16, target_compound="HARD", urgent=True)
        assert box_msg.category == RadioCategory.BOX
        assert box_msg.priority == RadioPriority.CRITICAL
        assert "Box" in box_msg.message

        line_msg = engine.generate_sector_advice(lap_number=16, sector=2, delta_s=0.28, turn_number=4)
        assert line_msg.category == RadioCategory.BRAKE
        assert "Turn 4" in line_msg.message


@pytest.mark.asyncio
async def test_multipoint_aggregator_concurrency():
    aggregator = MultiPointAggregator(operating_mode="LIVE")
    mock_src = MockDataSource(source_id="mock_test", target_driver="VER", tick_interval_s=0.05)
    aggregator.register_source(mock_src, priority=1)

    await aggregator.start()
    ev = await aggregator.get_next_event(timeout=1.5)
    assert ev is not None
    assert ev.event_type is not None

    health = aggregator.get_health_status()
    assert "mock_test" in health["sources"]
    await aggregator.stop()
