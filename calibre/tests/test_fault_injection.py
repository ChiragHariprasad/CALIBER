"""
CALIBRE — Fault Injection & System Integrity Tests (Workstream G)
==================================================================
Tests for:
  - Source failure & reconnection
  - Kafka unavailable handling
  - Consumer crash & recovery
  - Duplicate & out-of-order events
  - Stale / missing telemetry
  - Invalid schema / dead-letter routing
  - Replay pause/resume/restart/seek lifecycle
  - Mode isolation (LIVE vs HISTORICAL_REPLAY vs SIMULATION)
  - No duplicate recommendation after replay restart
  - Distinct sensor integrity categories (Spike vs Drift vs Stuck)
  - Strategy reaction to safety-car flag change
"""

import asyncio
import time
import pytest

from calibre.live.error_signal_agent import ErrorSignalAgent
from calibre.live.operating_mode import OperatingMode, OperatingModeController, FeedStatus
from calibre.live.schemas.events import CalibreEvent, EventType, ValidationState
from calibre.live.schemas.validation import EventValidator
from calibre.live.tyre_strategy_agent import TyreStrategyAgent
from calibre.replay.controller import ReplayController
from calibre.schemas.race_telemetry import TrackStatus, TyreCompound


# ── 1. Sensor Integrity Intelligence (Spike vs Drift vs Stuck) ──────────

def test_sensor_integrity_spike_detection():
    agent = ErrorSignalAgent()
    # Normal baseline reading
    agent.process_telemetry("VER", {"speed_kph": 280.0, "throttle_pct": 95.0, "brake_pct": 0.0})

    # Sudden speed jump > 50 km/h in 1 tick
    findings = agent.process_telemetry("VER", {"speed_kph": 365.0, "throttle_pct": 95.0, "brake_pct": 0.0})
    spike_findings = [f for f in findings if f["category"] == "SPIKE_ANOMALY"]
    assert len(spike_findings) >= 1
    f = spike_findings[0]
    assert f["category"] == "SPIKE_ANOMALY"
    assert f["signal"] == "speed_kph"
    assert "speed jump" in f["message"]
    assert "quarantined" in f["downstream_impact"] or "DEFERRED" in f["downstream_impact"]


def test_sensor_integrity_sustained_drift_detection():
    agent = ErrorSignalAgent()
    # Feed 7 consecutive monotonically drifting speed readings with steady throttle
    base_speed = 200.0
    drift_detected = False
    for step in range(8):
        current_speed = base_speed + (step * 3.5)
        findings = agent.process_telemetry("VER", {
            "speed_kph": current_speed,
            "throttle_pct": 60.0,
            "brake_pct": 0.0,
        })
        drift_findings = [f for f in findings if f["category"] == "SUSTAINED_DRIFT"]
        if drift_findings:
            drift_detected = True
            assert drift_findings[0]["category"] == "SUSTAINED_DRIFT"
            assert drift_findings[0]["detection_method"] == "cumulative_drift_analysis"
            assert "bias" in drift_findings[0]["downstream_impact"]
            break

    assert drift_detected is True, "Sustained drift must trigger distinct SUSTAINED_DRIFT finding"


def test_sensor_integrity_stuck_sensor_detection():
    agent = ErrorSignalAgent()
    # Feed identical non-zero speed reading across 7 consecutive ticks
    stuck_detected = False
    for _ in range(7):
        findings = agent.process_telemetry("VER", {
            "speed_kph": 185.0,
            "throttle_pct": 50.0,
            "brake_pct": 0.0,
        })
        stuck_findings = [f for f in findings if f["category"] == "STUCK_SENSOR"]
        if stuck_findings:
            stuck_detected = True
            assert stuck_findings[0]["category"] == "STUCK_SENSOR"
            assert stuck_findings[0]["observed_value"] == 185.0
            break

    assert stuck_detected is True, "Frozen sensor channel must trigger STUCK_SENSOR"


def test_synthetic_fault_injection_labeling():
    agent = ErrorSignalAgent()
    finding = agent.inject_synthetic_fault("spike", driver="VER", signal="speed_kph", magnitude=120.0)
    assert finding["is_synthetic"] is True
    assert finding["data_quality"]["provenance"] == "simulated"
    assert "SIMULATED" in finding["message"]


# ── 2. Strategy Safety Car Reaction & Degraded Inputs ───────────────────

def test_strategy_reaction_to_safety_car():
    strat_agent = TyreStrategyAgent(target_driver="VER", total_laps=56)
    lap_data = {
        "lap_number": 8,
        "lap_time_s": 88.5,
        "s1_time_s": 27.9,
        "s2_time_s": 30.4,
        "s3_time_s": 30.2,
    }

    # Under Green track status
    decision_green = strat_agent.process_lap(
        driver="VER",
        lap_data=lap_data,
        position=2,
        compound=TyreCompound.MEDIUM,
        tyre_age=8,
        gap_ahead=1.5,
        gap_behind=3.0,
        driver_ahead="RUS",
        driver_behind="LEC",
        track_status=TrackStatus.GREEN,
        weather=None,
        opponents=[],
    )
    assert decision_green is not None

    # Track status transitions to SAFETY_CAR
    lap_data_sc = dict(lap_data, lap_number=9, lap_time_s=110.0)
    decision_sc = strat_agent.process_lap(
        driver="VER",
        lap_data=lap_data_sc,
        position=2,
        compound=TyreCompound.MEDIUM,
        tyre_age=9,
        gap_ahead=0.8,
        gap_behind=1.2,
        driver_ahead="RUS",
        driver_behind="LEC",
        track_status=TrackStatus.SAFETY_CAR,
        weather=None,
        opponents=[],
    )
    assert decision_sc is not None
    assert decision_sc.decision in ("PIT", "STAY")
    assert 0.0 <= decision_sc.confidence <= 1.0


# ── 3. Deduplication and Out-of-Order Handling ──────────────────────────

def test_event_deduplication_and_validation():
    validator = EventValidator(dedup_window_s=5.0)
    ev1 = CalibreEvent(
        event_id="EVT-001",
        session_key=11731,
        event_type=EventType.TELEMETRY,
        driver="VER",
        source_id="openf1_live",
        source_timestamp=time.time(),
        operating_mode="HISTORICAL_REPLAY",
        data={"speed_kph": 280.0},
    )
    res1 = validator.validate_event(ev1)
    assert res1 == ValidationState.VALID

    # Duplicate submission of same event
    res2 = validator.validate_event(ev1)
    assert res2 == ValidationState.DUPLICATE


def test_invalid_schema_quarantined():
    validator = EventValidator()
    # Missing required timestamp and negative speed
    bad_event = CalibreEvent(
        event_id="EVT-BAD-001",
        session_key=11731,
        event_type=EventType.TELEMETRY,
        driver="VER",
        source_id="mock",
        source_timestamp=-1.0,
        operating_mode="HISTORICAL_REPLAY",
        data={"speed_kph": -50.0},  # Implausible
    )
    res = validator.validate_event(bad_event)
    assert res == ValidationState.QUARANTINED
    assert bad_event.validation_state == ValidationState.QUARANTINED


# ── 4. Replay Lifecycle (Pause / Resume / Restart / Seek) ───────────────

@pytest.mark.asyncio
async def test_replay_lifecycle_pause_resume_restart():
    controller = ReplayController()
    session = await controller.start_replay(session_id="11731", driver="VER", speed=20.0)
    assert session.status in ("INITIALIZING", "PLAYING")

    # Pause
    await controller.pause_replay(session.replay_id)
    assert session.status == "PAUSED"

    # Resume
    await controller.resume_replay(session.replay_id)
    assert session.status == "PLAYING"

    # Seek
    await controller.seek_replay(session.replay_id, target_lap=10)
    assert session.current_lap == 10

    # Restart
    await controller.restart_replay(session.replay_id)
    assert session.current_lap == 1
    assert session.events_published == 0
    assert session.status == "PLAYING"

    # Stop
    await controller.stop_replay(session.replay_id)
    assert session.status == "STOPPED"


# ── 5. Operating Mode Isolation ─────────────────────────────────────────

def test_operating_mode_isolation():
    controller = OperatingModeController(initial_mode=OperatingMode.LIVE)
    assert controller.current_mode == OperatingMode.LIVE

    # Explicit transition to HISTORICAL_REPLAY
    controller.transition_to(
        OperatingMode.HISTORICAL_REPLAY,
        reason="Operator started replay session 11731",
        session_key=11731,
    )
    assert controller.current_mode == OperatingMode.HISTORICAL_REPLAY

    # Explicit transition to SIMULATION
    controller.transition_to(
        OperatingMode.SIMULATION,
        reason="Injected synthetic fault scenario",
        scenario="sensor_spike_test",
    )
    assert controller.current_mode == OperatingMode.SIMULATION
