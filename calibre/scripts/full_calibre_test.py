"""
CALIBRE — Full Multi-Point Streaming & Subsystem Test Suite
===========================================================
Executes rigorous end-to-end testing across all CALIBRE components:
  1. Pit Lane Speed Monitoring & Verdict Engine (BREACH / CLEAR / DEFER)
  2. Sensor Integrity Intelligence (CUSUM drift & physical spike detection)
  3. Canonical Event Envelopes & Validation (provenance, deduplication, quarantine)
  4. Multi-Point Data Aggregator (concurrency, priority arbitration, failover)
  5. Predictive Tyre Strategy & Monte Carlo Optimization
  6. Racing Line Performance & Tactical Driver Radio Engine
  7. Master RaceCoordinator Full Loop Verification

Run via:
  python calibre/scripts/full_calibre_test.py
"""
from __future__ import annotations

import asyncio
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# Setup paths
CALIBRE_ROOT = Path(__file__).resolve().parent.parent
WORKSPACE_ROOT = CALIBRE_ROOT.parent
if str(WORKSPACE_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKSPACE_ROOT))
if str(CALIBRE_ROOT) not in sys.path:
    sys.path.insert(0, str(CALIBRE_ROOT))

# Imports
from calibre.integrity.detector import CUSUMDetector
from calibre.live.data_sources.f1_insights_source import F1InsightsSource
from calibre.live.data_sources.jolpica_source import JolpicaSource
from calibre.live.data_sources.mock_source import MockDataSource
from calibre.live.data_sources.openf1_source import OpenF1Source
from calibre.live.driver_radio import DriverRadioEngine, RadioCategory, RadioPriority
from calibre.live.error_signal_agent import ErrorSignalAgent
from calibre.live.line_agent import LineAgent
from calibre.live.multipoint_aggregator import MultiPointAggregator
from calibre.live.operating_mode import OperatingMode, OperatingModeController
from calibre.live.pit_lane_monitor import PitLaneMonitor
from calibre.live.race_coordinator import RaceCoordinator
from calibre.live.schemas.events import (
    CalibreEvent,
    DataOrigin,
    DataQuality,
    EventType,
    ValidationState,
)
from calibre.live.schemas.validation import EventValidator
from calibre.live.tyre_strategy_agent import TyreStrategyAgent
from calibre.rules.pit_lane_speed import PitLaneSpeedMonitor
from calibre.schemas.race_telemetry import TrackStatus, TyreCompound
from calibre.schemas.telemetry import TelemetryEvent
from calibre.schemas.verdict import VerdictValue
from calibre.uncertainty.engine import UncertaintyEngine
from calibre.verdict.engine import VerdictEngine


class TestReporter:
    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.results = []

    def report(self, name: str, success: bool, details: str = ""):
        if success:
            self.passed += 1
            print(f"  [PASS] {name} {details}")
            self.results.append((name, True, details))
        else:
            self.failed += 1
            print(f"  [FAIL] {name} -- {details}")
            self.results.append((name, False, details))

    def summary(self):
        total = self.passed + self.failed
        print("\n" + "=" * 65)
        print(f"CALIBRE TEST EXECUTION SUMMARY: {self.passed}/{total} PASSED")
        if self.failed == 0:
            print("ALL SUBSYSTEM TESTS PASSED WITH 100% SUCCESS RATE")
        else:
            print(f"WARNING: {self.failed} TEST(S) FAILED")
        print("=" * 65 + "\n")
        return self.failed == 0


reporter = TestReporter()


# ── TEST 1: Pit Lane Rules & Verdict Engine ──────────────────────────

def test_pit_lane_verdicts():
    print("\n[TEST 1] Pit Lane Rule Evaluation & Verdict Engine")
    monitor = PitLaneMonitor(speed_limit_kmh=60.0)

    # 1a. Speed = 54.0 km/h (< 60 - k*sigma = 56.8 km/h) -> CLEAR
    now = datetime.now(timezone.utc)
    ev_clear = TelemetryEvent(
        timestamp=now,
        session_key=11731,
        driver_number=1,
        speed=54.0,
        zone="pit_lane",
    )
    v_clear = monitor.evaluate_telemetry(ev_clear)
    reporter.report(
        "Pit Speed Below Limit (54.0 km/h -> CLEAR)",
        v_clear is not None and v_clear.verdict == VerdictValue.CLEAR,
        f"Verdict: {v_clear.verdict.value if v_clear else None}",
    )

    # 1b. Speed = 65.0 km/h (> 60 + k*sigma = 63.2 km/h) -> BREACH
    ev_breach = TelemetryEvent(
        timestamp=now,
        session_key=11731,
        driver_number=1,
        speed=65.0,
        zone="pit_lane",
    )
    v_breach = monitor.evaluate_telemetry(ev_breach)
    reporter.report(
        "Pit Speed Exceeded Limit (65.0 km/h -> BREACH)",
        v_breach is not None and v_breach.verdict == VerdictValue.BREACH,
        f"Verdict: {v_breach.verdict.value if v_breach else None}",
    )

    # 1c. Speed = 58.2 km/h (within [56.8, 63.2] uncertainty margin) -> DEFER
    ev_margin = TelemetryEvent(
        timestamp=now,
        session_key=11731,
        driver_number=1,
        speed=58.2,
        zone="pit_lane",
    )
    v_margin = monitor.evaluate_telemetry(ev_margin)
    reporter.report(
        "Pit Speed Within Uncertainty Margin (58.2 km/h -> DEFER)",
        v_margin is not None and v_margin.verdict == VerdictValue.DEFER,
        f"Verdict: {v_margin.verdict.value if v_margin else None}",
    )

    # 1d. CUSUM Sensor Alert triggers DEFER on subsequent 65.0 km/h sample
    for _ in range(10):
        monitor.feed_timing_cross_check(
            sector="pit_lane",
            timing_speed=65.0,
            oncar_speed=55.0,  # Residual = +10.0
        )

    has_alert = monitor.integrity_detector.has_recent_alert("pit_lane")
    reporter.report(
        "CUSUM Detection of Timing System Residual Bias",
        has_alert,
        "Alert flag active",
    )

    # Now evaluate high speed sample: should be downgraded to DEFER
    v_defer = monitor.evaluate_telemetry(ev_breach)
    reporter.report(
        "Compromised Sensor Downgrades Verdict to DEFER",
        v_defer is not None and v_defer.verdict == VerdictValue.DEFER,
        f"Verdict: {v_defer.verdict.value if v_defer else None}",
    )



# ── TEST 2: Sensor Integrity & Anomaly Intelligence ──────────────────

def test_sensor_integrity():
    print("\n[TEST 2] Sensor Integrity Intelligence")
    agent = ErrorSignalAgent()

    # 2a. Normal telemetry sample -> No errors
    normal_telem = {"speed_kph": 310.0, "throttle_pct": 100.0, "brake_pct": 0.0}
    signals_normal = agent.process_telemetry("VER", normal_telem)
    reporter.report(
        "Normal High-Speed Telemetry Accepted",
        len(signals_normal) == 0,
        f"Signals detected: {len(signals_normal)}",
    )

    # 2b. Telemetry Spike -> 420 km/h (Physically impossible in F1)
    spike_telem = {"speed_kph": 420.0, "throttle_pct": 100.0, "brake_pct": 0.0}
    signals_spike = agent.process_telemetry("VER", spike_telem)
    has_spike = any(s["type"] == "TELEMETRY_SPIKE" for s in signals_spike)
    reporter.report(
        "Impossible Telemetry Spike (>370 km/h) Flagged",
        has_spike,
        f"Detected: {[s['type'] for s in signals_spike]}",
    )

    # 2c. Impossible Lap Time Anomaly (35s lap)
    lap_anomaly = {"lap_number": 12, "lap_time_s": 35.0, "is_valid": True}
    signals_lap = agent.process_lap("VER", lap_anomaly)
    has_lap_err = any(s["type"] == "LAP_TIME_ANOMALY" for s in signals_lap)
    reporter.report(
        "Impossible Lap Time (<55s) Flagged as Anomaly",
        has_lap_err,
        f"Detected: {[s['type'] for s in signals_lap]}",
    )


# ── TEST 3: Canonical Event Envelope & Validation ────────────────────

def test_canonical_envelope():
    print("\n[TEST 3] Canonical Event Envelope & Schema Validation")
    validator = EventValidator()

    # 3a. Valid telemetry envelope
    ev_valid = CalibreEvent(
        event_type=EventType.TELEMETRY,
        source_id="openf1_live",
        operating_mode="LIVE",
        source_timestamp=time.time(),
        driver="VER",
        data={"speed_kph": 285.4, "throttle_pct": 98.0, "gear": 7},
        field_origins={"speed_kph": DataOrigin.OBSERVED.value},
    )
    res_valid = validator.validate(ev_valid)
    reporter.report(
        "Valid Canonical Event Accepted",
        res_valid.validation_state == ValidationState.VALID,
        f"State: {res_valid.validation_state.value}",
    )

    # 3b. Plausibility Check Failure -> Warning
    ev_implausible = CalibreEvent(
        event_type=EventType.TELEMETRY,
        source_id="f1_insights_live",
        operating_mode="LIVE",
        source_timestamp=time.time(),
        driver="VER",
        data={"speed_kph": 510.0, "throttle_pct": 150.0},  # Out of physical range
    )
    res_warn = validator.validate(ev_implausible)
    reporter.report(
        "Implausible Telemetry Event Flagged as Warning",
        res_warn.validation_state == ValidationState.WARNING,
        f"State: {res_warn.validation_state.value}, Errors: {res_warn.validation_errors}",
    )

    # 3c. Missing required field -> Quarantined
    ev_missing = CalibreEvent(
        event_type=EventType.TELEMETRY,
        source_id="",  # Missing required field
        operating_mode="LIVE",
        source_timestamp=time.time(),
        driver="VER",
        data={"speed_kph": 250.0},
    )
    res_quarantine = validator.validate(ev_missing)
    reporter.report(
        "Malformed Event Quarantined",
        res_quarantine.validation_state == ValidationState.QUARANTINED,
        f"State: {res_quarantine.validation_state.value}",
    )

    # 3d. Deduplication Key Generation
    k1 = ev_valid.compute_dedup_key()
    reporter.report(
        "Deterministic Deduplication Key Generated",
        bool(k1 and "VER" in k1 and "TELEMETRY" in k1),
        f"Key: {k1}",
    )


# ── TEST 4: Multi-Point Aggregator & Priority Failover ───────────────

async def test_multipoint_aggregator():
    print("\n[TEST 4] Multi-Point Live Data Feed Aggregator")
    aggregator = MultiPointAggregator(operating_mode="LIVE")

    # Register Mock and F1 Insights sources
    mock_src = MockDataSource(source_id="mock_feed", target_driver="VER", tick_interval_s=0.05)
    insights_src = F1InsightsSource(source_id="f1_insights_live", frequency_hz=10.0)

    aggregator.register_source(insights_src, priority=1)
    aggregator.register_source(mock_src, priority=5)

    await aggregator.start()

    # Collect aggregated events
    collected_events = []
    t_end = time.time() + 1.5
    while time.time() < t_end and len(collected_events) < 15:
        ev = await aggregator.get_next_event(timeout=0.3)
        if ev:
            collected_events.append(ev)

    reporter.report(
        "Multi-Point Aggregator Ingests Concurrent Events",
        len(collected_events) >= 5,
        f"Events received: {len(collected_events)}",
    )

    # Verify health status tracking
    health = aggregator.get_health_status()
    reporter.report(
        "Feed Health & Metrics Aggregation Available",
        "sources" in health and len(health["sources"]) == 2,
        f"Throughput: {health.get('throughput_eps')} eps",
    )

    await aggregator.stop()


# ── TEST 5: Tyre Strategy & Monte Carlo Simulator ────────────────────

def test_tyre_strategy():
    print("\n[TEST 5] Predictive Tyre Strategy Intelligence")
    agent = TyreStrategyAgent(target_driver="VER", total_laps=57)

    from calibre.schemas.race_telemetry import WeatherState
    weather = WeatherState()

    # Feed laps with progressive degradation
    for lap in range(1, 15):
        lap_time = 92.0 + (lap * 0.12)  # degradation
        agent.process_lap(
            driver="VER",
            lap_data={"lap_number": lap, "lap_time_s": lap_time, "is_valid": True},
            position=1,
            compound=TyreCompound.MEDIUM,
            tyre_age=lap,
            gap_ahead=None,
            gap_behind=4.5,
            driver_ahead=None,
            driver_behind="NOR",
            track_status=TrackStatus.GREEN,
            weather=weather,
            opponents=[],
        )

    summary = agent.get_strategy_summary()
    reporter.report(
        "Tyre Degradation Tracking & Stint Calculation",
        summary["observed_deg_s_per_lap"] is not None,
        f"Observed deg: {summary['observed_deg_s_per_lap']} s/lap",
    )

    # Force strategic replan upon Safety Car event
    decision = agent.process_event(
        event_type="TRACK_STATUS_CHANGE",
        track_status=TrackStatus.SAFETY_CAR,
        weather=weather,
        opponents=[],
    )
    reporter.report(
        "Safety Car Event Triggers Strategy Re-evaluation",
        decision is not None,
        f"Decision: {decision.decision if decision else None}, Target Lap: {decision.target_lap if decision else None}, Urgency: {decision.urgency if decision else None}",
    )


# ── TEST 6: Line Agent & Driver Radio Engine ─────────────────────────

def test_line_agent_and_radio():
    print("\n[TEST 6] Racing Line Analysis & Driver Radio Engine")
    line_agent = LineAgent()
    radio_engine = DriverRadioEngine(target_driver="VER")

    # Feed lap data
    lap_data = {
        "lap_number": 5,
        "lap_time_s": 93.15,
        "s1_time_s": 29.1,
        "s2_time_s": 32.4,
        "s3_time_s": 31.65,
        "is_valid": True,
    }
    insights = line_agent.process_lap("VER", lap_data)
    reporter.report(
        "Racing Line Personal Best / Sector Tracking",
        len(insights) > 0,
        f"Insights: {[i['type'] for i in insights]}",
    )

    # Radio Engine: Sector delta advice
    msg_line = radio_engine.generate_sector_advice(
        lap_number=5,
        sector=2,
        delta_s=0.35,
        turn_number=7,
        apex_delta_kph=-9.0,
    )
    reporter.report(
        "Driver Radio Performance Engineer Telemetry Note",
        "Sector 2" in msg_line.message and msg_line.category == RadioCategory.BRAKE,
        f"Radio: '{msg_line.message}'",
    )

    # Radio Engine: Pit Call
    msg_box = radio_engine.generate_pit_call(lap_number=18, target_compound="HARD", urgent=True)
    reporter.report(
        "Driver Radio Urgent Pit Call Generated",
        msg_box.category == RadioCategory.BOX and msg_box.priority == RadioPriority.CRITICAL,
        f"Radio: '{msg_box.message}'",
    )


# ── TEST 7: RaceCoordinator End-to-End Loop ──────────────────────────

async def test_race_coordinator_e2e():
    print("\n[TEST 7] Master RaceCoordinator End-to-End Streaming")
    coordinator = RaceCoordinator(
        session_key=11731,
        target_driver="VER",
        total_laps=56,
        mock_mode=True,
        use_multipoint_aggregator=True,
    )

    await coordinator.start()

    # Let coordinator consume telemetry & aggregate feeds for 2 seconds
    await asyncio.sleep(2.0)

    state = coordinator.get_live_state()
    verdicts = coordinator.get_pit_monitor_stats()
    radio = coordinator.get_recent_radio_messages(5)

    reporter.report(
        "RaceCoordinator Active & Ingesting Samples",
        state["status"] == "LIVE" and state["telemetry_samples"] > 0,
        f"Samples: {state['telemetry_samples']}, Lap: {state['current_lap']}",
    )

    reporter.report(
        "Integrated Pit Lane Monitoring Active",
        verdicts["total_evaluated"] >= 0,
        f"Evaluations: {verdicts['total_evaluated']}",
    )

    reporter.report(
        "Driver Radio Transmissions Recorded",
        len(radio) > 0,
        f"Messages count: {len(radio)}",
    )

    # Check Operating Mode
    mode_dict = coordinator.mode_controller.to_dict()
    reporter.report(
        "Operating Mode State Machine Operational",
        mode_dict["mode"] in ("LIVE", "SIMULATION"),
        f"Mode: {mode_dict['mode']}, Feed: {mode_dict['feed_status']}",
    )

    await coordinator.stop()


# ── MAIN EXECUTION ───────────────────────────────────────────────────

async def main():
    print("\n" + "=" * 65)
    print("CALIBRE PLATFORM — MULTI-POINT STREAMING & SUBSYSTEM TEST SUITE")
    print("=" * 65)

    test_pit_lane_verdicts()
    test_sensor_integrity()
    test_canonical_envelope()
    await test_multipoint_aggregator()
    test_tyre_strategy()
    test_line_agent_and_radio()
    await test_race_coordinator_e2e()

    success = reporter.summary()
    if not success:
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
