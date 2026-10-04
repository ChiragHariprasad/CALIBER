"""
CALIBRE – Verdict Engine & Integrity Detector Tests
=====================================================
"""

import sys
import os
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("DATABASE_URL", "sqlite+aiosqlite:///test.db")
os.environ.setdefault("DATABASE_URL_SYNC", "sqlite:///test.db")

from schemas.telemetry import TelemetryEvent
from schemas.verdict import VerdictValue
from rules.base import RuleResult
from uncertainty.engine import UncertaintyEngine
from integrity.detector import CUSUMDetector
from verdict.engine import VerdictEngine


def _make_event(speed: float, zone: str = "pit_lane") -> TelemetryEvent:
    return TelemetryEvent(
        timestamp=datetime(2024, 5, 26, 14, 30, 0, tzinfo=timezone.utc),
        session_key=9999,
        driver_number=1,
        speed=speed,
        zone=zone,
        source="simulation",
    )


def _make_rule_result(measured: float, threshold: float = 60.0) -> RuleResult:
    return RuleResult(
        rule_id="pit_lane_speed",
        measured_value=measured,
        threshold=threshold,
        exceeded=measured > threshold,
    )


class TestCUSUMDetector:
    """CUSUM change-point detector tests."""

    def test_no_alert_on_small_residuals(self):
        detector = CUSUMDetector(threshold=5.0, drift=0.5)
        for _ in range(10):
            alert = detector.update("pit_lane", timing_speed=60.0, oncar_speed=59.8)
        assert alert is None

    def test_alert_on_sustained_bias(self):
        detector = CUSUMDetector(threshold=5.0, drift=0.5)
        alerts = []
        for _ in range(50):
            alert = detector.update("pit_lane", timing_speed=62.0, oncar_speed=60.0)
            if alert:
                alerts.append(alert)
        assert len(alerts) > 0
        assert alerts[0].sector == "pit_lane"
        assert alerts[0].severity in ("warning", "critical")

    def test_cusum_resets_after_alert(self):
        detector = CUSUMDetector(threshold=3.0, drift=0.0)
        # Force an alert
        for _ in range(10):
            detector.update("s1", timing_speed=65.0, oncar_speed=60.0)
        # After alert + reset, CUSUM should be near 0
        assert detector.get_cusum("s1") < 3.0

    def test_different_sectors_are_independent(self):
        detector = CUSUMDetector(threshold=5.0, drift=0.5)
        detector.update("s1", timing_speed=65.0, oncar_speed=60.0)
        detector.update("s2", timing_speed=60.0, oncar_speed=60.0)
        assert detector.get_cusum("s1") > detector.get_cusum("s2")


class TestVerdictEngine:
    """Verdict engine integration tests."""

    def test_clear_verdict(self):
        unc = UncertaintyEngine(measurement_uncertainty=1.5, parameter_uncertainty=0.5, k=2.0)
        integrity = CUSUMDetector(threshold=5.0, drift=0.5)
        engine = VerdictEngine(uncertainty_engine=unc, integrity_detector=integrity)

        result = _make_rule_result(measured=48.0)
        event = _make_event(speed=48.0)
        verdict = engine.decide(result, event)
        assert verdict.verdict == VerdictValue.CLEAR

    def test_breach_verdict(self):
        unc = UncertaintyEngine(measurement_uncertainty=1.5, parameter_uncertainty=0.5, k=2.0)
        integrity = CUSUMDetector(threshold=5.0, drift=0.5)
        engine = VerdictEngine(uncertainty_engine=unc, integrity_detector=integrity)

        result = _make_rule_result(measured=67.0)
        event = _make_event(speed=67.0)
        verdict = engine.decide(result, event)
        assert verdict.verdict == VerdictValue.BREACH

    def test_defer_near_threshold(self):
        unc = UncertaintyEngine(measurement_uncertainty=1.5, parameter_uncertainty=0.5, k=2.0)
        integrity = CUSUMDetector(threshold=5.0, drift=0.5)
        engine = VerdictEngine(uncertainty_engine=unc, integrity_detector=integrity)

        result = _make_rule_result(measured=59.5)
        event = _make_event(speed=59.5)
        verdict = engine.decide(result, event)
        assert verdict.verdict == VerdictValue.DEFER

    def test_breach_downgraded_to_defer_on_integrity_alert(self):
        """If integrity is compromised, BREACH → DEFER."""
        unc = UncertaintyEngine(measurement_uncertainty=1.5, parameter_uncertainty=0.5, k=2.0)
        integrity = CUSUMDetector(threshold=3.0, drift=0.0)

        # Trigger an integrity alert on pit_lane sector
        for _ in range(10):
            integrity.update("pit_lane", timing_speed=65.0, oncar_speed=60.0)

        engine = VerdictEngine(uncertainty_engine=unc, integrity_detector=integrity)

        result = _make_rule_result(measured=67.0)
        event = _make_event(speed=67.0, zone="pit_lane")
        verdict = engine.decide(result, event)
        # Should be downgraded due to integrity concern
        assert verdict.verdict == VerdictValue.DEFER

    def test_verdict_contains_required_fields(self):
        unc = UncertaintyEngine(measurement_uncertainty=1.5, parameter_uncertainty=0.5, k=2.0)
        integrity = CUSUMDetector(threshold=5.0, drift=0.5)
        engine = VerdictEngine(uncertainty_engine=unc, integrity_detector=integrity)

        result = _make_rule_result(measured=55.0)
        event = _make_event(speed=55.0)
        verdict = engine.decide(result, event)

        assert verdict.verdict_id is not None
        assert verdict.rule_id == "pit_lane_speed"
        assert verdict.session_key == 9999
        assert verdict.driver_number == 1
        assert verdict.uncertainty > 0
        assert verdict.parameter_version == "v1.0"
