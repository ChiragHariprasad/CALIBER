"""
CALIBRE – Uncertainty Engine Tests
====================================
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("DATABASE_URL", "sqlite+aiosqlite:///test.db")
os.environ.setdefault("DATABASE_URL_SYNC", "sqlite:///test.db")

import math
from schemas.verdict import VerdictValue
from uncertainty.engine import UncertaintyEngine


class TestUncertaintyEngine:
    """Uncertainty engine classification tests."""

    def test_clear_large_margin(self):
        """Speed well below threshold → CLEAR."""
        engine = UncertaintyEngine(
            measurement_uncertainty=1.5,
            parameter_uncertainty=0.5,
            k=2.0,
        )
        result = engine.evaluate(measured_value=48.0, threshold=60.0)
        assert result.verdict == VerdictValue.CLEAR
        # margin = 12, combined ≈ 1.58, normalised ≈ 7.6 >> 2
        assert result.normalised_margin > 2.0

    def test_breach_large_margin(self):
        """Speed well above threshold → BREACH."""
        engine = UncertaintyEngine(
            measurement_uncertainty=1.5,
            parameter_uncertainty=0.5,
            k=2.0,
        )
        result = engine.evaluate(measured_value=67.0, threshold=60.0)
        assert result.verdict == VerdictValue.BREACH
        assert result.normalised_margin < -2.0

    def test_defer_near_threshold(self):
        """Speed near threshold within uncertainty band → DEFER."""
        engine = UncertaintyEngine(
            measurement_uncertainty=1.5,
            parameter_uncertainty=0.5,
            k=2.0,
        )
        # margin = 60 - 59.5 = 0.5; combined ≈ 1.58; normalised ≈ 0.32
        result = engine.evaluate(measured_value=59.5, threshold=60.0)
        assert result.verdict == VerdictValue.DEFER
        assert -2.0 < result.normalised_margin < 2.0

    def test_combined_uncertainty_calculation(self):
        """Verify root-sum-square formula."""
        engine = UncertaintyEngine(
            measurement_uncertainty=3.0,
            parameter_uncertainty=4.0,
            k=2.0,
        )
        result = engine.evaluate(measured_value=50.0, threshold=60.0)
        assert math.isclose(result.combined_uncertainty, 5.0, rel_tol=1e-9)

    def test_override_per_call(self):
        """Per-call uncertainty overrides should take effect."""
        engine = UncertaintyEngine(
            measurement_uncertainty=1.0,
            parameter_uncertainty=1.0,
            k=2.0,
        )
        result = engine.evaluate(
            measured_value=59.0, threshold=60.0,
            measurement_uncertainty=10.0,
        )
        # With σ_m=10, combined ≈ 10.05, margin=1, normalised ≈ 0.1 → DEFER
        assert result.verdict == VerdictValue.DEFER
