"""
CALIBRE — Stateful Tyre Degradation Model (§28)
==============================================
Calculates lap time pace deltas, compound degradation curves,
thermal degradation sensitivity, and tyre cliff arrival probabilities.
"""
from __future__ import annotations

import math
from typing import Dict, List, Tuple
from calibre.schemas.race_telemetry import TyreCompound, TyreState


class TyreDegradationModel:
    """Stateful non-linear tyre wear and degradation estimator."""

    # Base degradation constants per lap (s / lap)
    BASE_DEGRADATION = {
        TyreCompound.SOFT: 0.135,
        TyreCompound.MEDIUM: 0.075,
        TyreCompound.HARD: 0.045,
        TyreCompound.INTERMEDIATE: 0.110,
        TyreCompound.WET: 0.160,
    }

    # Typical usable life (laps) before non-linear thermal cliff
    CLIFF_HORIZON_LAPS = {
        TyreCompound.SOFT: 16,
        TyreCompound.MEDIUM: 26,
        TyreCompound.HARD: 36,
        TyreCompound.INTERMEDIATE: 22,
        TyreCompound.WET: 28,
    }

    # Initial lap time pace offset relative to Medium (seconds)
    COMPOUND_PACE_DELTA = {
        TyreCompound.SOFT: -0.65,    # Soft is 0.65s faster when fresh
        TyreCompound.MEDIUM: 0.00,   # Baseline
        TyreCompound.HARD: +0.70,    # Hard is 0.70s slower when fresh
        TyreCompound.INTERMEDIATE: +4.50,
        TyreCompound.WET: +8.50,
    }

    def estimate_state(
        self,
        compound: TyreCompound,
        age_laps: int,
        track_temp_c: float = 35.0,
    ) -> TyreState:
        base_rate = self.BASE_DEGRADATION.get(compound, 0.08)
        cliff_horizon = self.CLIFF_HORIZON_LAPS.get(compound, 25)

        # Thermal adjustment: track temps > 40°C accelerate degradation
        temp_factor = 1.0 + max(0.0, (track_temp_c - 35.0) * 0.015)

        # Quadratic degradation component as tyre reaches cliff
        wear_fraction = age_laps / float(cliff_horizon)
        non_linear_factor = 1.0 + (0.5 * max(0.0, wear_fraction - 0.7) ** 2)

        deg_rate = base_rate * temp_factor * non_linear_factor
        pace_delta = (self.COMPOUND_PACE_DELTA.get(compound, 0.0) +
                      (deg_rate * age_laps))

        # Cliff probability calculation
        if age_laps < (cliff_horizon - 3):
            cliff_prob = 0.02
        else:
            cliff_prob = min(0.99, 0.10 + 0.30 * (age_laps - (cliff_horizon - 3)))

        usable_remaining = max(0, int(cliff_horizon - age_laps))

        return TyreState(
            compound=compound,
            age_laps=age_laps,
            stint_lap=age_laps,
            degradation_rate_s_per_lap=round(deg_rate, 4),
            current_pace_delta_s=round(pace_delta, 3),
            tyre_cliff_probability=round(cliff_prob, 3),
            usable_laps_remaining=usable_remaining,
            confidence=0.92,
        )

    def predict_future_pace(
        self,
        compound: TyreCompound,
        current_age: int,
        future_laps: int,
        track_temp_c: float = 35.0,
    ) -> List[float]:
        """Predicts lap time penalty for each upcoming lap."""
        paces = []
        for i in range(future_laps):
            state = self.estimate_state(compound, current_age + i + 1, track_temp_c)
            # Add heavy penalty if beyond cliff
            extra_penalty = 1.8 * (state.tyre_cliff_probability ** 2)
            paces.append(state.current_pace_delta_s + extra_penalty)
        return paces
