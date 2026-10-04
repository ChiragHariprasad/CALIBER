"""
CALIBRE — Slip-Risk Model (§30)
================================
Estimates non-linear wheel slip risk (0.0 to 1.0) derived from
throttle aggression, steering curvature, gear ratio, and tyre condition.
"""
from __future__ import annotations

import math
from typing import Dict, Any


class SlipRiskModel:
    """Estimates vehicle slip probability and traction margin."""

    def estimate_slip_risk(
        self,
        speed_kph: float,
        throttle_pct: float,
        gear: int,
        tyre_age_laps: int,
        is_wet: bool = False,
    ) -> float:
        # Base risk from low gear + high throttle torque surge
        gear_multiplier = {1: 1.8, 2: 1.5, 3: 1.2, 4: 1.0, 5: 0.8, 6: 0.6, 7: 0.4, 8: 0.2}.get(gear, 1.0)
        throttle_intensity = (throttle_pct / 100.0) ** 1.8
        torque_risk = throttle_intensity * gear_multiplier * 0.45

        # Tyre wear reduction of traction
        tyre_penalty = min(0.35, tyre_age_laps * 0.012)

        # Wet surface penalty
        wet_penalty = 0.40 if is_wet else 0.0

        total_risk = min(1.0, max(0.0, torque_risk + tyre_penalty + wet_penalty))
        return round(total_risk, 3)
