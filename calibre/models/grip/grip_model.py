"""
CALIBRE — Grip Model (§29)
==========================
Estimates dynamic track grip index, rubbering-in evolution, and regime changes.
"""
from __future__ import annotations

from typing import Dict, Any
from calibre.schemas.race_telemetry import TrackState, TrackStatus


class GripModel:
    """Estimates latent grip and surface evolution."""

    def __init__(self, base_grip: float = 0.85):
        self.base_grip = base_grip

    def estimate_grip(
        self,
        lap: int,
        total_laps: int,
        track_temp_c: float = 35.0,
        is_wet: bool = False,
    ) -> TrackState:
        if is_wet:
            return TrackState(
                grip_index=0.45,
                grip_trend="decreasing",
                track_temp_c=track_temp_c,
                status=TrackStatus.GREEN,
                evolution_delta_s=+4.5,
            )

        # Dry track rubbers in across first 60% of race
        rubber_factor = min(0.08, (lap / float(total_laps)) * 0.10)
        # Optimal track temp around 32-38°C
        temp_loss = abs(track_temp_c - 35.0) * 0.002
        current_grip = min(0.98, max(0.60, self.base_grip + rubber_factor - temp_loss))

        trend = "increasing" if lap < int(total_laps * 0.5) else "stable"
        evolution_delta = round(-0.02 * min(lap, 35), 3)

        return TrackState(
            grip_index=round(current_grip, 3),
            grip_trend=trend,
            track_temp_c=track_temp_c,
            status=TrackStatus.GREEN,
            evolution_delta_s=evolution_delta,
        )
