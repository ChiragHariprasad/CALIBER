"""
CALIBRE — Driver Error & Corner Loss Model (§27, §40)
=====================================================
Analyzes driver performance against the circuit reference profile.
Classifies time losses and prevents false blame when losses are explained
by traffic, yellow flags, or tyre degradation.
"""
from __future__ import annotations

from typing import Dict, Any, Optional
from calibre.schemas.race_telemetry import DriverInsight, TrackStatus
from calibre.track.circuit_model import CircuitModel, CornerGeometry


class DriverErrorModel:
    """Corner-by-corner time loss and driver attribution engine."""

    def __init__(self, circuit: Optional[CircuitModel] = None):
        self.circuit = circuit or CircuitModel()

    def analyze_corner(
        self,
        corner_id: str,
        actual_telemetry: Dict[str, Any],
        track_status: TrackStatus = TrackStatus.GREEN,
        in_traffic: bool = False,
        tyre_cliff_active: bool = False,
    ) -> Optional[DriverInsight]:
        ref = self.circuit.get_corner(corner_id)
        if not ref:
            return None

        # Extract actuals
        actual_braking_m = actual_telemetry.get("braking_point_m", ref.ref_braking_point_m)
        actual_apex_kph = actual_telemetry.get("apex_speed_kph", ref.ref_apex_speed_kph)
        actual_throttle_m = actual_telemetry.get("throttle_pickup_m", ref.ref_throttle_pickup_m)

        # 1. Check for external explanations first (§27 non-driver attribution)
        if track_status in (TrackStatus.YELLOW, TrackStatus.SAFETY_CAR, TrackStatus.VIRTUAL_SAFETY_CAR):
            return DriverInsight(
                type="ENVIRONMENTAL_TIME_LOSS",
                corner=corner_id,
                time_loss_s=round(actual_telemetry.get("time_loss_s", 0.4), 2),
                cause="YELLOW_FLAG",
                confidence=0.98,
                evidence={"track_status": track_status.value},
                recommendation="Maintain delta time; loss required by race control regulations."
            )

        if in_traffic:
            return DriverInsight(
                type="TACTICAL_TIME_LOSS",
                corner=corner_id,
                time_loss_s=round(actual_telemetry.get("time_loss_s", 0.35), 2),
                cause="TRAFFIC",
                confidence=0.92,
                evidence={"traffic": True, "dirty_air_loss_pct": 25.0},
                recommendation="Managing tyre temperature in dirty air; avoid forcing offline apex."
            )

        if tyre_cliff_active:
            return DriverInsight(
                type="VEHICLE_TIME_LOSS",
                corner=corner_id,
                time_loss_s=round(actual_telemetry.get("time_loss_s", 0.30), 2),
                cause="TYRE_DEGRADATION",
                confidence=0.94,
                evidence={"tyre_cliff": True},
                recommendation="Tyre surface thermal degradation active; prepare for immediate pit stop."
            )

        # 2. Driver-specific loss attribution
        brake_delta_m = actual_braking_m - ref.ref_braking_point_m
        apex_delta_kph = ref.ref_apex_speed_kph - actual_apex_kph
        throttle_delta_m = actual_throttle_m - ref.ref_throttle_pickup_m

        # Early braking: braked more than 5m early
        if brake_delta_m < -5.0:
            time_loss = round(abs(brake_delta_m) * 0.025, 2)
            return DriverInsight(
                type="DRIVER_TIME_LOSS",
                corner=corner_id,
                time_loss_s=time_loss,
                cause="EARLY_BRAKING",
                confidence=0.91,
                evidence={
                    "reference_braking_point_m": ref.ref_braking_point_m,
                    "actual_braking_point_m": actual_braking_m,
                    "delta_m": round(brake_delta_m, 1)
                },
                recommendation=f"Move braking point approximately {abs(brake_delta_m):.1f}m later into {ref.name}."
            )

        # Late throttle pickup: delayed pickup by > 8m
        if throttle_delta_m > 8.0:
            time_loss = round(throttle_delta_m * 0.022, 2)
            return DriverInsight(
                type="DRIVER_TIME_LOSS",
                corner=corner_id,
                time_loss_s=time_loss,
                cause="LATE_THROTTLE",
                confidence=0.88,
                evidence={
                    "reference_throttle_pickup_m": ref.ref_throttle_pickup_m,
                    "actual_throttle_pickup_m": actual_throttle_m,
                    "delta_m": round(throttle_delta_m, 1)
                },
                recommendation=f"Earlier throttle commitment on exit of {ref.name} to maximize straight-line speed."
            )

        # Slow apex rotation
        if apex_delta_kph > 6.0:
            time_loss = round(apex_delta_kph * 0.030, 2)
            return DriverInsight(
                type="DRIVER_TIME_LOSS",
                corner=corner_id,
                time_loss_s=time_loss,
                cause="SLOW_ROTATION",
                confidence=0.85,
                evidence={
                    "reference_apex_speed_kph": ref.ref_apex_speed_kph,
                    "actual_apex_speed_kph": actual_apex_kph,
                    "delta_kph": round(apex_delta_kph, 1)
                },
                recommendation=f"Carry +{apex_delta_kph:.1f} kph more rolling speed through apex of {ref.name}."
            )

        return None
