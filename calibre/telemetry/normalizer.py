"""
CALIBRE — Telemetry Normalizer (§23)
=====================================
Normalizes heterogeneous raw sensor streams into strongly-typed
TelemetryEvent objects with validated units and standard coordinates.
"""
from __future__ import annotations

import math
from typing import Dict, Any, Optional
from calibre.schemas.race_telemetry import TelemetryEvent, SessionType


class TelemetryNormalizer:
    """Normalizes raw input samples into canonical TelemetryEvents."""

    @staticmethod
    def normalize_sample(raw: Dict[str, Any], session: SessionType = SessionType.RACE) -> TelemetryEvent:
        # Standardize timestamp
        ts = float(raw.get("timestamp", raw.get("time", 0.0)))
        driver = str(raw.get("driver", "CAR-1")).upper()
        lap = int(raw.get("lap", 1))

        # Speed (ensure kph)
        speed = float(raw.get("speed_kph", raw.get("speed", 0.0)))
        if speed < 0.0:
            speed = 0.0

        # Throttle & Brake percentages (0 to 100)
        throttle = float(raw.get("throttle_pct", raw.get("throttle", 0.0)))
        if throttle <= 1.0 and throttle > 0.0 and "pct" not in raw:
            throttle *= 100.0  # normalize normalized 0.0-1.0 to percentage
        throttle = min(100.0, max(0.0, throttle))

        brake = float(raw.get("brake_pct", raw.get("brake", 0.0)))
        if brake <= 1.0 and brake > 0.0 and "pct" not in raw:
            brake *= 100.0
        brake = min(100.0, max(0.0, brake))

        # Gear (0 to 8)
        gear = int(raw.get("gear", 0))
        gear = min(8, max(0, gear))

        # DRS
        drs_raw = raw.get("drs", False)
        drs = bool(drs_raw in (1, "1", True, "true", "True", "OPEN"))

        return TelemetryEvent(
            timestamp=ts,
            session=session,
            driver=driver,
            lap=lap,
            speed_kph=round(speed, 2),
            throttle_pct=round(throttle, 1),
            brake_pct=round(brake, 1),
            gear=gear,
            rpm=int(raw.get("rpm", 0)),
            drs=drs,
            steering_angle_deg=float(raw.get("steering", 0.0)) if "steering" in raw else None,
            lat_accel_g=float(raw.get("lat_accel", 0.0)) if "lat_accel" in raw else None,
            lon_accel_g=float(raw.get("lon_accel", 0.0)) if "lon_accel" in raw else None,
            distance_m=float(raw.get("distance_m", raw.get("dist", 0.0))),
            x_m=float(raw.get("x", 0.0)) if "x" in raw else None,
            y_m=float(raw.get("y", 0.0)) if "y" in raw else None,
            corner=raw.get("corner"),
        )
