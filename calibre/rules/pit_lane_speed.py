"""
CALIBRE – Pit-Lane Speed Monitor
==================================
First concrete rule implementation: FIA pit-lane speed limit.

Typical limits:
  - 60 km/h during practice / qualifying / race
  - 80 km/h on some circuits during certain sessions

The limit is configurable via ``PIT_LANE_SPEED_LIMIT_KMH`` env var.
"""

from __future__ import annotations

from typing import Optional

from config.settings import settings
from rules.base import RuleMonitor, RuleResult
from schemas.telemetry import TelemetryEvent


class PitLaneSpeedMonitor(RuleMonitor):
    """Checks whether a car exceeds the pit-lane speed limit."""

    rule_id: str = "pit_lane_speed"

    def __init__(self, limit_kmh: Optional[float] = None) -> None:
        self.limit_kmh = limit_kmh or settings.pit_lane_speed_limit_kmh

    def evaluate(self, event: TelemetryEvent) -> Optional[RuleResult]:
        """
        Only applicable when the car is in the pit lane.

        Returns ``None`` for events outside the pit lane.
        """
        if event.zone not in ("pit_lane", "pit_entry", "pit_exit"):
            return None

        exceeded = event.speed > self.limit_kmh
        return RuleResult(
            rule_id=self.rule_id,
            measured_value=event.speed,
            threshold=self.limit_kmh,
            exceeded=exceeded,
            metadata={
                "driver": event.driver_number,
                "zone": event.zone,
            },
        )
