"""
CALIBRE — Meaningful Event Detector (§37)
=========================================
Detects tactical regime shifts to trigger agent strategic replanning
only when meaningful events occur, conserving computational budget.
"""
from __future__ import annotations

from typing import List, Optional, Dict, Any
from calibre.schemas.race_telemetry import RaceState, TrackStatus


class MeaningfulEventDetector:
    """Monitors incoming race state and fires tactical triggers."""

    def __init__(self):
        self.last_track_status: TrackStatus = TrackStatus.GREEN
        self.last_gap_ahead: Optional[float] = None
        self.last_rain_prob: float = 0.0
        self.last_cliff_flag: bool = False

    def check_events(self, state: RaceState) -> List[Dict[str, Any]]:
        triggers: List[Dict[str, Any]] = []

        # 1. Safety Car / VSC status changes
        if state.track_status != self.last_track_status:
            triggers.append({
                "type": "TRACK_STATUS_CHANGE",
                "old": self.last_track_status.value,
                "new": state.track_status.value,
                "impact": "Pit loss delta: -10s under neutralised race",
                "urgency": "CRITICAL" if state.track_status in (TrackStatus.SAFETY_CAR, TrackStatus.VIRTUAL_SAFETY_CAR) else "HIGH",
            })
            self.last_track_status = state.track_status

        # 2. Tyre cliff approaching trigger
        if state.tyre.tyre_cliff_probability >= 0.50 and not self.last_cliff_flag:
            triggers.append({
                "type": "TYRE_CLIFF_IMMINENT",
                "compound": state.tyre.compound.value,
                "age_laps": state.tyre.age_laps,
                "cliff_probability": state.tyre.tyre_cliff_probability,
                "impact": "Pace loss will exceed 1.2s/lap within 2 laps",
                "urgency": "HIGH",
            })
            self.last_cliff_flag = True

        # 3. Undercut threat emergence (gap ahead or behind <= 1.8s)
        if state.gap_ahead_s is not None and state.gap_ahead_s <= 1.8:
            if self.last_gap_ahead is None or self.last_gap_ahead > 1.8:
                triggers.append({
                    "type": "UNDERCUT_WINDOW_OPEN",
                    "driver_ahead": state.driver_ahead,
                    "gap_ahead_s": state.gap_ahead_s,
                    "impact": "In-lap pace advantage can yield track position gain",
                    "urgency": "HIGH",
                })
        self.last_gap_ahead = state.gap_ahead_s

        # 4. Weather crossover approaching
        if state.weather.rain_probability_pct >= 60.0 and self.last_rain_prob < 60.0:
            triggers.append({
                "type": "WEATHER_CROSSOVER_IMMINENT",
                "rain_prob": state.weather.rain_probability_pct,
                "impact": "Intermediate tyre crossover anticipated within 4 laps",
                "urgency": "HIGH",
            })
        self.last_rain_prob = state.weather.rain_probability_pct

        return triggers
