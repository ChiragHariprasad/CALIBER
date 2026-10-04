"""
CALIBRE — Live Race State Estimator (§23)
=========================================
Maintains chronological ground-truth state of the car, competitor field,
tyre condition, and track conditions during live or replay sessions.
"""
from __future__ import annotations

from typing import Dict, List, Optional
from calibre.schemas.race_telemetry import (
    RaceState,
    TyreState,
    TyreCompound,
    TrackState,
    TrackStatus,
    WeatherState,
    OpponentState,
    LapData,
)


class LiveRaceStateEstimator:
    """State estimator maintaining running session and vehicle status."""

    def __init__(
        self,
        driver: str = "VER",
        total_laps: int = 57,
        starting_compound: TyreCompound = TyreCompound.MEDIUM,
    ):
        self.driver = driver
        self.total_laps = total_laps
        self.current_lap = 1
        self.session_time_s = 0.0
        self.position = 2
        self.gap_ahead_s: Optional[float] = 1.4
        self.gap_behind_s: float = 3.2
        self.driver_ahead: Optional[str] = "NOR"
        self.driver_behind: Optional[str] = "LEC"
        self.track_status = TrackStatus.GREEN
        self.pit_lane_loss_s = 21.5

        # Tyre Tracking
        self.current_compound = starting_compound
        self.tyre_age_laps = 1
        self.stint_number = 1
        self.lap_history: List[LapData] = []

        # Opponents
        self.opponents: Dict[str, OpponentState] = {
            "NOR": OpponentState(
                driver="NOR",
                position=1,
                gap_s=1.4,
                gap_trend_s_per_lap=-0.05,
                compound=TyreCompound.MEDIUM,
                tyre_age_laps=1,
                undercut_threat=True,
                overcut_opportunity=False,
            ),
            "LEC": OpponentState(
                driver="LEC",
                position=3,
                gap_s=3.2,
                gap_trend_s_per_lap=0.10,
                compound=TyreCompound.HARD,
                tyre_age_laps=1,
                undercut_threat=False,
                overcut_opportunity=False,
            ),
        }

        # Track and Weather
        self.track_temp_c = 36.0
        self.air_temp_c = 25.0
        self.grip_index = 0.85
        self.rain_probability_pct = 0.0

    def update_lap(self, lap_data: LapData):
        """Called when a lap completes."""
        self.current_lap = lap_data.lap_number + 1
        self.tyre_age_laps += 1
        self.lap_history.append(lap_data)
        self.session_time_s += lap_data.lap_time_s

        # Update opponent tyre ages
        for opp in self.opponents.values():
            opp.tyre_age_laps += 1

    def record_pit_stop(self, new_compound: TyreCompound):
        """Register a pit stop and compound change."""
        self.current_compound = new_compound
        self.tyre_age_laps = 0
        self.stint_number += 1

    def set_track_status(self, status: TrackStatus):
        self.track_status = status
        # Under SC or VSC, pit loss is drastically reduced
        if status in (TrackStatus.SAFETY_CAR, TrackStatus.VIRTUAL_SAFETY_CAR):
            self.pit_lane_loss_s = 11.5
        else:
            self.pit_lane_loss_s = 21.5

    def get_race_state(self) -> RaceState:
        """Produces a snapshot of current race state."""
        # Calculate dynamic degradation rate based on compound & age
        base_deg = 0.05 if self.current_compound == TyreCompound.HARD else 0.08 if self.current_compound == TyreCompound.MEDIUM else 0.14
        degradation_rate = base_deg * (1.0 + 0.02 * self.tyre_age_laps)
        current_pace_delta = degradation_rate * self.tyre_age_laps

        # Usable laps remaining before severe cliff
        max_laps = 35 if self.current_compound == TyreCompound.HARD else 26 if self.current_compound == TyreCompound.MEDIUM else 16
        usable_remaining = max(0, max_laps - self.tyre_age_laps)
        cliff_prob = min(1.0, max(0.0, (self.tyre_age_laps - (max_laps - 4)) / 5.0)) if self.tyre_age_laps > (max_laps - 4) else 0.02

        tyre_state = TyreState(
            compound=self.current_compound,
            age_laps=self.tyre_age_laps,
            stint_lap=self.tyre_age_laps,
            degradation_rate_s_per_lap=round(degradation_rate, 4),
            current_pace_delta_s=round(current_pace_delta, 3),
            tyre_cliff_probability=round(cliff_prob, 3),
            usable_laps_remaining=usable_remaining,
            confidence=0.92,
        )

        track_state = TrackState(
            grip_index=self.grip_index,
            grip_trend="increasing" if self.current_lap < 30 else "stable",
            track_temp_c=self.track_temp_c,
            air_temp_c=self.air_temp_c,
            status=self.track_status,
            evolution_delta_s=round(-0.015 * min(self.current_lap, 40), 3),
        )

        weather_state = WeatherState(
            air_temp_c=self.air_temp_c,
            track_temp_c=self.track_temp_c,
            rain_probability_pct=self.rain_probability_pct,
            precipitation_intensity=0.0,
            wind_speed_kph=12.0,
            crossover_approaching=self.rain_probability_pct > 60.0,
            intermediate_window=self.rain_probability_pct > 75.0,
        )

        return RaceState(
            session_time_s=round(self.session_time_s, 2),
            lap=self.current_lap,
            total_laps=self.total_laps,
            driver=self.driver,
            position=self.position,
            gap_ahead_s=self.gap_ahead_s,
            leader_gap_s=0.0 if self.position == 1 else (self.gap_ahead_s or 1.4),
            gap_behind_s=self.gap_behind_s,
            driver_ahead=self.driver_ahead,
            driver_behind=self.driver_behind,
            track_status=self.track_status,
            pit_lane_loss_s=self.pit_lane_loss_s,
            tyre=tyre_state,
            track=track_state,
            weather=weather_state,
            opponents=list(self.opponents.values()),
        )
