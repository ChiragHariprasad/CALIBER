"""
CALIBRE — Race Engineering Telemetry Schemas (§23)
===================================================
Defines strongly-typed, normalized data contracts for F1 telemetry,
session states, vehicle dynamics, tyre wear, opponent tracking,
Monte Carlo strategy evaluations, and agent recommendations.
"""
from __future__ import annotations

import enum
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any
from pydantic import BaseModel, Field


class SessionType(str, enum.Enum):
    FP1 = "FP1"
    FP2 = "FP2"
    FP3 = "FP3"
    QUALIFYING = "QUALIFYING"
    RACE = "RACE"


class TyreCompound(str, enum.Enum):
    SOFT = "SOFT"
    MEDIUM = "MEDIUM"
    HARD = "HARD"
    INTERMEDIATE = "INTERMEDIATE"
    WET = "WET"


class TrackStatus(str, enum.Enum):
    GREEN = "GREEN"
    YELLOW = "YELLOW"
    SAFETY_CAR = "SAFETY_CAR"
    VIRTUAL_SAFETY_CAR = "VIRTUAL_SAFETY_CAR"
    RED = "RED"


class TelemetryEvent(BaseModel):
    """Normalized high-frequency telemetry sample (§23)."""
    timestamp: float = Field(..., description="Epoch or session timestamp in seconds")
    session: SessionType = SessionType.RACE
    driver: str = Field(..., description="Driver code, e.g. VER, NOR, HAM, LEC")
    lap: int = Field(..., ge=1)
    event_type: str = "telemetry_sample"
    source: str = "normalized_telemetry"
    quality: float = Field(1.0, ge=0.0, le=1.0)

    # Physical channels with explicit units
    speed_kph: float = Field(..., ge=0.0)
    throttle_pct: float = Field(..., ge=0.0, le=100.0)
    brake_pct: float = Field(..., ge=0.0, le=100.0)
    gear: int = Field(..., ge=0, le=8)
    rpm: int = Field(0, ge=0)
    drs: bool = False
    steering_angle_deg: Optional[float] = None
    lat_accel_g: Optional[float] = None
    lon_accel_g: Optional[float] = None

    # Track position
    distance_m: float = 0.0
    x_m: Optional[float] = None
    y_m: Optional[float] = None
    z_m: Optional[float] = None
    corner: Optional[str] = None


class SectorData(BaseModel):
    sector_number: int = Field(..., ge=1, le=3)
    sector_time_s: float
    is_personal_best: bool = False
    is_session_best: bool = False


class LapData(BaseModel):
    lap_number: int
    driver: str
    lap_time_s: float
    s1_time_s: float
    s2_time_s: float
    s3_time_s: float
    compound: TyreCompound
    tyre_age_laps: int
    is_valid: bool = True
    is_pit_in_lap: bool = False
    is_pit_out_lap: bool = False


class Stint(BaseModel):
    stint_number: int
    compound: TyreCompound
    start_lap: int
    end_lap: Optional[int] = None
    laps_completed: int = 0
    mean_pace_s: float = 0.0
    degradation_slope_s_per_lap: float = 0.0


class TyreState(BaseModel):
    """Estimated state of tyres (§28)."""
    compound: TyreCompound
    age_laps: int
    stint_lap: int
    degradation_rate_s_per_lap: float
    current_pace_delta_s: float
    tyre_cliff_probability: float = Field(0.0, ge=0.0, le=1.0)
    usable_laps_remaining: int
    confidence: float = Field(0.9, ge=0.0, le=1.0)


class TrackState(BaseModel):
    """Track and grip status (§29)."""
    grip_index: float = Field(0.85, ge=0.0, le=1.0)
    grip_trend: str = "increasing"  # increasing | decreasing | stable
    track_temp_c: float = 35.0
    air_temp_c: float = 24.0
    status: TrackStatus = TrackStatus.GREEN
    evolution_delta_s: float = 0.0


class WeatherState(BaseModel):
    """Current and forecast weather (§32)."""
    air_temp_c: float = 24.0
    track_temp_c: float = 35.0
    rain_probability_pct: float = 0.0
    precipitation_intensity: float = 0.0
    wind_speed_kph: float = 10.0
    crossover_approaching: bool = False
    intermediate_window: bool = False


class OpponentState(BaseModel):
    """Competitor tracking (§31)."""
    driver: str
    position: int
    gap_s: float
    gap_trend_s_per_lap: float
    compound: TyreCompound
    tyre_age_laps: int
    estimated_pit_lap: Optional[int] = None
    undercut_threat: bool = False
    overcut_opportunity: bool = False
    time_to_catch_laps: Optional[float] = None


class DriverState(BaseModel):
    """Driver status and recent loss analysis (§27)."""
    driver: str
    consistency_score: float = 0.95
    tyre_management_score: float = 0.90
    last_lap_loss_s: float = 0.0
    primary_loss_cause: Optional[str] = None
    loss_corner: Optional[str] = None


class RaceState(BaseModel):
    """Full snapshot of the race at an instant."""
    session_time_s: float
    lap: int
    total_laps: int = 57
    driver: str = "VER"
    position: int = 1
    gap_ahead_s: Optional[float] = None
    leader_gap_s: float = 0.0
    gap_behind_s: float = 2.4
    driver_ahead: Optional[str] = None
    driver_behind: Optional[str] = "NOR"
    track_status: TrackStatus = TrackStatus.GREEN
    pit_lane_loss_s: float = 21.5
    tyre: TyreState
    track: TrackState
    weather: WeatherState
    opponents: List[OpponentState] = Field(default_factory=list)


class StrategyCandidate(BaseModel):
    """A simulated strategic option (§33, §34)."""
    strategy_id: str
    action: str  # "PIT_NOW", "PIT_NEXT_LAP", "STAY_OUT", "ONE_STOP", "TWO_STOP"
    pit_lap: Optional[int] = None
    target_compound: Optional[TyreCompound] = None
    expected_position: float
    p10_position: float
    p50_position: float
    p90_position: float
    podium_probability: float
    expected_points: float
    pit_loss_s: float
    traffic_risk: float
    tyre_cliff_risk: float
    confidence: float
    why: List[str] = Field(default_factory=list)


class StrategyDecision(BaseModel):
    """Final strategic decision and counterfactuals (§39)."""
    decision: str  # "PIT", "STAY"
    urgency: str   # "LOW", "MEDIUM", "HIGH", "CRITICAL"
    target_lap: int
    target_compound: TyreCompound
    window: Tuple[int, int]
    expected_position: float
    podium_probability: float
    expected_points: float
    confidence: float
    why: List[str]
    counterfactual: Dict[str, Any] = Field(default_factory=dict)


class DriverInsight(BaseModel):
    """Granular driver performance feedback (§40)."""
    type: str = "DRIVER_TIME_LOSS"
    corner: str
    time_loss_s: float
    cause: str
    confidence: float
    evidence: Dict[str, Any]
    recommendation: str
