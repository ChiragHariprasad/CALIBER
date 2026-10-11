"""
CALIBRE — Canonical Event Envelope Schemas
============================================
Versioned, provenance-tracked event contracts used across all operating
modes. Every event—live, replayed or simulated—uses this envelope.

Design principles:
- Unique event ID for deduplication.
- Source and ingestion timestamps kept separate.
- Operating mode and data-quality indicators travel with the event.
- Observed vs derived vs estimated vs unavailable fields are distinguished.
- Raw payload preserved for investigation.
"""
from __future__ import annotations

import enum
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


# ── Enums ──────────────────────────────────────────────────────────────

class EventType(str, enum.Enum):
    """Top-level event categories."""
    TELEMETRY = "TELEMETRY"
    LAP_COMPLETED = "LAP_COMPLETED"
    POSITION_UPDATE = "POSITION_UPDATE"
    STINT_UPDATE = "STINT_UPDATE"
    RACE_CONTROL = "RACE_CONTROL"
    WEATHER = "WEATHER"
    INTERVAL = "INTERVAL"
    PIT_STOP = "PIT_STOP"
    PENALTY = "PENALTY"
    DRIVER_INFO = "DRIVER_INFO"
    SESSION_STATUS = "SESSION_STATUS"
    SENSOR_INTEGRITY = "SENSOR_INTEGRITY"
    PERFORMANCE_REFERENCE = "PERFORMANCE_REFERENCE"
    PERFORMANCE_DIAGNOSIS = "PERFORMANCE_DIAGNOSIS"
    STRATEGY_PREDICTION = "STRATEGY_PREDICTION"
    AGENT_DECISION = "AGENT_DECISION"
    VERDICT = "VERDICT"
    ALERT = "ALERT"
    RECOMMENDATION = "RECOMMENDATION"
    SYSTEM = "SYSTEM"


class DataOrigin(str, enum.Enum):
    """How the data value was obtained."""
    OBSERVED = "OBSERVED"       # Direct sensor/API measurement
    DERIVED = "DERIVED"         # Calculated from observed values
    ESTIMATED = "ESTIMATED"     # Model prediction with uncertainty
    UNAVAILABLE = "UNAVAILABLE" # Field not provided by source
    SYNTHETIC = "SYNTHETIC"     # Injected by simulation/scenario


class ValidationState(str, enum.Enum):
    """Result of schema and plausibility validation."""
    VALID = "VALID"
    WARNING = "WARNING"          # Passed validation with concerns
    QUARANTINED = "QUARANTINED"  # Failed validation, held for review
    REJECTED = "REJECTED"       # Failed validation, not processed
    DUPLICATE = "DUPLICATE"     # Duplicate event detected


class DataQuality(str, enum.Enum):
    """Overall quality assessment of the event data."""
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"
    UNRELIABLE = "UNRELIABLE"
    UNKNOWN = "UNKNOWN"


# ── Event Envelope ─────────────────────────────────────────────────────

class CalibreEvent(BaseModel):
    """
    Canonical event envelope. Every piece of data flowing through CALIBRE
    is wrapped in this contract—regardless of operating mode.
    """
    # Identity
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    event_type: EventType
    schema_version: str = "1.0.0"

    # Context
    session_key: Optional[int] = None
    meeting_key: Optional[int] = None
    driver: Optional[str] = None
    driver_number: Optional[str] = None

    # Timestamps (all epoch seconds)
    source_timestamp: float = Field(
        ..., description="Original timestamp from the data source"
    )
    ingestion_timestamp: float = Field(
        default_factory=time.time, description="When CALIBRE ingested this event"
    )
    replay_timestamp: Optional[float] = Field(
        None, description="Simulated clock time during replay (if applicable)"
    )

    # Source provenance
    source_id: str = Field(
        ..., description="Identifier of the data source (openf1, jolpica, mock, replay, etc.)"
    )
    operating_mode: str = Field(
        ..., description="LIVE, HISTORICAL_REPLAY or SIMULATION"
    )

    # Validation and quality
    validation_state: ValidationState = ValidationState.VALID
    data_quality: DataQuality = DataQuality.UNKNOWN
    quality_score: float = Field(
        1.0, ge=0.0, le=1.0,
        description="Numeric quality score (1.0 = fully trusted)"
    )
    validation_errors: List[str] = Field(default_factory=list)

    # Unit metadata
    units: Dict[str, str] = Field(
        default_factory=dict,
        description="Map of field name to unit (e.g. {'speed_kph': 'km/h'})"
    )

    # Payload
    data: Dict[str, Any] = Field(
        default_factory=dict,
        description="Normalised event payload"
    )
    raw_payload: Optional[Dict[str, Any]] = Field(
        None, description="Original raw payload from the source"
    )

    # Data origin annotations
    field_origins: Dict[str, str] = Field(
        default_factory=dict,
        description="Per-field DataOrigin: {'speed_kph': 'OBSERVED', 'accel_g': 'DERIVED'}"
    )

    # Deduplication
    dedup_key: Optional[str] = Field(
        None,
        description="Stable key for deduplication (source+session+driver+event_type+source_ts)"
    )

    def compute_dedup_key(self) -> str:
        """Generate a stable deduplication key."""
        et = self.event_type.value if hasattr(self.event_type, "value") else str(self.event_type)
        parts = [
            self.source_id,
            str(self.session_key or ""),
            self.driver or "",
            et,
            f"{self.source_timestamp:.3f}",
        ]
        key = "|".join(parts)
        self.dedup_key = key
        return key

    class Config:
        use_enum_values = True


# ── Sensor Integrity Finding ──────────────────────────────────────────

class SensorIntegrityFinding(BaseModel):
    """Structured detection from the sensor integrity subsystem."""
    finding_id: str = Field(default_factory=lambda: f"SIF-{uuid.uuid4().hex[:8]}")
    signal_name: str = Field(..., description="Sensor or signal involved (e.g. 'speed_kph')")
    driver: str
    session_key: Optional[int] = None

    # Values
    observed_value: Optional[float] = None
    expected_value: Optional[float] = None
    reference_value: Optional[float] = None

    # Detection
    detection_method: str = Field(..., description="spike_detector, cusum_drift, stuck_sensor, etc.")
    rule_id: str = Field(..., description="Identifier of the detection rule")
    severity: str = Field(..., description="INFO, WARNING, ERROR, CRITICAL")
    confidence: float = Field(..., ge=0.0, le=1.0)

    # Timing
    first_observed_ts: float = Field(default_factory=time.time)
    last_observed_ts: float = Field(default_factory=time.time)

    # Evidence
    preceding_samples: List[float] = Field(default_factory=list)
    explanation: str = ""
    evidence: Dict[str, Any] = Field(default_factory=dict)

    # Action
    recommended_action: str = "MONITOR"  # MONITOR, QUARANTINE, REJECT, DEFER
    downstream_usable: bool = True  # Whether downstream agents may use the signal


# ── Performance Reference ─────────────────────────────────────────────

class PerformanceReference(BaseModel):
    """Reference performance estimate for a track segment or sector."""
    segment: str  # "S1", "S2", "S3", "T7", "LAP"
    driver: str
    session_key: Optional[int] = None

    # Reference values
    best_observed_time_s: Optional[float] = None
    estimated_reference_time_s: Optional[float] = None
    predicted_achievable_time_s: Optional[float] = None

    # Uncertainty
    reference_uncertainty_s: float = 0.0
    data_coverage: float = Field(1.0, ge=0.0, le=1.0, description="Fraction of segment with data")
    confidence: float = Field(0.5, ge=0.0, le=1.0)

    # Context
    compound: Optional[str] = None
    tyre_age_laps: Optional[int] = None
    track_temp_c: Optional[float] = None
    eligible_laps_used: int = 0
    excluded_laps: int = 0
    source_origin: str = "ESTIMATED"  # OBSERVED or ESTIMATED

    class Config:
        use_enum_values = True


# ── Performance Diagnosis ──────────────────────────────────────────────

class PerformanceDiagnosis(BaseModel):
    """Diagnosis of performance gap at a track segment."""
    diagnosis_id: str = Field(default_factory=lambda: f"DIAG-{uuid.uuid4().hex[:8]}")
    segment: str  # "S1", "S2", "S3", "T7", "LAP"
    driver: str
    lap_number: int

    # Gap
    actual_time_s: float
    reference_time_s: float
    delta_s: float  # positive = slower than reference
    is_gain: bool = False  # True if faster than reference

    # Evidence
    supporting_evidence: Dict[str, Any] = Field(default_factory=dict)
    probable_cause: str = "UNKNOWN"
    alternative_causes: List[str] = Field(default_factory=list)
    confidence: float = Field(0.5, ge=0.0, le=1.0)
    data_quality: str = "UNKNOWN"

    # Recommendation
    recommended_correction: str = ""
    expected_effect_s: Optional[float] = None
    effect_uncertainty_s: Optional[float] = None

    # Context
    compound: Optional[str] = None
    tyre_age_laps: Optional[int] = None
    track_status: str = "GREEN"
    is_traffic_affected: bool = False
    is_weather_affected: bool = False


# ── Recommendation ─────────────────────────────────────────────────────

class RecommendationState(str, enum.Enum):
    PROPOSED = "PROPOSED"
    UPDATED = "UPDATED"
    EXPIRED = "EXPIRED"
    WITHDRAWN = "WITHDRAWN"


class RecommendationCategory(str, enum.Enum):
    PERFORMANCE = "PERFORMANCE"
    TYRE_STRATEGY = "TYRE_STRATEGY"
    PIT_STOP = "PIT_STOP"
    TRAFFIC = "TRAFFIC"
    WEATHER = "WEATHER"
    RACE_CONTROL = "RACE_CONTROL"
    SENSOR_ALERT = "SENSOR_ALERT"
    PENALTY = "PENALTY"


class DriverRecommendation(BaseModel):
    """A prioritised, lifecycle-tracked recommendation."""
    recommendation_id: str = Field(default_factory=lambda: f"REC-{uuid.uuid4().hex[:8]}")
    category: RecommendationCategory
    state: RecommendationState = RecommendationState.PROPOSED
    severity: str = "INFO"  # INFO, WARNING, URGENT, CRITICAL
    priority: int = Field(50, ge=0, le=100, description="0=lowest, 100=highest")

    # Target
    driver: str
    session_key: Optional[int] = None
    lap_number: Optional[int] = None
    timestamp: float = Field(default_factory=time.time)

    # Content
    message: str
    short_message: str = ""  # Radio-style brief
    source_evidence: Dict[str, Any] = Field(default_factory=dict)
    confidence: float = Field(0.5, ge=0.0, le=1.0)
    recommended_action: str = ""

    # Lifecycle
    expiry_ts: Optional[float] = None
    reevaluation_condition: str = ""
    supersedes: Optional[str] = None  # ID of recommendation this replaces
