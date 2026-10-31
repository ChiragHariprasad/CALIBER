"""CALIBRE Common Event Contracts per CAL-DOC-004."""
from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional


def utc_now() -> str:
    """Current UTC timestamp in RFC-3339 format with millisecond precision."""
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def stable_id(*parts: Any) -> str:
    """Deterministic 24-character hexadecimal identifier from component parts."""
    raw = "|".join(str(part) for part in parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


def generate_uuid() -> str:
    """Random RFC-4122 UUID v4."""
    return str(uuid.uuid4())


@dataclass(slots=True)
class RawSourceEnvelope:
    """Standard envelope for raw messages received from live, historical, or replay sources.
    
    Conforms to CAL-DOC-004 RawSourceEnvelope v1.
    """
    event_id: str
    correlation_id: str
    provider: str
    feed: str
    source_mode: str  # LIVE, HISTORICAL_REPLAY, BATCH
    run_id: str
    received_at_utc: str
    source_time: dict[str, Any]  # {value, semantics, unit}
    payload_format: str  # JSON, TEXT, BINARY_REFERENCE
    payload: Any
    schema_name: str = "calibre.raw_source_envelope"
    schema_version: str = "1.0"
    event_type: str = "raw_source_event"
    session: dict[str, Any] = field(default_factory=dict)
    scope: dict[str, Any] = field(default_factory=dict)
    payload_sha256: Optional[str] = None
    event_time_utc: Optional[str] = None
    session_time_s: Optional[float] = None

    def __post_init__(self) -> None:
        if not self.payload_sha256 and self.payload is not None:
            raw_bytes = json.dumps(self.payload, sort_keys=True, default=str).encode("utf-8")
            self.payload_sha256 = hashlib.sha256(raw_bytes).hexdigest()

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, separators=(",", ":"), default=str)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "RawSourceEnvelope":
        fields = {
            "schema_name": data.get("schema_name", "calibre.raw_source_envelope"),
            "schema_version": data.get("schema_version", "1.0"),
            "event_id": data["event_id"],
            "correlation_id": data.get("correlation_id", data["event_id"]),
            "event_type": data.get("event_type", "raw_source_event"),
            "provider": data["provider"],
            "feed": data["feed"],
            "source_mode": data.get("source_mode", "LIVE"),
            "run_id": data.get("run_id", "default_run"),
            "received_at_utc": data.get("received_at_utc", utc_now()),
            "source_time": data.get("source_time", {"value": None, "semantics": "UNKNOWN", "unit": "N/A"}),
            "session": data.get("session", {}),
            "scope": data.get("scope", {}),
            "payload_format": data.get("payload_format", "JSON"),
            "payload": data.get("payload"),
            "payload_sha256": data.get("payload_sha256"),
            "event_time_utc": data.get("event_time_utc"),
            "session_time_s": data.get("session_time_s"),
        }
        return cls(**fields)

    @classmethod
    def from_json(cls, json_str: str) -> "RawSourceEnvelope":
        return cls.from_dict(json.loads(json_str))


@dataclass(slots=True)
class Finding:
    """Specific observation, data anomaly, or verification finding emitted by an agent.
    
    Conforms to CAL-DOC-004 Finding v1.
    """
    finding_id: str
    code: str
    category: str  # MISSING_DATA, STALE_DATA, OUT_OF_RANGE, TIME_ALIGNMENT, DUPLICATE, INCONSISTENCY, PROVENANCE_LIMITATION, PROCESSING_ERROR
    severity: str  # INFO, LOW, MEDIUM, HIGH, CRITICAL
    message: str
    source_refs: list[str] = field(default_factory=list)
    observed_value: Any = None
    expected_range: Optional[dict[str, Any]] = None
    confidence: Optional[float] = 1.0
    requires_review: bool = False
    details: dict[str, Any] = field(default_factory=dict)
    limitations: list[str] = field(default_factory=list)
    created_at_utc: str = field(default_factory=utc_now)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class EvidenceEvent:
    """Agent / processor normalized observation with lineage, scope, and findings.
    
    Conforms to CAL-DOC-004 EvidenceEvent v1.
    """
    event_id: str
    correlation_id: str
    event_type: str  # telemetry_sample, race_control_message, weather_sample, lap_timing, source_health, processing_finding
    domain: str  # vehicle, incident, environment, timing, system
    producer: dict[str, Any]  # {processor_id, version, build}
    produced_at_utc: str
    source_refs: list[str]
    scope: dict[str, Any]
    time: dict[str, Any]  # {source_value, source_semantics, source_unit, session_time_s, event_time_utc, conversion_method}
    value_origin: str  # SOURCE_REPORTED, DERIVED, INTERPOLATED, ESTIMATED, PROCESSOR_ASSESSMENT, UNKNOWN
    evidence: dict[str, Any]
    schema_name: str = "calibre.evidence_event"
    schema_version: str = "1.0"
    session: dict[str, Any] = field(default_factory=dict)
    findings: list[dict[str, Any]] = field(default_factory=list)
    limitations: list[str] = field(default_factory=list)
    dependency_refs: list[str] = field(default_factory=list)
    provenance: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, separators=(",", ":"), default=str)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "EvidenceEvent":
        fields = {
            "schema_name": data.get("schema_name", "calibre.evidence_event"),
            "schema_version": data.get("schema_version", "1.0"),
            "event_id": data["event_id"],
            "correlation_id": data.get("correlation_id", data["event_id"]),
            "event_type": data.get("event_type", "processing_finding"),
            "domain": data.get("domain", "system"),
            "producer": data.get("producer", {"processor_id": "unknown", "version": "0.1.0"}),
            "produced_at_utc": data.get("produced_at_utc", utc_now()),
            "source_refs": data.get("source_refs", []),
            "session": data.get("session", {}),
            "scope": data.get("scope", {}),
            "time": data.get("time", {"source_value": None, "source_semantics": "UNKNOWN"}),
            "value_origin": data.get("value_origin", "SOURCE_REPORTED"),
            "evidence": data.get("evidence", {}),
            "findings": data.get("findings", []),
            "limitations": data.get("limitations", []),
            "dependency_refs": data.get("dependency_refs", []),
            "provenance": data.get("provenance", {}),
        }
        return cls(**fields)

    @classmethod
    def from_json(cls, json_str: str) -> "EvidenceEvent":
        return cls.from_dict(json.loads(json_str))


@dataclass(slots=True)
class FlinkOutputEnvelope:
    """Envelope emitted by Apache Flink after validation, enrichment, and routing.
    
    Maintains semantic separation:
    - pipeline health: HEALTHY / VALIDATED
    - evidence integrity: explicitly "NOT_EVALUATED" during scaffold phase
    """
    envelope_id: str
    correlation_id: str
    processing_stage: str  # e.g. "FLINK_INTEGRITY_VALIDATED"
    integrity_status: str  # explicitly "NOT_EVALUATED" per architectural principle
    processed_at_utc: str
    event: dict[str, Any]  # The validated EvidenceEvent
    routed_to: list[str]  # e.g. ["PERSISTENCE", "DASHBOARD", "ANALYTICS"]
    schema_name: str = "calibre.flink_output_envelope"
    schema_version: str = "1.0"
    flink_job_id: str = "calibre-flink-stream-job-v1"
    processing_latency_ms: Optional[float] = None
    validation_status: str = "VALID"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, separators=(",", ":"), default=str)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "FlinkOutputEnvelope":
        fields = {
            "schema_name": data.get("schema_name", "calibre.flink_output_envelope"),
            "schema_version": data.get("schema_version", "1.0"),
            "envelope_id": data["envelope_id"],
            "correlation_id": data.get("correlation_id", data["envelope_id"]),
            "processing_stage": data.get("processing_stage", "FLINK_INTEGRITY_VALIDATED"),
            "integrity_status": data.get("integrity_status", "NOT_EVALUATED"),
            "processed_at_utc": data.get("processed_at_utc", utc_now()),
            "event": data.get("event", {}),
            "routed_to": data.get("routed_to", []),
            "flink_job_id": data.get("flink_job_id", "calibre-flink-stream-job-v1"),
            "processing_latency_ms": data.get("processing_latency_ms"),
            "validation_status": data.get("validation_status", "VALID"),
        }
        return cls(**fields)

    @classmethod
    def from_json(cls, json_str: str) -> "FlinkOutputEnvelope":
        return cls.from_dict(json.loads(json_str))


@dataclass(slots=True)
class DeadLetterEnvelope:
    """Envelope for events that fail validation, parsing, or processing."""
    error_id: str
    correlation_id: str
    occurred_at_utc: str
    component: str  # ingestion, processor, flink, persistence, dashboard, analytics
    stage: str
    error_code: str
    error_message: str
    raw_payload: Any
    retryable: bool = False
    attempt_count: int = 1
    schema_name: str = "calibre.dead_letter_envelope"
    schema_version: str = "1.0"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, separators=(",", ":"), default=str)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "DeadLetterEnvelope":
        fields = {
            "schema_name": data.get("schema_name", "calibre.dead_letter_envelope"),
            "schema_version": data.get("schema_version", "1.0"),
            "error_id": data["error_id"],
            "correlation_id": data.get("correlation_id", "unknown"),
            "occurred_at_utc": data.get("occurred_at_utc", utc_now()),
            "component": data.get("component", "unknown"),
            "stage": data.get("stage", "unknown"),
            "error_code": data.get("error_code", "ERROR"),
            "error_message": data.get("error_message", ""),
            "raw_payload": data.get("raw_payload"),
            "retryable": data.get("retryable", False),
            "attempt_count": data.get("attempt_count", 1),
        }
        return cls(**fields)

    @classmethod
    def from_json(cls, json_str: str) -> "DeadLetterEnvelope":
        return cls.from_dict(json.loads(json_str))

