from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any
import hashlib
import json


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def stable_id(*parts: Any) -> str:
    raw = "|".join(str(part) for part in parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


@dataclass(slots=True)
class SourceRecord:
    """One source-level record with explicit provenance and time semantics."""

    record_id: str
    provider: str
    feed: str
    payload: dict[str, Any]
    context: dict[str, Any] = field(default_factory=dict)
    source_timestamp: Any = None
    session_time_s: float | None = None
    time_semantics: str = "unknown"
    driver_code: str | None = None
    driver_number: str | None = None
    lap_number: int | None = None
    source_path: str | None = None
    received_at: str = field(default_factory=utc_now)
    provenance: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class Finding:
    finding_id: str
    agent_id: str
    code: str
    severity: str
    message: str
    scope: dict[str, Any] = field(default_factory=dict)
    evidence_record_ids: list[str] = field(default_factory=list)
    details: dict[str, Any] = field(default_factory=dict)
    limitations: list[str] = field(default_factory=list)
    created_at: str = field(default_factory=utc_now)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class EvidenceEvent:
    """Agent output contract. This is suitable for later Kafka serialization."""

    event_id: str
    schema_version: str
    agent_id: str
    agent_version: str
    event_type: str
    provider: str
    feed: str
    source_record_id: str
    source_timestamp: Any = None
    session_time_s: float | None = None
    time_semantics: str = "unknown"
    scope: dict[str, Any] = field(default_factory=dict)
    evidence: dict[str, Any] = field(default_factory=dict)
    findings: list[dict[str, Any]] = field(default_factory=list)
    provenance: dict[str, Any] = field(default_factory=dict)
    emitted_at: str = field(default_factory=utc_now)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class AgentOutput:
    events: list[EvidenceEvent] = field(default_factory=list)
    findings: list[Finding] = field(default_factory=list)

    def extend(self, other: "AgentOutput") -> None:
        self.events.extend(other.events)
        self.findings.extend(other.findings)

    def to_dict(self) -> dict[str, Any]:
        return {
            "events": [event.to_dict() for event in self.events],
            "findings": [finding.to_dict() for finding in self.findings],
        }


def json_line(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=str)
