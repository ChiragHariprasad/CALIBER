"""CALIBRE SIA v2.2 - Stream Evidence and Fact Models per CAL-DOC-007 §2.1, §2.2, §2.3, §2.8."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence


@dataclass(frozen=True, slots=True)
class RawSourceEnvelope:
    """Raw source envelope representing unmodified provider ingress."""
    provider: str
    feed: str
    source_record_key: Optional[str] = None
    session_id: Optional[str] = None
    scope: Dict[str, Any] = field(default_factory=dict)
    source_time: Dict[str, Any] = field(default_factory=dict)
    payload: Dict[str, Any] = field(default_factory=dict)
    payload_sha256: str = ""
    received_at_utc: Optional[str] = None
    raw_event_id: str = ""
    identity_stability: str = "REPLAY_STABLE"


@dataclass(frozen=True, slots=True)
class EvidenceAtom:
    """Normalised evidence atom emitted by processors or directly ingested."""
    evidence_atom_id: str
    role_id: str
    value: Any
    unit: str = "s"
    raw_event_ids: Sequence[str] = field(default_factory=tuple)
    processor_family_id: str = "generic_parser_family"
    transform_version: str = "1.0.0"
    scope_key: str = ""
    time: Dict[str, Any] = field(default_factory=dict)
    quality_grade: str = "HEALTHY"
    source_origin_set: Sequence[str] = field(default_factory=tuple)
    processing_origin_set: Sequence[str] = field(default_factory=tuple)
    supersedes_atom_id: Optional[str] = None
    reference_atom_ids: Sequence[str] = field(default_factory=tuple)
    origin_instance_refs: Sequence[str] = field(default_factory=tuple)
    transport_event_time_utc: str = ""
    transport_time_basis: str = "EVENT_UTC"


@dataclass(frozen=True, slots=True)
class ReferenceFact:
    """Durable reference fact used for time domain conversions or ground invariants."""
    fact_id: str
    fact_type: str
    session_id: Optional[str] = None
    driver_id: Optional[str] = None
    lap_number: Optional[int] = None
    value: Any = None
    time_class: str = "SESSION_ELAPSED"
    grade: str = "HEALTHY"
    origin_set: Sequence[str] = field(default_factory=tuple)
    validity_window: Optional[Dict[str, str]] = None
    rejection_reason: Optional[str] = None


@dataclass(frozen=True, slots=True)
class GuardFact:
    """Liveness guard fact emitted by independent supervisor."""
    fact_id: str
    origin_instance_id: str
    epoch: int = 1
    status: str = "RESUMED"  # STALLED | RESUMED
    observed_gap: float = 0.0
    detection_clock: str = ""
    source_origin_set: Sequence[str] = field(default_factory=tuple)


@dataclass(frozen=True, slots=True)
class RunClockTick:
    """Run-clock tick providing explicit durable time passage."""
    run_id: str
    clock_domain: str = "LIVE_UTC"  # LIVE_UTC | REPLAY_VIRTUAL
    clock_utc: str = ""
    seq: int = 0
    emitter_id: str = "supervisor"
