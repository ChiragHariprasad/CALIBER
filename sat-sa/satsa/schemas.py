"""
SAT-SA — Data Contract Schemas (§4.1)
=======================================
Pydantic models for every table in the data contract.
Tier 1 = mandatory; Tier 2 = enriches signals when available.
"""
from __future__ import annotations

import enum
from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, Field


# ── Tier 1 — the tool must run on this alone ─────────────────────────────

class Severity(str, enum.Enum):
    CRITICAL = "CRITICAL"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"
    INFO = "INFO"


class Disposition(str, enum.Enum):
    TRUE_POSITIVE = "TRUE_POSITIVE"
    FALSE_POSITIVE = "FALSE_POSITIVE"
    BENIGN = "BENIGN"
    ESCALATED = "ESCALATED"
    UNRESOLVED = "UNRESOLVED"
    AUTO_CLOSED = "AUTO_CLOSED"


class CaseStatus(str, enum.Enum):
    OPEN = "OPEN"
    IN_PROGRESS = "IN_PROGRESS"
    CLOSED = "CLOSED"
    ESCALATED = "ESCALATED"


class Alert(BaseModel):
    """A single SOC alert from a CSE submission."""
    alert_id: str
    cse_id: str
    asset_id: str
    category: str
    severity: Severity
    rule_id: str
    created_ts: datetime
    ack_ts: Optional[datetime] = None
    closed_ts: Optional[datetime] = None
    disposition: Optional[Disposition] = None
    analyst_id: Optional[str] = None


class Case(BaseModel):
    """An investigation case grouping related alerts."""
    case_id: str
    cse_id: str
    alert_ids: List[str] = Field(default_factory=list)
    opened_ts: datetime
    closed_ts: Optional[datetime] = None
    severity: Severity
    status: CaseStatus
    closure_note: Optional[str] = None
    reviewer_id: Optional[str] = None
    remediation_ref: Optional[str] = None
    root_cause: Optional[str] = None


class Escalation(BaseModel):
    """An escalation event on a case."""
    esc_id: str
    case_id: str
    ts: datetime
    to_level: str
    recipient_action_ts: Optional[datetime] = None


class Asset(BaseModel):
    """A monitored asset in the entity's inventory."""
    asset_id: str
    cse_id: str
    type: str   # "server", "workstation", "network_device", "scada", ...
    criticality: str   # "critical", "high", "medium", "low"
    internet_facing: bool = False
    expected_sources: List[str] = Field(default_factory=list)


class Manifest(BaseModel):
    """Submission manifest signed by the CSE."""
    cse_id: str
    period: str                     # e.g. "2026-Q3"
    files: List[dict]               # [{name, rows, sha256}, ...]
    merkle_root: str
    submitted_at: datetime
    signature: Optional[str] = None  # Ed25519 sig hex


# ── Tier 2 — richer signals when available ───────────────────────────────

class WorkflowEvent(BaseModel):
    """An investigation workflow action."""
    case_id: str
    ts: datetime
    actor: str
    action_type: str  # "artifact_collected", "note_added", "handoff", ...
    artifact_ref: Optional[str] = None


class TelemetrySummary(BaseModel):
    """Daily telemetry counts (never raw logs)."""
    asset_id: str
    source: str
    date: datetime
    event_count: int


class ReportedKPI(BaseModel):
    """A KPI the entity reported to the regulator."""
    cse_id: str
    period: str
    metric: str   # e.g. "mttr_critical", "escalation_rate", ...
    value: float


class PolicyRule(BaseModel):
    """An SLA or escalation policy from policy.yaml."""
    severity: Severity
    max_ack_minutes: Optional[int] = None
    max_close_minutes: Optional[int] = None
    must_escalate: bool = False
    shift_model: str = "24x7"  # "24x7" | "office_hours"


# ── Table Constants ───────────────────────────────────────────────────────

MANDATORY_TIER1_TABLES = ["alerts", "cases", "escalations", "assets"]
OPTIONAL_TIER2_TABLES = ["workflow_events", "telemetry_summary", "reported_kpis", "policy_rules"]

