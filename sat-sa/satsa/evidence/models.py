"""
SAT-SA — Evidence Object (§6.3)
================================
The single structure that every finding, screen, report and ledger
entry is built from.  Every finding is proof-carrying.
"""
from __future__ import annotations

import enum
from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, Field


class FindingStatus(str, enum.Enum):
    REQUIRES_REVIEW = "REQUIRES_REVIEW"
    ACCEPTED = "ACCEPTED"
    DISMISSED = "DISMISSED"
    ESCALATED = "ESCALATED"


class SignalType(str, enum.Enum):
    EXECUTION_GAP = "Execution gap"
    NEGATIVE_SPACE = "Negative space"
    DATA_INTEGRITY = "Data integrity"
    ANOMALY = "Anomaly"


class Dimension(str, enum.Enum):
    THREAT_DETECTION = "Threat detection"
    INVESTIGATION = "Investigation"
    ESCALATION = "Escalation"
    INCIDENT_RESPONSE = "Incident response"
    SECURITY_OPERATIONS = "Security operations"
    GOVERNANCE = "Governance"
    OPERATIONAL_DISCIPLINE = "Operational discipline"
    CYBER_RESILIENCE = "Cyber resilience"
    SUBMISSION_INTEGRITY = "Submission integrity"


class BaselineType(str, enum.Enum):
    PEER = "peer"
    SELF_HISTORY = "self_history"
    POLICY = "policy"


class Finding(BaseModel):
    """
    A single evidence-backed supervisory finding.

    From any finding the examiner can drill down:
    finding → case → alerts → workflow events → escalation → raw row.
    """
    finding_id: str
    cse_id: str
    dimension: Dimension
    signal_type: SignalType
    signal_card_id: str          # e.g. "TD-01", "IN-01"
    baseline_type: BaselineType
    stratum: str                 # e.g. "severity=CRITICAL"

    metric: str                  # e.g. "investigation_depth_pct"
    observed: float
    expected: float
    p_value: Optional[float] = None
    q_value: Optional[float] = None     # FDR-adjusted

    severity: float = Field(ge=0.0, le=1.0)
    confidence: float = Field(ge=0.0, le=1.0)
    evidence_count: int
    data_completeness: float = Field(ge=0.0, le=1.0)

    rationale: str               # filled from a fixed template
    evidence_records: List[str] = Field(default_factory=list)  # IDs

    ruleset_version: str = "1.0.0"
    merkle_proof: List[str] = Field(default_factory=list)

    status: FindingStatus = FindingStatus.REQUIRES_REVIEW
    created_at: Optional[datetime] = None


class EntityScore(BaseModel):
    """Per-entity supervisory score."""
    cse_id: str
    dimension_scores: dict[str, float] = Field(default_factory=dict)  # dim → D_k
    sai: float = 0.0             # Supervisory Attention Index
    confidence: float = 0.0      # entity-level confidence
    rank: int = 0
    rank_stable: bool = True     # survives ±20% weight perturbation
    findings_count: int = 0
    assessable_signals: int = 0
    total_signals: int = 0


class ReviewItem(BaseModel):
    """A record selected for examiner review."""
    record_id: str
    record_type: str             # "alert", "case", "escalation"
    cse_id: str
    priority: float
    stream: str                  # "concern", "negspace", "anomaly", "normal", "random"
    why_selected: List[str] = Field(default_factory=list)
    linked_findings: List[str] = Field(default_factory=list)
