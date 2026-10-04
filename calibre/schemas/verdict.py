"""
CALIBRE – Verdict Schema
=========================
The core output of CALIBRE's rule-evaluation pipeline.
"""

from __future__ import annotations

import enum
import uuid
from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, Field


class VerdictValue(str, enum.Enum):
    """Possible verdict outcomes."""

    BREACH = "BREACH"
    CLEAR = "CLEAR"
    DEFER = "DEFER"


class Verdict(BaseModel):
    """A single regulatory verdict produced by the Verdict Engine."""

    verdict_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: datetime
    session_key: int
    driver_number: int

    rule_id: str = Field(..., description="Identifier of the evaluated rule")
    measured_value: float
    threshold: float
    uncertainty: float = Field(
        ..., description="Combined uncertainty (root-sum-square)"
    )
    confidence_margin: float = Field(
        ...,
        description="Normalised margin = (threshold - measured) / combined_uncertainty",
    )
    verdict: VerdictValue

    source_event_ids: List[str] = Field(default_factory=list)
    parameter_version: str = Field(
        default="v1.0",
        description="Version tag of the rule parameters used",
    )

    # Bitemporal bookkeeping (populated by the storage layer)
    valid_from: Optional[datetime] = None
    valid_to: Optional[datetime] = None
    recorded_at: Optional[datetime] = None
    superseded_at: Optional[datetime] = None
