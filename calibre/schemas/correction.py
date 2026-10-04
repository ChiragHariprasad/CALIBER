"""
CALIBRE – Correction Schema
=============================
Models for the future correction / lineage-propagation engine.
"""

from __future__ import annotations

import enum
import uuid
from datetime import datetime, timezone
from typing import Optional

from pydantic import BaseModel, Field


class CorrectionType(str, enum.Enum):
    """Category of parameter correction."""

    RETRO = "RETRO"                 # Retroactive recalibration
    PROSPECTIVE = "PROSPECTIVE"     # Forward-looking parameter change
    WHAT_IF = "WHAT_IF"             # Hypothetical scenario analysis


class Correction(BaseModel):
    """
    A recorded parameter correction.

    Storage and model support only – full lineage propagation is
    intentionally deferred to a future version.

    TODO: find affected verdicts through lineage and recompute only
          affected outputs (lineage-indexed selective replay).
    """

    correction_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    correction_type: CorrectionType
    parameter_id: str
    old_value: float
    new_value: float
    effective_time: datetime
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    reason: Optional[str] = None
