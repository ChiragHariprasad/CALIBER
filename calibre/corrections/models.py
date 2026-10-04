"""
CALIBRE – Corrections Models (Storage Layer)
==============================================
SQLAlchemy model for recording parameter corrections.

The full correction engine (lineage propagation, selective replay) is
deferred to a future version.

TODO: find affected verdicts through lineage and recompute only
      affected outputs (lineage-indexed selective replay).
"""

from __future__ import annotations

from sqlalchemy import Column, DateTime, Enum, Float, String, Text, func
from storage.database import Base

from schemas.correction import CorrectionType


class CorrectionRecord(Base):
    """Persisted correction entry."""

    __tablename__ = "corrections"

    correction_id = Column(String, primary_key=True)
    correction_type = Column(Enum(CorrectionType), nullable=False)
    parameter_id = Column(String, nullable=False, index=True)
    old_value = Column(Float, nullable=False)
    new_value = Column(Float, nullable=False)
    effective_time = Column(DateTime(timezone=True), nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    reason = Column(Text, nullable=True)
