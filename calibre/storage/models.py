"""
CALIBRE – SQLAlchemy ORM Models
================================
Tables for telemetry, timing, race-control, verdicts, integrity alerts,
and parameter registry.  Includes bitemporal columns on verdicts.
"""

from __future__ import annotations

from sqlalchemy import (
    Column, DateTime, Float, Integer, String, Text, func, Index,
)
from sqlalchemy.dialects.postgresql import JSONB

from storage.database import Base


# ── Raw data tables ──────────────────────────────────────────────────────

class TelemetryRecord(Base):
    __tablename__ = "telemetry"

    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime(timezone=True), nullable=False, index=True)
    session_key = Column(Integer, nullable=False, index=True)
    driver_number = Column(Integer, nullable=False, index=True)
    speed = Column(Float, nullable=False)
    throttle = Column(Float)
    brake = Column(Float)
    gear = Column(Integer)
    rpm = Column(Float)
    source = Column(String, default="openf1")
    zone = Column(String, nullable=True)

    __table_args__ = (
        Index("ix_telemetry_session_driver", "session_key", "driver_number"),
    )


class TimingRecord(Base):
    __tablename__ = "timing_observations"

    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime(timezone=True), nullable=False, index=True)
    session_key = Column(Integer, nullable=False, index=True)
    driver_number = Column(Integer, nullable=False, index=True)
    sector = Column(String, nullable=False)
    timing_speed = Column(Float, nullable=False)
    source = Column(String, default="simulation")
    uncertainty = Column(Float, nullable=True)


class RaceControlRecord(Base):
    __tablename__ = "race_control"

    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime(timezone=True), nullable=False, index=True)
    session_key = Column(Integer, nullable=False)
    category = Column(String, nullable=False)
    message = Column(Text, nullable=False)
    driver_number = Column(Integer, nullable=True)
    source = Column(String, default="openf1")


# ── Verdict table (bitemporal-ready) ─────────────────────────────────────

class VerdictRecord(Base):
    __tablename__ = "verdicts"

    verdict_id = Column(String, primary_key=True)
    timestamp = Column(DateTime(timezone=True), nullable=False, index=True)
    session_key = Column(Integer, nullable=False, index=True)
    driver_number = Column(Integer, nullable=False, index=True)
    rule_id = Column(String, nullable=False, index=True)
    measured_value = Column(Float, nullable=False)
    threshold = Column(Float, nullable=False)
    uncertainty = Column(Float, nullable=False)
    confidence_margin = Column(Float, nullable=False)
    verdict = Column(String, nullable=False)
    source_event_ids = Column(JSONB, default=[])
    parameter_version = Column(String, default="v1.0")

    # Bitemporal bookkeeping
    valid_from = Column(DateTime(timezone=True), nullable=True)
    valid_to = Column(DateTime(timezone=True), nullable=True)
    recorded_at = Column(DateTime(timezone=True), server_default=func.now())
    superseded_at = Column(DateTime(timezone=True), nullable=True)

    # For future rule-issue tracking
    rule_issue = Column(String, nullable=True)


# ── Integrity alerts ─────────────────────────────────────────────────────

class IntegrityAlertRecord(Base):
    __tablename__ = "integrity_alerts"

    id = Column(Integer, primary_key=True, autoincrement=True)
    sector = Column(String, nullable=False, index=True)
    residual = Column(Float, nullable=False)
    cusum = Column(Float, nullable=False)
    threshold = Column(Float, nullable=False)
    severity = Column(String, nullable=False)
    timestamp = Column(DateTime(timezone=True), nullable=False, index=True)


# ── Parameter registry ───────────────────────────────────────────────────

class ParameterRecord(Base):
    __tablename__ = "parameters"

    id = Column(Integer, primary_key=True, autoincrement=True)
    parameter_id = Column(String, nullable=False, unique=True)
    value = Column(Float, nullable=False)
    version = Column(String, default="v1.0")
    valid_from = Column(DateTime(timezone=True), server_default=func.now())
    valid_to = Column(DateTime(timezone=True), nullable=True)
    description = Column(Text, nullable=True)
