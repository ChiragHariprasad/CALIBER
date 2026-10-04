"""
CALIBRE – Telemetry Event Schema
=================================
Represents on-car telemetry samples (speed, throttle, brake, …).
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class TelemetryEvent(BaseModel):
    """A single telemetry sample from an on-car data stream."""

    timestamp: datetime
    session_key: int
    driver_number: int
    speed: float = Field(..., description="Speed in km/h")
    throttle: float = Field(default=0.0, ge=0, le=100)
    brake: float = Field(default=0.0, ge=0, le=100)
    gear: int = Field(default=0, ge=0, le=8)
    rpm: float = Field(default=0.0, ge=0)

    # Provenance
    source: str = Field(
        default="openf1",
        description="Data source tag (openf1 | simulation | replay)",
    )
    zone: Optional[str] = Field(
        default=None,
        description="Track zone if known (e.g. 'pit_entry', 'pit_lane', 'sector_1')",
    )
