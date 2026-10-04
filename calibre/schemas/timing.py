"""
CALIBRE – Timing Event Schema
==============================
Represents timing-loop or independent speed observations.
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class TimingEvent(BaseModel):
    """An independent timing observation (e.g. from a timing loop or radar)."""

    timestamp: datetime
    session_key: int
    driver_number: int
    sector: str = Field(..., description="Sector or measurement-point identifier")
    timing_speed: float = Field(..., description="Measured speed in km/h")
    source: str = Field(
        default="simulation",
        description=(
            "Origin of this observation.  "
            "'simulation' = synthetic for scaffold; "
            "'timing_loop' = real FIA timing data (future)."
        ),
    )
    uncertainty: Optional[float] = Field(
        default=None,
        description="1-σ measurement uncertainty in km/h, if known",
    )
