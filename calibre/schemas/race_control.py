"""
CALIBRE – Race-Control Message Schema
=======================================
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class RaceControlEvent(BaseModel):
    """A race-control message (flags, penalties, notes, etc.)."""

    timestamp: datetime
    session_key: int
    category: str = Field(..., description="e.g. Flag, SafetyCar, Penalty, Other")
    message: str
    driver_number: Optional[int] = None
    source: str = "openf1"
