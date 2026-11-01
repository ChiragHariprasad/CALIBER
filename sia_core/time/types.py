"""CALIBRE SIA v2.2 - Typed Time Classes and Invariants per CAL-DOC-007 §2.2.1."""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Optional


class TimeClass(str, Enum):
    """Semantic and transport time classes per CAL-DOC-007 §2.2.1."""
    ABSOLUTE_UTC = "ABSOLUTE_UTC"
    ABSOLUTE_WITH_OFFSET = "ABSOLUTE_WITH_OFFSET"
    SESSION_ELAPSED = "SESSION_ELAPSED"
    LAP_RELATIVE = "LAP_RELATIVE"
    LAP_DURATION = "LAP_DURATION"          # Quantity only, never a timestamp
    RECEIPT_TIME_ONLY = "RECEIPT_TIME_ONLY" # Arrival metadata, never satisfies semantic time
    PROCESSING_TIME = "PROCESSING_TIME"     # Operational metadata
    UNKNOWN = "UNKNOWN"

    @property
    def is_semantic_timestamp(self) -> bool:
        """Check if time class may satisfy a contract's semantic time requirement."""
        return self in {
            TimeClass.ABSOLUTE_UTC,
            TimeClass.ABSOLUTE_WITH_OFFSET,
            TimeClass.SESSION_ELAPSED,
            TimeClass.LAP_RELATIVE,
        }


@dataclass(frozen=True, slots=True)
class TypedTime:
    """Typed time value carrying units and provenance-bound uncertainty."""
    value: Any
    time_class: TimeClass
    unit: str = "s"
    uncertainty_interval: Optional[float] = None
