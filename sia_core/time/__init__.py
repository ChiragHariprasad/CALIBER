"""SIA Core Time Module."""
from __future__ import annotations

from .types import TimeClass, TypedTime
from .resolver import (
    TimeResolver,
    parse_iso_utc,
    format_iso_utc,
    add_seconds_to_iso,
    diff_seconds_iso,
)
from .timeline import (
    TimelineBreakpoint,
    ValidityTimeline,
    compose_pointwise,
)
from .bitemporal import BitemporalRecord, BitemporalIndex

__all__ = [
    "TimeClass",
    "TypedTime",
    "TimeResolver",
    "parse_iso_utc",
    "format_iso_utc",
    "add_seconds_to_iso",
    "diff_seconds_iso",
    "TimelineBreakpoint",
    "ValidityTimeline",
    "compose_pointwise",
    "BitemporalRecord",
    "BitemporalIndex",
]
