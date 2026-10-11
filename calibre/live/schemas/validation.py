"""
CALIBRE — Event Validation Engine
====================================
Schema validation, plausibility checks, deduplication and quarantine
for incoming events before they enter the processing pipeline.
"""
from __future__ import annotations

import logging
import time
from collections import OrderedDict
from typing import Any, Dict, List, Optional, Set, Tuple

from calibre.live.schemas.events import (
    CalibreEvent,
    DataQuality,
    ValidationState,
)

logger = logging.getLogger(__name__)


# Physical plausibility bounds for F1 telemetry
PLAUSIBILITY_BOUNDS = {
    "speed_kph": (0.0, 370.0),
    "throttle_pct": (0.0, 100.0),
    "brake_pct": (0.0, 100.0),
    "gear": (0, 8),
    "rpm": (0, 15500),
    "lap_time_s": (55.0, 300.0),
    "s1_time_s": (15.0, 120.0),
    "s2_time_s": (15.0, 120.0),
    "s3_time_s": (15.0, 120.0),
    "air_temp_c": (-10.0, 55.0),
    "track_temp_c": (-10.0, 75.0),
    "humidity_pct": (0.0, 100.0),
    "wind_speed_kph": (0.0, 200.0),
    "position": (1, 24),
}


class EventValidator:
    """Validates incoming events for schema conformance and plausibility."""

    def __init__(self, max_dedup_cache: int = 50000, dedup_window_s: float = 60.0):
        self._seen_keys: OrderedDict[str, float] = OrderedDict()
        self._max_dedup = max_dedup_cache
        self.dedup_window_s = dedup_window_s
        self._quarantine: List[CalibreEvent] = []
        self._stats = {
            "validated": 0,
            "warnings": 0,
            "quarantined": 0,
            "rejected": 0,
            "duplicates": 0,
        }

    def validate_event(self, event: CalibreEvent) -> ValidationState:
        """Helper returning the resulting ValidationState directly."""
        v = self.validate(event)
        return v.validation_state

    def validate(self, event: CalibreEvent) -> CalibreEvent:
        """
        Validate an event. Sets validation_state and data_quality.
        Returns the event (possibly modified) or raises if rejected.
        """
        errors: List[str] = []
        warnings: List[str] = []

        # 1. Required fields
        if not event.source_id:
            errors.append("Missing source_id")
        if not event.event_type:
            errors.append("Missing event_type")
        if event.source_timestamp <= 0:
            errors.append(f"Invalid source_timestamp: {event.source_timestamp}")

        # 2. Timestamp sanity
        now = time.time()
        if event.source_timestamp > now + 60:
            warnings.append(f"Source timestamp is in the future by {event.source_timestamp - now:.1f}s")
        if 0 < event.source_timestamp < 946684800:  # Before 2000-01-01
            warnings.append(f"Source timestamp predates year 2000")

        # 3. Plausibility checks on data payload
        for field_name, (lo, hi) in PLAUSIBILITY_BOUNDS.items():
            if field_name in event.data:
                val = event.data[field_name]
                if val is not None:
                    try:
                        fval = float(val)
                        if fval < lo or fval > hi:
                            if field_name == "speed_kph" and fval < 0:
                                errors.append(f"{field_name}={fval} physically implausible (< 0)")
                            else:
                                warnings.append(
                                    f"{field_name}={fval} outside plausible range [{lo}, {hi}]"
                                )
                    except (ValueError, TypeError):
                        pass

        # 4. Deduplication
        dedup_key = event.dedup_key or event.compute_dedup_key()
        if dedup_key in self._seen_keys:
            self._stats["duplicates"] += 1
            event.validation_state = ValidationState.DUPLICATE
            event.validation_errors = ["Duplicate event"]
            return event

        # Register in dedup cache
        self._seen_keys[dedup_key] = now
        # Evict oldest if cache is full
        while len(self._seen_keys) > self._max_dedup:
            self._seen_keys.popitem(last=False)

        # 5. Set validation state
        if errors:
            event.validation_state = ValidationState.QUARANTINED
            event.validation_errors = errors
            event.data_quality = DataQuality.UNRELIABLE
            event.quality_score = 0.1
            self._quarantine.append(event)
            self._stats["quarantined"] += 1
            logger.warning("Event quarantined: %s — %s", event.event_id, errors)
        elif warnings:
            event.validation_state = ValidationState.WARNING
            event.validation_errors = warnings
            event.data_quality = DataQuality.MEDIUM
            event.quality_score = 0.7
            self._stats["warnings"] += 1
        else:
            event.validation_state = ValidationState.VALID
            event.data_quality = DataQuality.HIGH
            event.quality_score = 1.0

        self._stats["validated"] += 1
        return event

    def is_processable(self, event: CalibreEvent) -> bool:
        """Check whether an event should proceed to downstream processing."""
        return event.validation_state in (ValidationState.VALID, ValidationState.WARNING)

    def clear_dedup_cache(self) -> None:
        """Clear dedup cache (e.g. on replay restart)."""
        self._seen_keys.clear()

    def get_quarantine(self) -> List[CalibreEvent]:
        """Return quarantined events for investigation."""
        return list(self._quarantine)

    def get_stats(self) -> Dict[str, int]:
        return dict(self._stats)
