"""CALIBRE SIA v2.2 - Typed Time Resolver and Legal Conversions per CAL-DOC-007 §2.2.2."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

from .types import TimeClass, TypedTime
from ..models.evidence import ReferenceFact
from ..models.states import and_state


def parse_iso_utc(ts_str: str) -> datetime:
    """Parse an ISO-8601 UTC timestamp string into timezone-aware datetime."""
    clean_ts = ts_str.strip()
    if clean_ts.endswith("Z"):
        clean_ts = clean_ts[:-1] + "+00:00"
    dt = datetime.fromisoformat(clean_ts)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def format_iso_utc(dt: datetime) -> str:
    """Format timezone-aware datetime as ISO-8601 UTC string with millisecond precision."""
    dt_utc = dt.astimezone(timezone.utc)
    ms = dt_utc.microsecond // 1000
    return dt_utc.strftime(f"%Y-%m-%dT%H:%M:%S.{ms:03d}Z")


def add_seconds_to_iso(ts_str: str, seconds: float) -> str:
    """Add float seconds to an ISO timestamp and return formatted UTC string."""
    from datetime import timedelta
    dt = parse_iso_utc(ts_str)
    dt_res = dt + timedelta(seconds=seconds)
    return format_iso_utc(dt_res)


def diff_seconds_iso(ts_later: str, ts_earlier: str) -> float:
    """Compute difference in seconds between two ISO timestamps (ts_later - ts_earlier)."""
    dt_later = parse_iso_utc(ts_later)
    dt_earlier = parse_iso_utc(ts_earlier)
    return (dt_later - dt_earlier).total_seconds()


class TimeResolver:
    """Legal time domain conversion engine enforcing Invariants G10 and G1."""

    @staticmethod
    def can_convert(from_class: TimeClass | str, to_class: TimeClass | str) -> bool:
        """Check if conversion is permitted by the normative conversion table."""
        fc = TimeClass(from_class) if isinstance(from_class, str) else from_class
        tc = TimeClass(to_class) if isinstance(to_class, str) else to_class

        if fc == tc:
            return True
        if fc in {TimeClass.LAP_DURATION, TimeClass.RECEIPT_TIME_ONLY, TimeClass.PROCESSING_TIME, TimeClass.UNKNOWN}:
            return False
        if tc in {TimeClass.LAP_DURATION, TimeClass.RECEIPT_TIME_ONLY, TimeClass.PROCESSING_TIME, TimeClass.UNKNOWN}:
            return False

        if fc == TimeClass.LAP_RELATIVE and tc == TimeClass.SESSION_ELAPSED:
            return True
        if fc == TimeClass.SESSION_ELAPSED and tc in {TimeClass.ABSOLUTE_UTC, TimeClass.ABSOLUTE_WITH_OFFSET}:
            return True
        if fc in {TimeClass.ABSOLUTE_UTC, TimeClass.ABSOLUTE_WITH_OFFSET} and tc == TimeClass.SESSION_ELAPSED:
            return True

        return False

    @staticmethod
    def resolve_time(
        input_time: TypedTime,
        target_class: TimeClass,
        reference_facts: Dict[str, ReferenceFact],
        session_id: Optional[str] = None,
        driver_id: Optional[str] = None,
        lap_number: Optional[int] = None,
    ) -> Optional[Tuple[TypedTime, Set[str], str]]:
        """Resolve and convert input_time to target_class using available reference facts.
        
        Returns:
            Tuple of (converted_time, combined_origin_set, combined_grade) or None on failure.
        """
        if not TimeResolver.can_convert(input_time.time_class, target_class):
            return None

        if input_time.time_class == target_class:
            in_grade = getattr(input_time, "grade", "HEALTHY") or "HEALTHY"
            return input_time, set(), in_grade

        # Conversion: LAP_RELATIVE -> SESSION_ELAPSED
        if input_time.time_class == TimeClass.LAP_RELATIVE and target_class == TimeClass.SESSION_ELAPSED:
            # Requires lap_start fact
            key = f"{session_id}|{driver_id}|{lap_number}"
            ref_fact = reference_facts.get("lap_start_reference")
            if not ref_fact:
                return None
            try:
                lap_start_val = float(ref_fact.value)
                rel_val = float(input_time.value)
                converted_val = lap_start_val + rel_val
                ref_unc = getattr(ref_fact, "uncertainty_interval", None) or 0.0
                unc = (input_time.uncertainty_interval or 0.0) + ref_unc
                in_grade = getattr(input_time, "grade", "HEALTHY") or "HEALTHY"
                combined_grade = and_state(in_grade, ref_fact.grade).value
                res_time = TypedTime(
                    value=converted_val,
                    time_class=TimeClass.SESSION_ELAPSED,
                    unit="s",
                    uncertainty_interval=unc if unc > 0 else None,
                )
                return res_time, set(ref_fact.origin_set), combined_grade
            except (ValueError, TypeError):
                return None

        # Conversion: SESSION_ELAPSED -> ABSOLUTE_UTC
        if input_time.time_class == TimeClass.SESSION_ELAPSED and target_class in {
            TimeClass.ABSOLUTE_UTC,
            TimeClass.ABSOLUTE_WITH_OFFSET,
        }:
            ref_fact = reference_facts.get("session_t0_reference")
            if not ref_fact:
                return None
            try:
                t0_str = str(ref_fact.value)
                elapsed_s = float(input_time.value)
                converted_str = add_seconds_to_iso(t0_str, elapsed_s)
                ref_unc = getattr(ref_fact, "uncertainty_interval", None) or 0.0
                unc = (input_time.uncertainty_interval or 0.0) + ref_unc
                in_grade = getattr(input_time, "grade", "HEALTHY") or "HEALTHY"
                combined_grade = and_state(in_grade, ref_fact.grade).value
                res_time = TypedTime(
                    value=converted_str,
                    time_class=TimeClass.ABSOLUTE_UTC,
                    unit="iso8601",
                    uncertainty_interval=unc if unc > 0 else None,
                )
                return res_time, set(ref_fact.origin_set), combined_grade
            except Exception:
                return None

        # Conversion: ABSOLUTE_UTC -> SESSION_ELAPSED
        if input_time.time_class in {TimeClass.ABSOLUTE_UTC, TimeClass.ABSOLUTE_WITH_OFFSET} and target_class == TimeClass.SESSION_ELAPSED:
            ref_fact = reference_facts.get("session_t0_reference")
            if not ref_fact:
                return None
            try:
                t0_str = str(ref_fact.value)
                event_str = str(input_time.value)
                elapsed_s = diff_seconds_iso(event_str, t0_str)
                ref_unc = getattr(ref_fact, "uncertainty_interval", None) or 0.0
                unc = (input_time.uncertainty_interval or 0.0) + ref_unc
                in_grade = getattr(input_time, "grade", "HEALTHY") or "HEALTHY"
                combined_grade = and_state(in_grade, ref_fact.grade).value
                res_time = TypedTime(
                    value=elapsed_s,
                    time_class=TimeClass.SESSION_ELAPSED,
                    unit="s",
                    uncertainty_interval=unc if unc > 0 else None,
                )
                return res_time, set(ref_fact.origin_set), combined_grade
            except Exception:
                return None

        return None
