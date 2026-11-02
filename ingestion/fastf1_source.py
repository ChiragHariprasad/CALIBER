"""FastF1 historical session source streamed through the live Kafka pipeline.

FastF1 fetches/caches public historical session data, then this adapter emits
each source row in session-time order as an independent raw Kafka event. It
never emits the final session classification/results table.
"""
from __future__ import annotations

import logging
import math
import heapq
import time
from collections import defaultdict
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from ingestion.service import IngestionService

logger = logging.getLogger("calibre.fastf1_source")
FASTF1_FEEDS = ("telemetry", "laps", "weatherdata", "racecontrolmessages", "trackstatus")


def _clean(value: Any) -> Any:
    """Convert pandas/numpy values to JSON-safe values without inventing data."""
    if value is None:
        return None
    if hasattr(value, "to_pydatetime"):
        value = value.to_pydatetime()
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.isoformat()
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    if isinstance(value, date):
        return value.isoformat()
    if hasattr(value, "total_seconds"):
        return float(value.total_seconds())
    if hasattr(value, "item"):
        try:
            value = value.item()
        except (ValueError, TypeError):
            pass
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    try:
        if value != value:  # pandas.NaT / numpy.nan
            return None
    except (TypeError, ValueError):
        pass
    return str(value)


def _row_dict(row: Any) -> dict[str, Any]:
    if hasattr(row, "to_dict"):
        row = row.to_dict()
    return {str(key): _clean(value) for key, value in dict(row).items()}


def _seconds(value: Any) -> float | None:
    if value is None:
        return None
    if hasattr(value, "total_seconds"):
        return float(value.total_seconds())
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


class FastF1HistoricalSource:
    """Load a FastF1 session and stream its time-stamped observations to Kafka."""

    def __init__(self, cache_dir: Path | None = None) -> None:
        self.cache_dir = cache_dir or Path(__file__).resolve().parents[1] / ".fastf1-cache"

    def load(self, year: int, event: str | int, session_name: str) -> Any:
        try:
            import fastf1
        except ImportError as exc:
            raise RuntimeError("FastF1 is required. Install the project requirements (fastf1>=3.0).") from exc
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        fastf1.Cache.enable_cache(str(self.cache_dir))
        session = fastf1.get_session(year, event, session_name)
        session.load(laps=True, telemetry=True, weather=True, messages=True)
        return session

    @staticmethod
    def records(session: Any) -> Iterable[dict[str, Any]]:
        """Yield observable rows in event-time order without materializing telemetry."""
        def safe_table(name: str, default: Any = None) -> Any:
            try:
                return getattr(session, name)
            except Exception as exc:
                logger.warning("FastF1 table %s is unavailable: %s", name, exc)
                return default

        event_info = {
            "season": _clean(session.event.get("EventDate", "")) if hasattr(session.event, "get") else "",
            "event_name": str(session.event.get("EventName", "")) if hasattr(session.event, "get") else "",
            "session_name": str(getattr(session, "name", "")),
            "circuit": str(session.event.get("Location", "")) if hasattr(session.event, "get") else "",
        }

        def make_record(feed: str, payload: dict[str, Any], time_value: Any, driver: Any = None, lap: Any = None) -> dict[str, Any] | None:
            seconds = _seconds(time_value)
            if seconds is None:
                return None
            scope: dict[str, Any] = {}
            if driver is not None and _clean(driver) is not None:
                scope["driver_number"] = str(_clean(driver))
            if lap is not None and _clean(lap) is not None:
                try:
                    scope["lap_number"] = int(float(_clean(lap)))
                except (TypeError, ValueError):
                    pass
            return {"feed": feed, "seconds": seconds, "payload": payload, "scope": scope, "session": event_info}

        def frame_records(feed: str, frame: Any, time_col: str, driver_col: str | None = None, lap_col: str | None = None):
            if frame is None or not hasattr(frame, "iterrows"):
                return
            for _, row in frame.iterrows():
                payload = _row_dict(row)
                if feed == "telemetry":
                    brake_value = payload.get("Brake")
                    payload.update({
                        "speed": payload.get("Speed"), "rpm": payload.get("RPM"),
                        "gear": payload.get("nGear"), "throttle": payload.get("Throttle"),
                        "brake": payload.get("Brake"), "drs": payload.get("DRS"),
                        "brake_signal_semantics": "BOOLEAN_BRAKE_APPLIED" if isinstance(brake_value, bool) else "SOURCE_REPORTED_BRAKE_VALUE",
                        "drs_signal_semantics": "F1_DRS_STATUS_CODE" if payload.get("DRS") is not None else "UNAVAILABLE",
                        "date": payload.get("Date"),
                        "session_time_s": _seconds(payload.get("SessionTime", payload.get("Time"))),
                        "set_relative_time_s": _seconds(payload.get("Time")),
                        "lap_relative_time_s": None,
                        "time": None,
                    })
                elif feed == "laps":
                    # Per-lap position can be final/retrospective; emit timing facts only.
                    payload.pop("Position", None)
                    payload.update({
                        "driver_number": payload.get("DriverNumber"), "lap_number": payload.get("LapNumber"),
                        "lap_duration": _seconds(payload.get("LapTime")),
                        "time": _seconds(payload.get("LapTime")),
                        "s1": _seconds(payload.get("Sector1Time")), "s2": _seconds(payload.get("Sector2Time")),
                        "s3": _seconds(payload.get("Sector3Time")), "sesT": _seconds(payload.get("Time")),
                        "lST": _seconds(payload.get("LapStartTime")), "compound": payload.get("Compound"),
                        "pin": _seconds(payload.get("PitInTime")), "pout": _seconds(payload.get("PitOutTime")),
                        "stint": payload.get("Stint"), "del": payload.get("Deleted"),
                        "delR": payload.get("DeletedReason"),
                    })
                time_value = row.get(time_col)
                if _seconds(time_value) is None:
                    time_value = row.get("Time")
                record = make_record(
                    feed, payload, time_value,
                    driver=row.get(driver_col) if driver_col else None,
                    lap=row.get(lap_col) if lap_col else None,
                )
                if record:
                    yield record

        iterators = []

        # Car data uses the real sampled telemetry from FastF1. Do not interpolate
        # or synthesize absent channels; preserve nulls and source rows as returned.
        for driver_number, frame in (safe_table("car_data", {}) or {}).items():
            iterators.append(iter(frame_records("telemetry", frame, "SessionTime", "DriverNumber")))

        laps = safe_table("laps")
        if laps is not None:
            iterators.append(iter(frame_records("laps", laps, "Time", "DriverNumber", "LapNumber")))

        for attr, feed in (("weather_data", "weatherdata"), ("race_control_messages", "racecontrolmessages"), ("track_status", "trackstatus")):
            frame = safe_table(attr)
            if frame is not None:
                iterators.append(iter(frame_records(feed, frame, "Time")))

        # Merge the individually ordered FastF1 tables by timestamp. This keeps
        # the adapter bounded by row count in memory rather than session length.
        heap: list[tuple[float, int, dict[str, Any]]] = []
        for index, iterator in enumerate(iterators):
            try:
                record = next(iterator)
                heapq.heappush(heap, (record["seconds"], index, record))
            except StopIteration:
                continue
        while heap:
            _, index, record = heapq.heappop(heap)
            yield record
            try:
                next_record = next(iterators[index])
                heapq.heappush(heap, (next_record["seconds"], index, next_record))
            except StopIteration:
                continue

    def stream(
        self,
        session: Any,
        ingestion: IngestionService,
        *,
        run_id: str,
        replay_speed: float = 1.0,
        start_seconds: float = 0.0,
        max_session_seconds: float | None = None,
        max_events: int | None = None,
    ) -> dict[str, Any]:
        """Publish records in session-second buckets, paced by replay_speed."""
        if replay_speed <= 0:
            raise ValueError("replay_speed must be positive")
        event_info = {
            "season": int(session.event.get("EventDate").year),
            "meeting": str(session.event.get("EventName", "Unknown event")),
            "session_name": str(getattr(session, "name", "")),
            "circuit": str(session.event.get("Location", "")),
        }
        selected = 0
        published = 0
        by_feed: dict[str, int] = defaultdict(int)
        last_second: int | None = None
        wall_started = time.monotonic()
        bucket: list[dict[str, Any]] = []
        bucket_second: int | None = None
        first_second: int | None = None
        in_window_by_feed: dict[str, int] = defaultdict(int)

        def send_bucket(second: int, rows: list[dict[str, Any]]) -> None:
            nonlocal published
            for index, row in enumerate(rows):
                if max_events is not None and published >= max_events:
                    break
                correlation = f"{run_id}:{row['feed']}:{second:06d}:{index:05d}"
                failed_before = ingestion.publish_failed_count
                ingestion.publish_raw_event(
                    provider="FASTF1", feed=row["feed"], payload=row["payload"],
                    source_timestamp=second, time_semantics="SESSION_ELAPSED_SECONDS",
                    session_info=event_info,
                    scope={**row["scope"], "session_time_s": row["seconds"]},
                    correlation_id=correlation,
                )
                if ingestion.publish_failed_count == failed_before:
                    published += 1
                    by_feed[row["feed"]] += 1

        for row in self.records(session):
            if max_events is not None and selected >= max_events:
                break
            second = int(row["seconds"])
            if second < int(start_seconds):
                continue
            if max_session_seconds is not None and second >= start_seconds + max_session_seconds:
                break
            selected += 1
            in_window_by_feed[row["feed"]] += 1
            if bucket_second is None:
                bucket_second = second
                first_second = second
            if second != bucket_second:
                send_bucket(bucket_second, bucket)
                if max_events is not None and published >= max_events:
                    break
                if last_second is not None:
                    time.sleep(max(0.0, (bucket_second - last_second) / replay_speed))
                last_second = bucket_second
                bucket = []
                bucket_second = second
            bucket.append(row)
        if bucket and (max_events is None or published < max_events):
            send_bucket(bucket_second, bucket)
            last_second = bucket_second
        return {
            "provider": "FASTF1",
            "session": event_info,
            "source_rows_selected": selected,
            "published_events": published,
            "published_by_feed": dict(sorted(by_feed.items())),
            "records_seen_by_feed_in_window": dict(sorted(in_window_by_feed.items())),
            "feeds_without_records_in_window": [feed for feed in FASTF1_FEEDS if not in_window_by_feed.get(feed)],
            "missing_feed_limitation": "No source rows were available in the selected session-time window; no values were synthesized.",
            "first_session_second": first_second,
            "last_session_second": last_second,
            "wall_seconds": round(time.monotonic() - wall_started, 3),
            "replay_speed": replay_speed,
            "results_emitted": False,
        }
