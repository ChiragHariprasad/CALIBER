from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
import re

from .models import SourceRecord, stable_id


def _float_or_none(value: Any) -> float | None:
    if value is None or value == "None" or value == "null":
        return None
    try:
        result = float(value)
        return result if result == result and abs(result) != float("inf") else None
    except (TypeError, ValueError):
        return None


def _int_or_none(value: Any) -> int | None:
    number = _float_or_none(value)
    return int(number) if number is not None else None


def columnar_to_rows(value: dict[str, Any]) -> list[dict[str, Any]] | None:
    arrays = {k: v for k, v in value.items() if isinstance(v, list)}
    if not arrays:
        return None
    lengths = {len(v) for v in arrays.values()}
    if len(lengths) != 1:
        return None
    count = lengths.pop()
    scalar_fields = {k: v for k, v in value.items() if not isinstance(v, list)}
    rows: list[dict[str, Any]] = []
    for index in range(count):
        row = {key: values[index] for key, values in arrays.items()}
        row.update(scalar_fields)
        rows.append(row)
    return rows


def payload_to_rows(payload: Any) -> list[dict[str, Any]]:
    """Decode the two observed structures: row arrays and columnar archives."""
    if isinstance(payload, list):
        return [item if isinstance(item, dict) else {"value": item} for item in payload]
    if not isinstance(payload, dict):
        return [{"value": payload}]

    # Circuit corner files are static geometry represented as parallel arrays,
    # not time-series rows. Preserve the original object as one record.
    if "CornerNumber" in payload or "corner_number" in payload:
        return [payload]

    for wrapper in ("drivers", "messages", "laps", "results", "records"):
        value = payload.get(wrapper)
        if isinstance(value, list):
            return [item if isinstance(item, dict) else {"value": item} for item in value]

    for wrapper in ("tel", "data", "records"):
        value = payload.get(wrapper)
        if isinstance(value, dict):
            rows = columnar_to_rows(value)
            if rows is not None:
                return rows

    rows = columnar_to_rows(payload)
    return rows if rows is not None else [payload]


def parse_datetime(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    # Some exports use nanosecond precision; datetime supports microseconds.
    text = re.sub(r"(\.\d{6})\d+(?=Z|[+-]\d\d:\d\d|$)", r"\1", text)
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except ValueError:
        return None


def parse_session_time(value: Any, t0_date: Any = None) -> tuple[float | None, str]:
    """Convert absolute ISO timestamps to elapsed session seconds when possible."""
    event_dt = parse_datetime(value)
    start_dt = parse_datetime(t0_date)
    if event_dt is not None and start_dt is not None:
        return (event_dt - start_dt).total_seconds(), "session_elapsed_from_t0"
    return None, "absolute_timestamp_without_t0" if event_dt else "unknown"


def _first(row: dict[str, Any], names: Iterable[str]) -> Any:
    for name in names:
        if name in row and row[name] is not None:
            return row[name]
    return None


def normalize_record(
    *,
    provider: str,
    feed: str,
    row: dict[str, Any],
    context: dict[str, Any] | None = None,
    source_path: str | None = None,
    sequence: int = 0,
    received_at: str | None = None,
) -> SourceRecord:
    context = dict(context or {})
    feed_key = feed.lower().replace(".json", "")
    t0_date = context.get("t0_date")

    driver_code = _first(row, ("drv", "driver", "Driver", "abbreviation", "name_acronym"))
    driver_code = str(driver_code) if driver_code not in (None, "None") else None
    driver_number = _first(row, ("dNum", "driver_number", "RacingNumber"))
    driver_number = str(driver_number) if driver_number not in (None, "None") else None

    # The per-lap telemetry dataKey contains DRIVER and LAP. Do not infer
    # driver/lap from the numeric filename alone.
    data_key = row.get("dataKey")
    if isinstance(data_key, str):
        parts = data_key.split("-")
        if len(parts) >= 3:
            if driver_code is None:
                candidate = parts[-2]
                if re.fullmatch(r"[A-Z]{3}", candidate):
                    driver_code = candidate
            if context.get("lap_number") is None:
                context["lap_number"] = _int_or_none(parts[-1])

    lap_number = _int_or_none(_first(row, ("lap", "LapNumber")))
    if lap_number is None:
        lap_number = _int_or_none(context.get("lap_number"))

    source_timestamp: Any = None
    session_time_s: float | None = None
    time_semantics = "unknown"

    if feed_key.endswith("_tel") or feed_key == "tel" or "_tel.json" in (source_path or ""):
        source_timestamp = row.get("time")
        relative = _float_or_none(source_timestamp)
        lap_start = _float_or_none(context.get("lap_start_session_time"))
        if relative is not None and lap_start is not None:
            session_time_s = lap_start + relative
            time_semantics = "lap_relative_plus_lap_start"
        elif relative is not None:
            time_semantics = "lap_relative_seconds"

    elif feed_key in ("session_laptimes", "laptimes"):
        # `time` is lap duration; never promote it to an event timestamp.
        source_timestamp = _first(row, ("sesT", "lST", "lSD"))
        session_end = _float_or_none(row.get("sesT"))
        session_start = _float_or_none(row.get("lST"))
        if session_end is not None:
            session_time_s = session_end
            time_semantics = "session_elapsed_at_lap_end"
        elif session_start is not None:
            session_time_s = session_start
            time_semantics = "session_elapsed_at_lap_start"
        else:
            time_semantics = "lap_duration_only"

    elif feed_key in ("weather",):
        source_timestamp = _first(row, ("wT", "Time", "time"))
        session_time_s = _float_or_none(source_timestamp)
        time_semantics = "session_elapsed_seconds" if session_time_s is not None else "unknown"

    elif feed_key in ("rcm", "race_control", "racecontrolmessages"):
        source_timestamp = _first(row, ("time", "Timestamp", "timestamp"))
        session_time_s, time_semantics = parse_session_time(source_timestamp, t0_date)

    else:
        source_timestamp = _first(row, ("timestamp", "date", "time", "Timestamp"))
        session_time_s, time_semantics = parse_session_time(source_timestamp, t0_date)

    if received_at is None:
        from .models import utc_now
        received_at = utc_now()

    record_id = stable_id(
        provider, feed, context.get("year"), context.get("meeting"),
        context.get("session"), driver_code, lap_number,
        source_timestamp, sequence, row,
    )

    provenance = {
        "provider": provider,
        "source_path": source_path,
        "record_sequence": sequence,
        "time_semantics": time_semantics,
        "source_payload_preserved": True,
    }

    return SourceRecord(
        record_id=record_id,
        provider=provider,
        feed=feed_key,
        payload=dict(row),
        context=context,
        source_timestamp=source_timestamp,
        session_time_s=session_time_s,
        time_semantics=time_semantics,
        driver_code=driver_code,
        driver_number=driver_number,
        lap_number=lap_number,
        source_path=source_path,
        received_at=received_at,
        provenance=provenance,
    )


def normalize_payload(
    *,
    provider: str,
    feed: str,
    payload: Any,
    context: dict[str, Any] | None = None,
    source_path: str | None = None,
) -> list[SourceRecord]:
    rows = payload_to_rows(payload)
    return [
        normalize_record(
            provider=provider,
            feed=feed,
            row=row,
            context=context,
            source_path=source_path,
            sequence=index,
        )
        for index, row in enumerate(rows)
    ]


def infer_feed_from_filename(path: str | Path) -> str:
    name = Path(path).name
    stem = re.sub(r"_\d{8}T\d{6}Z\.json$", "", name, flags=re.IGNORECASE)
    # The feed starts after a recognised session token. This avoids treating
    # the year/event/session prefix as part of the feed name.
    match = re.search(
        r"_(Practice_1|Practice_2|Practice_3|Sprint_Qualifying|Qualifying|Sprint|Race)_(.+)$",
        stem,
        flags=re.IGNORECASE,
    )
    if match:
        return match.group(2).lower()
    return Path(name).stem.lower()


def infer_context_from_filename(path: str | Path) -> dict[str, Any]:
    name = Path(path).name
    match = re.match(
        r"(?:TRACINGINSIGHTS|OPENF1)(?:_HISTORICAL)?_(\d{4})_(.+?)_"
        r"(Sprint_Qualifying|Practice_[123]|Qualifying|Sprint|Race)_",
        name,
        flags=re.IGNORECASE,
    )
    if not match:
        return {}
    return {
        "year": int(match.group(1)),
        "meeting": match.group(2).replace("_", " "),
        "session": match.group(3).replace("_", " "),
    }
