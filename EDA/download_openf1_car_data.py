"""Capture auditable historical OpenF1 car telemetry for one session.

The response rows are kept unchanged. Metadata about the query, retrieval,
driver coverage, field coverage, and digest is written to a sidecar JSON file.
Run inside WSL, for example:

    python3 EDA/download_openf1_car_data.py --session-key 11299
"""
from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import json
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

BASE_URL = "https://api.openf1.org/v1"
REQUIRED_FIELDS = (
    "session_key", "driver_number", "date", "speed", "throttle",
    "brake", "rpm", "n_gear",
)


def _get_json(endpoint: str, params: dict[str, Any]) -> Any:
    last_error: Exception | None = None
    for attempt in range(3):
        try:
            response = requests.get(
                f"{BASE_URL}/{endpoint}",
                params=params,
                headers={"Accept": "application/json", "User-Agent": "CALIBRE-historical-capture/1.0"},
                timeout=(10, 120),
            )
            if response.status_code == 429 or response.status_code >= 500:
                retry_after = response.headers.get("Retry-After", "")
                try:
                    delay = float(retry_after) if retry_after else min(1 + attempt, 3)
                except ValueError:
                    delay = min(1 + attempt, 3)
                delay = min(delay, 10)
                last_error = RuntimeError(
                    f"OpenF1 HTTP {response.status_code} for {response.url}: "
                    f"{response.text[:200]}"
                )
                time.sleep(delay)
                continue
            response.raise_for_status()
            return response.json()
        except (requests.RequestException, ValueError) as exc:
            last_error = exc
            if attempt < 4:
                time.sleep(min(2 ** attempt, 30))
    if last_error:
        raise last_error
    raise RuntimeError(f"OpenF1 request failed: {endpoint} {params}")


def _fetch_driver(session_key: int, driver_number: int, start: str, end: str) -> tuple[list[dict[str, Any]], list[dict[str, str]], int]:
    start_dt = datetime.fromisoformat(start.replace("Z", "+00:00")).astimezone(timezone.utc)
    end_dt = datetime.fromisoformat(end.replace("Z", "+00:00")).astimezone(timezone.utc)
    try:
        # OpenF1 currently returns HTTP 500/404 for some date-filtered car_data
        # intervals even though the session+driver endpoint succeeds. Query by
        # authoritative session key and driver, then retain only rows inside
        # that session's metadata bounds locally.
        rows = _get_json("car_data", {"session_key": session_key, "driver_number": driver_number})
        if not isinstance(rows, list):
            raise TypeError("car_data response is not a list")
        selected = []
        for row in rows:
            if int(row.get("session_key", -1)) != session_key:
                raise ValueError(f"OpenF1 returned a row from another session for driver {driver_number}")
            if int(row.get("driver_number", -1)) != driver_number:
                raise ValueError(f"OpenF1 returned a row for another driver in driver {driver_number} query")
            if not row.get("date"):
                raise ValueError(f"OpenF1 returned an undated car_data row for driver {driver_number}")
            when = datetime.fromisoformat(row["date"].replace("Z", "+00:00")).astimezone(timezone.utc)
            if start_dt <= when < end_dt:
                selected.append(row)
        return selected, [], len(rows) - len(selected)
    except Exception as exc:
        return [], [{"driver_number": str(driver_number), "start_utc": start, "end_utc": end, "error": str(exc)}], 0


def capture(session_key: int, output: Path, workers: int, selected_drivers: list[int] | None = None) -> tuple[Path, Path]:
    sessions = _get_json("sessions", {"session_key": session_key})
    if not sessions:
        raise RuntimeError(f"OpenF1 has no session metadata for session_key={session_key}")
    session = sessions[0]
    start, end = session.get("date_start"), session.get("date_end")
    if not start or not end:
        raise RuntimeError("Session metadata lacks date_start/date_end; refusing an unbounded telemetry query")

    drivers = _get_json("drivers", {"session_key": session_key})
    all_driver_numbers = sorted({int(row["driver_number"]) for row in drivers if row.get("driver_number") is not None})
    driver_numbers = sorted(set(selected_drivers)) if selected_drivers else all_driver_numbers
    if not driver_numbers or set(driver_numbers) - set(all_driver_numbers):
        raise RuntimeError(f"OpenF1 has no driver inventory for session_key={session_key}")

    by_driver: dict[int, list[dict[str, Any]]] = {}
    unavailable_intervals: list[dict[str, str]] = []
    rows_outside_session = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        futures = {
            pool.submit(_fetch_driver, session_key, driver, start, end): driver
            for driver in driver_numbers
        }
        for future in concurrent.futures.as_completed(futures):
            driver = futures[future]
            by_driver[driver], gaps, outside_session = future.result()
            unavailable_intervals.extend(gaps)
            rows_outside_session += outside_session
            print(f"driver={driver} records={len(by_driver[driver])}", flush=True)

    rows = [row for driver in driver_numbers for row in by_driver[driver]]
    rows.sort(key=lambda row: (row["date"], int(row["driver_number"])))
    if not rows:
        raise RuntimeError("OpenF1 returned zero car_data rows; no capture file was written")

    start_dt = datetime.fromisoformat(start.replace("Z", "+00:00"))
    end_dt = datetime.fromisoformat(end.replace("Z", "+00:00"))
    for row in rows:
        when = datetime.fromisoformat(row["date"].replace("Z", "+00:00"))
        if when.tzinfo is None or when.astimezone(timezone.utc) < start_dt.astimezone(timezone.utc) or when.astimezone(timezone.utc) >= end_dt.astimezone(timezone.utc):
            raise ValueError(f"car_data date outside authoritative session bounds: {row['date']}")

    output.parent.mkdir(parents=True, exist_ok=True)
    encoded = json.dumps(rows, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    digest = hashlib.sha256(encoded).hexdigest()
    tmp = output.with_suffix(output.suffix + ".tmp")
    tmp.write_bytes(encoded)
    tmp.replace(output)

    missing = Counter()
    invalid_ranges = Counter()
    for row in rows:
        for field in REQUIRED_FIELDS:
            if row.get(field) is None:
                missing[field] += 1
        for field, upper in (("speed", 450), ("throttle", 100), ("brake", 100), ("rpm", 20000), ("n_gear", 8)):
            value = row.get(field)
            if isinstance(value, (int, float)) and (value < 0 or value > upper):
                invalid_ranges[field] += 1

    sidecar = output.with_name(output.stem + ".provenance.json")
    provenance = {
        "provider": "OpenF1",
        "endpoint": f"{BASE_URL}/car_data",
        "query": {"session_key": session_key, "driver_number": "session driver inventory; one request per selected driver"},
        "retrieved_at_utc": datetime.now(timezone.utc).isoformat(),
        "session": session,
        "driver_numbers_expected": all_driver_numbers,
        "driver_numbers_requested": driver_numbers,
        "driver_records": {str(driver): len(by_driver[driver]) for driver in driver_numbers},
        "unavailable_intervals": unavailable_intervals,
        "coverage_status": "COMPLETE" if not unavailable_intervals and driver_numbers == all_driver_numbers else "PARTIALLY_AVAILABLE",
        "records": len(rows),
        "provider_rows_outside_session_bounds_excluded": rows_outside_session,
        "fields": sorted({key for row in rows for key in row}),
        "required_field_missing_counts": dict(missing),
        "documented_range_screen_counts": dict(invalid_ranges),
        "sha256_of_compact_json": digest,
        "timestamp_semantics": "provider date is UTC ISO-8601; values retained unchanged",
        "synthetic": False,
    }
    side_tmp = sidecar.with_suffix(sidecar.suffix + ".tmp")
    side_tmp.write_text(json.dumps(provenance, indent=2) + "\n", encoding="utf-8")
    side_tmp.replace(sidecar)
    print(f"session={session.get('session_name')} key={session_key} start={start} end={end}")
    print(f"rows={len(rows)} drivers={len(driver_numbers)} bytes={len(encoded)} sha256={digest}")
    print(f"raw={output} provenance={sidecar}")
    print(f"missing_required_fields={dict(missing)} out_of_documented_range={dict(invalid_ranges)}")
    return output, sidecar


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--session-key", type=int, required=True)
    parser.add_argument("--output", type=Path, default=None)
    parser.add_argument("--workers", type=int, default=2, help="Concurrent per-driver requests (default 2 to avoid API rate limits)")
    parser.add_argument("--drivers", type=int, nargs="+", default=None, help="Optional driver subset; provenance reports partial session coverage")
    args = parser.parse_args()
    output = args.output or Path(__file__).resolve().parent / "data" / "raw" / f"car_data_session_{args.session_key}.json"
    capture(args.session_key, output, args.workers, args.drivers)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
