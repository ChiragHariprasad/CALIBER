
#!/usr/bin/env python3
"""CALIBRE - Free F1 source viewer.

LIVE:
  Pitwall / F1 SignalR free topics. No F1 TV token.
  CarData.z and Position.z are excluded because they are F1-TV gated.

HISTORICAL:
  OpenF1 API: 2023 onward, historical endpoints only, no authentication.
  TracingInsights GitHub repositories: 2018-2026, public archive.

This is a source viewer only. It does not publish to Kafka or generate
CALIBRE verdicts. Retrieved evidence is saved under ./source_logs.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = BASE_DIR / "source_logs"

OPENF1_BASE = "https://api.openf1.org/v1"
GITHUB_API = "https://api.github.com/repos/TracingInsights/{year}/contents"
GITHUB_RAW = "https://raw.githubusercontent.com/TracingInsights/{year}/main/{path}"

USER_AGENT = "CALIBRE-Free-F1-Source-Viewer/1.0"
TIMEOUT = (10, 60)

OPENF1_FEEDS = [
    ("car_data", "Car telemetry: speed, RPM, throttle, brake, gear, DRS"),
    ("location", "GPS / car position coordinates"),
    ("position", "Race position changes"),
    ("intervals", "Gap to leader and interval to car ahead"),
    ("laps", "Lap and sector timing"),
    ("pit", "Pit stop and pit lane timing"),
    ("race_control", "Flags, incidents and race-control messages"),
    ("stints", "Tyre compound and stint information"),
    ("weather", "Track and ambient weather samples"),
    ("drivers", "Driver and team metadata"),
    ("session_result", "Session classification / results"),
    ("starting_grid", "Starting grid"),
    ("overtakes", "Overtake events, where available"),
    ("team_radio", "Published team-radio metadata, where available"),
]

OPENF1_DRIVER_FEEDS = {
    "car_data", "location", "position", "intervals",
    "laps", "pit", "stints", "team_radio"
}

PITWALL_CANDIDATES = [
    BASE_DIR.parent / "pitwall_source",
    BASE_DIR / "pitwall_source",
    BASE_DIR.parent / "pitwall_source" / "plugins" / "pitwall",
]


# ---------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------

def make_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "application/json, */*",
    })

    retry = Retry(
        total=3,
        connect=3,
        read=2,
        backoff_factor=0.7,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
        respect_retry_after_header=True,
    )

    adapter = HTTPAdapter(
        max_retries=retry,
        pool_connections=10,
        pool_maxsize=10,
    )

    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


HTTP = make_session()


def fetch_json(
    url: str,
    params: dict[str, Any] | None = None,
) -> Any:
    response = HTTP.get(url, params=params, timeout=TIMEOUT)

    if response.status_code == 403 and "api.github.com" in response.url:
        reset = response.headers.get("X-RateLimit-Reset")
        extra = (
            f" GitHub rate limit resets at Unix time {reset}."
            if reset else ""
        )
        raise RuntimeError(
            "GitHub's unauthenticated API rate limit was reached (HTTP 403)."
            + extra
            + " Wait for the reset. No token or payment is required."
        )

    if response.status_code == 401:
        raise RuntimeError(
            f"Source rejected unauthenticated access (401): {response.url}"
        )

    if response.status_code == 404:
        raise FileNotFoundError(
            f"Data file was not found: {response.url}"
        )

    response.raise_for_status()

    try:
        return response.json()
    except ValueError as exc:
        raise RuntimeError(
            f"Source returned invalid JSON: {response.url}"
        ) from exc


# ---------------------------------------------------------------------
# MENU HELPERS
# ---------------------------------------------------------------------

def choose_one(items: list[Any], title: str, label_fn=str) -> Any | None:
    if not items:
        print("No entries found.")
        return None

    print("\n" + "=" * 76)
    print(title)
    print("=" * 76)

    for index, item in enumerate(items, 1):
        print(f"{index:>3}. {label_fn(item)}")

    while True:
        raw = input("\nSelect number (0 to go back): ").strip()

        if raw == "0":
            return None

        if raw.isdigit() and 1 <= int(raw) <= len(items):
            return items[int(raw) - 1]

        print("Invalid selection. Try again.")


def choose_many(
    items: list[Any],
    title: str,
    label_fn=str,
) -> list[Any]:
    if not items:
        return []

    print("\n" + "=" * 76)
    print(title)
    print("=" * 76)

    for index, item in enumerate(items, 1):
        print(f"{index:>3}. {label_fn(item)}")

    print("  A. All")
    print("  0. Back")

    while True:
        raw = input(
            "Select comma-separated numbers, A for all, or 0: "
        ).strip().lower()

        if raw == "0":
            return []

        if raw == "a":
            return list(items)

        try:
            numbers = sorted(set(
                int(part.strip())
                for part in raw.split(",")
                if part.strip()
            ))

            if numbers and all(1 <= n <= len(items) for n in numbers):
                return [items[n - 1] for n in numbers]

        except ValueError:
            pass

        print("Invalid selection. Example: 1,3,4 or A.")


# ---------------------------------------------------------------------
# PAYLOAD NORMALISATION
# ---------------------------------------------------------------------

def _columnar_to_rows(
    value: dict[str, Any],
) -> list[dict[str, Any]] | None:
    array_fields = {
        key: val
        for key, val in value.items()
        if isinstance(val, list)
    }

    if not array_fields:
        return None

    lengths = {len(val) for val in array_fields.values()}

    if len(lengths) != 1:
        return None

    count = lengths.pop()

    if count == 0:
        return []

    scalar_fields = {
        key: val
        for key, val in value.items()
        if not isinstance(val, list)
    }

    output = []

    for idx in range(count):
        row = {
            key: values[idx]
            for key, values in array_fields.items()
        }
        row.update(scalar_fields)
        output.append(row)

    return output


def rows_from_payload(payload: Any) -> list[dict[str, Any]]:
    """Convert OpenF1 row arrays and TracingInsights columnar JSON."""

    if isinstance(payload, list):
        return [
            item if isinstance(item, dict) else {"value": item}
            for item in payload
        ]

    if not isinstance(payload, dict):
        return [{"value": payload}]

    # Unwrap common list-based JSON containers.
    for wrapper in (
        "drivers", "messages", "laps", "results", "records"
    ):
        nested_list = payload.get(wrapper)

        if (
            isinstance(nested_list, list)
            and all(isinstance(item, dict) for item in nested_list)
        ):
            return nested_list

    # TracingInsights telemetry is commonly nested under "tel".
    for wrapper in ("tel", "data", "records"):
        nested = payload.get(wrapper)

        if isinstance(nested, dict):
            nested_rows = _columnar_to_rows(nested)

            if nested_rows is not None:
                return nested_rows

    rows = _columnar_to_rows(payload)

    if rows is not None:
        return rows

    return [payload]


def record_timestamp(row: dict[str, Any]) -> Any:
    for key in (
        "date", "timestamp", "time", "wT",
        "sesT", "Time", "t"
    ):
        if key in row and row[key] is not None:
            return row[key]

    return "N/A"


def timestamp_seconds(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)

    if not isinstance(value, str):
        return None

    try:
        parsed = datetime.fromisoformat(
            value.replace("Z", "+00:00")
        )

        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)

        return parsed.timestamp()

    except ValueError:
        try:
            return float(value)
        except ValueError:
            return None


# ---------------------------------------------------------------------
# LOCAL STORAGE
# ---------------------------------------------------------------------

def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(
        timespec="milliseconds"
    )


def safe_name(value: str) -> str:
    text = re.sub(
        r"[^A-Za-z0-9._-]+",
        "_",
        str(value).strip(),
    )
    return text.strip("._") or "data"


def save_raw_payload(
    payload: Any,
    provider: str,
    feed: str,
    context: dict[str, Any],
) -> Path:
    """Save original response before transforming it."""

    raw_dir = LOG_DIR / "raw_payloads"
    raw_dir.mkdir(parents=True, exist_ok=True)

    stamp = datetime.now(timezone.utc).strftime(
        "%Y%m%dT%H%M%SZ"
    )

    suffix = "_".join(
        safe_name(str(context.get(key, "")))
        for key in ("year", "meeting", "session")
        if context.get(key)
    )

    path = raw_dir / (
        f"{safe_name(provider)}_{suffix}_"
        f"{safe_name(feed)}_{stamp}.json"
    )

    path.write_text(
        json.dumps(
            payload,
            ensure_ascii=False,
            separators=(",", ":"),
            default=str,
        ),
        encoding="utf-8",
    )

    print(f"[RAW SOURCE SAVED] {path}")
    return path


def replay_and_save(
    records: list[dict[str, Any]],
    provider: str,
    feed: str,
    context: dict[str, Any],
    *,
    display_limit: int = 200,
    pace: float = 0.0,
) -> Path:
    """
    Save every record as JSONL.
    Print only display_limit records unless it is zero.
    """

    LOG_DIR.mkdir(parents=True, exist_ok=True)

    stamp = datetime.now(timezone.utc).strftime(
        "%Y%m%dT%H%M%SZ"
    )

    suffix = "_".join(
        safe_name(str(context.get(key, "")))
        for key in ("year", "meeting", "session")
        if context.get(key)
    )

    output_path = LOG_DIR / (
        f"{safe_name(provider)}_{suffix}_"
        f"{safe_name(feed)}_{stamp}.jsonl"
    )

    previous_time: float | None = None

    with output_path.open("w", encoding="utf-8") as handle:
        print(f"\n[{provider}] {feed}: {len(records):,} records")
        print(f"[SAVE] {output_path}")

        for index, row in enumerate(records, 1):
            source_ts = record_timestamp(row)

            envelope = {
                "provider": provider,
                "feed": feed,
                "source_timestamp": source_ts,
                "received_at": utc_now(),
                "context": context,
                "record": row,
            }

            handle.write(
                json.dumps(
                    envelope,
                    ensure_ascii=False,
                    separators=(",", ":"),
                    default=str,
                ) + "\n"
            )

            if display_limit == 0 or index <= display_limit:
                compact = json.dumps(
                    row,
                    ensure_ascii=False,
                    separators=(",", ":"),
                    default=str,
                )

                print(
                    f"{index:>7} | {source_ts} | {compact[:1100]}"
                )

                if pace > 0:
                    current_time = timestamp_seconds(source_ts)

                    if (
                        current_time is not None
                        and previous_time is not None
                    ):
                        delay = (
                            max(0.0, current_time - previous_time)
                            / pace
                        )
                        time.sleep(min(delay, 2.0))

                    previous_time = current_time

    if display_limit and len(records) > display_limit:
        print(
            f"[DISPLAY] Showing first {display_limit:,} records; "
            f"all {len(records):,} records were saved."
        )

    print(f"[DONE] Saved {len(records):,} records.")
    return output_path


def ask_display_options() -> tuple[int, float]:
    raw = input(
        "Maximum lines to print [200, 0 = all]: "
    ).strip()

    try:
        limit = max(0, int(raw)) if raw else 200
    except ValueError:
        limit = 200

    speed = input(
        "Replay speed [F = fast, 1 = real-time, 5 = 5x] (F): "
    ).strip().lower()

    pace = {
        "1": 1.0,
        "5": 5.0,
    }.get(speed, 0.0)

    return limit, pace


# ---------------------------------------------------------------------
# HISTORICAL SOURCE 1: OPENF1
# 2023 onward, historical API only, no authentication.
# ---------------------------------------------------------------------

def openf1_historical() -> None:
    print(
        "\n[OPENF1] Historical API only. "
        "Free; no username, password or token required."
    )

    raw_year = input("Season (2023-2026): ").strip()

    if (
        not raw_year.isdigit()
        or not 2023 <= int(raw_year) <= datetime.now().year
    ):
        print(
            "OpenF1 historical coverage starts in 2023. "
            "Choose a year from 2023 onward."
        )
        return

    year = int(raw_year)

    meetings = fetch_json(
        f"{OPENF1_BASE}/meetings",
        {"year": year},
    )

    if not isinstance(meetings, list) or not meetings:
        print("OpenF1 returned no meetings for that season.")
        return

    meeting = choose_one(
        meetings,
        f"OPENF1 {year} GRAND PRIX",
        lambda m: (
            f"{m.get('meeting_name', 'Unknown meeting')} | "
            f"{m.get('location', '')} | "
            f"{str(m.get('date_start', ''))[:10]}"
        ),
    )

    if meeting is None:
        return

    sessions = fetch_json(
        f"{OPENF1_BASE}/sessions",
        {"meeting_key": meeting["meeting_key"]},
    )

    if not isinstance(sessions, list) or not sessions:
        print("No sessions found for this meeting.")
        return

    session = choose_one(
        sessions,
        "SELECT SESSION",
        lambda s: (
            f"{s.get('session_name', s.get('session_type', 'Session'))} | "
            f"{s.get('date_start', '')} to {s.get('date_end', '')}"
        ),
    )

    if session is None:
        return

    drivers = fetch_json(
        f"{OPENF1_BASE}/drivers",
        {"session_key": session["session_key"]},
    )

    feeds = choose_many(
        OPENF1_FEEDS,
        "SELECT OPENF1 FEEDS",
        lambda item: f"{item[0]} — {item[1]}",
    )

    if not feeds:
        return

    selected_driver: str | None = None

    driver_feeds = [
        name for name, _ in feeds
        if name in OPENF1_DRIVER_FEEDS
    ]

    if driver_feeds and isinstance(drivers, list) and drivers:
        print(
            "\nDriver filter applies to car_data, location, position, "
            "intervals, laps, pit, stints and team_radio."
        )
        print("  0. All drivers (larger response)")

        for driver in drivers:
            print(
                f"{driver.get('driver_number')} | "
                f"{driver.get('name_acronym')} | "
                f"{driver.get('full_name')} | "
                f"{driver.get('team_name')}"
            )

        choice = input(
            "Driver number from this list, or 0 for all: "
        ).strip()

        if choice not in ("", "0"):
            available = {
                str(d.get("driver_number"))
                for d in drivers
            }

            if choice in available:
                selected_driver = choice
            else:
                print(
                    "Driver number not found; continuing with all drivers."
                )

    display_limit, pace = ask_display_options()

    context = {
        "year": year,
        "meeting": meeting.get("meeting_name"),
        "meeting_key": meeting.get("meeting_key"),
        "session": session.get("session_name"),
        "session_key": session.get("session_key"),
        "driver_number": selected_driver or "ALL",
    }

    for feed, description in feeds:
        params: dict[str, Any] = {
            "session_key": session["session_key"]
        }

        if selected_driver and feed in OPENF1_DRIVER_FEEDS:
            params["driver_number"] = selected_driver

        print(f"\n[FETCH] {feed} — {description}")

        try:
            payload = fetch_json(
                f"{OPENF1_BASE}/{feed}",
                params,
            )

            save_raw_payload(
                payload,
                "OPENF1_HISTORICAL",
                feed,
                context,
            )

            records = rows_from_payload(payload)

            replay_and_save(
                records,
                "OPENF1_HISTORICAL",
                feed,
                context,
                display_limit=display_limit,
                pace=pace,
            )

        except (
            requests.RequestException,
            RuntimeError,
            ValueError,
            FileNotFoundError,
        ) as exc:
            print(f"[FEED ERROR] {feed}: {exc}")


# ---------------------------------------------------------------------
# HISTORICAL SOURCE 2: TRACINGINSIGHTS GITHUB ARCHIVE
# Public season repositories, 2018-2026.
# ---------------------------------------------------------------------

def github_contents_url(
    year: int,
    path: str = "",
) -> str:
    base = GITHUB_API.format(year=year)

    if path:
        base += "/" + quote(path, safe="/")

    return base


def tracing_years() -> list[int]:
    return list(
        range(2018, min(datetime.now().year, 2026) + 1)
    )


def lap_file_sort_key(name: str) -> tuple[int, str]:
    match = re.match(
        r"(\d+)_tel\.json$",
        name,
        re.IGNORECASE,
    )

    if match:
        return int(match.group(1)), name

    return -1, name


def tracing_historical() -> None:
    print(
        "\n[TRACINGINSIGHTS] Public GitHub season archive. "
        "No GitHub login or token required."
    )

    year = choose_one(
        tracing_years(),
        "SELECT TRACINGINSIGHTS SEASON",
    )

    if year is None:
        return

    print(
        f"\n[FETCH] {year} season index from public GitHub..."
    )

    root = fetch_json(github_contents_url(year))

    if not isinstance(root, list):
        print("Unexpected GitHub season index format.")
        return

    meetings = [
        entry
        for entry in root
        if (
            entry.get("type") == "dir"
            and "grand prix" in entry.get("name", "").lower()
        )
    ]

    meetings.sort(
        key=lambda entry: entry["name"].lower()
    )

    meeting = choose_one(
        meetings,
        f"SELECT {year} GRAND PRIX",
        lambda entry: entry["name"],
    )

    if meeting is None:
        return

    session_entries = fetch_json(
        github_contents_url(year, meeting["path"])
    )

    sessions = [
        entry
        for entry in session_entries
        if entry.get("type") == "dir"
    ]

    sessions.sort(
        key=lambda entry: entry["name"].lower()
    )

    selected_session = choose_one(
        sessions,
        "SELECT SESSION",
        lambda entry: entry["name"],
    )

    if selected_session is None:
        return

    session_path = selected_session["path"]

    entries = fetch_json(
        github_contents_url(year, session_path)
    )

    session_files = [
        entry
        for entry in entries
        if (
            entry.get("type") == "file"
            and entry.get("name", "").lower().endswith(".json")
        )
    ]

    session_files.sort(
        key=lambda entry: entry["name"].lower()
    )

    driver_dirs = [
        entry
        for entry in entries
        if entry.get("type") == "dir"
    ]

    driver_dirs.sort(
        key=lambda entry: entry["name"].lower()
    )

    print("\nChoose data mode:")
    print(
        "  1. Session feeds "
        "(weather, race control, timing, drivers, metadata, etc.)"
    )
    print(
        "  2. Individual driver telemetry "
        "(per-lap telemetry files)"
    )

    mode = input("Choice: ").strip()

    context = {
        "year": year,
        "meeting": meeting["name"],
        "session": selected_session["name"],
    }

    display_limit, pace = ask_display_options()

    if mode == "1":
        available = [
            (entry["name"], entry)
            for entry in session_files
        ]

        chosen = choose_many(
            available,
            "SELECT SESSION FEEDS",
            lambda item: item[0],
        )

        if not chosen:
            return

        for feed_name, entry in chosen:
            url = GITHUB_RAW.format(
                year=year,
                path=quote(entry["path"], safe="/"),
            )

            print(f"\n[FETCH] {feed_name}")

            try:
                payload = fetch_json(url)

                feed = feed_name.removesuffix(".json")

                save_raw_payload(
                    payload,
                    "TRACINGINSIGHTS",
                    feed,
                    context,
                )

                records = rows_from_payload(payload)

                replay_and_save(
                    records,
                    "TRACINGINSIGHTS",
                    feed,
                    context,
                    display_limit=display_limit,
                    pace=pace,
                )

            except (
                requests.RequestException,
                RuntimeError,
                ValueError,
                FileNotFoundError,
            ) as exc:
                print(f"[FEED ERROR] {feed_name}: {exc}")

    elif mode == "2":
        if not driver_dirs:
            print("No driver folders were found for this session.")
            return

        driver = choose_one(
            driver_dirs,
            "SELECT DRIVER",
            lambda entry: entry["name"],
        )

        if driver is None:
            return

        driver_entries = fetch_json(
            github_contents_url(year, driver["path"])
        )

        telemetry_files = [
            entry
            for entry in driver_entries
            if (
                entry.get("type") == "file"
                and (
                    entry.get("name", "").lower().endswith("_tel.json")
                    or entry.get("name", "").lower() == "laptimes.json"
                )
            )
        ]

        telemetry_files.sort(
            key=lambda entry: lap_file_sort_key(entry["name"])
        )

        selected_file = choose_one(
            telemetry_files,
            f"{driver['name']} TELEMETRY / LAP FILES",
            lambda entry: entry["name"],
        )

        if selected_file is None:
            return

        url = GITHUB_RAW.format(
            year=year,
            path=quote(selected_file["path"], safe="/"),
        )

        print(f"\n[FETCH] {selected_file['name']}")

        try:
            payload = fetch_json(url)

            context["driver"] = driver["name"]

            feed = selected_file["name"].removesuffix(".json")

            save_raw_payload(
                payload,
                "TRACINGINSIGHTS",
                feed,
                context,
            )

            records = rows_from_payload(payload)

            replay_and_save(
                records,
                "TRACINGINSIGHTS",
                feed,
                context,
                display_limit=display_limit,
                pace=pace,
            )

        except (
            requests.RequestException,
            RuntimeError,
            ValueError,
            FileNotFoundError,
        ) as exc:
            print(f"[FEED ERROR] {selected_file['name']}: {exc}")

    else:
        print("Invalid mode.")


# ---------------------------------------------------------------------
# LIVE SOURCE: PITWALL SIGNALR FREE TOPICS
# ---------------------------------------------------------------------

def locate_pitwall() -> Path | None:
    for folder in PITWALL_CANDIDATES:
        if (
            (folder / "signalr_client.py").exists()
            and (folder / "topics.py").exists()
        ):
            return folder

    return None


async def live_mode() -> None:
    pitwall_dir = locate_pitwall()

    if pitwall_dir is None:
        print("\n[ERROR] Could not find the local Pitwall clone.")
        print(
            "Expected: E:\\CALIBER\\pitwall_source\\signalr_client.py"
        )
        return

    sys.path.insert(0, str(pitwall_dir))

    try:
        from signalr_client import F1LiveClient
        from topics import FREE_TOPICS

    except Exception as exc:
        print(f"[ERROR] Pitwall dependencies/import failed: {exc}")
        print(
            "Install with: "
            "pip install aiohttp certifi websockets"
        )
        return

    print(
        "\n[LIVE] Free F1 SignalR topics. "
        "No F1 TV token or account is required."
    )

    print(
        "[LIVE] CarData.z and Position.z are excluded "
        "because they are F1-TV gated."
    )

    print("  1. Subscribe to all free topics")
    print("  2. Choose topics")

    choice = input("Choice (1): ").strip() or "1"

    if choice == "2":
        topic_choices = choose_many(
            list(FREE_TOPICS),
            "SELECT LIVE TOPICS",
        )

        if not topic_choices:
            return

    else:
        topic_choices = list(FREE_TOPICS)

    LOG_DIR.mkdir(parents=True, exist_ok=True)

    stamp = datetime.now(timezone.utc).strftime(
        "%Y%m%dT%H%M%SZ"
    )

    path = LOG_DIR / f"PITWALL_LIVE_{stamp}.jsonl"

    client = F1LiveClient(
        topics=topic_choices,
        no_auth=True,
        auto_decompress=True,
        auto_merge=True,
    )

    with path.open("a", encoding="utf-8") as handle:

        def on_event(
            topic: str,
            data: Any,
            source_timestamp: str,
        ) -> None:
            envelope = {
                "provider": "PITWALL_F1_SIGNALR_FREE",
                "topic": topic,
                "source_timestamp": source_timestamp,
                "received_at": utc_now(),
                "data": data,
            }

            handle.write(
                json.dumps(
                    envelope,
                    ensure_ascii=False,
                    separators=(",", ":"),
                    default=str,
                ) + "\n"
            )

            handle.flush()

            compact = json.dumps(
                data,
                ensure_ascii=False,
                separators=(",", ":"),
                default=str,
            )

            print(
                f"[{utc_now()}] {topic:<22} | {compact[:1200]}",
                flush=True,
            )

        client.on_all(on_event)

        print(
            f"\n[CONNECTED PENDING] "
            f"{len(topic_choices)} topics selected."
        )
        print(f"[SAVE] {path}")
        print(
            "[LIVE] Events print when received. "
            "Press Ctrl+C to stop.\n"
        )

        try:
            await client.connect()

        except asyncio.CancelledError:
            raise

        except KeyboardInterrupt:
            print("\n[STOP] Keyboard interrupt received.")

        finally:
            await client.stop()


# ---------------------------------------------------------------------
# OFFLINE SELF-TESTS
# ---------------------------------------------------------------------

def run_self_tests() -> int:
    import unittest

    class SourceViewerTests(unittest.TestCase):

        def test_openf1_row_list(self):
            source = [
                {
                    "date": "2025-01-01T00:00:00Z",
                    "speed": 300,
                },
                {
                    "date": "2025-01-01T00:00:01Z",
                    "speed": 301,
                },
            ]

            self.assertEqual(
                rows_from_payload(source),
                source,
            )

        def test_tracing_columnar_weather(self):
            source = {
                "wT": [1.0, 2.0],
                "wAT": [20.0, 21.0],
                "wR": [False, True],
            }

            self.assertEqual(
                rows_from_payload(source),
                [
                    {"wT": 1.0, "wAT": 20.0, "wR": False},
                    {"wT": 2.0, "wAT": 21.0, "wR": True},
                ],
            )

        def test_tracing_nested_telemetry(self):
            source = {
                "tel": {
                    "time": [0.0, 0.27],
                    "speed": [100, 101],
                    "gear": [3, 4],
                }
            }

            self.assertEqual(
                rows_from_payload(source),
                [
                    {"time": 0.0, "speed": 100, "gear": 3},
                    {"time": 0.27, "speed": 101, "gear": 4},
                ],
            )

        def test_drivers_wrapper(self):
            source = {
                "drivers": [
                    {"driver": "VER"},
                    {"driver": "NOR"},
                ]
            }

            self.assertEqual(
                rows_from_payload(source),
                source["drivers"],
            )

        def test_scalar_payload(self):
            source = {
                "status": "Finished",
                "count": 2,
            }

            self.assertEqual(
                rows_from_payload(source),
                [source],
            )

        def test_github_url_encoding(self):
            url = github_contents_url(
                2025,
                "Australian Grand Prix/Practice 1",
            )

            self.assertTrue(
                url.endswith(
                    "Australian%20Grand%20Prix/Practice%201"
                )
            )

        def test_lap_sort(self):
            self.assertLess(
                lap_file_sort_key("2_tel.json"),
                lap_file_sort_key("10_tel.json"),
            )

        def test_timestamp_numeric(self):
            self.assertEqual(
                timestamp_seconds(1.25),
                1.25,
            )

    suite = unittest.defaultTestLoader.loadTestsFromTestCase(
        SourceViewerTests
    )

    result = unittest.TextTestRunner(
        verbosity=2
    ).run(suite)

    return 0 if result.wasSuccessful() else 1


# ---------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Free-only CALIBRE F1 source viewer"
    )

    parser.add_argument(
        "--self-test",
        action="store_true",
        help="Run offline parser tests and exit",
    )

    args = parser.parse_args()

    if args.self_test:
        return run_self_tests()

    print("=" * 76)
    print("                 CALIBRE - FREE F1 SOURCE VIEWER")
    print("=" * 76)
    print(
        "Authentication: none for the configured free data paths."
    )

    while True:
        print(
            "\n1. LIVE F1 SESSION "
            "(Pitwall free SignalR topics)"
        )

        print(
            "2. HISTORICAL DATA "
            "(OpenF1 / TracingInsights)"
        )

        print("3. EXIT")

        choice = input("\nChoice: ").strip()

        try:
            if choice == "1":
                asyncio.run(live_mode())

            elif choice == "2":
                print("\nHISTORICAL PROVIDER")
                print(
                    "  1. TracingInsights GitHub archive "
                    "(2018-2026)"
                )
                print(
                    "  2. OpenF1 API "
                    "(2023 onward)"
                )
                print("  0. Back")

                provider = input("Choice: ").strip()

                if provider == "1":
                    tracing_historical()

                elif provider == "2":
                    openf1_historical()

                elif provider == "0":
                    continue

                else:
                    print("Invalid provider selection.")

            elif choice == "3":
                print("Exiting CALIBRE source viewer.")
                return 0

            else:
                print("Invalid selection. Try again.")

        except KeyboardInterrupt:
            print("\n[INTERRUPTED] Returning to main menu.")

        except requests.RequestException as exc:
            print(f"\n[NETWORK ERROR] {exc}")
            print(
                "Check internet connectivity and retry. "
                "No paid credentials are expected."
            )

        except (
            RuntimeError,
            ValueError,
            KeyError,
            TypeError,
            FileNotFoundError,
        ) as exc:
            print(f"\n[ERROR] {exc}")


if __name__ == "__main__":
    raise SystemExit(main())
