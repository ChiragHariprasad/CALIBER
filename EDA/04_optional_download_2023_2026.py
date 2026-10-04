"""
OPTIONAL: build a historical OpenF1 cache for CALIBRE experiments.

This is intentionally separate from the Monaco EDA so you can demonstrate
the exact data provenance without downloading everything every time.

Run:
    python 04_optional_download_2023_2026.py

WARNING:
Downloading car_data and location for every session from 2023-2026 can be
very large. Start with the lightweight endpoints first.
"""

import json
import time
from pathlib import Path

import requests

from EDA.figures.config import API_BASE, RAW_DIR

YEARS = [2023, 2024, 2025, 2026]
LIGHT_ENDPOINTS = [
    "sessions",
    "laps",
    "pit",
    "stints",
    "race_control",
    "intervals",
    "position",
    "weather",
    "drivers",
]
HEAVY_ENDPOINTS = ["car_data", "location"]

DOWNLOAD_HEAVY = False
TIMEOUT = 120


def get(endpoint, params=None, max_retries=3):
    url = f"{API_BASE}/{endpoint}"
    for attempt in range(max_retries):
        try:
            r = requests.get(url, params=params or {}, timeout=TIMEOUT)
            if r.status_code == 404:
                # OpenF1 returns 404 when no records exist for the given query/session
                return []
            if r.status_code == 422:
                # Unprocessable Entity (e.g., telemetry not published for this session)
                return []
            if r.status_code == 429:
                # Rate limited: wait with exponential backoff and retry
                wait = 2 ** attempt + 2
                print(f"[rate-limit] waiting {wait}s...")
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r.json()
        except (requests.ConnectionError, requests.Timeout) as exc:
            if attempt == max_retries - 1:
                print(f"[warning] {endpoint} request failed after retries: {exc}")
                return []
            time.sleep(2 ** attempt + 1)
        except requests.HTTPError as exc:
            print(f"[warning] {endpoint} HTTP error: {exc}")
            return []
    return []


def save(name, payload):
    path = RAW_DIR / f"historical_{name}.json"
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f)
    return path


def main():
    for year in YEARS:
        meetings_file = RAW_DIR / f"historical_meetings_{year}.json"
        if meetings_file.exists():
            with meetings_file.open("r", encoding="utf-8") as f:
                meetings = json.load(f)
        else:
            meetings = get("meetings", {"year": year})
            save(f"meetings_{year}", meetings)
        print(f"\n=== {year}: {len(meetings):,} meetings ===", flush=True)

        for meeting in meetings:
            mk = int(meeting["meeting_key"])
            sessions_file = RAW_DIR / f"historical_sessions_meeting_{mk}.json"
            if sessions_file.exists():
                with sessions_file.open("r", encoding="utf-8") as f:
                    sessions = json.load(f)
            else:
                sessions = get("sessions", {"meeting_key": mk})
                save(f"sessions_meeting_{mk}", sessions)

            for session in sessions:
                sk = int(session["session_key"])
                s_name = session.get("session_name", "")
                print(f"{year} meeting={mk} session={sk} ({s_name})", flush=True)

                endpoints = list(LIGHT_ENDPOINTS)
                if DOWNLOAD_HEAVY:
                    endpoints += HEAVY_ENDPOINTS

                for endpoint in endpoints:
                    target_file = RAW_DIR / f"historical_{endpoint}_meeting_{mk}_session_{sk}.json"
                    if target_file.exists():
                        continue

                    # Save each session separately to make retries cheap.
                    payload = get(endpoint, {"session_key": sk})
                    save(
                        f"{endpoint}_meeting_{mk}_session_{sk}",
                        payload,
                    )

    print("\nHistorical cache complete.", flush=True)


if __name__ == "__main__":
    main()
