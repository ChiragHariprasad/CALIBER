"""
CALIBRE – Data Loader
======================
Loads events from JSON files, CSV files, or the built-in sample data.
"""

from __future__ import annotations

import csv
import json
import logging
from pathlib import Path
from typing import Iterable

from replay.sample_events import (
    SAMPLE_RACE_CONTROL,
    SAMPLE_TELEMETRY,
    SAMPLE_TIMING,
)

logger = logging.getLogger(__name__)


def load_json(path: str | Path) -> list[dict]:
    """Load a JSON array of events from *path*."""
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Data file not found: {p}")
    with p.open() as f:
        data = json.load(f)
    logger.info("Loaded %d events from %s", len(data), p)
    return data


def load_csv(path: str | Path) -> list[dict]:
    """Load events from a CSV file (header row expected)."""
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Data file not found: {p}")
    with p.open(newline="") as f:
        reader = csv.DictReader(f)
        data = list(reader)
    logger.info("Loaded %d events from %s", len(data), p)
    return data


def load_sample() -> dict[str, list[dict]]:
    """
    Return the built-in sample dataset.

    Returns a dict keyed by topic suffix:
      ``{"telemetry": [...], "timing": [...], "race_control": [...]}``
    """
    return {
        "telemetry": SAMPLE_TELEMETRY,
        "timing": SAMPLE_TIMING,
        "race_control": SAMPLE_RACE_CONTROL,
    }


def merge_and_sort(events_by_topic: dict[str, list[dict]]) -> list[tuple[str, dict]]:
    """
    Merge multiple event lists and sort by timestamp for deterministic
    replay.

    Returns
    -------
    list of (topic, event_dict) tuples sorted by timestamp.
    """
    merged: list[tuple[str, dict]] = []
    topic_map = {
        "telemetry": "f1.raw.telemetry",
        "timing": "f1.raw.timing",
        "race_control": "f1.raw.race_control",
    }
    for key, events in events_by_topic.items():
        topic = topic_map.get(key, f"f1.raw.{key}")
        for ev in events:
            merged.append((topic, ev))

    merged.sort(key=lambda x: x[1].get("timestamp", ""))
    return merged
