"""
CALIBRE – Replay Producer
===========================
Sends recorded / sample events to Kafka in timestamp order with
configurable playback speed.

Usage::

    python -m replay.producer              # default: 10× speed, sample data
    python -m replay.producer --speed 1    # real-time
    python -m replay.producer --speed 50   # fast-forward
    python -m replay.producer --file data/my_session.json
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import datetime

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

from config.settings import settings
from replay.loader import load_json, load_sample, merge_and_sort
from streaming.kafka_topics import ensure_topics

logger = logging.getLogger(__name__)


def _parse_ts(ts: str) -> float:
    """Parse ISO-8601 timestamp to epoch seconds."""
    return datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp()


def run_replay(
    speed: float = 10.0,
    data_file: str | None = None,
) -> None:
    """
    Main replay loop.

    Parameters
    ----------
    speed : float
        Replay speed multiplier (1 = real-time).
    data_file : str, optional
        Path to a JSON file with events.  If ``None``, uses built-in
        sample data.
    """
    # Load events
    if data_file:
        raw = load_json(data_file)
        events = [("f1.raw.telemetry", ev) for ev in raw]
        events.sort(key=lambda x: x[1].get("timestamp", ""))
    else:
        sample = load_sample()
        events = merge_and_sort(sample)

    if not events:
        logger.error("No events to replay.")
        return

    # Connect to Kafka
    try:
        producer = KafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, default=str).encode(),
            key_serializer=lambda k: k.encode() if k else None,
        )
    except NoBrokersAvailable:
        logger.error(
            "Cannot connect to Kafka at %s. Is it running?",
            settings.kafka_bootstrap_servers,
        )
        sys.exit(1)

    # Ensure topics exist
    try:
        ensure_topics()
    except Exception as exc:
        logger.warning("Could not ensure Kafka topics: %s", exc)

    logger.info(
        "Starting replay: %d events at %.1f× speed", len(events), speed
    )

    prev_ts: float | None = None
    sent = 0

    for topic, event in events:
        ts = _parse_ts(event["timestamp"])

        if prev_ts is not None and speed > 0:
            delta = (ts - prev_ts) / speed
            if delta > 0:
                time.sleep(delta)

        key = str(event.get("driver_number", ""))
        producer.send(topic, value=event, key=key)
        sent += 1

        logger.info(
            "[%d/%d] → %s  driver=%s  ts=%s",
            sent,
            len(events),
            topic.split(".")[-1],
            event.get("driver_number", "?"),
            event.get("timestamp", "?"),
        )
        prev_ts = ts

    producer.flush()
    producer.close()
    logger.info("Replay complete: %d events sent.", sent)


def main() -> None:
    parser = argparse.ArgumentParser(description="CALIBRE replay producer")
    parser.add_argument(
        "--speed",
        type=float,
        default=10.0,
        help="Replay speed multiplier (1 = real-time, 0 = instant)",
    )
    parser.add_argument(
        "--file",
        type=str,
        default=None,
        help="Path to JSON events file (omit for built-in sample data)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s  %(message)s",
    )
    run_replay(speed=args.speed, data_file=args.file)


if __name__ == "__main__":
    main()
