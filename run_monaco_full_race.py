#!/usr/bin/env python3
"""Compatibility wrapper for CALIBRE's universal FastF1 session streamer.

The former implementation replayed fixed Monaco/OpenF1 capture files. It has
been removed; this entry point now accepts any FastF1 season, event, and session
and streams those source observations through Kafka and Flink.
"""
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional

from run_pipeline import cmd_stream_fastf1


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Stream a historical F1 session through CALIBRE using FastF1")
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--event", required=True, help="FastF1 event name or round number")
    parser.add_argument("--session", default="R", help="FastF1 session identifier (R, Q, FP1, etc.)")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--replay-speed", type=float, default=1.0, help="Session seconds per wall second")
    parser.add_argument("--start-seconds", type=float, default=0.0)
    parser.add_argument("--max-session-seconds", type=float, default=None)
    parser.add_argument("--max-events", type=int, default=None)
    parser.add_argument("--cache-dir", type=Path, default=None)
    args = parser.parse_args(argv)
    return cmd_stream_fastf1(
        year=args.year,
        event=args.event,
        session_name=args.session,
        run_id=args.run_id,
        replay_speed=args.replay_speed,
        start_seconds=args.start_seconds,
        max_session_seconds=args.max_session_seconds,
        max_events=args.max_events,
        cache_dir=args.cache_dir,
    )


if __name__ == "__main__":
    raise SystemExit(main())
