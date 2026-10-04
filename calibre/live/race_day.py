"""
CALIBRE — Race Day CLI Entry Point
====================================
Launches the live race intelligence system for a specific session.

Usage:
    python -m calibre.live.race_day --session-key 11731 --driver VER
    python -m calibre.live.race_day --session-key 11731 --driver NOR --laps 56
    python -m calibre.live.race_day --discover  # Find today's sessions
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import signal
import sys
from pathlib import Path

# Ensure calibre package is importable
CALIBRE_ROOT = Path(__file__).resolve().parent.parent
CALIBRE_PARENT = CALIBRE_ROOT.parent
if str(CALIBRE_PARENT) not in sys.path:
    sys.path.insert(0, str(CALIBRE_PARENT))
if str(CALIBRE_ROOT) not in sys.path:
    sys.path.insert(0, str(CALIBRE_ROOT))

import httpx


async def discover_sessions():
    """Find active or upcoming sessions."""
    print("\n" + "=" * 65)
    print("CALIBRE — SESSION DISCOVERY")
    print("=" * 65)

    async with httpx.AsyncClient(base_url="https://api.openf1.org/v1") as client:
        # Get latest sessions
        resp = await client.get("/sessions?year=2026")
        resp.raise_for_status()
        sessions = resp.json()

        # Show most recent 10
        recent = sessions[-10:]
        print(f"\nLatest {len(recent)} sessions:\n")
        print(f"{'Key':<8} {'Type':<12} {'Name':<20} {'Circuit':<20} {'Start (UTC)':<22} {'Status'}")
        print("-" * 100)
        for s in recent:
            cancelled = "CANCELLED" if s.get("is_cancelled") else "OK"
            print(
                f"{s['session_key']:<8} {s['session_type']:<12} {s['session_name']:<20} "
                f"{s['circuit_short_name']:<20} {s['date_start'][:19]:<22} {cancelled}"
            )

    print("\n" + "=" * 65)
    print("Use: python -m calibre.live.race_day --session-key <KEY> --driver <CODE>")
    print("=" * 65 + "\n")


async def run_live(session_key: int, target_driver: str, total_laps: int):
    """Main live race loop."""
    from calibre.live.race_coordinator import RaceCoordinator

    print("\n" + "=" * 65)
    print("🏎️  CALIBRE — LIVE RACE INTELLIGENCE ENGINE")
    print("=" * 65)
    print(f"  Session Key  : {session_key}")
    print(f"  Target Driver: {target_driver}")
    print(f"  Total Laps   : {total_laps}")
    print(f"  Log Directory: data/race_logs/")
    print("=" * 65)
    print("\n  Starting live data feed...\n")

    coordinator = RaceCoordinator(
        session_key=session_key,
        target_driver=target_driver,
        total_laps=total_laps,
    )

    # Handle graceful shutdown
    shutdown_event = asyncio.Event()

    def _signal_handler():
        print("\n\n  [SHUTDOWN] Ctrl+C received — stopping gracefully...")
        shutdown_event.set()

    loop = asyncio.get_running_loop()
    try:
        loop.add_signal_handler(signal.SIGINT, _signal_handler)
        loop.add_signal_handler(signal.SIGTERM, _signal_handler)
    except NotImplementedError:
        # Windows doesn't support add_signal_handler
        pass

    try:
        await coordinator.start()

        print("  ✅ LIVE — All systems operational")
        print("  📡 Polling OpenF1 API for session", session_key)
        print("  📊 Dashboard: http://localhost:8000")
        print("  📝 Logs: ", coordinator.log_file)
        print("\n  Press Ctrl+C to stop\n")

        # Keep running until shutdown
        try:
            await shutdown_event.wait()
        except KeyboardInterrupt:
            pass

    finally:
        await coordinator.stop()
        print("\n  Race log saved to:", coordinator.log_file)
        print("  Use this log for post-race analysis and model calibration.\n")


def main():
    parser = argparse.ArgumentParser(
        description="CALIBRE — Live Race Intelligence Engine",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Discover available sessions
  python -m calibre.live.race_day --discover

  # Run live for Kuala Lumpur GP 2026
  python -m calibre.live.race_day --session-key 11731 --driver VER

  # Track a specific driver
  python -m calibre.live.race_day --session-key 11731 --driver NOR --laps 56
        """,
    )
    parser.add_argument("--discover", action="store_true", help="Discover available sessions")
    parser.add_argument("--session-key", type=int, default=11731, help="OpenF1 session key")
    parser.add_argument("--driver", type=str, default="VER", help="Target driver code (e.g., VER, NOR, HAM)")
    parser.add_argument("--laps", type=int, default=56, help="Total race laps")
    parser.add_argument("--log-level", type=str, default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])

    args = parser.parse_args()

    # Setup logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)-8s [%(name)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    if args.discover:
        asyncio.run(discover_sessions())
    else:
        asyncio.run(run_live(args.session_key, args.driver.upper(), args.laps))


if __name__ == "__main__":
    main()
