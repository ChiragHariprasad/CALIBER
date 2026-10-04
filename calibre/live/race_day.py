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
import os
import signal
import sys
from pathlib import Path
from typing import Optional

# Ensure calibre package is importable
CALIBRE_ROOT = Path(__file__).resolve().parent.parent
CALIBRE_PARENT = CALIBRE_ROOT.parent
if str(CALIBRE_PARENT) not in sys.path:
    sys.path.insert(0, str(CALIBRE_PARENT))
if str(CALIBRE_ROOT) not in sys.path:
    sys.path.insert(0, str(CALIBRE_ROOT))

# Ensure UTF-8 output encoding on Windows consoles
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

import httpx


async def discover_sessions(api_token: Optional[str] = None):
    """Find active or upcoming sessions."""
    print("\n" + "=" * 65)
    print("CALIBRE — SESSION DISCOVERY")
    print("=" * 65)

    token = api_token or os.environ.get("OPENF1_TOKEN") or os.environ.get("OPENF1_API_KEY")
    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    try:
        async with httpx.AsyncClient(base_url="https://api.openf1.org/v1", headers=headers, timeout=10.0) as client:
            resp = await client.get("/sessions?year=2026")
            resp.raise_for_status()
            sessions = resp.json()

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

    except httpx.HTTPStatusError as e:
        if e.response.status_code == 401:
            detail = ""
            try:
                detail = e.response.json().get("detail", "")
            except Exception:
                detail = e.response.text
            print(f"\n[AUTH REQUIRED] OpenF1 API Authentication Required (HTTP 401)")
            print(f"Detail: {detail or 'Live F1 session in progress or authentication required.'}")
            print("\nPublic OpenF1 access is paused during active F1 sessions.")
            print("To discover sessions, provide an auth token: python -m calibre.live.race_day --discover --api-token <TOKEN>")
            print("Or run mock live race: python -m calibre.live.race_day --mock\n")
            return
        raise


async def run_live(
    session_key: int,
    target_driver: str,
    total_laps: int,
    mock: bool = False,
    api_token: Optional[str] = None,
):
    """Main live race loop."""
    from calibre.live.race_coordinator import RaceCoordinator

    print("\n" + "=" * 65)
    print("CALIBRE — LIVE RACE INTELLIGENCE ENGINE")
    print("=" * 65)
    print(f"  Session Key  : {session_key}")
    print(f"  Target Driver: {target_driver}")
    print(f"  Total Laps   : {total_laps}")
    print(f"  Mode         : {'SIMULATION (MOCK)' if mock else 'OPENF1 LIVE'}")
    print(f"  Log Directory: data/race_logs/")
    print("=" * 65)
    print("\n  Starting data feed...\n")

    coordinator = RaceCoordinator(
        session_key=session_key,
        target_driver=target_driver,
        total_laps=total_laps,
        mock_mode=mock,
        api_token=api_token,
    )

    # Handle graceful shutdown
    shutdown_event = asyncio.Event()

    def _signal_handler():
        print("\n\n  [SHUTDOWN] Signal received — stopping gracefully...")
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

        print("  [OK] LIVE — All systems operational")
        if mock:
            print("  [MOCK] Running simulated live telemetry and lap events (zero API dependency)")
        else:
            print(f"  [OPENF1] Polling OpenF1 API for session {session_key}")
        print("  [DASHBOARD] http://localhost:8000")
        print("  [LOGS] ", coordinator.log_file)
        print("\n  Press Ctrl+C to stop\n")

        # Keep running until shutdown (sleep loop for clean Windows Ctrl+C handling)
        while not shutdown_event.is_set():
            await asyncio.sleep(0.5)

    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    except PermissionError as e:
        print(f"{e}")
        return
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
  # Run live in mock/simulation mode (no API key needed, zero network failure)
  python -m calibre.live.race_day --mock --driver VER

  # Run live with OpenF1 API key/token during live sessions
  python -m calibre.live.race_day --session-key 11731 --driver VER --api-token <TOKEN>

  # Discover available sessions
  python -m calibre.live.race_day --discover
        """,
    )
    parser.add_argument("--discover", action="store_true", help="Discover available sessions")
    parser.add_argument("--session-key", type=int, default=11731, help="OpenF1 session key")
    parser.add_argument("--driver", type=str, default="VER", help="Target driver code (e.g., VER, NOR, HAM)")
    parser.add_argument("--laps", type=int, default=56, help="Total race laps")
    parser.add_argument("--mock", action="store_true", help="Run with simulated telemetry (no OpenF1 API access required)")
    parser.add_argument("--api-token", type=str, default=None, help="OpenF1 Bearer auth token (or set OPENF1_TOKEN env var)")
    parser.add_argument("--log-level", type=str, default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])

    args = parser.parse_args()

    # Setup logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)-8s [%(name)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    if args.discover:
        asyncio.run(discover_sessions(args.api_token))
    else:
        asyncio.run(
            run_live(
                session_key=args.session_key,
                target_driver=args.driver.upper(),
                total_laps=args.laps,
                mock=args.mock,
                api_token=args.api_token,
            )
        )


if __name__ == "__main__":
    main()
