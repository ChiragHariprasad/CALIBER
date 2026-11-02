#!/usr/bin/env python3
"""CALIBRE — Master Pipeline Orchestrator and Operational CLI.

Provides single-command control for:
- Starting the infrastructure and streaming pipeline
- Checking container and service health
- Injecting deterministic synthetic events
- Running end-to-end integration tests
- Inspecting Kafka topics, persistence database, and Flink state
- Stopping the environment
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import subprocess
import sys
import time
import threading
import uuid
from pathlib import Path
from typing import Any, Optional

# Ensure project root and agents are on sys.path
BASE_DIR = Path(__file__).resolve().parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))
if str(BASE_DIR / "agents") not in sys.path:
    sys.path.insert(0, str(BASE_DIR / "agents"))

from calibre_core.contracts import RawSourceEnvelope, utc_now
from calibre_core.health import ComponentHealth, HealthStatus, SystemHealthReport
from calibre_core.kafka_bus import CalibreProducer, CalibreConsumer
from calibre_core.topics import all_standard_topics, raw_topic
from ingestion.service import IngestionService
from ingestion.fastf1_source import FastF1HistoricalSource
from processors.service import ProcessorWorkerService
from flink.job import CalibreFlinkJob
from persistence.service import PersistenceService
from dashboard.service import DashboardService
from analytics.service import AnalyticsService

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("calibre.cli")

BOOTSTRAP_SERVERS = os.environ.get("CALIBRE_KAFKA_BOOTSTRAP", "127.0.0.1:9092")
PERSISTENCE_DB_PATH = BASE_DIR / "data" / "calibre_persistence.db"


def cmd_start_infra() -> int:
    """Start Docker infrastructure (Zookeeper, Kafka, Flink)."""
    print("\n[CALIBRE] Starting Docker infrastructure (Kafka, Zookeeper, Flink)...")
    res = subprocess.run(["docker", "compose", "up", "-d"], cwd=str(BASE_DIR))
    if res.returncode != 0:
        print("[ERROR] Docker Compose failed to start.")
        return res.returncode
    
    print("[CALIBRE] Waiting for Kafka broker to become ready...")
    time.sleep(3.0)
    return 0


def cmd_stop_infra() -> int:
    """Stop Docker infrastructure."""
    print("\n[CALIBRE] Stopping Docker infrastructure...")
    res = subprocess.run(["docker", "compose", "down"], cwd=str(BASE_DIR))
    return res.returncode


def cmd_stop_all() -> int:
    """Stop all running pipeline processes and Docker infrastructure in one command."""
    print("\n" + "=" * 60)
    print("CALIBRE PLATFORM — ONE-CLICK SHUTDOWN")
    print("=" * 60)

    # 1. Kill any active Python pipeline worker processes
    print("\n[1/3] Terminating any active Python pipeline worker processes...")
    current_pid = os.getpid()
    if sys.platform == "win32":
        try:
            cmd = "wmic process where \"name='python.exe' and commandline like '%run_pipeline.py run%'\" call terminate"
            subprocess.run(cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except Exception:
            pass
    else:
        try:
            subprocess.run(["pkill", "-f", "run_pipeline.py run"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            subprocess.run(["pkill", "-f", "run_pipeline.py test"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except Exception:
            pass
    print("      Active Python workers stopped.")

    # 2. Docker compose down
    print("\n[2/3] Stopping Docker infrastructure (Kafka, Zookeeper, Flink)...")
    subprocess.run(["docker", "compose", "down", "--remove-orphans"], cwd=str(BASE_DIR))

    # 3. Clean up any leftover Calibre containers
    print("\n[3/3] Verifying clean shutdown of all Calibre containers...")
    containers = ["calibre-kafka", "calibre-zookeeper", "calibre-flink-jobmanager", "calibre-flink-taskmanager", "calibre-init-kafka"]
    for c in containers:
        subprocess.run(["docker", "rm", "-f", c], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    print("\n" + "=" * 60)
    print("SUCCESS: All CALIBRE pipeline services, Docker containers,")
    print("and streaming workers have been completely stopped.")
    print("=" * 60 + "\n")
    return 0


def cmd_status() -> int:
    """Check status of containers, Kafka topics, Flink cluster, and persistence database."""
    print("\n" + "=" * 60)
    print("CALIBRE PLATFORM OPERATIONAL STATUS")
    print("=" * 60)

    # 1. Container Status
    print("\n1. Docker Containers:")
    subprocess.run(["docker", "ps", "--filter", "name=calibre-", "--format", "table {{.Names}}\t{{.Status}}\t{{.Ports}}"])

    # 2. Kafka Connectivity & Topics
    print("\n2. Kafka Broker & Topics:")
    try:
        from kafka.admin import KafkaAdminClient
        admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS, client_id="calibre-status-probe")
        topics = [t for t in sorted(list(admin.list_topics())) if "calibre" in t]
        admin.close()
        print(f"   Broker ({BOOTSTRAP_SERVERS}): CONNECTED")
        print(f"   Discovered {len(topics)} CALIBRE topics:")
        for t in topics:
            print(f"     - {t}")
    except Exception as exc:
        print(f"   Broker ({BOOTSTRAP_SERVERS}): UNREACHABLE ({exc})")

    # 3. Flink Cluster Status
    print("\n3. Apache Flink Cluster:")
    try:
        import requests
        r = requests.get("http://localhost:8081/overview", timeout=2.0)
        if r.status_code == 200:
            data = r.json()
            print(f"   JobManager (http://localhost:8081): HEALTHY")
            print(f"   Flink Version: {data.get('flink-version')}")
            print(f"   TaskManagers: {data.get('taskmanagers')}")
            print(f"   Total Slots: {data.get('slots-total')}, Available: {data.get('slots-available')}")
        else:
            print(f"   JobManager returned status: {r.status_code}")
    except Exception as exc:
        print(f"   Flink JobManager: NOT REACHABLE ({exc})")

    # 4. Persistence Database
    print("\n4. Persistence Database:")
    if PERSISTENCE_DB_PATH.exists():
        import sqlite3
        conn = sqlite3.connect(str(PERSISTENCE_DB_PATH))
        cursor = conn.cursor()
        cursor.execute("SELECT count(*) FROM persisted_events")
        count = cursor.fetchone()[0]
        conn.close()
        print(f"   Database Path: {PERSISTENCE_DB_PATH}")
        print(f"   Total Persisted Events: {count}")
    else:
        print(f"   Database not yet created at {PERSISTENCE_DB_PATH}")

    print("\n" + "=" * 60)
    return 0


def cmd_topics() -> int:
    """List all CALIBRE topics on Kafka."""
    try:
        from kafka.admin import KafkaAdminClient
        admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS, client_id="calibre-topic-list")
        topics = [t for t in sorted(list(admin.list_topics())) if "calibre" in t]
        admin.close()
        print(f"\nDiscovered {len(topics)} CALIBRE Kafka Topics:")
        for t in topics:
            print(f"  {t}")
        return 0
    except Exception as exc:
        print(f"[ERROR] Failed to list topics: {exc}")
        return 1


def cmd_test() -> int:
    """Execute end-to-end integration test suite."""
    print("\n[CALIBRE] Running End-to-End Pipeline Integration Test Suite...\n")
    env = os.environ.copy()
    env["PYTHONPATH"] = f"{BASE_DIR}{os.pathsep}{BASE_DIR / 'agents'}"
    res = subprocess.run([sys.executable, "-m", "pytest", "tests/test_e2e_pipeline.py", "-v", "-s"], cwd=str(BASE_DIR), env=env)
    return res.returncode


def cmd_test_flink() -> int:
    """Execute Apache Flink streaming verification test suite."""
    print("\n[CALIBRE] Running Apache Flink Streaming Verification Test Suite...\n")
    env = os.environ.copy()
    env["PYTHONPATH"] = f"{BASE_DIR}{os.pathsep}{BASE_DIR / 'agents'}"
    res = subprocess.run([sys.executable, "-m", "pytest", "tests/test_flink_streaming.py", "-v", "-s"], cwd=str(BASE_DIR), env=env)
    return res.returncode


def cmd_replay(file_path: Optional[str] = None, max_events: int = 10) -> int:
    """Replay a historical log fixture into Kafka."""
    path = Path(file_path) if file_path else BASE_DIR / "main" / "source_logs" / "TRACINGINSIGHTS_2025_United_States_Grand_Prix_Race_rcm_20261031T180354Z.jsonl"
    if not path.exists():
        print(f"[ERROR] File not found: {path}")
        return 1

    svc = IngestionService(source_mode="HISTORICAL_REPLAY", run_id="cli-replay")
    replayed = svc.replay_log_file(path, max_events=max_events)
    svc.close()
    print(f"\n[CALIBRE] Replayed {replayed} events from {path.name} into Kafka.")
    return 0


def cmd_query_db(limit: int = 10, correlation_id: Optional[str] = None) -> int:
    """Query recent persisted events from SQLite database."""
    svc = PersistenceService(db_path=PERSISTENCE_DB_PATH)
    if correlation_id:
        records = svc.query_by_correlation_id(correlation_id)
        print(f"\nFound {len(records)} events matching correlation_id='{correlation_id}':")
    else:
        records = svc.query_recent(limit=limit)
        print(f"\nLast {len(records)} Persisted Events:")

    for r in records:
        print(f"  [{r.get('persisted_at_utc')}] Corr: {r.get('correlation_id')} | Ev: {r.get('event_type')} ({r.get('domain')}) | Status: {r.get('integrity_status')}")
    return 0


def cmd_run_pipeline(
    run_id: Optional[str] = None,
    source: str = "signalr",
    duration_seconds: Optional[float] = None,
) -> int:
    """Start all pipeline services in the current process (interactive runner)."""
    run_id = run_id or f"service-{uuid.uuid4().hex[:12]}"
    infra_result = cmd_start_infra()
    if infra_result:
        return infra_result

    print("\n[CALIBRE] Initializing Streaming Pipeline Components...")
    processors = ProcessorWorkerService(bootstrap_servers=BOOTSTRAP_SERVERS, run_id=run_id)
    flink = CalibreFlinkJob(bootstrap_servers=BOOTSTRAP_SERVERS, job_id=f"calibre-{run_id}", run_id=run_id)
    persistence = PersistenceService(db_path=PERSISTENCE_DB_PATH, bootstrap_servers=BOOTSTRAP_SERVERS, run_id=run_id)
    dashboard = DashboardService(bootstrap_servers=BOOTSTRAP_SERVERS, run_id=run_id)
    analytics = AnalyticsService(bootstrap_servers=BOOTSTRAP_SERVERS, run_id=run_id)
    ingestion = IngestionService(bootstrap_servers=BOOTSTRAP_SERVERS, source_mode="LIVE", run_id=run_id)

    processors.start()
    flink.start()
    persistence.start()
    dashboard.start()
    analytics.start()

    # Fresh run-scoped consumer groups use `latest` offsets. Give every
    # subscriber time to join its topics before the source starts publishing;
    # otherwise the SignalR keyframe can arrive before group assignment.
    startup_deadline = time.monotonic() + 10.0
    while time.monotonic() < startup_deadline:
        if all(p.is_alive() for p in processors._threads) and flink.health.is_live:
            time.sleep(2.0)
            break
        time.sleep(0.1)

    source_thread = None
    if source == "signalr":
        def run_live_source() -> None:
            try:
                asyncio.run(ingestion.start_pitwall_live())
            except Exception:
                logger.exception("F1 SignalR source stopped with an error")
        source_thread = threading.Thread(target=run_live_source, name="calibre-f1-signalr", daemon=True)
        source_thread.start()

    print("\n[CALIBRE] All 5 pipeline subsystems are actively listening to Kafka streams.")
    print(f"  - Consumer run ID: {run_id} (fresh groups start at the live topic boundary)")
    print("  - Processors:  Consuming calibre.raw.* -> Emitting calibre.evidence.*")
    print("  - Flink Core:  Consuming calibre.evidence.* -> Emitting to database/dashboard/analytics")
    print("  - Persistence: Storing calibre.out.database -> SQLite")
    print("  - Dashboard:   Consuming calibre.out.dashboard -> In-memory gateway")
    print("  - Analytics:   Consuming calibre.out.analytics -> Emitting metrics")
    print("  - Source:      " + ("F1 SignalR free topics (no F1 TV token)" if source == "signalr" else "Kafka consumers only"))
    print("\nPress Ctrl+C to stop.\n")

    live_started = time.monotonic()
    try:
        while True:
            if duration_seconds is not None and time.monotonic() - live_started >= duration_seconds:
                break
            time.sleep(min(1.0, max(0.05, duration_seconds - (time.monotonic() - live_started))) if duration_seconds is not None else 1.0)
    except KeyboardInterrupt:
        print("\n[CALIBRE] Stop requested.")
    finally:
        print("[CALIBRE] Stopping pipeline services...")
        source_connected = bool(ingestion.health.dependencies.get("f1_signalr", False))
        if source_thread and source_thread.is_alive():
            ingestion.stop_live()
            source_thread.join(timeout=6.0)
        # Stop/flush the source producer first, then allow the downstream
        # consumers to drain everything that was accepted by Kafka.
        ingestion.close()
        drain_deadline = time.monotonic() + 60.0
        drain_timed_out = False
        while time.monotonic() < drain_deadline:
            source_processor = next(
                (p for p in processors.processors if "system_integrity" in p.processor_id),
                None,
            )
            evidence_expected = sum(p.emitted_count for p in processors.processors)
            flink_stats = flink.stats()
            persisted_expected = (
                flink_stats.get("events_routed", 0)
                + flink_stats.get("verdicts_emitted", 0)
            )
            if (
                source_processor is not None
                and source_processor.processed_count >= ingestion.published_count
                and flink_stats.get("events_received", 0) >= evidence_expected
                and persistence.persisted_count >= persisted_expected
            ):
                break
            time.sleep(0.25)
        else:
            drain_timed_out = True
        final_flink_stats = flink.stats()
        print("[CALIBRE] LIVE_RESULT=" + json.dumps({
            "run_id": run_id,
            "source": source,
            "source_connected": source_connected or bool(ingestion.live_topic_counts),
            "published_events": ingestion.published_count,
            "publish_failures": ingestion.publish_failed_count,
            "topic_counts": dict(sorted(ingestion.live_topic_counts.items())),
            "last_source_message_received_at_utc": ingestion.live_last_message_utc,
            "agent_counts": {
                p.processor_id: {"processed": p.processed_count, "emitted": p.emitted_count, "errors": p.error_count}
                for p in processors.processors
            },
            "expected_evidence_events": sum(p.emitted_count for p in processors.processors),
            "flink": final_flink_stats,
            "persistence": {"persisted_count": persistence.persisted_count},
            "drain_timed_out": drain_timed_out,
        }, default=str), flush=True)
        processors.stop()
        flink.stop()
        persistence.stop()
        dashboard.stop()
        analytics.stop()
        print("[CALIBRE] All services safely shut down.")
    if duration_seconds is not None and (
        drain_timed_out
        or final_flink_stats.get("identity_conflicts", 0)
        or final_flink_stats.get("events_malformed", 0)
        or ingestion.publish_failed_count
    ):
        return 2
    return 0


def cmd_stream_fastf1(
    year: int,
    event: str,
    session_name: str,
    run_id: Optional[str] = None,
    replay_speed: float = 1.0,
    start_seconds: float = 0.0,
    max_session_seconds: Optional[float] = None,
    max_events: Optional[int] = None,
    cache_dir: Optional[Path] = None,
) -> int:
    """Stream a public FastF1 historical session through all live Kafka consumers."""
    run_id = run_id or f"fastf1-{year}-{uuid.uuid4().hex[:10]}"
    infra_result = cmd_start_infra()
    if infra_result:
        return infra_result

    processors = ProcessorWorkerService(bootstrap_servers=BOOTSTRAP_SERVERS, run_id=run_id)
    flink = CalibreFlinkJob(bootstrap_servers=BOOTSTRAP_SERVERS, job_id=f"calibre-{run_id}", run_id=run_id)
    persistence = PersistenceService(db_path=PERSISTENCE_DB_PATH, bootstrap_servers=BOOTSTRAP_SERVERS, run_id=run_id)
    dashboard = DashboardService(bootstrap_servers=BOOTSTRAP_SERVERS, run_id=run_id)
    analytics = AnalyticsService(bootstrap_servers=BOOTSTRAP_SERVERS, run_id=run_id)
    ingestion = IngestionService(source_mode="HISTORICAL_REPLAY", run_id=run_id)
    services = [processors, flink, persistence, dashboard, analytics]
    source = FastF1HistoricalSource(cache_dir=cache_dir)
    try:
        for service in services:
            service.start()
        time.sleep(1.0)
        print(f"[CALIBRE] Loading FastF1 session {year} / {event} / {session_name}; run={run_id}", flush=True)
        session = source.load(year, event, session_name)
        print("[CALIBRE] Streaming session-time ordered rows into calibre.raw.fastf1.*", flush=True)
        result = source.stream(
            session,
            ingestion,
            run_id=run_id,
            replay_speed=replay_speed,
            start_seconds=start_seconds,
            max_session_seconds=max_session_seconds,
            max_events=max_events,
        )
        deadline = time.monotonic() + 45
        system_processor = next((p for p in processors.processors if "system_integrity" in p.processor_id), None)
        while time.monotonic() < deadline:
            stats = flink.stats()
            expected_evidence = sum(p.emitted_count for p in processors.processors)
            source_drained = system_processor is not None and system_processor.processed_count >= result["published_events"]
            flink_drained = stats.get("events_received", 0) >= expected_evidence
            persistence_drained = persistence.persisted_count >= stats.get("events_routed", 0)
            if source_drained and flink_drained and persistence_drained:
                break
            time.sleep(0.25)
        expected_evidence = sum(p.emitted_count for p in processors.processors)
        final_flink_stats = flink.stats()
        drain_timed_out = (
            system_processor is None
            or system_processor.processed_count < result["published_events"]
            or final_flink_stats.get("events_received", 0) < expected_evidence
            or persistence.persisted_count < final_flink_stats.get("events_routed", 0)
        )
        print("[CALIBRE] STREAM_RESULT=" + json.dumps({
            **result,
            "run_id": run_id,
            "ingestion_publish_failures": ingestion.publish_failed_count,
            "agent_counts": {
                p.processor_id: {"processed": p.processed_count, "emitted": p.emitted_count, "errors": p.error_count}
                for p in processors.processors
            },
            "flink": final_flink_stats,
            "persistence": {
                "persisted_count": persistence.persisted_count,
                "duplicate_event_deliveries": persistence.duplicate_event_deliveries,
            },
            "source_events_processed": system_processor.processed_count if system_processor else 0,
            "evidence_events_emitted": expected_evidence,
            "drain_timed_out": drain_timed_out,
        }, default=str), flush=True)
        return 0 if result["published_events"] and not ingestion.publish_failed_count and not drain_timed_out else 2
    except Exception as exc:
        logger.exception("FastF1 stream failed")
        print(f"[ERROR] FastF1 streaming failed: {exc}", flush=True)
        return 1
    finally:
        ingestion.close()
        for service in reversed(services):
            try:
                service.stop()
            except Exception:
                logger.exception("Error stopping %s", type(service).__name__)


def main() -> int:
    parser = argparse.ArgumentParser(description="CALIBRE Master Pipeline CLI")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    subparsers.add_parser("start", help="Start Docker infrastructure (Kafka, Flink, Zookeeper)")
    subparsers.add_parser("stop", help="Stop Docker infrastructure")
    subparsers.add_parser("stop-all", help="Stop all running Python pipeline processes and Docker infrastructure")
    subparsers.add_parser("status", help="Check containers, Kafka broker, Flink, and database status")
    subparsers.add_parser("topics", help="List all Kafka topics on the broker")
    subparsers.add_parser("test", help="Run End-to-End integration test suite")
    subparsers.add_parser("test-flink", help="Run Apache Flink streaming verification suite")
    run_parser = subparsers.add_parser("run", help="Run streaming services interactively")
    run_parser.add_argument("--run-id", default=None, help="Run ID used to isolate this worker set's Kafka consumer groups")
    run_parser.add_argument("--source", choices=["signalr", "none"], default="signalr", help="Use public F1 SignalR topics without authentication")
    run_parser.add_argument("--duration-seconds", type=float, default=None, help="Stop after a bounded live-source check and print actual source/consumer counters")

    fastf1_parser = subparsers.add_parser("stream-fastf1", help="Stream a FastF1 historical session through Kafka/Flink at session-time pace")
    fastf1_parser.add_argument("--year", type=int, required=True)
    fastf1_parser.add_argument("--event", required=True, help="FastF1 event name or round number")
    fastf1_parser.add_argument("--session", default="R", help="FastF1 session identifier (R, Q, FP1, etc.)")
    fastf1_parser.add_argument("--run-id", default=None)
    fastf1_parser.add_argument("--replay-speed", type=float, default=1.0, help="Session seconds per wall second; 1.0 is real-time")
    fastf1_parser.add_argument("--start-seconds", type=float, default=0.0)
    fastf1_parser.add_argument("--max-session-seconds", type=float, default=None)
    fastf1_parser.add_argument("--max-events", type=int, default=None)
    fastf1_parser.add_argument("--cache-dir", type=Path, default=None)

    replay_parser = subparsers.add_parser("replay-log", help="Replay historical JSONL fixture into Kafka")
    replay_parser.add_argument("--file", default=None, help="Path to JSONL log file")
    replay_parser.add_argument("--limit", type=int, default=10, help="Max records to replay")

    db_parser = subparsers.add_parser("query-db", help="Query persisted database events")
    db_parser.add_argument("--limit", type=int, default=10, help="Limit records")
    db_parser.add_argument("--corr", default=None, help="Filter by correlation ID")

    args = parser.parse_args()

    if args.command == "start":
        return cmd_start_infra()
    elif args.command == "stop":
        return cmd_stop_infra()
    elif args.command == "stop-all":
        return cmd_stop_all()
    elif args.command == "status":
        return cmd_status()
    elif args.command == "topics":
        return cmd_topics()
    elif args.command == "test":
        return cmd_test()
    elif args.command == "test-flink":
        return cmd_test_flink()
    elif args.command == "replay-log":
        return cmd_replay(file_path=args.file, max_events=args.limit)
    elif args.command == "query-db":
        return cmd_query_db(limit=args.limit, correlation_id=args.corr)
    elif args.command == "run":
        return cmd_run_pipeline(run_id=args.run_id, source=args.source, duration_seconds=args.duration_seconds)
    elif args.command == "stream-fastf1":
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
    else:
        parser.print_help()
        return 0


if __name__ == "__main__":
    sys.exit(main())

