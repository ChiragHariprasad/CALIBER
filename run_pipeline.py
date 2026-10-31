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
import json
import logging
import os
import subprocess
import sys
import time
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
from ingestion.mock_source import MockRaceProducer
from ingestion.service import IngestionService
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
    env["PYTHONPATH"] = f"{BASE_DIR};{BASE_DIR / 'agents'}"
    res = subprocess.run([sys.executable, "-m", "pytest", "tests/test_e2e_pipeline.py", "-v", "-s"], cwd=str(BASE_DIR), env=env)
    return res.returncode


def cmd_produce_mock(event_type: str = "weather", correlation_id: Optional[str] = None) -> int:
    """Inject a deterministic synthetic event into Kafka raw topics."""
    producer = MockRaceProducer(run_id="cli-manual-inject")
    envelope = producer.produce_single_test_event(event_type=event_type, correlation_id=correlation_id)
    print(f"\n[CALIBRE] Successfully injected {event_type} event into Kafka:")
    print(f"  Event ID:       {envelope.event_id}")
    print(f"  Correlation ID: {envelope.correlation_id}")
    print(f"  Topic:          {raw_topic(envelope.provider, envelope.feed)}")
    print(f"  Payload:        {json.dumps(envelope.payload, default=str)[:120]}...")
    producer.producer.close()
    return 0


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


def cmd_run_pipeline() -> int:
    """Start all pipeline services in the current process (interactive runner)."""
    cmd_start_infra()

    print("\n[CALIBRE] Initializing Streaming Pipeline Components...")
    processors = ProcessorWorkerService(bootstrap_servers=BOOTSTRAP_SERVERS)
    flink = CalibreFlinkJob(bootstrap_servers=BOOTSTRAP_SERVERS)
    persistence = PersistenceService(db_path=PERSISTENCE_DB_PATH, bootstrap_servers=BOOTSTRAP_SERVERS)
    dashboard = DashboardService(bootstrap_servers=BOOTSTRAP_SERVERS)
    analytics = AnalyticsService(bootstrap_servers=BOOTSTRAP_SERVERS)

    processors.start()
    flink.start()
    persistence.start()
    dashboard.start()
    analytics.start()

    print("\n[CALIBRE] All 5 pipeline subsystems are actively listening to Kafka streams.")
    print("  - Processors:  Consuming calibre.raw.* -> Emitting calibre.evidence.*")
    print("  - Flink Core:  Consuming calibre.evidence.* -> Emitting to database/dashboard/analytics")
    print("  - Persistence: Storing calibre.out.database -> SQLite")
    print("  - Dashboard:   Consuming calibre.out.dashboard -> In-memory gateway")
    print("  - Analytics:   Consuming calibre.out.analytics -> Emitting metrics")
    print("\nPress Ctrl+C to stop.\n")

    try:
        while True:
            time.sleep(1.0)
    except KeyboardInterrupt:
        print("\n[CALIBRE] Stopping pipeline services...")
        processors.stop()
        flink.stop()
        persistence.stop()
        dashboard.stop()
        analytics.stop()
        print("[CALIBRE] All services safely shut down.")
        return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="CALIBRE Master Pipeline CLI")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    subparsers.add_parser("start", help="Start Docker infrastructure (Kafka, Flink, Zookeeper)")
    subparsers.add_parser("stop", help="Stop Docker infrastructure")
    subparsers.add_parser("status", help="Check containers, Kafka broker, Flink, and database status")
    subparsers.add_parser("topics", help="List all Kafka topics on the broker")
    subparsers.add_parser("test", help="Run End-to-End integration test suite")
    subparsers.add_parser("run", help="Run streaming services interactively")

    mock_parser = subparsers.add_parser("produce-mock", help="Inject a deterministic mock event")
    mock_parser.add_argument("--type", choices=["weather", "telemetry", "rcm", "timing"], default="weather")
    mock_parser.add_argument("--corr", default=None, help="Custom correlation_id")

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
    elif args.command == "status":
        return cmd_status()
    elif args.command == "topics":
        return cmd_topics()
    elif args.command == "test":
        return cmd_test()
    elif args.command == "produce-mock":
        return cmd_produce_mock(event_type=args.type, correlation_id=args.corr)
    elif args.command == "replay-log":
        return cmd_replay(file_path=args.file, max_events=args.limit)
    elif args.command == "query-db":
        return cmd_query_db(limit=args.limit, correlation_id=args.corr)
    elif args.command == "run":
        return cmd_run_pipeline()
    else:
        parser.print_help()
        return 0


if __name__ == "__main__":
    sys.exit(main())

