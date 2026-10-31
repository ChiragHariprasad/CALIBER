"""CALIBRE Persistence Downstream Service.

Consumes from calibre.out.database and stores events idempotently with audit history.
"""
from __future__ import annotations

import json
import logging
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any, Optional

from calibre_core.contracts import FlinkOutputEnvelope, utc_now, generate_uuid
from calibre_core.health import ComponentHealth, HealthStatus
from calibre_core.kafka_bus import CalibreConsumer
from calibre_core.topics import TOPIC_OUT_DATABASE, GROUP_PERSISTENCE

logger = logging.getLogger("calibre.persistence")

DEFAULT_DB_PATH = Path(__file__).resolve().parent.parent / "data" / "calibre_persistence.db"


class PersistenceService:
    """Manages audit persistence of Flink-processed evidence into relational storage."""

    def __init__(
        self,
        db_path: Optional[Path] = None,
        bootstrap_servers: Optional[str] = None,
    ) -> None:
        self.db_path = db_path or DEFAULT_DB_PATH
        self.bootstrap_servers = bootstrap_servers
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.health = ComponentHealth(name="PersistenceService")
        self.is_running = False
        self._worker_thread: Optional[threading.Thread] = None
        self.persisted_count = 0
        self._init_db()

    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), timeout=10.0)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        """Create database tables with index on correlation_id for high-speed tracing."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS persisted_events (
                    envelope_id TEXT PRIMARY KEY,
                    correlation_id TEXT NOT NULL,
                    event_id TEXT NOT NULL,
                    event_type TEXT,
                    domain TEXT,
                    provider TEXT,
                    feed TEXT,
                    source_timestamp TEXT,
                    processing_stage TEXT,
                    integrity_status TEXT,
                    flink_processed_at_utc TEXT,
                    event_json TEXT NOT NULL,
                    persisted_at_utc TEXT NOT NULL
                )
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_persisted_correlation 
                ON persisted_events (correlation_id)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_persisted_event_id 
                ON persisted_events (event_id)
            """)
            conn.commit()
        self.health.set_dependency("database", True)
        logger.info("Initialized persistence database at %s", self.db_path)

    def persist_envelope(self, data: dict[str, Any]) -> bool:
        """Store a single FlinkOutputEnvelope idempotently."""
        try:
            envelope = FlinkOutputEnvelope.from_dict(data) if not isinstance(data, FlinkOutputEnvelope) else data
            ev = envelope.event
            prov = ev.get("provenance", {})
            time_obj = ev.get("time", {})

            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO persisted_events (
                        envelope_id, correlation_id, event_id, event_type, domain,
                        provider, feed, source_timestamp, processing_stage,
                        integrity_status, flink_processed_at_utc, event_json, persisted_at_utc
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(envelope_id) DO UPDATE SET
                        persisted_at_utc = excluded.persisted_at_utc
                """, (
                    envelope.envelope_id,
                    envelope.correlation_id,
                    ev.get("event_id", "unknown"),
                    ev.get("event_type"),
                    ev.get("domain"),
                    prov.get("provider"),
                    prov.get("feed"),
                    str(time_obj.get("source_value")),
                    envelope.processing_stage,
                    envelope.integrity_status,
                    envelope.processed_at_utc,
                    json.dumps(ev, default=str),
                    utc_now(),
                ))
                conn.commit()

            self.persisted_count += 1
            self.health.record_activity(events_in=1, events_out=1)
            logger.debug("Persisted event %s (corr=%s) into database", ev.get("event_id"), envelope.correlation_id)
            return True

        except Exception as exc:
            self.health.record_activity(error=True)
            logger.error("Failed to persist envelope: %s", exc)
            return False

    def query_recent(self, limit: int = 20) -> list[dict[str, Any]]:
        """Query recent persisted events."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT envelope_id, correlation_id, event_id, event_type, domain, 
                       provider, feed, flink_processed_at_utc, persisted_at_utc
                FROM persisted_events
                ORDER BY persisted_at_utc DESC
                LIMIT ?
            """, (limit,))
            return [dict(row) for row in cursor.fetchall()]

    def query_by_correlation_id(self, correlation_id: str) -> list[dict[str, Any]]:
        """Query all events associated with a specific end-to-end trace ID."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM persisted_events
                WHERE correlation_id = ?
                ORDER BY persisted_at_utc ASC
            """, (correlation_id,))
            return [dict(row) for row in cursor.fetchall()]

    def _stream_loop(self) -> None:
        """Background consumer loop for calibre.out.database."""
        consumer = CalibreConsumer(
            TOPIC_OUT_DATABASE,
            group_id=GROUP_PERSISTENCE,
            bootstrap_servers=self.bootstrap_servers,
            auto_offset_reset="earliest",
            consumer_timeout_ms=500,
        )

        logger.info("Persistence service consuming from %s", TOPIC_OUT_DATABASE)
        while self.is_running:
            try:
                messages = consumer.poll_messages(timeout_ms=500, max_records=20)
                for msg in messages:
                    self.persist_envelope(msg)
            except Exception as exc:
                logger.error("Persistence consumer loop error: %s", exc)
                time.sleep(1.0)

        consumer.close()

    def start(self) -> None:
        """Start the persistence consumer thread."""
        self.is_running = True
        self.health.is_ready = True
        self._worker_thread = threading.Thread(
            target=self._stream_loop,
            name="calibre-persistence-consumer",
            daemon=True,
        )
        self._worker_thread.start()
        logger.info("PersistenceService started")

    def stop(self) -> None:
        self.is_running = False
        if self._worker_thread and self._worker_thread.is_alive():
            self._worker_thread.join(timeout=2.0)
        self.health.is_ready = False
        logger.info("PersistenceService stopped")
