"""CALIBRE Persistence Downstream Service.

Consumes from calibre.out.database and stores events idempotently with audit history.
"""
from __future__ import annotations

import json
import hashlib
import logging
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any, Optional

from calibre_core.contracts import FlinkOutputEnvelope, utc_now, generate_uuid
from calibre_core.health import ComponentHealth, HealthStatus
from calibre_core.kafka_bus import CalibreConsumer
from calibre_core.run_scope import belongs_to_run
from calibre_core.topics import TOPIC_OUT_DATABASE, TOPIC_SIDE_OVERFLOW, GROUP_PERSISTENCE

logger = logging.getLogger("calibre.persistence")

DEFAULT_DB_PATH = Path(__file__).resolve().parent.parent / "data" / "calibre_persistence.db"


class PersistenceService:
    """Manages audit persistence of Flink-processed evidence into relational storage."""

    def __init__(
        self,
        db_path: Optional[Path] = None,
        bootstrap_servers: Optional[str] = None,
        run_id: Optional[str] = None,
    ) -> None:
        self.db_path = db_path or DEFAULT_DB_PATH
        self.bootstrap_servers = bootstrap_servers
        self.run_id = run_id
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.health = ComponentHealth(name="PersistenceService")
        self.is_running = False
        self._worker_thread: Optional[threading.Thread] = None
        self.persisted_count = 0
        self.duplicate_event_deliveries = 0
        self.duplicate_verdict_deliveries = 0
        self.scope_filtered_count = 0
        self.persisted_overflow_count = 0
        self.persisted_overflow_deliveries = 0
        self.duplicate_overflow_deliveries = 0
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
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS persisted_verdicts (
                    verdict_hash TEXT PRIMARY KEY,
                    decision_instance_id TEXT NOT NULL,
                    contract_id TEXT NOT NULL,
                    contract_version TEXT NOT NULL,
                    integrity_state TEXT NOT NULL,
                    resolution_maturity TEXT NOT NULL,
                    finality_status TEXT NOT NULL,
                    revision_number INTEGER NOT NULL,
                    previous_verdict_hash TEXT,
                    valid_time_utc TEXT,
                    knowledge_time_utc TEXT NOT NULL,
                    lease_until_utc TEXT,
                    lease_clock_domain TEXT NOT NULL,
                    primary_binding_witness TEXT,
                    verdict_json TEXT NOT NULL,
                    persisted_at_utc TEXT NOT NULL
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS persisted_overflows (
                    overflow_id TEXT PRIMARY KEY,
                    run_id TEXT,
                    decision_instance_id TEXT NOT NULL,
                    reason_code TEXT NOT NULL,
                    source_topic TEXT,
                    source_partition INTEGER,
                    source_offset INTEGER,
                    kafka_topic TEXT NOT NULL,
                    kafka_partition INTEGER NOT NULL,
                    kafka_offset INTEGER NOT NULL,
                    payload_json TEXT NOT NULL,
                    persisted_at_utc TEXT NOT NULL,
                    UNIQUE(kafka_topic, kafka_partition, kafka_offset)
                )
            """)
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_overflow_run_reason ON persisted_overflows(run_id, reason_code)")
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_verdict_decision 
                ON persisted_verdicts (decision_instance_id, revision_number)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_verdict_as_of 
                ON persisted_verdicts (decision_instance_id, knowledge_time_utc)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_verdict_prev_hash 
                ON persisted_verdicts (previous_verdict_hash)
            """)
            conn.commit()
        self.health.set_dependency("database", True)
        logger.info("Initialized persistence database at %s", self.db_path)

    def persist_overflow(self, data: dict[str, Any], metadata: dict[str, Any]) -> bool:
        """Store structured SIA bounded-search records idempotently."""
        if not belongs_to_run(data, self.run_id):
            self.scope_filtered_count += 1
            return True
        identity = "|".join(str(data.get(key, "")) for key in (
            "run_id", "decision_instance_id", "correlation_id", "reason_code", "verdict_hash"
        ))
        overflow_id = hashlib.sha256(identity.encode("utf-8")).hexdigest()
        try:
            with self._get_connection() as conn:
                cursor = conn.execute("""
                    INSERT OR IGNORE INTO persisted_overflows
                    (overflow_id, run_id, decision_instance_id, reason_code, source_topic,
                     source_partition, source_offset, kafka_topic, kafka_partition, kafka_offset,
                     payload_json, persisted_at_utc)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    overflow_id, data.get("run_id"), data.get("decision_instance_id"),
                    data.get("reason_code", "UNKNOWN"), data.get("source_topic"),
                    data.get("source_partition"), data.get("source_offset"),
                    metadata.get("topic", TOPIC_SIDE_OVERFLOW), int(metadata.get("partition", 0)),
                    int(metadata.get("offset", -1)), json.dumps(data, sort_keys=True, separators=(",", ":")), utc_now(),
                ))
                inserted = cursor.rowcount > 0
            if inserted:
                self.persisted_overflow_count += 1
            else:
                self.duplicate_overflow_deliveries += 1
            self.persisted_overflow_deliveries += 1
            return True
        except Exception as exc:
            logger.error("Failed to persist SIA overflow record: %s", exc)
            return False

    def persist_envelope(self, data: dict[str, Any]) -> bool:
        """Store a single FlinkOutputEnvelope or IntegrityAssessmentVerdict idempotently."""
        if not belongs_to_run(data, self.run_id):
            self.scope_filtered_count += 1
            return True
        if isinstance(data, dict) and "verdict_hash" in data:
            return self.persist_verdict(data)

        try:
            envelope = FlinkOutputEnvelope.from_dict(data) if not isinstance(data, FlinkOutputEnvelope) else data
            ev = envelope.event
            prov = ev.get("provenance", {})
            time_obj = ev.get("time", {})

            with self._get_connection() as conn:
                cursor = conn.cursor()
                already_seen = cursor.execute(
                    "SELECT 1 FROM persisted_events WHERE envelope_id = ?",
                    (envelope.envelope_id,),
                ).fetchone() is not None
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

            if already_seen:
                self.duplicate_event_deliveries += 1

            self.persisted_count += 1
            self.health.record_activity(events_in=1, events_out=1)
            logger.debug("Persisted event %s (corr=%s) into database", ev.get("event_id"), envelope.correlation_id)
            return True

        except Exception as exc:
            self.health.record_activity(error=True)
            logger.error("Failed to persist envelope: %s", exc)
            return False

    def persist_verdict(self, verdict: dict[str, Any]) -> bool:
        """Persist an IntegrityAssessmentVerdict with bitemporal indexing and revision chain link."""
        try:
            verdict_hash = verdict["verdict_hash"]
            decision_id = verdict.get("decision_instance_id", "unknown")
            contract_meta = verdict.get("contract", {})
            contract_id = contract_meta.get("id", "unknown")
            contract_version = contract_meta.get("version", "1.0")
            state = verdict.get("integrity_state", "UNKNOWN")
            maturity = verdict.get("resolution_maturity", "OPEN")
            finality = verdict.get("finality_status", "OPEN")
            rev_num = int(verdict.get("revision_number", 1))
            prev_hash = verdict.get("previous_verdict_hash")
            
            # Extract bitemporal coordinates
            frontier = verdict.get("knowledge_frontier", {})
            knowledge_time = frontier.get("run_clock") or frontier.get("watermark") or utc_now()
            timeline = verdict.get("validity_timeline", [])
            valid_time = timeline[0].get("from") if timeline else knowledge_time
            
            lease_until = verdict.get("lease_until")
            lease_clock_domain = verdict.get("lease_clock_domain", "LIVE_UTC")
            witness = verdict.get("primary_binding_witness", "")
            now_str = utc_now()

            with self._get_connection() as conn:
                cursor = conn.cursor()
                already_seen = cursor.execute(
                    "SELECT 1 FROM persisted_verdicts WHERE verdict_hash = ?",
                    (verdict_hash,),
                ).fetchone() is not None
                cursor.execute("""
                    INSERT INTO persisted_verdicts (
                        verdict_hash, decision_instance_id, contract_id, contract_version,
                        integrity_state, resolution_maturity, finality_status, revision_number,
                        previous_verdict_hash, valid_time_utc, knowledge_time_utc,
                        lease_until_utc, lease_clock_domain, primary_binding_witness,
                        verdict_json, persisted_at_utc
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(verdict_hash) DO NOTHING
                """, (
                    verdict_hash, decision_id, contract_id, contract_version,
                    state, maturity, finality, rev_num,
                    prev_hash, valid_time, knowledge_time,
                    lease_until, lease_clock_domain, witness,
                    json.dumps(verdict, default=str), now_str
                ))
                conn.commit()

            if already_seen:
                self.duplicate_verdict_deliveries += 1

            self.persisted_count += 1
            self.health.record_activity(events_in=1, events_out=1)
            logger.debug("Persisted verdict %s (rev=%d) for decision %s", verdict_hash[:8], rev_num, decision_id)
            return True

        except Exception as exc:
            self.health.record_activity(error=True)
            logger.error("Failed to persist verdict: %s", exc)
            return False

    def query_verdicts_by_decision(self, decision_instance_id: str) -> list[dict[str, Any]]:
        """Query all revisions of a decision ordered by revision number."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT verdict_hash, decision_instance_id, contract_id, contract_version,
                       integrity_state, resolution_maturity, finality_status, revision_number,
                       previous_verdict_hash, valid_time_utc, knowledge_time_utc,
                       lease_until_utc, lease_clock_domain, primary_binding_witness, verdict_json
                FROM persisted_verdicts
                WHERE decision_instance_id = ?
                ORDER BY revision_number ASC
            """, (decision_instance_id,))
            rows = cursor.fetchall()
            return [json.loads(row["verdict_json"]) for row in rows]

    def query_verdict_as_of_knowledge_time(self, decision_instance_id: str, as_of_utc: str) -> Optional[dict[str, Any]]:
        """Bitemporal As-Of-K Query: Return the latest revision known at or before as_of_utc."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT verdict_json
                FROM persisted_verdicts
                WHERE decision_instance_id = ? AND knowledge_time_utc <= ?
                ORDER BY knowledge_time_utc DESC, revision_number DESC
                LIMIT 1
            """, (decision_instance_id, as_of_utc))
            row = cursor.fetchone()
            if row:
                return json.loads(row["verdict_json"])
            return None

    def query_verdict_bitemporal(
        self,
        decision_instance_id: str,
        valid_as_of: str,
        knowledge_as_of: str,
        clock_domain: str,
    ) -> Optional[dict[str, Any]]:
        """Return the latest revision valid at one time and known at another."""
        with self._get_connection() as conn:
            row = conn.execute(
                """
                SELECT verdict_json
                FROM persisted_verdicts
                WHERE decision_instance_id = ?
                  AND valid_time_utc <= ?
                  AND knowledge_time_utc <= ?
                  AND lease_clock_domain = ?
                ORDER BY knowledge_time_utc DESC, revision_number DESC
                LIMIT 1
                """,
                (decision_instance_id, valid_as_of, knowledge_as_of, clock_domain),
            ).fetchone()
            return json.loads(row["verdict_json"]) if row else None

    def query_revision_chain(self, leaf_verdict_hash: str) -> list[dict[str, Any]]:
        """Traverse previous_verdict_hash pointers from leaf back to initial revision."""
        chain: list[dict[str, Any]] = []
        current_hash: Optional[str] = leaf_verdict_hash

        with self._get_connection() as conn:
            cursor = conn.cursor()
            while current_hash:
                cursor.execute("""
                    SELECT previous_verdict_hash, verdict_json
                    FROM persisted_verdicts
                    WHERE verdict_hash = ?
                """, (current_hash,))
                row = cursor.fetchone()
                if not row:
                    break
                v_obj = json.loads(row["verdict_json"])
                chain.append(v_obj)
                current_hash = row["previous_verdict_hash"]

        return list(reversed(chain))  # Return in chronological order H1 -> H2 -> ...

    @staticmethod
    def is_verdict_lease_valid(verdict: dict[str, Any], query_time_utc: str) -> bool:
        """Verify whether the consumer lease on this verdict is active.
        
        If expired, downstream consumer must treat verdict as stale/UNKNOWN.
        """
        lease_until = verdict.get("lease_until")
        if not lease_until:
            return True
        return query_time_utc <= lease_until

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
            TOPIC_OUT_DATABASE, TOPIC_SIDE_OVERFLOW,
            group_id=f"{GROUP_PERSISTENCE}-{self.run_id}" if self.run_id else GROUP_PERSISTENCE,
            bootstrap_servers=self.bootstrap_servers,
            auto_offset_reset="latest" if self.run_id else "earliest",
            consumer_timeout_ms=500,
        )

        logger.info("Persistence service consuming from %s", TOPIC_OUT_DATABASE)
        while self.is_running:
            try:
                records = consumer.poll_records(timeout_ms=500, max_records=20)
                for msg, metadata in records:
                    if metadata.get("topic") == TOPIC_SIDE_OVERFLOW:
                        self.persist_overflow(msg, metadata)
                    else:
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
