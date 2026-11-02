"""CALIBRE Dashboard and Live Event API Gateway.

Consumes from calibre.out.dashboard and serves REST/SSE endpoints for pipeline monitoring,
recent event traces, and operational health.
"""
from __future__ import annotations

import collections
import asyncio
import json
import logging
import os
import threading
import time
from pathlib import Path
from typing import Any, Optional

from fastapi import FastAPI, Header, HTTPException, Query
from fastapi.responses import StreamingResponse

from calibre_core.contracts import FlinkOutputEnvelope, utc_now
from calibre_core.health import ComponentHealth, HealthStatus, SystemHealthReport
from calibre_core.kafka_bus import CalibreConsumer
from calibre_core.run_scope import belongs_to_run
from calibre_core.topics import TOPIC_OUT_DASHBOARD, GROUP_DASHBOARD

logger = logging.getLogger("calibre.dashboard")


class DashboardService:
    """Gateway aggregating live operational pipeline telemetry and serving HTTP visibility APIs."""

    def __init__(
        self,
        bootstrap_servers: Optional[str] = None,
        max_buffer_size: int = 200,
        run_id: Optional[str] = None,
    ) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.max_buffer_size = max_buffer_size
        self.run_id = run_id
        self.recent_events: collections.deque[dict[str, Any]] = collections.deque(maxlen=max_buffer_size)
        self.events_by_correlation: dict[str, list[dict[str, Any]]] = collections.defaultdict(list)
        self.health = ComponentHealth(name="DashboardGateway")
        self.is_running = False
        self._worker_thread: Optional[threading.Thread] = None
        self.received_count = 0
        self.app = create_dashboard_app(self)

    def record_incoming_event(self, event_data: dict[str, Any]) -> None:
        """Store in-memory for real-time dashboard display and correlation tracing."""
        if not belongs_to_run(event_data, self.run_id):
            return
        self.received_count += 1
        self.recent_events.appendleft(event_data)
        
        corr_id = event_data.get("correlation_id")
        if corr_id:
            self.events_by_correlation[corr_id].append(event_data)
            # Bound dictionary size
            if len(self.events_by_correlation) > 1000:
                oldest_key = next(iter(self.events_by_correlation))
                del self.events_by_correlation[oldest_key]

        self.health.record_activity(events_in=1, events_out=1)
        logger.debug("Dashboard received event (corr=%s)", corr_id)

    def get_events(self, limit: int = 50) -> list[dict[str, Any]]:
        return list(self.recent_events)[:max(0, min(limit, self.max_buffer_size))]

    def get_trace(self, correlation_id: str) -> list[dict[str, Any]]:
        return self.events_by_correlation.get(correlation_id, [])

    def _stream_loop(self) -> None:
        """Continuous consumer loop for calibre.out.dashboard."""
        consumer = CalibreConsumer(
            TOPIC_OUT_DASHBOARD,
            group_id=f"{GROUP_DASHBOARD}-{self.run_id}" if self.run_id else GROUP_DASHBOARD,
            bootstrap_servers=self.bootstrap_servers,
            auto_offset_reset="latest" if self.run_id else "earliest",
            consumer_timeout_ms=500,
        )

        logger.info("Dashboard gateway consuming from %s", TOPIC_OUT_DASHBOARD)
        while self.is_running:
            try:
                messages = consumer.poll_messages(timeout_ms=500, max_records=20)
                for msg in messages:
                    self.record_incoming_event(msg)
            except Exception as exc:
                logger.error("Dashboard consumer loop error: %s", exc)
                time.sleep(1.0)

        consumer.close()

    def start(self) -> None:
        self.is_running = True
        self.health.is_ready = True
        self._worker_thread = threading.Thread(
            target=self._stream_loop,
            name="calibre-dashboard-consumer",
            daemon=True,
        )
        self._worker_thread.start()
        logger.info("DashboardService started")

    def stop(self) -> None:
        self.is_running = False
        if self._worker_thread and self._worker_thread.is_alive():
            self._worker_thread.join(timeout=2.0)
        self.health.is_ready = False
        logger.info("DashboardService stopped")


def create_dashboard_app(
    dashboard_service: Optional[DashboardService] = None,
    persistence_db_path: Optional[str | Path] = None,
) -> FastAPI:
    """Create FastAPI application for CALIBRE operations and trace visibility."""
    db_path = Path(persistence_db_path or os.environ.get(
        "CALIBRE_PERSISTENCE_DB",
        Path(__file__).resolve().parent.parent / "data" / "calibre_persistence.db",
    ))
    app = FastAPI(
        title="CALIBRE Operational Dashboard API",
        version="0.1.0",
        description="Live status, trace inspection, and health observability for CALIBRE streaming backbone.",
    )
    
    svc = dashboard_service or DashboardService()

    @app.get("/")
    def index() -> dict[str, Any]:
        return {
            "system": "CALIBRE",
            "version": "0.1.0",
            "status": "OPERATIONAL",
            "docs_url": "/docs",
            "endpoints": [
                "/health",
                "/metrics",
                "/events/recent",
                "/trace/{correlation_id}",
            "/v1/health/live",
            "/v1/health/ready",
            "/v1/system/status",
            "/v1/stream/events",
            "/v1/sources",
            "/v1/assessments",
            "/v1/agents",
            "/v1/runs/{run_id}",
            "/v1/decisions/{decision_instance_id}",
            "/v1/decisions/{decision_instance_id}/at",
            ],
        }

    @app.get("/health")
    def health_check() -> dict[str, Any]:
        return svc.health.to_dict()

    @app.get("/metrics")
    def metrics() -> dict[str, Any]:
        return {
            "total_events_received": svc.received_count,
            "buffer_depth": len(svc.recent_events),
            "tracked_traces": len(svc.events_by_correlation),
            "last_activity_utc": svc.health.last_activity_utc,
            "errors": svc.health.errors_count,
        }

    @app.get("/events/recent")
    def recent_events(limit: int = Query(20, ge=0, le=200)) -> list[dict[str, Any]]:
        return svc.get_events(limit=limit)

    @app.get("/trace/{correlation_id}")
    def trace_by_id(correlation_id: str) -> dict[str, Any]:
        events = svc.get_trace(correlation_id)
        if not events:
            raise HTTPException(status_code=404, detail=f"No events found for correlation_id '{correlation_id}'")
        return {
            "correlation_id": correlation_id,
            "event_count": len(events),
            "trace_events": events,
        }

    @app.get("/v1/health/live")
    def liveness() -> dict[str, str]:
        return {"status": "LIVE", "service": "calibre-api"}

    @app.get("/v1/health/ready")
    def readiness() -> dict[str, Any]:
        ready = svc.is_running and svc.health.is_ready
        return {
            "status": "READY" if ready else "NOT_READY",
            "dashboard_consumer_running": svc.is_running,
            "health": svc.health.to_dict(),
        }

    @app.get("/v1/sources")
    def source_coverage() -> dict[str, Any]:
        import sqlite3
        if not db_path.exists():
            return {"sources": [], "database": str(db_path)}
        try:
            with sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=2.0) as conn:
                rows = conn.execute("""
                    SELECT provider, feed, count(*) AS persisted_events,
                           max(source_timestamp) AS latest_source_time,
                           max(persisted_at_utc) AS latest_persisted_at
                    FROM persisted_events GROUP BY provider, feed ORDER BY provider, feed
                """).fetchall()
        except sqlite3.Error as exc:
            raise HTTPException(status_code=503, detail=f"Source inventory unavailable: {exc}") from exc
        return {"sources": [dict(zip(("provider", "feed", "persisted_events", "latest_source_time", "latest_persisted_at"), row)) for row in rows], "database": str(db_path)}

    @app.get("/v1/assessments")
    def recent_assessments(limit: int = Query(50, ge=1, le=200)) -> dict[str, Any]:
        import sqlite3
        if not db_path.exists():
            return {"assessments": []}
        try:
            with sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=2.0) as conn:
                rows = conn.execute("""
                    SELECT v.verdict_json FROM persisted_verdicts AS v
                    WHERE v.rowid = (
                        SELECT candidate.rowid FROM persisted_verdicts AS candidate
                        WHERE candidate.decision_instance_id = v.decision_instance_id
                        ORDER BY candidate.knowledge_time_utc DESC, candidate.revision_number DESC
                        LIMIT 1
                    )
                    ORDER BY v.knowledge_time_utc DESC LIMIT ?
                """, (limit,)).fetchall()
        except sqlite3.Error as exc:
            raise HTTPException(status_code=503, detail=f"Assessment store unavailable: {exc}") from exc
        return {"assessments": [json.loads(row[0]) for row in rows]}

    @app.get("/v1/agents")
    def agent_receipts(run_id: str) -> dict[str, Any]:
        import sqlite3
        if not db_path.exists():
            return {"run_id": run_id, "agents": []}
        try:
            with sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=2.0) as conn:
                has_table = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='replay_agent_receipts'").fetchone()
                if not has_table:
                    return {"run_id": run_id, "agents": []}
                rows = conn.execute("""
                    SELECT processor_id, count(*) AS consumed_records,
                           sum(status='EMITTED') AS emitted_records,
                           sum(status='FILTERED') AS filtered_records,
                           sum(status='FAILED') AS failed_records,
                           sum(emitted_total) AS evidence_events,
                           sum(attempts) AS processing_attempts
                    FROM replay_agent_receipts WHERE run_id=? GROUP BY processor_id ORDER BY processor_id
                """, (run_id,)).fetchall()
        except sqlite3.Error as exc:
            raise HTTPException(status_code=503, detail=f"Agent receipts unavailable: {exc}") from exc
        fields = ("processor_id", "consumed_records", "emitted_records", "filtered_records", "failed_records", "evidence_events", "processing_attempts")
        return {"run_id": run_id, "agents": [dict(zip(fields, row)) for row in rows]}

    @app.get("/v1/runs/{run_id}")
    def run_summary(run_id: str) -> dict[str, Any]:
        import sqlite3
        if not db_path.exists():
            raise HTTPException(status_code=404, detail="Run summary database is not available")
        try:
            with sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=2.0) as conn:
                has_table = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='replay_run_summaries'").fetchone()
                row = conn.execute("SELECT status,summary_json FROM replay_run_summaries WHERE run_id=?", (run_id,)).fetchone() if has_table else None
        except sqlite3.Error as exc:
            raise HTTPException(status_code=503, detail=f"Run registry unavailable: {exc}") from exc
        if not row:
            raise HTTPException(status_code=404, detail="Run summary not found in the configured database")
        return {"run_id": run_id, "status": row[0], "summary": json.loads(row[1])}

    @app.get("/v1/system/status")
    def system_status() -> dict[str, Any]:
        result: dict[str, Any] = {
            "api": "READY",
            "dashboard": svc.health.to_dict(),
            "kafka": {"status": "UNKNOWN"},
            "flink": {"status": "UNKNOWN"},
            "persistence": {"status": "UNKNOWN", "database": str(db_path)},
        }
        try:
            from kafka.admin import KafkaAdminClient
            from calibre_core.kafka_bus import DEFAULT_BOOTSTRAP_SERVERS
            admin = KafkaAdminClient(
                bootstrap_servers=os.environ.get("CALIBRE_KAFKA_BOOTSTRAP", DEFAULT_BOOTSTRAP_SERVERS),
                client_id="calibre-api-status",
                request_timeout_ms=2500,
            )
            topics = sorted(topic for topic in admin.list_topics() if topic.startswith("calibre."))
            admin.close()
            result["kafka"] = {"status": "CONNECTED", "topic_count": len(topics), "topics": topics}
        except Exception as exc:
            result["kafka"] = {"status": "UNAVAILABLE", "error": str(exc)}
        try:
            import requests
            response = requests.get(os.environ.get("CALIBRE_FLINK_REST", "http://localhost:8081/overview"), timeout=2.5)
            result["flink"] = {"status": "CONNECTED" if response.ok else "UNAVAILABLE", "overview": response.json() if response.ok else None}
        except Exception as exc:
            result["flink"] = {"status": "UNAVAILABLE", "error": str(exc)}
        try:
            import sqlite3
            with sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=1.0) as conn:
                tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
                event_count = conn.execute("SELECT count(*) FROM persisted_events").fetchone()[0] if "persisted_events" in tables else 0
                verdict_count = conn.execute("SELECT count(*) FROM persisted_verdicts").fetchone()[0] if "persisted_verdicts" in tables else 0
            result["persistence"] = {"status": "CONNECTED", "event_rows": event_count, "verdict_rows": verdict_count, "database": str(db_path)}
        except Exception as exc:
            result["persistence"] = {"status": "UNAVAILABLE", "database": str(db_path), "error": str(exc)}
        return result

    @app.get("/v1/decisions/{decision_instance_id}")
    def decision_history(decision_instance_id: str) -> dict[str, Any]:
        import sqlite3
        if not db_path.exists():
            raise HTTPException(status_code=404, detail="Persistence database is not available")
        try:
            with sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=2.0) as conn:
                rows = conn.execute(
                    "SELECT verdict_json FROM persisted_verdicts WHERE decision_instance_id=? ORDER BY revision_number ASC",
                    (decision_instance_id,),
                ).fetchall()
        except sqlite3.Error as exc:
            raise HTTPException(status_code=503, detail=f"Verdict store unavailable: {exc}") from exc
        if not rows:
            raise HTTPException(status_code=404, detail="Decision instance not found")
        return {"decision_instance_id": decision_instance_id, "revision_count": len(rows), "revisions": [json.loads(row[0]) for row in rows]}

    @app.get("/v1/decisions/{decision_instance_id}/at")
    def decision_bitemporal(
        decision_instance_id: str,
        valid_time: str,
        knowledge_time: str,
        clock_domain: str = "LIVE_UTC",
    ) -> dict[str, Any]:
        import sqlite3
        if not db_path.exists():
            raise HTTPException(status_code=404, detail="Persistence database is not available")
        try:
            with sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=2.0) as conn:
                row = conn.execute(
                    """SELECT verdict_json FROM persisted_verdicts
                       WHERE decision_instance_id=? AND valid_time_utc<=? AND knowledge_time_utc<=?
                         AND lease_clock_domain=?
                       ORDER BY knowledge_time_utc DESC, revision_number DESC LIMIT 1""",
                    (decision_instance_id, valid_time, knowledge_time, clock_domain),
                ).fetchone()
        except sqlite3.Error as exc:
            raise HTTPException(status_code=503, detail=f"Verdict store unavailable: {exc}") from exc
        if not row:
            raise HTTPException(status_code=404, detail="No verdict was valid and known at the requested times")
        return {
            "decision_instance_id": decision_instance_id,
            "valid_time": valid_time,
            "knowledge_time": knowledge_time,
            "clock_domain": clock_domain,
            "verdict": json.loads(row[0]),
        }

    @app.get("/v1/stream/events")
    async def event_stream(last_event_id: Optional[str] = Header(None, alias="Last-Event-ID")) -> StreamingResponse:
        async def stream():
            cursor = last_event_id
            last_heartbeat = time.monotonic()
            while True:
                batch = list(reversed(svc.get_events(limit=svc.max_buffer_size)))
                ids = [str(event.get("event_id", "")) for event in batch]
                fresh = batch[ids.index(cursor) + 1:] if cursor in ids else batch
                for event in fresh:
                    cursor = str(event.get("event_id", "")) or cursor
                    yield f"id: {cursor or ''}\nevent: evidence\ndata: {json.dumps(event, default=str)}\n\n"
                if time.monotonic() - last_heartbeat >= 15.0:
                    yield ": keep-alive\n\n"
                    last_heartbeat = time.monotonic()
                await asyncio.sleep(0.5)
        return StreamingResponse(stream(), media_type="text/event-stream", headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

    return app
