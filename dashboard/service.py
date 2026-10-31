"""CALIBRE Dashboard and Live Event API Gateway.

Consumes from calibre.out.dashboard and serves REST/SSE endpoints for pipeline monitoring,
recent event traces, and operational health.
"""
from __future__ import annotations

import collections
import json
import logging
import threading
import time
from typing import Any, Optional

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse, StreamingResponse

from calibre_core.contracts import FlinkOutputEnvelope, utc_now
from calibre_core.health import ComponentHealth, HealthStatus, SystemHealthReport
from calibre_core.kafka_bus import CalibreConsumer
from calibre_core.topics import TOPIC_OUT_DASHBOARD, GROUP_DASHBOARD

logger = logging.getLogger("calibre.dashboard")


class DashboardService:
    """Gateway aggregating live operational pipeline telemetry and serving HTTP visibility APIs."""

    def __init__(
        self,
        bootstrap_servers: Optional[str] = None,
        max_buffer_size: int = 200,
    ) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.max_buffer_size = max_buffer_size
        self.recent_events: collections.deque[dict[str, Any]] = collections.deque(maxlen=max_buffer_size)
        self.events_by_correlation: dict[str, list[dict[str, Any]]] = collections.defaultdict(list)
        self.health = ComponentHealth(name="DashboardGateway")
        self.is_running = False
        self._worker_thread: Optional[threading.Thread] = None
        self.received_count = 0
        self.app = create_dashboard_app(self)

    def record_incoming_event(self, event_data: dict[str, Any]) -> None:
        """Store in-memory for real-time dashboard display and correlation tracing."""
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
        return list(self.recent_events)[:limit]

    def get_trace(self, correlation_id: str) -> list[dict[str, Any]]:
        return self.events_by_correlation.get(correlation_id, [])

    def _stream_loop(self) -> None:
        """Continuous consumer loop for calibre.out.dashboard."""
        consumer = CalibreConsumer(
            TOPIC_OUT_DASHBOARD,
            group_id=GROUP_DASHBOARD,
            bootstrap_servers=self.bootstrap_servers,
            auto_offset_reset="earliest",
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


def create_dashboard_app(dashboard_service: Optional[DashboardService] = None) -> FastAPI:
    """Create FastAPI application for CALIBRE operations and trace visibility."""
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
    def recent_events(limit: int = 20) -> list[dict[str, Any]]:
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

    return app
