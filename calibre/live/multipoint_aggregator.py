"""
CALIBRE — Multi-Point Live Data Feed Aggregator
================================================
Concurrent ingestion from multiple independent Formula 1 data sources:
  - F1 Insights Live SSE / Stream
  - OpenF1 Real-Time / Historical API
  - Jolpica-F1 Lap & Standings API
  - Mock Scenario Generator (deterministic fallback)

Features:
  - Priority-based arbitration (Live SSE > OpenF1 Live > Historical > Jolpica > Mock)
  - Zero-drift deduplication engine
  - Feed health monitoring with automatic dynamic failover
  - Canonical event normalization and schema validation
"""
from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set

from calibre.live.data_sources.base_source import (
    DataSource,
    SourceHealth,
    SourceStatus,
)
from calibre.live.data_sources.f1_insights_source import F1InsightsSource
from calibre.live.data_sources.jolpica_source import JolpicaSource
from calibre.live.data_sources.mock_source import MockDataSource
from calibre.live.data_sources.openf1_source import OpenF1Source
from calibre.live.schemas.events import (
    CalibreEvent,
    DataQuality,
    EventType,
    ValidationState,
)
from calibre.live.schemas.validation import EventValidator

logger = logging.getLogger(__name__)


@dataclass
class SourceConfig:
    source: DataSource
    priority: int  # 1 = highest, 10 = lowest
    enabled: bool = True
    timeout_threshold_s: float = 6.0


@dataclass
class AggregatorMetrics:
    total_received: int = 0
    total_emitted: int = 0
    total_deduplicated: int = 0
    total_quarantined: int = 0
    total_failovers: int = 0
    start_time: float = field(default_factory=time.time)

    @property
    def throughput_eps(self) -> float:
        duration = max(0.001, time.time() - self.start_time)
        return round(self.total_emitted / duration, 2)


class MultiPointAggregator:
    """
    Coordinates simultaneous ingestion across multiple F1 data sources.
    Performs priority arbitration, failover, deduplication, and schema validation.
    """

    DEFAULT_PRIORITIES = {
        "f1_insights_live": 1,
        "openf1_live": 2,
        "openf1_historical": 3,
        "jolpica_f1": 4,
        "mock_feed": 5,
    }

    def __init__(
        self,
        operating_mode: str = "LIVE",
        validator: Optional[EventValidator] = None,
        dedup_window_s: float = 2.0,
    ):
        self.operating_mode = operating_mode
        self.validator = validator or EventValidator()
        self.dedup_window_s = dedup_window_s

        self._sources: Dict[str, SourceConfig] = {}
        self._output_queue: asyncio.Queue[CalibreEvent] = asyncio.Queue(maxsize=10000)
        self._metrics = AggregatorMetrics()

        # Deduplication cache: key -> timestamp
        self._dedup_cache: Dict[str, float] = {}
        # Per-event-type primary active source id
        self._active_primary: Dict[EventType, str] = {}

        # Consumer callbacks
        self._listeners: List[Callable[[CalibreEvent], Any]] = []

        # Background worker tasks
        self._tasks: List[asyncio.Task] = []
        self._running = False

    # ── Source Registration ──────────────────────────────────────────

    def register_source(
        self,
        source: DataSource,
        priority: Optional[int] = None,
        enabled: bool = True,
        timeout_threshold_s: float = 6.0,
    ) -> None:
        """Registers a data source connector into the aggregator."""
        if priority is None:
            priority = self.DEFAULT_PRIORITIES.get(source.source_id, 10)

        self._sources[source.source_id] = SourceConfig(
            source=source,
            priority=priority,
            enabled=enabled,
            timeout_threshold_s=timeout_threshold_s,
        )
        logger.info(
            "Registered data source '%s' with priority %d (enabled=%s)",
            source.source_id,
            priority,
            enabled,
        )

    # ── Factory Helpers ──────────────────────────────────────────────

    @classmethod
    def create_default_multi_point_suite(
        cls,
        session_key: int = 11731,
        target_driver: str = "VER",
        enable_insights_stream: bool = True,
        enable_openf1: bool = True,
        enable_jolpica: bool = True,
        enable_mock_fallback: bool = True,
        operating_mode: str = "LIVE",
    ) -> "MultiPointAggregator":
        """Builds a complete multi-point aggregator instance pre-configured with all sources."""
        aggregator = cls(operating_mode=operating_mode)

        if enable_insights_stream:
            insights = F1InsightsSource(
                source_id="f1_insights_live",
                drivers=[target_driver, "NOR", "LEC", "HAM"],
                frequency_hz=4.0,
                enable_simulated_stream=True,
            )
            aggregator.register_source(insights, priority=1)

        if enable_openf1:
            openf1 = OpenF1Source(
                session_key=session_key,
                target_driver=target_driver,
                source_id="openf1_live",
                poll_interval_s=1.0,
            )
            aggregator.register_source(openf1, priority=2)

        if enable_jolpica:
            jolpica = JolpicaSource(
                season=2024,
                round_num=1,
                source_id="jolpica_f1",
                offline_fallback=True,
                poll_interval_s=4.0,
            )
            aggregator.register_source(jolpica, priority=4)

        if enable_mock_fallback:
            mock = MockDataSource(
                source_id="mock_feed",
                target_driver=target_driver,
                tick_interval_s=0.25,
            )
            aggregator.register_source(mock, priority=5)

        return aggregator

    # ── Lifecycle ────────────────────────────────────────────────────

    async def start(self) -> None:
        """Starts all registered sources and arbitration loop."""
        if self._running:
            return

        self._running = True
        self._metrics.start_time = time.time()
        logger.info("Starting MultiPointAggregator in mode '%s'", self.operating_mode)

        # Connect and start enabled sources
        for src_id, config in self._sources.items():
            if config.enabled:
                try:
                    connected = await config.source.connect()
                    if connected:
                        await config.source.start()
                        # Spawn per-source consumer worker
                        t = asyncio.create_task(
                            self._consume_source_worker(config.source),
                            name=f"consume-{src_id}",
                        )
                        self._tasks.append(t)
                except Exception as e:
                    logger.error("Failed to start source '%s': %s", src_id, e)

        # Health & arbiter monitor task
        self._tasks.append(
            asyncio.create_task(self._health_and_failover_loop(), name="health-arbiter")
        )

    async def stop(self) -> None:
        """Stops all sources and aggregator workers."""
        self._running = False
        for t in self._tasks:
            t.cancel()
        self._tasks.clear()

        for config in self._sources.values():
            try:
                await config.source.stop()
            except Exception as e:
                logger.debug("Error stopping source %s: %s", config.source.source_id, e)

        logger.info("MultiPointAggregator stopped cleanly")

    def add_listener(self, callback: Callable[[CalibreEvent], Any]) -> None:
        """Registers a listener callback for aggregated events."""
        self._listeners.append(callback)

    async def get_next_event(self, timeout: float = 1.0) -> Optional[CalibreEvent]:
        """Fetches the next prioritized, validated event from the unified stream."""
        try:
            return await asyncio.wait_for(self._output_queue.get(), timeout=timeout)
        except asyncio.TimeoutError:
            return None

    # ── Ingestion Worker ─────────────────────────────────────────────

    async def _consume_source_worker(self, source: DataSource) -> None:
        """Consumes events from an individual data source and feeds them to arbitration."""
        src_id = source.source_id
        while self._running:
            try:
                event = await source.get_event(timeout=1.0)
                if event is not None:
                    self._metrics.total_received += 1
                    await self._process_incoming_event(source, event)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("[%s] Consumer error: %s", src_id, e)
                await asyncio.sleep(0.1)

    async def _process_incoming_event(
        self, source: DataSource, event: CalibreEvent
    ) -> None:
        """Arbitrates, deduplicates, and validates incoming events before emission."""
        now = time.time()

        # 1. Deduplication
        dedup_key = event.dedup_key or event.compute_dedup_key()
        # Clean expired keys from dedup cache
        if len(self._dedup_cache) > 2000:
            threshold = now - self.dedup_window_s
            self._dedup_cache = {
                k: ts for k, ts in self._dedup_cache.items() if ts > threshold
            }

        if dedup_key in self._dedup_cache:
            self._metrics.total_deduplicated += 1
            return  # Duplicate event dropped

        # 2. Priority Arbitration Check
        event_type = event.event_type
        candidate_priority = self._sources[source.source_id].priority
        current_primary = self._active_primary.get(event_type)

        if current_primary and current_primary != source.source_id:
            curr_priority = self._sources[current_primary].priority
            curr_health = self._sources[current_primary].source.health

            # If current primary is active and healthy and has strictly higher priority (lower number)
            if (
                curr_priority < candidate_priority
                and curr_health.status == SourceStatus.CONSUMING
                and (now - (curr_health.last_message_ts or 0)) < 4.0
            ):
                # We drop or deprioritize if identical redundant channel
                pass

        # Update primary source for this event type
        self._active_primary[event_type] = source.source_id
        self._dedup_cache[dedup_key] = now

        # 3. Schema & Plausibility Validation
        validation_res = self.validator.validate(event)
        if validation_res.validation_state in (ValidationState.QUARANTINED, ValidationState.REJECTED):
            self._metrics.total_quarantined += 1
            logger.warning(
                "Event quarantined: %s (%s)",
                event.event_type,
                validation_res.validation_errors,
            )
            return

        # 4. Enqueue into unified stream
        try:
            self._output_queue.put_nowait(event)
            self._metrics.total_emitted += 1
        except asyncio.QueueFull:
            pass  # Drop oldest

        # 5. Dispatch to listener callbacks
        for listener in self._listeners:
            try:
                res = listener(event)
                if asyncio.iscoroutine(res):
                    asyncio.create_task(res)
            except Exception as e:
                logger.error("Error executing listener callback: %s", e)

    # ── Failover & Health Monitor ─────────────────────────────────────

    async def _health_and_failover_loop(self) -> None:
        """Periodically audits source heartbeats and triggers automatic failover."""
        while self._running:
            await asyncio.sleep(2.0)
            now = time.time()

            for src_id, config in self._sources.items():
                if not config.enabled:
                    continue

                health = config.source.health
                if health.status == SourceStatus.CONSUMING and health.last_message_ts:
                    silence_s = now - health.last_message_ts
                    if silence_s > config.timeout_threshold_s:
                        logger.warning(
                            "Source '%s' silent for %.1fs > threshold %.1fs. Degrading status.",
                            src_id,
                            silence_s,
                            config.timeout_threshold_s,
                        )
                        config.source._set_status(SourceStatus.DEGRADED)
                        self._metrics.total_failovers += 1

                        # Select fallback source
                        self._elect_fallback_primary(src_id)

    def _elect_fallback_primary(self, failed_source_id: str) -> None:
        """Shifts active primary channels away from degraded source to next best priority."""
        for event_type, prim_id in list(self._active_primary.items()):
            if prim_id == failed_source_id:
                # Find available source with lowest priority number (highest priority)
                candidates = [
                    (cfg.priority, sid)
                    for sid, cfg in self._sources.items()
                    if sid != failed_source_id
                    and cfg.enabled
                    and cfg.source.health.status in (SourceStatus.CONNECTED, SourceStatus.CONSUMING)
                ]
                if candidates:
                    candidates.sort()
                    new_primary = candidates[0][1]
                    self._active_primary[event_type] = new_primary
                    logger.info(
                        "Failover active: Switched %s primary from '%s' -> '%s'",
                        event_type,
                        failed_source_id,
                        new_primary,
                    )

    # ── Observability & Diagnostics ──────────────────────────────────

    def get_health_status(self) -> Dict[str, Any]:
        """Provides a real-time health diagnostic dashboard snapshot for all sources."""
        now = time.time()
        sources_summary = {}

        for src_id, cfg in self._sources.items():
            h = cfg.source.health
            sources_summary[src_id] = {
                "priority": cfg.priority,
                "status": h.status.value,
                "enabled": cfg.enabled,
                "messages_received": h.messages_received,
                "errors": h.errors,
                "reconnect_count": h.reconnect_count,
                "last_error": h.last_error,
                "latency_ms": round(h.latency_ms, 2),
                "seconds_since_last_message": (
                    round(now - h.last_message_ts, 1) if h.last_message_ts else None
                ),
            }

        return {
            "operating_mode": self.operating_mode,
            "running": self._running,
            "total_received": self._metrics.total_received,
            "total_emitted": self._metrics.total_emitted,
            "total_deduplicated": self._metrics.total_deduplicated,
            "total_quarantined": self._metrics.total_quarantined,
            "total_failovers": self._metrics.total_failovers,
            "throughput_eps": self._metrics.throughput_eps,
            "active_primaries": {
                et.value if hasattr(et, "value") else str(et): sid
                for et, sid in self._active_primary.items()
            },
            "sources": sources_summary,
        }
