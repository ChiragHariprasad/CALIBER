"""
CALIBRE — Data Source Abstract Interface
==========================================
All source connectors (OpenF1, Jolpica, mock, replay, live-timing)
implement this interface for consistent lifecycle management.
"""
from __future__ import annotations

import abc
import asyncio
import enum
import logging
import time
from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Dict, List, Optional, Set

from calibre.live.schemas.events import CalibreEvent, EventType

logger = logging.getLogger(__name__)


class SourceStatus(str, enum.Enum):
    DISCONNECTED = "DISCONNECTED"
    CONNECTING = "CONNECTING"
    CONNECTED = "CONNECTED"
    CONSUMING = "CONSUMING"
    DEGRADED = "DEGRADED"
    FAILED = "FAILED"
    STOPPED = "STOPPED"


@dataclass
class SourceCapabilities:
    """Describes what event types and fields a source actually provides."""
    source_id: str
    supported_event_types: Set[EventType] = field(default_factory=set)
    available_fields: Dict[str, List[str]] = field(default_factory=dict)
    requires_auth: bool = False
    is_live_capable: bool = False
    is_historical_capable: bool = False
    max_polling_rate_hz: float = 1.0
    notes: str = ""


@dataclass
class SourceHealth:
    """Runtime health of a connected source."""
    status: SourceStatus = SourceStatus.DISCONNECTED
    last_message_ts: Optional[float] = None
    messages_received: int = 0
    errors: int = 0
    reconnect_count: int = 0
    last_error: Optional[str] = None
    latency_ms: float = 0.0

    @property
    def seconds_since_last_message(self) -> Optional[float]:
        if self.last_message_ts is None:
            return None
        return time.time() - self.last_message_ts


class DataSource(abc.ABC):
    """
    Abstract async data source interface.
    
    Every connector—OpenF1, Jolpica, mock, replay, live timing—implements
    this to provide a uniform lifecycle for the multipoint aggregator.
    """

    def __init__(self, source_id: str):
        self.source_id = source_id
        self._health = SourceHealth()
        self._event_queue: asyncio.Queue[CalibreEvent] = asyncio.Queue(maxsize=5000)
        self._running = False

    # ── Lifecycle ──────────────────────────────────────────────────

    @abc.abstractmethod
    async def connect(self) -> bool:
        """Establish connection. Returns True on success."""
        ...

    @abc.abstractmethod
    async def disconnect(self) -> None:
        """Clean disconnection."""
        ...

    @abc.abstractmethod
    async def start(self) -> None:
        """Start consuming/generating events."""
        ...

    @abc.abstractmethod
    async def stop(self) -> None:
        """Stop consumption."""
        ...

    # ── Capabilities ──────────────────────────────────────────────

    @abc.abstractmethod
    def get_capabilities(self) -> SourceCapabilities:
        """Return what this source can provide."""
        ...

    # ── Events ────────────────────────────────────────────────────

    async def get_event(self, timeout: float = 1.0) -> Optional[CalibreEvent]:
        """Get the next event from this source (non-blocking with timeout)."""
        try:
            return await asyncio.wait_for(self._event_queue.get(), timeout=timeout)
        except asyncio.TimeoutError:
            return None

    def try_get_event(self) -> Optional[CalibreEvent]:
        """Non-blocking event retrieval."""
        try:
            return self._event_queue.get_nowait()
        except asyncio.QueueEmpty:
            return None

    async def _emit(self, event: CalibreEvent) -> None:
        """Push an event to the output queue."""
        try:
            self._event_queue.put_nowait(event)
            self._health.messages_received += 1
            self._health.last_message_ts = time.time()
        except asyncio.QueueFull:
            pass  # Drop oldest—source is producing faster than consumer

    # ── Health ────────────────────────────────────────────────────

    @property
    def health(self) -> SourceHealth:
        return self._health

    @property
    def is_running(self) -> bool:
        return self._running

    def _set_status(self, status: SourceStatus) -> None:
        self._health.status = status

    def _record_error(self, error: str) -> None:
        self._health.errors += 1
        self._health.last_error = error
        logger.warning("[%s] Error: %s", self.source_id, error)

    # ── Retry Policy ──────────────────────────────────────────────

    async def _reconnect_loop(
        self,
        max_retries: int = 5,
        base_delay: float = 2.0,
        max_delay: float = 60.0,
    ) -> bool:
        """Exponential backoff reconnection."""
        for attempt in range(max_retries):
            delay = min(base_delay * (2 ** attempt), max_delay)
            logger.info("[%s] Reconnect attempt %d/%d in %.1fs",
                        self.source_id, attempt + 1, max_retries, delay)
            await asyncio.sleep(delay)

            try:
                success = await self.connect()
                if success:
                    self._health.reconnect_count += 1
                    return True
            except Exception as e:
                self._record_error(f"Reconnect failed: {e}")

        self._set_status(SourceStatus.FAILED)
        return False
