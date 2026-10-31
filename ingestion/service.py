"""CALIBRE Source Ingestion Service.

Integrates:
1. Pitwall F1 Live SignalR client (unauthenticated live timing feeds)
2. TracingInsights historical session files
3. Local source log replay fixtures
4. Deterministic Mock Producer
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any, Callable, Optional

from calibre_core.contracts import RawSourceEnvelope, utc_now, stable_id, generate_uuid
from calibre_core.health import ComponentHealth, HealthStatus
from calibre_core.kafka_bus import CalibreProducer
from calibre_core.topics import raw_topic

logger = logging.getLogger("calibre.ingestion")

# Resolve pitwall source folder
BASE_DIR = Path(__file__).resolve().parent.parent
PITWALL_DIR = BASE_DIR / "pitwall_source"
SOURCE_LOGS_DIR = BASE_DIR / "main" / "source_logs"


class IngestionService:
    """Manages acquisition of raw motorsport evidence from live or replay sources and publishes to Kafka."""

    def __init__(
        self,
        bootstrap_servers: Optional[str] = None,
        source_mode: str = "REPLAY",  # LIVE, REPLAY, MOCK
        run_id: Optional[str] = None,
    ) -> None:
        self.source_mode = source_mode
        self.run_id = run_id or f"run-{int(asyncio.get_event_loop().time() if asyncio.get_event_loop_policy().get_event_loop().is_running() else 1)}"
        self.producer = CalibreProducer(bootstrap_servers=bootstrap_servers, client_id="calibre-ingestion-service")
        self.health = ComponentHealth(name="IngestionService")
        self.health.set_dependency("kafka", self.producer.is_connected)
        self.is_running = False

    def publish_raw_event(
        self,
        provider: str,
        feed: str,
        payload: Any,
        source_timestamp: Any = None,
        time_semantics: str = "UNKNOWN",
        session_info: Optional[dict[str, Any]] = None,
        scope: Optional[dict[str, Any]] = None,
        correlation_id: Optional[str] = None,
    ) -> RawSourceEnvelope:
        """Standard pipeline method: ingest, wrap, validate, and publish to Kafka raw topic."""
        corr_id = correlation_id or generate_uuid()
        now_str = utc_now()
        event_id = stable_id(provider, feed, corr_id, now_str)

        envelope = RawSourceEnvelope(
            event_id=event_id,
            correlation_id=corr_id,
            provider=provider,
            feed=feed,
            source_mode=self.source_mode,
            run_id=self.run_id,
            received_at_utc=now_str,
            source_time={
                "value": source_timestamp,
                "semantics": time_semantics,
                "unit": "seconds" if isinstance(source_timestamp, (int, float)) else "ISO-8601",
            },
            payload_format="JSON",
            payload=payload,
            session=session_info or {},
            scope=scope or {},
        )

        topic = raw_topic(provider, feed)
        success = self.producer.send(topic=topic, value=envelope, correlation_id=corr_id)
        
        self.health.record_activity(events_in=1, events_out=1 if success else 0, error=not success)
        if success:
            logger.debug("Ingested & published to %s: %s (corr=%s)", topic, event_id, corr_id)
        else:
            logger.error("Failed to publish raw event %s to %s", event_id, topic)

        return envelope

    def replay_log_file(self, file_path: Path, max_events: Optional[int] = None) -> int:
        """Replay historical JSONL file through the standard Kafka ingestion pathway."""
        logger.info("Replaying log file: %s", file_path)
        count = 0
        if not file_path.exists():
            logger.error("Log file not found: %s", file_path)
            return 0

        with file_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                    # Extract fields from recorded structure
                    provider = record.get("provider") or record.get("source") or "TRACINGINSIGHTS"
                    feed = record.get("feed") or record.get("topic") or "unknown_feed"
                    payload = record.get("payload") or record.get("data") or record.get("record") or record
                    src_ts = record.get("source_timestamp") or record.get("ts")
                    corr_id = record.get("correlation_id") or f"trace-hist-{count:05d}"
                    session_info = record.get("context") or {}

                    self.publish_raw_event(
                        provider=provider,
                        feed=feed,
                        payload=payload,
                        source_timestamp=src_ts,
                        session_info=session_info,
                        correlation_id=corr_id,
                    )
                    count += 1
                    if max_events and count >= max_events:
                        break
                except Exception as exc:
                    logger.error("Error reading log line %d in %s: %s", count, file_path, exc)
                    self.health.record_activity(error=True)

        logger.info("Replayed %d records from %s into Kafka", count, file_path.name)
        return count

    async def start_pitwall_live(self, topics: Optional[list[str]] = None) -> None:
        """Connect to live Pitwall SignalR stream and forward incoming messages to Kafka."""
        if PITWALL_DIR.exists():
            sys.path.insert(0, str(PITWALL_DIR))
        try:
            from signalr_client import F1LiveClient
            from topics import FREE_TOPICS
        except ImportError as exc:
            logger.error("Pitwall SignalR client not importable: %s", exc)
            self.health.set_dependency("pitwall", False)
            return

        selected_topics = topics or list(FREE_TOPICS)
        logger.info("Starting Pitwall live ingestion for %d topics", len(selected_topics))
        self.health.set_dependency("pitwall", True)
        self.is_running = True

        client = F1LiveClient(
            topics=selected_topics,
            no_auth=True,
            auto_decompress=True,
            auto_merge=True,
        )

        def on_signalr_event(topic: str, data: Any, timestamp: str) -> None:
            self.publish_raw_event(
                provider="PITWALL",
                feed=topic.lower(),
                payload=data,
                source_timestamp=timestamp,
                time_semantics="PROVIDER_TIMESTAMP",
            )

        client.on_all(on_signalr_event)
        try:
            await client.connect()
        except asyncio.CancelledError:
            pass
        finally:
            await client.stop()
            self.is_running = False

    def close(self) -> None:
        self.producer.close()
        self.health.is_ready = False

