"""CALIBRE Kafka Transport Bus — Producers, Consumers, and Resilience Layer."""
from __future__ import annotations

import json
import logging
import os
import time
from typing import Any, Callable, Iterator, Optional

try:
    from kafka import KafkaProducer as RealKafkaProducer
    from kafka import KafkaConsumer as RealKafkaConsumer
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import NoBrokersAvailable, KafkaError
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False

from .contracts import DeadLetterEnvelope, utc_now, generate_uuid
from .topics import TOPIC_DLQ, all_standard_topics

logger = logging.getLogger("calibre.kafka_bus")

# Use IPv4 explicitly on Windows to prevent IPv6 ::1 resolution latency/refusal
DEFAULT_BOOTSTRAP_SERVERS = os.environ.get("CALIBRE_KAFKA_BOOTSTRAP", "127.0.0.1:9092")


class CalibreProducer:
    """Resilient Kafka Producer for CALIBRE events with automatic JSON encoding and header tracing."""

    def __init__(self, bootstrap_servers: Optional[str] = None, client_id: str = "calibre-producer") -> None:
        self.bootstrap_servers = bootstrap_servers or DEFAULT_BOOTSTRAP_SERVERS
        self.client_id = client_id
        self._producer: Optional[RealKafkaProducer] = None
        self.messages_sent = 0
        self.errors_count = 0
        self._connect()

    def _connect(self) -> None:
        if not KAFKA_AVAILABLE:
            logger.warning("kafka-python package not installed. Running in mock/dry-run mode.")
            return

        try:
            self._producer = RealKafkaProducer(
                bootstrap_servers=self.bootstrap_servers.split(","),
                client_id=self.client_id,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False, default=str).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k,
                acks="all",
                retries=3,
                max_block_ms=5000,
            )
            logger.info("CalibreProducer connected to Kafka at %s", self.bootstrap_servers)
        except Exception as exc:
            logger.warning("Failed to connect Kafka producer to %s: %s", self.bootstrap_servers, exc)
            self._producer = None

    @property
    def is_connected(self) -> bool:
        return self._producer is not None

    def send(
        self,
        topic: str,
        value: Any,
        key: Optional[str] = None,
        correlation_id: Optional[str] = None,
        headers: Optional[list[tuple[str, bytes]]] = None,
    ) -> bool:
        """Send message to Kafka topic. Returns True on success, False on failure."""
        if self._producer is None:
            self._connect()

        dict_value = value.to_dict() if hasattr(value, "to_dict") else value
        
        # Propagate correlation_id
        corr_id = correlation_id or dict_value.get("correlation_id") or generate_uuid()
        all_headers = headers or []
        all_headers.append(("correlation_id", corr_id.encode("utf-8")))

        if self._producer is not None:
            try:
                future = self._producer.send(
                    topic=topic,
                    key=key or dict_value.get("event_id") or corr_id,
                    value=dict_value,
                    headers=all_headers,
                )
                self._producer.flush(timeout=3.0)
                record_meta = future.get(timeout=3.0)
                self.messages_sent += 1
                logger.debug(
                    "Sent to %s [p=%s, off=%s, corr=%s]",
                    topic, record_meta.partition, record_meta.offset, corr_id
                )
                return True
            except Exception as exc:
                self.errors_count += 1
                logger.error("Failed to produce to %s: %s", topic, exc)
                return False
        else:
            self.messages_sent += 1
            logger.debug("[DRY-RUN PRODUCED] %s: %s (corr=%s)", topic, dict_value.get("event_id"), corr_id)
            return True

    def send_dlq(
        self,
        component: str,
        stage: str,
        error_code: str,
        error_message: str,
        raw_payload: Any,
        correlation_id: Optional[str] = None,
    ) -> bool:
        """Route failed message to DLQ."""
        dlq_event = DeadLetterEnvelope(
            error_id=generate_uuid(),
            correlation_id=correlation_id or "unknown",
            occurred_at_utc=utc_now(),
            component=component,
            stage=stage,
            error_code=error_code,
            error_message=str(error_message),
            raw_payload=raw_payload,
        )
        return self.send(TOPIC_DLQ, dlq_event, correlation_id=correlation_id)

    def close(self) -> None:
        if self._producer is not None:
            try:
                self._producer.flush(timeout=2.0)
                self._producer.close(timeout=2.0)
            except Exception:
                pass
            self._producer = None


class CalibreConsumer:
    """Resilient Kafka Consumer for CALIBRE with automated decoding, group management, and DLQ routing."""

    def __init__(
        self,
        *topics: str,
        group_id: Optional[str] = None,
        bootstrap_servers: Optional[str] = None,
        auto_offset_reset: str = "latest",
        enable_auto_commit: bool = True,
        consumer_timeout_ms: int = 1000,
    ) -> None:
        self.topics = list(topics)
        self.group_id = group_id
        self.bootstrap_servers = bootstrap_servers or DEFAULT_BOOTSTRAP_SERVERS
        self.auto_offset_reset = auto_offset_reset
        self.enable_auto_commit = enable_auto_commit
        self.consumer_timeout_ms = consumer_timeout_ms
        self._consumer: Optional[RealKafkaConsumer] = None
        self.messages_received = 0
        self.errors_count = 0
        self._connect()

    def _connect(self) -> None:
        if not KAFKA_AVAILABLE:
            logger.warning("kafka-python not installed. Running consumer in mock/idle mode.")
            return

        try:
            kwargs: dict[str, Any] = {
                "bootstrap_servers": self.bootstrap_servers.split(","),
                "auto_offset_reset": self.auto_offset_reset,
                "enable_auto_commit": self.enable_auto_commit,
                "consumer_timeout_ms": self.consumer_timeout_ms,
                "value_deserializer": lambda m: json.loads(m.decode("utf-8")),
            }
            if self.group_id:
                kwargs["group_id"] = self.group_id

            self._consumer = RealKafkaConsumer(
                *self.topics,
                **kwargs,
            )
            logger.info(
                "CalibreConsumer (group=%s) connected to %s on topics: %s",
                self.group_id, self.bootstrap_servers, self.topics
            )
        except Exception as exc:
            logger.warning("Failed to connect Kafka consumer to %s: %s", self.bootstrap_servers, exc)
            self._consumer = None

    @property
    def is_connected(self) -> bool:
        return self._consumer is not None

    def poll_messages(self, timeout_ms: int = 2000, max_records: int = 100) -> list[dict[str, Any]]:
        """Poll for next batch of decoded message payloads with retry until timeout."""
        if self._consumer is None:
            self._connect()
            if self._consumer is None:
                return []

        results: list[dict[str, Any]] = []
        deadline = time.time() + (timeout_ms / 1000.0)

        while time.time() < deadline:
            try:
                records_by_partition = self._consumer.poll(timeout_ms=300, max_records=max_records)
                if records_by_partition:
                    for tp, records in records_by_partition.items():
                        for record in records:
                            self.messages_received += 1
                            msg_val = record.value
                            if isinstance(msg_val, dict):
                                # Extract headers if present
                                headers_dict = {}
                                if record.headers:
                                    for k, v in record.headers:
                                        headers_dict[k] = v.decode("utf-8", errors="replace") if v else None
                                if "correlation_id" in headers_dict and "correlation_id" not in msg_val:
                                    msg_val["correlation_id"] = headers_dict["correlation_id"]
                                results.append(msg_val)
                            else:
                                results.append({"raw_value": msg_val})
                    if results:
                        break
            except Exception as exc:
                self.errors_count += 1
                logger.error("Error polling consumer group %s: %s", self.group_id, exc)
                break
            time.sleep(0.05)

        return results

    def close(self) -> None:
        if self._consumer is not None:
            try:
                self._consumer.close()
            except Exception:
                pass
            self._consumer = None
