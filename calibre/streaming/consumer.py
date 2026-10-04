"""
CALIBRE – Kafka Consumer
=========================
Async consumer that reads raw events and feeds the processing pipeline.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import AsyncIterator, Callable, Optional

from aiokafka import AIOKafkaConsumer

from config.settings import settings

logger = logging.getLogger(__name__)


async def consume(
    topics: list[str],
    group_id: str = "calibre-processor",
    handler: Optional[Callable] = None,
) -> AsyncIterator[dict]:
    """
    Consume messages from *topics* and yield deserialised dicts.

    If *handler* is provided it is called for every message instead of
    yielding – useful for fire-and-forget pipeline wiring.
    """
    consumer = AIOKafkaConsumer(
        *topics,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=group_id,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
    )
    await consumer.start()
    logger.info("Consumer started on topics=%s group=%s", topics, group_id)
    try:
        async for msg in consumer:
            event = msg.value
            if handler:
                await handler(event, topic=msg.topic)
            else:
                yield event
    finally:
        await consumer.stop()
