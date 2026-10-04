"""
CALIBRE – Kafka Topic Management
==================================
Centralised topic creation and configuration.
"""

from __future__ import annotations

import logging
import sys

from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError

from config.settings import settings

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Topic registry – single source of truth
# ---------------------------------------------------------------------------
TOPICS: list[dict] = [
    # Raw ingestion topics – partitioned by driver_number
    {"name": "f1.raw.telemetry",     "partitions": 20, "replication": 1},
    {"name": "f1.raw.timing",        "partitions": 20, "replication": 1},
    {"name": "f1.raw.race_control",  "partitions": 3,  "replication": 1},
    # Derived / output topics
    {"name": "f1.verdicts",          "partitions": 3,  "replication": 1},
    {"name": "f1.integrity.alerts",  "partitions": 3,  "replication": 1},
]


def ensure_topics() -> None:
    """Create all registered Kafka topics (idempotent)."""
    admin = KafkaAdminClient(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        client_id="calibre-admin",
    )
    new_topics = [
        NewTopic(
            name=t["name"],
            num_partitions=t["partitions"],
            replication_factor=t["replication"],
        )
        for t in TOPICS
    ]
    for topic in new_topics:
        try:
            admin.create_topics([topic])
            logger.info("Created topic: %s", topic.name)
        except TopicAlreadyExistsError:
            logger.debug("Topic already exists: %s", topic.name)
    admin.close()


# Allow `python -m streaming.kafka_topics` to bootstrap topics.
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    try:
        ensure_topics()
        print("✓ All Kafka topics ensured.")
    except Exception as exc:
        print(f"✗ Could not create topics: {exc}", file=sys.stderr)
        sys.exit(1)
