"""
CALIBRE – Kafka Topic Management
==================================
Centralised topic creation, registry, and cluster metadata inspection.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Any, Dict, List

# Ensure parent and calibre roots are in sys.path
_ROOT = Path(__file__).resolve().parent.parent
_WORKSPACE = _ROOT.parent
if str(_WORKSPACE) not in sys.path:
    sys.path.insert(0, str(_WORKSPACE))
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

try:
    from kafka import KafkaAdminClient
    from kafka.admin import NewTopic
    from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable
except ImportError:
    KafkaAdminClient = None  # type: ignore
    NewTopic = None  # type: ignore
    TopicAlreadyExistsError = Exception
    NoBrokersAvailable = Exception

try:
    from calibre.config.settings import settings
except ImportError:
    from config.settings import settings

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Topic registry – single source of truth (§5 FR-KAFKA-002 & Architecture)
# ---------------------------------------------------------------------------
TOPICS: list[dict] = [
    # Canonical CALIBRE backbone topics
    {"name": "calibre.raw.telemetry",           "partitions": 10, "replication": 1},
    {"name": "calibre.normalized.telemetry",    "partitions": 10, "replication": 1},
    {"name": "calibre.race.events",             "partitions": 3,  "replication": 1},
    {"name": "calibre.weather",                 "partitions": 1,  "replication": 1},
    {"name": "calibre.sensor.integrity",        "partitions": 3,  "replication": 1},
    {"name": "calibre.reference.performance",   "partitions": 3,  "replication": 1},
    {"name": "calibre.performance.diagnostics", "partitions": 3,  "replication": 1},
    {"name": "calibre.strategy.predictions",    "partitions": 3,  "replication": 1},
    {"name": "calibre.agent.decisions",         "partitions": 3,  "replication": 1},
    {"name": "calibre.alerts",                  "partitions": 3,  "replication": 1},
    {"name": "calibre.deadletter",              "partitions": 3,  "replication": 1},

    # Compatibility topics
    {"name": "f1.raw.telemetry",                "partitions": 10, "replication": 1},
    {"name": "f1.raw.timing",                   "partitions": 5,  "replication": 1},
    {"name": "f1.raw.race_control",             "partitions": 3,  "replication": 1},
    {"name": "f1.verdicts",                     "partitions": 3,  "replication": 1},
    {"name": "f1.integrity.alerts",             "partitions": 3,  "replication": 1},
]


def ensure_topics(bootstrap_servers: str | None = None) -> bool:
    """Create all registered Kafka topics (idempotent). Returns True if successful."""
    if KafkaAdminClient is None:
        logger.warning("kafka-python not installed; cannot ensure topics.")
        return False

    servers = bootstrap_servers or settings.kafka_bootstrap_servers
    try:
        admin = KafkaAdminClient(
            bootstrap_servers=servers,
            client_id="calibre-admin",
            request_timeout_ms=5000,
        )
    except Exception as exc:
        logger.warning("Could not connect to Kafka admin client at %s: %s", servers, exc)
        return False

    new_topics = [
        NewTopic(
            name=t["name"],
            num_partitions=t["partitions"],
            replication_factor=t["replication"],
        )
        for t in TOPICS
    ]
    created = 0
    for topic in new_topics:
        try:
            admin.create_topics([topic])
            created += 1
            logger.info("Created topic: %s", topic.name)
        except TopicAlreadyExistsError:
            logger.debug("Topic already exists: %s", topic.name)
        except Exception as e:
            logger.debug("Topic creation note for %s: %s", topic.name, e)
    admin.close()
    logger.info("Kafka topic verification complete (%d newly created).", created)
    return True


def get_topic_metadata(bootstrap_servers: str | None = None) -> Dict[str, Any]:
    """Inspects Kafka cluster and returns real live topic and broker details."""
    servers = bootstrap_servers or settings.kafka_bootstrap_servers
    if KafkaAdminClient is None:
        return {
            "connected": False,
            "broker": servers,
            "error": "kafka-python library not available",
            "topics": [],
        }

    try:
        admin = KafkaAdminClient(
            bootstrap_servers=servers,
            client_id="calibre-admin-inspect",
            request_timeout_ms=4000,
        )
        raw_topics = admin.list_topics()
        topic_details = []
        for t in sorted(raw_topics):
            topic_details.append({
                "topic": t,
                "is_calibre": t.startswith("calibre.") or t.startswith("f1."),
            })
        admin.close()
        return {
            "connected": True,
            "broker": servers,
            "topics_count": len(topic_details),
            "topics": topic_details,
            "error": None,
        }
    except Exception as ex:
        return {
            "connected": False,
            "broker": servers,
            "topics_count": 0,
            "topics": [],
            "error": str(ex),
        }


# Allow `python -m calibre.streaming.kafka_topics` to bootstrap topics.
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    try:
        ok = ensure_topics()
        if ok:
            print("[OK] All Kafka topics ensured successfully on broker.")
        else:
            print("[ERROR] Could not ensure topics (broker unreachable or library missing).")
            sys.exit(1)
    except Exception as exc:
        print(f"[ERROR] Could not create topics: {exc}", file=sys.stderr)
        sys.exit(1)
