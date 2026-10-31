"""CALIBRE Core Package — Event contracts, Kafka topics, and infrastructure primitives."""
from .contracts import (
    RawSourceEnvelope,
    EvidenceEvent,
    Finding,
    FlinkOutputEnvelope,
    DeadLetterEnvelope,
    utc_now,
    stable_id,
)
from .topics import (
    TOPIC_RAW_PREFIX,
    TOPIC_EVIDENCE_PREFIX,
    TOPIC_OUT_DATABASE,
    TOPIC_OUT_DASHBOARD,
    TOPIC_OUT_ANALYTICS,
    TOPIC_ANALYTICS_METRICS,
    TOPIC_ALERTS,
    TOPIC_DLQ,
    raw_topic,
    evidence_topic,
)
from .health import HealthStatus, ComponentHealth, SystemHealthReport

__all__ = [
    "RawSourceEnvelope",
    "EvidenceEvent",
    "Finding",
    "FlinkOutputEnvelope",
    "DeadLetterEnvelope",
    "utc_now",
    "stable_id",
    "TOPIC_RAW_PREFIX",
    "TOPIC_EVIDENCE_PREFIX",
    "TOPIC_OUT_DATABASE",
    "TOPIC_OUT_DASHBOARD",
    "TOPIC_OUT_ANALYTICS",
    "TOPIC_ANALYTICS_METRICS",
    "TOPIC_ALERTS",
    "TOPIC_DLQ",
    "raw_topic",
    "evidence_topic",
    "HealthStatus",
    "ComponentHealth",
    "SystemHealthReport",
]

