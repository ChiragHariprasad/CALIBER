"""CALIBRE Kafka Topic Catalogue and Consumer Group Definitions per CAL-DOC-004."""
from __future__ import annotations

import re

# Topic Prefixes
TOPIC_RAW_PREFIX = "calibre.raw"
TOPIC_EVIDENCE_PREFIX = "calibre.evidence"

# Specific Standard Topics per Contract
TOPIC_CORE_BUNDLE = "calibre.core.bundle"
TOPIC_CORE_INTEGRITY = "calibre.core.integrity"

# Flink Downstream Output Topics
TOPIC_OUT_DATABASE = "calibre.out.database"
TOPIC_OUT_DASHBOARD = "calibre.out.dashboard"
TOPIC_OUT_ANALYTICS = "calibre.out.analytics"

# Downstream Analytics & Alert Topics
TOPIC_ANALYTICS_METRICS = "calibre.analytics.metrics"
TOPIC_ALERTS = "calibre.alerts"
TOPIC_REVIEW_DISPOSITIONS = "calibre.review.dispositions"

# Dead-Letter Topic
TOPIC_DLQ = "calibre.dlq"

# Consumer Group IDs (Mandatory: independent processing services must NOT share a consumer group)
GROUP_PROCESSOR_PREFIX = "calibre-agent-processor-"
GROUP_FLINK_CORE = "calibre-flink-core-group"
GROUP_PERSISTENCE = "calibre-persistence-sink-group"
GROUP_DASHBOARD = "calibre-dashboard-gateway-group"
GROUP_ANALYTICS = "calibre-analytics-service-group"


def sanitize_topic_part(value: str) -> str:
    """Sanitize a string for safe Kafka topic naming (lowercase, alphanumeric, dot, underscore, dash)."""
    cleaned = re.sub(r"[^a-zA-Z0-9_\-\.]", "_", value.strip().lower())
    return re.sub(r"_+", "_", cleaned).strip("._")


def raw_topic(provider: str, feed: str) -> str:
    """Build canonical topic for raw incoming messages.
    
    Example: raw_topic('pitwall', 'weatherdata') -> 'calibre.raw.pitwall.weatherdata'
             raw_topic('tracinginsights', 'laptimes') -> 'calibre.raw.tracinginsights.laptimes'
             raw_topic('mock', 'telemetry') -> 'calibre.raw.mock.telemetry'
    """
    clean_provider = sanitize_topic_part(provider)
    clean_feed = sanitize_topic_part(feed)
    return f"{TOPIC_RAW_PREFIX}.{clean_provider}.{clean_feed}"


def evidence_topic(domain: str) -> str:
    """Build canonical topic for agent-normalized evidence.
    
    Example: evidence_topic('vehicle') -> 'calibre.evidence.vehicle'
             evidence_topic('incident') -> 'calibre.evidence.incident'
             evidence_topic('environment') -> 'calibre.evidence.environment'
             evidence_topic('timing') -> 'calibre.evidence.timing'
             evidence_topic('system') -> 'calibre.evidence.system'
    """
    clean_domain = sanitize_topic_part(domain)
    return f"{TOPIC_EVIDENCE_PREFIX}.{clean_domain}"


def all_standard_topics() -> list[str]:
    """List of all base topics to ensure pre-creation on broker startup."""
    return [
        f"{TOPIC_RAW_PREFIX}.pitwall.all",
        f"{TOPIC_RAW_PREFIX}.tracinginsights.all",
        f"{TOPIC_RAW_PREFIX}.tracinginsights.rcm",
        f"{TOPIC_RAW_PREFIX}.mock.telemetry",
        f"{TOPIC_RAW_PREFIX}.mock.rcm",
        f"{TOPIC_RAW_PREFIX}.mock.weather",
        f"{TOPIC_RAW_PREFIX}.mock.timing",
        f"{TOPIC_RAW_PREFIX}.mock.system",
        f"{TOPIC_EVIDENCE_PREFIX}.vehicle",
        f"{TOPIC_EVIDENCE_PREFIX}.incident",
        f"{TOPIC_EVIDENCE_PREFIX}.environment",
        f"{TOPIC_EVIDENCE_PREFIX}.timing",
        f"{TOPIC_EVIDENCE_PREFIX}.system",
        TOPIC_CORE_BUNDLE,
        TOPIC_CORE_INTEGRITY,
        TOPIC_OUT_DATABASE,
        TOPIC_OUT_DASHBOARD,
        TOPIC_OUT_ANALYTICS,
        TOPIC_ANALYTICS_METRICS,
        TOPIC_ALERTS,
        TOPIC_REVIEW_DISPOSITIONS,
        TOPIC_DLQ,
    ]

