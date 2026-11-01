"""CALIBRE SIA v2.2 - Liveness Guard and Origin Health per CAL-DOC-007 §2.8.1."""
from __future__ import annotations

from typing import Dict, Optional, Sequence
from ..models.evidence import GuardFact


class LivenessGuard:
    """Tracks origin health state from durable GuardFacts."""

    def __init__(self) -> None:
        # origin_instance_id -> GuardFact
        self._origin_health: Dict[str, GuardFact] = {}

    def process_guard_fact(self, fact: GuardFact) -> None:
        """Update tracked health state for an origin instance."""
        existing = self._origin_health.get(fact.origin_instance_id)
        if existing is None or fact.epoch >= existing.epoch:
            self._origin_health[fact.origin_instance_id] = fact

    def is_origin_stalled(self, origin_instance_id: str) -> bool:
        """Check if an origin instance is currently marked STALLED."""
        fact = self._origin_health.get(origin_instance_id)
        if fact:
            return fact.status == "STALLED"
        return False

    def get_latest_guard_fact(self, origin_instance_id: str) -> Optional[GuardFact]:
        return self._origin_health.get(origin_instance_id)
