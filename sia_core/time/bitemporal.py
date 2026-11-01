"""CALIBRE SIA v2.2 - Bitemporal History and Query Engine per CAL-DOC-007 §2.12."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple


@dataclass(frozen=True, slots=True)
class BitemporalRecord:
    """Immutable bitemporal verdict record in append-only history."""
    decision_instance_id: str
    valid_window_start: str
    valid_window_end: str
    knowledge_time: str
    revision_number: int
    verdict: Dict[str, Any]


class BitemporalIndex:
    """In-memory append-only bitemporal index supporting As-of-K queries."""

    def __init__(self) -> None:
        self._records: List[BitemporalRecord] = []

    def append(self, record: BitemporalRecord) -> None:
        self._records.append(record)

    def as_of_k(
        self,
        decision_instance_id: str,
        knowledge_time_k: str,
    ) -> Optional[BitemporalRecord]:
        """Query: What did CALIBRE conclude about decision D, as known at time K?
        Returns the latest revision with knowledge_time <= K.
        """
        candidates = [
            r for r in self._records
            if r.decision_instance_id == decision_instance_id
            and r.knowledge_time <= knowledge_time_k
        ]
        if not candidates:
            return None
        # Highest revision number
        return max(candidates, key=lambda r: (r.revision_number, r.knowledge_time))

    def revision_chain(
        self,
        decision_instance_id: str,
    ) -> List[BitemporalRecord]:
        """Query: How did the verdict for decision D change over time?
        Returns full revision chain ordered by revision_number.
        """
        chain = [r for r in self._records if r.decision_instance_id == decision_instance_id]
        return sorted(chain, key=lambda r: r.revision_number)
