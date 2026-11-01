"""CALIBRE SIA v2.2 - State Algebra and Maturity Models per CAL-DOC-007 §2.5.1, §2.6.4."""
from __future__ import annotations

from enum import Enum
from functools import total_ordering
from typing import Sequence


@total_ordering
class IntegrityState(str, Enum):
    """Evidence-admissibility total order per CAL-DOC-007 §2.5.1:
    DEGRADED < UNKNOWN < SUSPECT < HEALTHY
    """
    DEGRADED = "DEGRADED"
    UNKNOWN = "UNKNOWN"
    SUSPECT = "SUSPECT"
    HEALTHY = "HEALTHY"

    @property
    def rank(self) -> int:
        _order = {
            IntegrityState.DEGRADED: 0,
            IntegrityState.UNKNOWN: 1,
            IntegrityState.SUSPECT: 2,
            IntegrityState.HEALTHY: 3,
        }
        return _order[self]

    def __lt__(self, other: object) -> bool:
        if isinstance(other, IntegrityState):
            return self.rank < other.rank
        if isinstance(other, str):
            try:
                return self.rank < IntegrityState(other).rank
            except ValueError:
                return NotImplemented
        return NotImplemented

    def __le__(self, other: object) -> bool:
        if isinstance(other, IntegrityState):
            return self.rank <= other.rank
        if isinstance(other, str):
            try:
                return self.rank <= IntegrityState(other).rank
            except ValueError:
                return NotImplemented
        return NotImplemented

    def __gt__(self, other: object) -> bool:
        if isinstance(other, IntegrityState):
            return self.rank > other.rank
        if isinstance(other, str):
            try:
                return self.rank > IntegrityState(other).rank
            except ValueError:
                return NotImplemented
        return NotImplemented

    def __ge__(self, other: object) -> bool:
        if isinstance(other, IntegrityState):
            return self.rank >= other.rank
        if isinstance(other, str):
            try:
                return self.rank >= IntegrityState(other).rank
            except ValueError:
                return NotImplemented
        return NotImplemented

    def __eq__(self, other: object) -> bool:
        if isinstance(other, IntegrityState):
            return self.value == other.value
        if isinstance(other, str):
            return self.value == other
        return False

    def __hash__(self) -> int:
        return hash(self.value)


class ResolutionMaturity(str, Enum):
    """Resolution maturity stages per CAL-DOC-007 §2.6.4."""
    OPEN = "OPEN"
    STATE_RESOLVED = "STATE_RESOLVED"
    TUPLE_RESOLVED = "TUPLE_RESOLVED"
    REOPENED = "REOPENED"


class FinalityStatus(str, Enum):
    """Finality status per CAL-DOC-007 §2.6.4."""
    OPEN = "OPEN"
    FINAL = "FINAL"
    FINALITY_VIOLATION = "FINALITY_VIOLATION"


class DecisionMaturity(str, Enum):
    """Derived display maturity per CAL-DOC-007 §2.6.4 and CAL-DOC-004."""
    PROVISIONAL = "PROVISIONAL"
    FINAL = "FINAL"
    REVISED = "REVISED"


def and_state(*states: IntegrityState | str) -> IntegrityState:
    """Lattice AND operator: minimum over admissibility total order. Empty meet returns HEALTHY."""
    if not states:
        return IntegrityState.HEALTHY
    parsed = [IntegrityState(s) if isinstance(s, str) else s for s in states]
    return min(parsed)


def or_state(*states: IntegrityState | str) -> IntegrityState:
    """Lattice OR operator: maximum over admissibility total order. Empty join returns DEGRADED."""
    if not states:
        return IntegrityState.DEGRADED
    parsed = [IntegrityState(s) if isinstance(s, str) else s for s in states]
    return max(parsed)


def k_of_n_state(k: int, *states: IntegrityState | str) -> IntegrityState:
    """Lattice k-of-n operator: k-th largest state in the list."""
    if not states:
        raise ValueError("k_of_n_state requires at least one state")
    n = len(states)
    if not (1 <= k <= n):
        raise ValueError(f"k must satisfy 1 <= k <= {n}, got {k}")
    parsed = [IntegrityState(s) if isinstance(s, str) else s for s in states]
    # Sort descending: largest first
    sorted_desc = sorted(parsed, reverse=True)
    return sorted_desc[k - 1]
