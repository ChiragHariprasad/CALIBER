"""CALIBRE SIA v2.2 - Pointwise Validity Timeline per CAL-DOC-007 §2.5.3."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence

from ..models.states import IntegrityState, and_state, or_state, k_of_n_state


@dataclass(frozen=True, slots=True)
class TimelineBreakpoint:
    """One transition breakpoint in a validity timeline."""
    from_time: str
    state: IntegrityState

    def to_dict(self) -> Dict[str, str]:
        return {
            "from": self.from_time,
            "state": self.state.value,
        }


class ValidityTimeline:
    """A non-increasing step function representing state over time."""

    def __init__(
        self,
        breakpoints: Sequence[TimelineBreakpoint | Dict[str, Any]],
        default_state: IntegrityState = IntegrityState.UNKNOWN,
    ):
        self._default_state = default_state
        self._breakpoints: List[TimelineBreakpoint] = []
        for bp in breakpoints:
            if isinstance(bp, TimelineBreakpoint):
                self._breakpoints.append(bp)
            elif isinstance(bp, dict):
                st = IntegrityState(bp["state"])
                self._breakpoints.append(TimelineBreakpoint(bp["from"], st))
            else:
                raise TypeError(f"Invalid breakpoint: {bp}")
        # Sort by from_time ascending
        self._breakpoints.sort(key=lambda b: b.from_time)

    @property
    def breakpoints(self) -> List[TimelineBreakpoint]:
        return list(self._breakpoints)

    def state_at(self, t: str) -> IntegrityState:
        """Evaluate the step function at timestamp t."""
        if not self._breakpoints:
            return self._default_state
        current_state = self._breakpoints[0].state
        for bp in self._breakpoints:
            if bp.from_time <= t:
                current_state = bp.state
            else:
                break
        return current_state

    def to_list(self) -> List[Dict[str, str]]:
        return [bp.to_dict() for bp in self._breakpoints]

    def compose_with(
        self,
        other: ValidityTimeline,
        operator: str,  # "AND" | "OR"
    ) -> ValidityTimeline:
        """Compose two validity timelines pointwise."""
        return compose_pointwise(operator, [self, other])


def compose_pointwise(
    operator: str,
    timelines: Sequence[ValidityTimeline],
    k: int = 1,
) -> ValidityTimeline:
    """Compose multiple validity timelines pointwise using lattice operators (min/max/k-of-n)."""
    if not timelines:
        return ValidityTimeline([])

    # Collect all unique breakpoint timestamps across all timelines
    all_timestamps = sorted(
        list(set(bp.from_time for tl in timelines for bp in tl.breakpoints))
    )

    if not all_timestamps:
        return ValidityTimeline([])

    composed_breakpoints: List[TimelineBreakpoint] = []
    prev_state: Optional[IntegrityState] = None

    for t in all_timestamps:
        states_at_t = [tl.state_at(t) for tl in timelines]
        if operator.upper() == "AND":
            combined = and_state(*states_at_t)
        elif operator.upper() == "OR":
            combined = or_state(*states_at_t)
        elif operator.upper().startswith("K_OF_N"):
            combined = k_of_n_state(k, *states_at_t)
        else:
            raise ValueError(f"Unknown operator: {operator}")

        if prev_state is None or combined != prev_state:
            composed_breakpoints.append(TimelineBreakpoint(t, combined))
            prev_state = combined

    return ValidityTimeline(composed_breakpoints)
