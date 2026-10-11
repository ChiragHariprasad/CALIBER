"""
CALIBRE — Operating Mode State Machine
========================================
Enforces explicit separation between LIVE, HISTORICAL_REPLAY and SIMULATION
modes. Mode transitions are explicit operator actions — no silent fallback.
"""
from __future__ import annotations

import enum
import logging
import time
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any

logger = logging.getLogger(__name__)


class OperatingMode(str, enum.Enum):
    """The three mutually exclusive operating modes."""
    LIVE = "LIVE"
    HISTORICAL_REPLAY = "HISTORICAL_REPLAY"
    SIMULATION = "SIMULATION"


class ModeTransitionError(Exception):
    """Raised when a mode transition is invalid."""
    pass


class FeedStatus(str, enum.Enum):
    """Health status of the data feed within a mode."""
    HEALTHY = "HEALTHY"
    DEGRADED = "DEGRADED"
    FAILED = "FAILED"
    NOT_STARTED = "NOT_STARTED"


@dataclass
class ModeState:
    """Current state of the operating mode machine."""
    mode: OperatingMode = OperatingMode.SIMULATION
    feed_status: FeedStatus = FeedStatus.NOT_STARTED
    entered_at: float = field(default_factory=time.time)
    transition_history: List[Dict[str, Any]] = field(default_factory=list)
    degradation_reasons: List[str] = field(default_factory=list)
    active_sources: List[str] = field(default_factory=list)
    session_key: Optional[int] = None
    replay_session_name: Optional[str] = None
    simulation_scenario: Optional[str] = None


class OperatingModeController:
    """
    Controls the operating mode lifecycle.
    
    Rules:
    - Mode transitions are explicit and logged.
    - LIVE mode never silently falls back to REPLAY or SIMULATION.
    - If a live feed fails, the mode is declared DEGRADED, not switched.
    - Every API, log, Kafka event and dashboard view can query the current mode.
    """

    # Valid explicit transitions
    VALID_TRANSITIONS = {
        OperatingMode.SIMULATION: {OperatingMode.LIVE, OperatingMode.HISTORICAL_REPLAY},
        OperatingMode.LIVE: {OperatingMode.SIMULATION, OperatingMode.HISTORICAL_REPLAY},
        OperatingMode.HISTORICAL_REPLAY: {OperatingMode.SIMULATION, OperatingMode.LIVE},
    }

    def __init__(self, initial_mode: OperatingMode = OperatingMode.SIMULATION):
        self._state = ModeState(mode=initial_mode)
        logger.info("OperatingModeController initialized in %s mode", initial_mode.value)

    @property
    def mode(self) -> OperatingMode:
        return self._state.mode

    @property
    def current_mode(self) -> OperatingMode:
        return self._state.mode

    @property
    def feed_status(self) -> FeedStatus:
        return self._state.feed_status

    @property
    def state(self) -> ModeState:
        return self._state

    def to_dict(self) -> Dict[str, Any]:
        return self.get_status_dict()

    def transition_to(
        self,
        target_mode: OperatingMode,
        *,
        session_key: Optional[int] = None,
        replay_session_name: Optional[str] = None,
        simulation_scenario: Optional[str] = None,
        scenario: Optional[str] = None,
        reason: str = "",
    ) -> None:
        """Explicitly transition to a new operating mode."""
        sim_scenario = simulation_scenario or scenario
        current = self._state.mode

        if target_mode == current:
            logger.info("Already in %s mode, no transition needed", current.value)
            return

        if target_mode not in self.VALID_TRANSITIONS.get(current, set()):
            raise ModeTransitionError(
                f"Cannot transition from {current.value} to {target_mode.value}"
            )

        # Record transition
        transition_record = {
            "from": current.value,
            "to": target_mode.value,
            "reason": reason,
            "ts": time.time(),
        }
        self._state.transition_history.append(transition_record)

        # Update state
        self._state.mode = target_mode
        self._state.feed_status = FeedStatus.NOT_STARTED
        self._state.entered_at = time.time()
        self._state.degradation_reasons.clear()
        self._state.session_key = session_key
        self._state.replay_session_name = replay_session_name
        self._state.simulation_scenario = sim_scenario

        logger.info(
            "Mode transition: %s → %s (reason: %s)",
            current.value, target_mode.value, reason or "operator action",
        )

    def declare_healthy(self, active_sources: Optional[List[str]] = None) -> None:
        """Declare the current feed as healthy."""
        self._state.feed_status = FeedStatus.HEALTHY
        self._state.degradation_reasons.clear()
        if active_sources:
            self._state.active_sources = active_sources

    def declare_degraded(self, reason: str) -> None:
        """
        Declare the current feed as degraded.
        This does NOT switch modes — it reports a problem within the current mode.
        """
        self._state.feed_status = FeedStatus.DEGRADED
        self._state.degradation_reasons.append(reason)
        logger.warning("Feed DEGRADED in %s mode: %s", self._state.mode.value, reason)

    def declare_failed(self, reason: str) -> None:
        """
        Declare the current feed as failed.
        This does NOT switch modes — switching is an explicit operator action.
        """
        self._state.feed_status = FeedStatus.FAILED
        self._state.degradation_reasons.append(reason)
        logger.error("Feed FAILED in %s mode: %s", self._state.mode.value, reason)

    def get_status_dict(self) -> Dict[str, Any]:
        """Returns a serialisable status snapshot for API consumers."""
        return {
            "mode": self._state.mode.value,
            "feed_status": self._state.feed_status.value,
            "entered_at": self._state.entered_at,
            "active_sources": self._state.active_sources,
            "degradation_reasons": self._state.degradation_reasons,
            "session_key": self._state.session_key,
            "replay_session_name": self._state.replay_session_name,
            "simulation_scenario": self._state.simulation_scenario,
            "transition_count": len(self._state.transition_history),
        }
