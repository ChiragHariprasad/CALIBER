"""
CALIBRE — Agent State Machine (§36)
===================================
Executes the closed-loop race-engineering agent control cycle:
OBSERVE -> ESTIMATE -> DETECT_CHANGE -> GENERATE_CANDIDATES -> SIMULATE -> OPTIMISE -> DECIDE -> EXPLAIN.
"""
from __future__ import annotations

import logging
from typing import Dict, Any, List, Optional
from calibre.schemas.race_telemetry import (
    RaceState,
    StrategyDecision,
    StrategyCandidate,
    TelemetryEvent,
)
from calibre.telemetry.state import LiveRaceStateEstimator
from calibre.telemetry.event_detector import MeaningfulEventDetector
from calibre.agent.tools.race_tools import RaceEngineeringTools

logger = logging.getLogger(__name__)


class AgentStateMachine:
    """Agent loop coordinating observation, detection, simulation, and tactical decisions."""

    def __init__(self, state_estimator: Optional[LiveRaceStateEstimator] = None):
        self.state_estimator = state_estimator or LiveRaceStateEstimator()
        self.tools = RaceEngineeringTools(self.state_estimator)
        self.event_detector = MeaningfulEventDetector()
        self.current_decision: Optional[StrategyDecision] = None
        self.recent_events: List[Dict[str, Any]] = []
        self.strategy_history: List[StrategyDecision] = []

    def process_telemetry_tick(self, event: TelemetryEvent) -> Optional[StrategyDecision]:
        """
        Processes a raw telemetry sample:
        Updates state cheaply; only runs expensive Monte Carlo simulation
        when a meaningful tactical event is detected.
        """
        # 1. OBSERVE & ESTIMATE
        state = self.tools.get_current_race_state()

        # 2. DETECT_CHANGE
        triggers = self.event_detector.check_events(state)

        # Re-plan if meaningful events occurred or no prior decision
        if triggers or self.current_decision is None:
            self.recent_events.extend(triggers)

            # 3. GENERATE_CANDIDATES & SIMULATE & OPTIMISE & DECIDE
            decision = self.tools.find_optimal_pit_window()

            # Append trigger context to why notes
            for trg in triggers:
                decision.why.append(f"Triggered by {trg['type']}: {trg.get('impact', '')}")

            self.current_decision = decision
            self.strategy_history.append(decision)
            return decision

        return self.current_decision

    def force_replan(self) -> StrategyDecision:
        """Forces immediate re-simulation and decision synthesis."""
        decision = self.tools.find_optimal_pit_window()
        self.current_decision = decision
        self.strategy_history.append(decision)
        return decision
