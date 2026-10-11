"""
CALIBRE — Tyre Strategy Agent (§33–§35)
========================================
Live tyre degradation tracking and pit strategy optimization agent.
Uses actual observed deg rates to calibrate the tyre model, runs
Monte Carlo simulations on meaningful events, and maintains a
rolling strategy evolution log.

Responsibilities:
  - Track actual degradation vs model predictions
  - Detect when drivers (us and opponents) are likely to pit
  - Recalculate strategy when meaningful events fire
  - Maintain strategy evolution trail for post-race analysis
"""
from __future__ import annotations

import logging
import time
from collections import defaultdict, deque
from typing import Any, Dict, List, Optional

from calibre.schemas.race_telemetry import (
    RaceState,
    StrategyCandidate,
    StrategyDecision,
    TyreCompound,
    TyreState,
    TrackStatus,
    OpponentState,
    WeatherState,
    TrackState,
)
from calibre.models.tyre.tyre_model import TyreDegradationModel
from calibre.strategy.pit_window import PitWindowEvaluator
from calibre.strategy.simulator.race_simulator import MonteCarloRaceSimulator
from calibre.strategy.optimizer.strategy_optimizer import StrategyOptimizer
from calibre.telemetry.event_detector import MeaningfulEventDetector

logger = logging.getLogger(__name__)


class TyreStrategyAgent:
    """Live tyre degradation tracking and strategy optimization agent."""

    def __init__(self, target_driver: str = "VER", total_laps: int = 56):
        self.target_driver = target_driver
        self.total_laps = total_laps

        # Models
        self.tyre_model = TyreDegradationModel()
        self.simulator = MonteCarloRaceSimulator(n_rollouts=200, seed=42)
        self.optimizer = StrategyOptimizer()
        self.event_detector = MeaningfulEventDetector()

        # Per-driver degradation tracking
        self.observed_deg: Dict[str, deque] = defaultdict(lambda: deque(maxlen=20))
        self.stint_base_pace: Dict[str, Optional[float]] = {}  # First few laps pace baseline
        self.pit_predictions: Dict[str, Optional[int]] = {}  # Predicted pit lap per driver

        # Current strategy state
        self.current_decision: Optional[StrategyDecision] = None
        self.current_candidates: List[StrategyCandidate] = []
        self.strategy_evolution: List[Dict[str, Any]] = []  # Full decision trail

        # Race state cache
        self._last_race_state: Optional[RaceState] = None

    def process_lap(
        self,
        driver: str,
        lap_data: Dict[str, Any],
        position: int,
        compound: TyreCompound,
        tyre_age: int,
        gap_ahead: Optional[float],
        gap_behind: float,
        driver_ahead: Optional[str],
        driver_behind: Optional[str],
        track_status: TrackStatus,
        weather: WeatherState,
        opponents: List[OpponentState],
    ) -> Optional[StrategyDecision]:
        """
        Process a completed lap for the target driver and potentially
        re-evaluate strategy.
        """
        lap_time = lap_data.get("lap_time_s", 0)
        lap_number = lap_data.get("lap_number", 0)
        is_pit_in = lap_data.get("is_pit_in_lap", False)
        is_pit_out = lap_data.get("is_pit_out_lap", False)

        # Track degradation for all drivers
        if not is_pit_in and not is_pit_out and lap_time > 0:
            self._track_degradation(driver, lap_time, compound, tyre_age)

        # Only run strategy for target driver
        if driver != self.target_driver:
            return None

        # Build current race state
        race_state = self._build_race_state(
            lap_number=lap_number,
            position=position,
            compound=compound,
            tyre_age=tyre_age,
            gap_ahead=gap_ahead,
            gap_behind=gap_behind,
            driver_ahead=driver_ahead,
            driver_behind=driver_behind,
            track_status=track_status,
            weather=weather,
            opponents=opponents,
        )
        self._last_race_state = race_state

        # Check for meaningful events that should trigger replanning
        triggers = self.event_detector.check_events(race_state)

        should_replan = (
            triggers or
            self.current_decision is None or
            lap_number % 3 == 0  # Re-evaluate every 3 laps regardless
        )

        if should_replan:
            decision = self._run_strategy(race_state)

            # Add trigger info to decision reasoning
            for trigger in triggers:
                decision.why.append(f"[TRIGGER] {trigger['type']}: {trigger.get('impact', '')}")

            # Log strategy evolution
            self.strategy_evolution.append({
                "lap": lap_number,
                "decision": decision.decision,
                "urgency": decision.urgency,
                "target_lap": decision.target_lap,
                "target_compound": decision.target_compound.value,
                "expected_position": decision.expected_position,
                "podium_probability": decision.podium_probability,
                "confidence": decision.confidence,
                "triggers": [t["type"] for t in triggers],
                "why": decision.why,
                "ts": time.time(),
            })

            self.current_decision = decision
            logger.info(
                "[STRATEGY] Lap %d | Decision: %s | Urgency: %s | Target Lap: %d | Compound: %s | E[Pos]: %.1f",
                lap_number, decision.decision, decision.urgency,
                decision.target_lap, decision.target_compound.value,
                decision.expected_position,
            )
            return decision

        return self.current_decision

    def process_event(
        self,
        event_type: str,
        track_status: TrackStatus,
        weather: WeatherState,
        opponents: List[OpponentState],
    ) -> Optional[StrategyDecision]:
        """
        Process a non-lap event (SC deployment, weather change, etc.)
        that requires immediate strategy re-evaluation.
        """
        if self._last_race_state is None:
            return None

        # Update the race state with new conditions
        updated_state = self._last_race_state.model_copy(update={
            "track_status": track_status,
            "weather": weather,
            "opponents": opponents,
            "pit_lane_loss_s": 11.5 if track_status in (TrackStatus.SAFETY_CAR, TrackStatus.VIRTUAL_SAFETY_CAR) else 21.5,
        })

        decision = self._run_strategy(updated_state)
        decision.why.append(f"[EVENT] Triggered by {event_type}")

        self.strategy_evolution.append({
            "lap": updated_state.lap,
            "event": event_type,
            "decision": decision.decision,
            "urgency": decision.urgency,
            "target_lap": decision.target_lap,
            "target_compound": decision.target_compound.value,
            "expected_position": decision.expected_position,
            "ts": time.time(),
        })

        self.current_decision = decision
        return decision

    def predict_opponent_pits(self) -> Dict[str, Dict[str, Any]]:
        """Predict when opponents are likely to pit based on observed degradation."""
        predictions = {}

        for driver, deg_history in self.observed_deg.items():
            if len(deg_history) < 3:
                continue

            # Simple linear extrapolation of degradation
            recent_deg = list(deg_history)[-5:]
            avg_deg_per_lap = sum(recent_deg) / len(recent_deg) if recent_deg else 0

            # Estimate critical degradation threshold (1.5s/lap loss)
            if avg_deg_per_lap > 0.02:
                laps_to_critical = max(1, int(1.5 / avg_deg_per_lap))
                predictions[driver] = {
                    "estimated_pit_in_laps": laps_to_critical,
                    "avg_deg_per_lap_s": round(avg_deg_per_lap, 3),
                    "confidence": min(0.9, 0.3 + 0.1 * len(deg_history)),
                }

        return predictions

    def get_strategy_summary(self) -> Dict[str, Any]:
        """Returns current strategy state summary."""
        observed = list(self.observed_deg.get(self.target_driver, []))
        avg_deg = round(sum(observed[-5:]) / len(observed[-5:]), 4) if observed else None
        return {
            "current_decision": self.current_decision.model_dump() if self.current_decision else None,
            "candidates": [c.model_dump() for c in self.current_candidates],
            "evolution_count": len(self.strategy_evolution),
            "last_evaluation_lap": self.strategy_evolution[-1]["lap"] if self.strategy_evolution else 0,
            "opponent_pit_predictions": self.predict_opponent_pits(),
            "observed_deg_s_per_lap": avg_deg,
        }

    def _track_degradation(self, driver: str, lap_time: float, compound: TyreCompound, tyre_age: int):
        """Track observed degradation vs previous lap."""
        if driver not in self.stint_base_pace or tyre_age <= 2:
            # Establish baseline from first few laps of stint
            if tyre_age <= 2:
                self.stint_base_pace[driver] = lap_time
            return

        base = self.stint_base_pace[driver]
        if base and base > 0:
            deg = (lap_time - base) / max(1, tyre_age)
            self.observed_deg[driver].append(max(0, deg))

    def _build_race_state(
        self,
        lap_number: int,
        position: int,
        compound: TyreCompound,
        tyre_age: int,
        gap_ahead: Optional[float],
        gap_behind: float,
        driver_ahead: Optional[str],
        driver_behind: Optional[str],
        track_status: TrackStatus,
        weather: WeatherState,
        opponents: List[OpponentState],
    ) -> RaceState:
        """Build a RaceState from live data."""
        # Use observed degradation if available, else model
        observed = list(self.observed_deg.get(self.target_driver, []))
        if observed:
            avg_deg = sum(observed[-5:]) / len(observed[-5:])
        else:
            tyre_state = self.tyre_model.estimate_state(compound, tyre_age)
            avg_deg = tyre_state.degradation_rate_s_per_lap

        tyre_state = TyreState(
            compound=compound,
            age_laps=tyre_age,
            stint_lap=tyre_age,
            degradation_rate_s_per_lap=round(avg_deg, 4),
            current_pace_delta_s=round(avg_deg * tyre_age, 3),
            tyre_cliff_probability=self.tyre_model.estimate_state(compound, tyre_age).tyre_cliff_probability,
            usable_laps_remaining=max(0, self.tyre_model.CLIFF_HORIZON_LAPS.get(compound, 25) - tyre_age),
            confidence=0.85 if observed else 0.70,
        )

        track_state = TrackState(
            grip_index=0.88,
            grip_trend="increasing" if lap_number < 30 else "stable",
            track_temp_c=weather.track_temp_c,
            air_temp_c=weather.air_temp_c,
            status=track_status,
        )

        return RaceState(
            session_time_s=0.0,
            lap=lap_number,
            total_laps=self.total_laps,
            driver=self.target_driver,
            position=position,
            gap_ahead_s=gap_ahead,
            leader_gap_s=0.0 if position == 1 else (gap_ahead or 0.0),
            gap_behind_s=gap_behind,
            driver_ahead=driver_ahead,
            driver_behind=driver_behind,
            track_status=track_status,
            pit_lane_loss_s=11.5 if track_status in (TrackStatus.SAFETY_CAR, TrackStatus.VIRTUAL_SAFETY_CAR) else 21.5,
            tyre=tyre_state,
            track=track_state,
            weather=weather,
            opponents=opponents,
        )

    def _run_strategy(self, state: RaceState) -> StrategyDecision:
        """Run Monte Carlo strategy evaluation."""
        branches = PitWindowEvaluator.generate_candidate_branches(state)
        candidates = self.simulator.evaluate_candidates(state, branches)
        self.current_candidates = candidates
        return self.optimizer.select_optimal_decision(state, candidates)
