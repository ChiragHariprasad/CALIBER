"""
CALIBRE — Agent Tools (§38)
===========================
Callable mathematical, simulation, and state inspection tools providing
numerical ground truth to the race-engineering intelligence agent.
"""
from __future__ import annotations

from typing import List, Dict, Any, Optional
from calibre.schemas.race_telemetry import (
    RaceState,
    TyreCompound,
    StrategyCandidate,
    StrategyDecision,
    DriverInsight,
)
from calibre.telemetry.state import LiveRaceStateEstimator
from calibre.track.circuit_model import CircuitModel
from calibre.models.tyre.tyre_model import TyreDegradationModel
from calibre.models.grip.grip_model import GripModel
from calibre.models.slip.slip_risk_model import SlipRiskModel
from calibre.models.opponent.opponent_model import OpponentModel
from calibre.models.weather.weather_model import WeatherModel
from calibre.models.driver.driver_error_model import DriverErrorModel
from calibre.strategy.pit_window import PitWindowEvaluator
from calibre.strategy.simulator.race_simulator import MonteCarloRaceSimulator
from calibre.strategy.optimizer.strategy_optimizer import StrategyOptimizer


class RaceEngineeringTools:
    """Numerical and strategy tools called by the agent."""

    def __init__(self, state_estimator: Optional[LiveRaceStateEstimator] = None):
        self.state_estimator = state_estimator or LiveRaceStateEstimator()
        self.circuit = CircuitModel()
        self.tyre_model = TyreDegradationModel()
        self.grip_model = GripModel()
        self.slip_model = SlipRiskModel()
        self.opponent_model = OpponentModel()
        self.weather_model = WeatherModel()
        self.driver_model = DriverErrorModel(self.circuit)
        self.simulator = MonteCarloRaceSimulator(n_rollouts=100)
        self.optimizer = StrategyOptimizer()

    def get_current_race_state(self) -> RaceState:
        return self.state_estimator.get_race_state()

    def estimate_tyre_degradation(self, compound: TyreCompound, age_laps: int):
        return self.tyre_model.estimate_state(compound, age_laps, self.state_estimator.track_temp_c)

    def estimate_grip(self):
        state = self.get_current_race_state()
        return self.grip_model.estimate_grip(state.lap, state.total_laps, state.track.track_temp_c)

    def estimate_slip_risk(self, speed_kph: float, throttle_pct: float, gear: int):
        state = self.get_current_race_state()
        return self.slip_model.estimate_slip_risk(speed_kph, throttle_pct, gear, state.tyre.age_laps)

    def get_opponent_state(self):
        state = self.get_current_race_state()
        return self.opponent_model.evaluate_threats(state.lap, state.position, state.opponents, state.pit_lane_loss_s)

    def find_optimal_pit_window(self) -> StrategyDecision:
        state = self.get_current_race_state()
        branches = PitWindowEvaluator.generate_candidate_branches(state)
        candidates = self.simulator.evaluate_candidates(state, branches)
        return self.optimizer.select_optimal_decision(state, candidates)

    def compare_strategies(self) -> List[StrategyCandidate]:
        state = self.get_current_race_state()
        branches = PitWindowEvaluator.generate_candidate_branches(state)
        return self.simulator.evaluate_candidates(state, branches)

    def get_driver_insights(self, corner_id: str, actual_telemetry: Dict[str, Any]) -> Optional[DriverInsight]:
        state = self.get_current_race_state()
        is_cliff = state.tyre.tyre_cliff_probability > 0.4
        in_traffic = state.gap_ahead_s is not None and state.gap_ahead_s < 1.0
        return self.driver_model.analyze_corner(
            corner_id=corner_id,
            actual_telemetry=actual_telemetry,
            track_status=state.track_status,
            in_traffic=in_traffic,
            tyre_cliff_active=is_cliff,
        )
