"""
CALIBRE — Monte Carlo Race Simulator (§34, §35)
================================================
Simulates forward race trajectories under uncertainty (degradation, traffic,
pit execution variance, safety cars) to evaluate strategic candidate branches.
"""
from __future__ import annotations

import math
import random
from typing import List, Dict, Any, Optional
import numpy as np

from calibre.schemas.race_telemetry import (
    RaceState,
    StrategyCandidate,
    TyreCompound,
    TrackStatus,
)
from calibre.models.tyre.tyre_model import TyreDegradationModel


class MonteCarloRaceSimulator:
    """Evaluates candidate strategies via stochastic Monte Carlo simulation."""

    POINTS_SCALE = {1: 25, 2: 18, 3: 15, 4: 12, 5: 10, 6: 8, 7: 6, 8: 4, 9: 2, 10: 1}

    def __init__(self, n_rollouts: int = 100, seed: int = 42):
        self.n_rollouts = n_rollouts
        self.seed = seed
        self.tyre_model = TyreDegradationModel()

    def evaluate_candidates(
        self,
        state: RaceState,
        candidate_branches: List[Dict[str, Any]],
        seed: Optional[int] = None,
    ) -> List[StrategyCandidate]:
        rng = np.random.default_rng(seed or self.seed)
        results: List[StrategyCandidate] = []

        rem_laps = max(1, state.total_laps - state.lap)
        base_lap_time = 90.0  # nominal 1:30.000 lap time

        for branch in candidate_branches:
            action = branch["action"]
            pit_lap = branch.get("pit_lap")
            target_comp = branch.get("target_compound", state.tyre.compound)
            strat_id = branch["strategy_id"]

            positions: List[float] = []

            for _ in range(self.n_rollouts):
                # 1. Simulate pit execution loss
                is_pitting = (action != "STAY_OUT" and pit_lap is not None)
                pit_time_loss = 0.0

                if is_pitting:
                    # SC reduces pit loss from 21.5s to 11.5s
                    base_loss = 11.5 if state.track_status in (TrackStatus.SAFETY_CAR, TrackStatus.VIRTUAL_SAFETY_CAR) else 21.5
                    # Add execution variance (e.g. wheel nut delay)
                    jitter = rng.normal(0.0, 0.4)
                    pit_time_loss = max(10.0, base_loss + jitter)

                # 2. Simulate forward stint pace
                total_race_time_delta = pit_time_loss

                if is_pitting:
                    # Laps on new tyre
                    laps_on_new = max(0, state.total_laps - (pit_lap or state.lap))
                    # Fresh tyre has pace advantage (e.g. -0.8s/lap vs old worn set)
                    new_pace = self.tyre_model.estimate_state(target_comp, age_laps=1).current_pace_delta_s
                    # Old tyre current degradation penalty
                    old_pace = state.tyre.current_pace_delta_s

                    # Stint delta advantage
                    pace_advantage = (old_pace - new_pace) * laps_on_new
                    total_race_time_delta -= pace_advantage

                    # Traffic delay when rejoining behind slower cars
                    traffic_loss = rng.exponential(scale=1.5)
                    total_race_time_delta += traffic_loss
                else:
                    # Stay out: accumulating tyre degradation + cliff penalty
                    stay_pace = self.tyre_model.estimate_state(state.tyre.compound, age_laps=state.tyre.age_laps + rem_laps)
                    degradation_cumulative = (stay_pace.current_pace_delta_s * rem_laps * 0.5)
                    if state.tyre.tyre_cliff_probability > 0.4:
                        degradation_cumulative += 12.0  # heavy cliff penalty
                    total_race_time_delta += degradation_cumulative

                # Weather penalty if slicks in wet
                if state.weather.rain_probability_pct > 60.0 and target_comp not in (TyreCompound.INTERMEDIATE, TyreCompound.WET):
                    total_race_time_delta += 45.0  # massive aquaplaning loss

                # 3. Project finish position based on total time delta relative to field
                # Every ~2.5s lost corresponds roughly to 1 track position
                pos_shift = total_race_time_delta / 3.0
                finish_pos = max(1.0, min(20.0, state.position + pos_shift + rng.normal(0.0, 0.3)))
                positions.append(finish_pos)

            # Summarize rollouts
            pos_arr = np.array(positions)
            mean_pos = float(np.mean(pos_arr))
            p10 = float(np.percentile(pos_arr, 10))
            p50 = float(np.percentile(pos_arr, 50))
            p90 = float(np.percentile(pos_arr, 90))

            podium_prob = float(np.mean(pos_arr <= 3.5))
            expected_points = float(np.mean([self.POINTS_SCALE.get(int(round(p)), 0) for p in pos_arr]))

            # Why explanation strings
            why = []
            if is_pitting:
                why.append(f"Pit stop on Lap {pit_lap} for {target_comp.value} compound.")
                if state.track_status in (TrackStatus.SAFETY_CAR, TrackStatus.VIRTUAL_SAFETY_CAR):
                    why.append("Capitalizes on 10.0s discounted pit loss under Safety Car conditions.")
                if state.gap_ahead_s is not None and state.gap_ahead_s <= 2.0:
                    why.append(f"Executes undercut against {state.driver_ahead} with fresh tyre out-lap pace.")
                if state.tyre.tyre_cliff_probability > 0.4:
                    why.append("Safeguards against critical tyre cliff arrival on current compound.")
            else:
                why.append("Extends current stint to maintain track position.")

            results.append(StrategyCandidate(
                strategy_id=strat_id,
                action=action,
                pit_lap=pit_lap,
                target_compound=target_comp,
                expected_position=round(mean_pos, 2),
                p10_position=round(p10, 1),
                p50_position=round(p50, 1),
                p90_position=round(p90, 1),
                podium_probability=round(podium_prob, 3),
                expected_points=round(expected_points, 1),
                pit_loss_s=21.5 if state.track_status == TrackStatus.GREEN else 11.5,
                traffic_risk=0.25 if is_pitting else 0.10,
                tyre_cliff_risk=state.tyre.tyre_cliff_probability if not is_pitting else 0.05,
                confidence=0.88,
                why=why,
            ))

        return sorted(results, key=lambda c: c.expected_position)
