"""
CALIBRE — Synthetic Strategy Scenarios (§49)
=============================================
Defines reproducible, deterministic scenarios for evaluating
race-engineering strategy optimization and agent tactical decision-making:
  1. Undercut (P5 behind P4 with tyre offset)
  2. Safety Car (SC deployment with discounted pit loss)
  3. Tyre Cliff (Accelerating degradation triggering urgent box)
  4. Weather Crossover (Rain onset forcing intermediate tyre switch)
  5. False Driver Blame (Traffic/yellow flag time loss attribution)
"""
from __future__ import annotations

from typing import List, Dict, Any
from calibre.schemas.race_telemetry import (
    RaceState,
    TyreCompound,
    TrackStatus,
    OpponentState,
)
from calibre.telemetry.state import LiveRaceStateEstimator


class ScenarioGenerator:
    """Generates synthetic scenarios for replay and testing."""

    @staticmethod
    def get_scenario(scenario_name: str) -> LiveRaceStateEstimator:
        estimator = LiveRaceStateEstimator(driver="VER", total_laps=57)

        if scenario_name in ("synthetic-undercut", "undercut"):
            # Scenario 1: Undercut (Lap 22, P5, P4 is 1.2s ahead on older tyres)
            estimator.current_lap = 22
            estimator.position = 5
            estimator.gap_ahead_s = 1.2
            estimator.driver_ahead = "RUS"
            estimator.current_compound = TyreCompound.MEDIUM
            estimator.tyre_age_laps = 21
            estimator.opponents = {
                "RUS": OpponentState(
                    driver="RUS", position=4, gap_s=1.2, gap_trend_s_per_lap=0.0,
                    compound=TyreCompound.MEDIUM, tyre_age_laps=21, undercut_threat=True
                )
            }

        elif scenario_name in ("safety-car", "sc"):
            # Scenario 2: Safety Car deployed on Lap 28
            estimator.current_lap = 28
            estimator.position = 3
            estimator.gap_ahead_s = 2.4
            estimator.current_compound = TyreCompound.MEDIUM
            estimator.tyre_age_laps = 18
            estimator.set_track_status(TrackStatus.SAFETY_CAR)

        elif scenario_name in ("tyre-cliff", "cliff"):
            # Scenario 3: Tyre cliff (Lap 25 on Mediums, high degradation)
            estimator.current_lap = 25
            estimator.position = 2
            estimator.current_compound = TyreCompound.MEDIUM
            estimator.tyre_age_laps = 25  # at cliff boundary

        elif scenario_name in ("weather-crossover", "rain"):
            # Scenario 4: Rain increasing (68% probability, intermediate approaching)
            estimator.current_lap = 34
            estimator.position = 1
            estimator.current_compound = TyreCompound.HARD
            estimator.tyre_age_laps = 12
            estimator.rain_probability_pct = 75.0

        elif scenario_name in ("false-driver-blame", "blame"):
            # Scenario 5: Car slowed under yellow flag or traffic
            estimator.current_lap = 15
            estimator.position = 4
            estimator.set_track_status(TrackStatus.YELLOW)

        return estimator
