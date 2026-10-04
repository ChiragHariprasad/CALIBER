"""
CALIBRE — Strategy Optimizer (§35, §39)
=======================================
Selects the globally optimal strategy from evaluated Monte Carlo candidates
and produces structured decisions with explicit counterfactual alternatives.
"""
from __future__ import annotations

from typing import List, Optional
from calibre.schemas.race_telemetry import (
    RaceState,
    StrategyCandidate,
    StrategyDecision,
    TyreCompound,
)


class StrategyOptimizer:
    """Multi-objective strategy optimizer prioritizing track finish and podium odds."""

    @staticmethod
    def select_optimal_decision(
        state: RaceState,
        candidates: List[StrategyCandidate]
    ) -> StrategyDecision:
        if not candidates:
            # Default fallback: stay out
            return StrategyDecision(
                decision="STAY",
                urgency="LOW",
                target_lap=state.lap + 5,
                target_compound=state.tyre.compound,
                window=(state.lap + 3, state.lap + 7),
                expected_position=float(state.position),
                podium_probability=1.0 if state.position <= 3 else 0.2,
                expected_points=15.0 if state.position <= 3 else 4.0,
                confidence=0.80,
                why=["Maintaining current track stint."],
                counterfactual={"stay_out": {"expected_position": float(state.position)}}
            )

        # Candidates are already sorted by expected finishing position (ascending)
        best = candidates[0]
        stay_out_cand = next((c for c in candidates if c.action == "STAY_OUT"), None)

        # Determine decision and urgency
        decision = "PIT" if best.action != "STAY_OUT" else "STAY"
        urgency = "LOW"
        if decision == "PIT":
            if state.track_status.value in ("SAFETY_CAR", "VIRTUAL_SAFETY_CAR") or state.tyre.tyre_cliff_probability >= 0.5:
                urgency = "CRITICAL"
            elif best.pit_lap == state.lap:
                urgency = "HIGH"
            else:
                urgency = "MEDIUM"

        target_lap = best.pit_lap or (state.lap + 4)
        target_compound = best.target_compound or TyreCompound.HARD
        window = (max(state.lap, target_lap - 1), target_lap + 2)

        # Counterfactual comparison
        counterfactual = {}
        if stay_out_cand:
            counterfactual["stay_out"] = {
                "expected_position": stay_out_cand.expected_position,
                "podium_probability": stay_out_cand.podium_probability,
                "expected_points": stay_out_cand.expected_points,
            }

        return StrategyDecision(
            decision=decision,
            urgency=urgency,
            target_lap=target_lap,
            target_compound=target_compound,
            window=window,
            expected_position=best.expected_position,
            podium_probability=best.podium_probability,
            expected_points=best.expected_points,
            confidence=best.confidence,
            why=best.why,
            counterfactual=counterfactual,
        )
