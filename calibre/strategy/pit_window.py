"""
CALIBRE — Pit Window Generator (§33)
=====================================
Generates viable tactical candidate strategies (pit now, pit +1, pit +2, stay out)
and valid tyre compound allocations under race regulations.
"""
from __future__ import annotations

from typing import List, Dict, Any
from calibre.schemas.race_telemetry import RaceState, TyreCompound, TrackStatus


class PitWindowEvaluator:
    """Generates strategy branches around current race lap."""

    @staticmethod
    def generate_candidate_branches(state: RaceState) -> List[Dict[str, Any]]:
        candidates = []
        cur_lap = state.lap
        rem_laps = state.total_laps - cur_lap

        # If wet/crossover approaching, recommend Intermediate
        if state.weather.rain_probability_pct >= 60.0:
            candidates.append({
                "strategy_id": "STRAT-INTER-NOW",
                "action": "PIT_NOW",
                "pit_lap": cur_lap,
                "target_compound": TyreCompound.INTERMEDIATE,
                "reason": "Immediate switch to Intermediate tyre before heavy rain crossover",
            })
            candidates.append({
                "strategy_id": "STRAT-STAY-SLICK",
                "action": "STAY_OUT",
                "pit_lap": None,
                "target_compound": state.tyre.compound,
                "reason": "Stay out on slicks gambling rain will clear",
            })
            return candidates

        # Available dry compound choices (F1 two-compound rule)
        next_compounds = [TyreCompound.HARD, TyreCompound.MEDIUM] if state.tyre.compound == TyreCompound.SOFT else [TyreCompound.HARD]

        # Branch 1: PIT NOW (Undercut or immediate cliff relief)
        for comp in next_compounds:
            candidates.append({
                "strategy_id": f"STRAT-PIT-NOW-{comp.value}",
                "action": "PIT_NOW",
                "pit_lap": cur_lap,
                "target_compound": comp,
                "reason": "Immediate pit stop to trigger undercut or escape tyre cliff",
            })

        # Branch 2: PIT NEXT LAP (Lap + 1)
        if rem_laps > 2:
            candidates.append({
                "strategy_id": "STRAT-PIT-NEXT-HARD",
                "action": "PIT_NEXT_LAP",
                "pit_lap": cur_lap + 1,
                "target_compound": TyreCompound.HARD,
                "reason": "One additional preparatory lap before box",
            })

        # Branch 3: PIT LATER (Lap + 3)
        if rem_laps > 5:
            candidates.append({
                "strategy_id": "STRAT-PIT-LATER-HARD",
                "action": "PIT_LATER",
                "pit_lap": cur_lap + 3,
                "target_compound": TyreCompound.HARD,
                "reason": "Extend stint to seek Safety Car window",
            })

        # Branch 4: STAY OUT
        candidates.append({
            "strategy_id": "STRAT-STAY-OUT",
            "action": "STAY_OUT",
            "pit_lap": None,
            "target_compound": state.tyre.compound,
            "reason": "Maintain track position on current set",
        })

        return candidates
