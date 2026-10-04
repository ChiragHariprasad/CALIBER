"""
CALIBRE — Opponent Battle & Undercut Model (§31)
=================================================
Tracks competitors, gap evolutions, tyre age offsets, and computes
undercut vulnerabilities and overcut windows.
"""
from __future__ import annotations

from typing import Dict, List, Optional
from calibre.schemas.race_telemetry import OpponentState, TyreCompound


class OpponentModel:
    """Evaluates competitor positions, pace deltas, and strategic threats."""

    def evaluate_threats(
        self,
        driver_lap: int,
        my_position: int,
        opponents: List[OpponentState],
        pit_lane_loss_s: float = 21.5,
    ) -> List[OpponentState]:
        evaluated: List[OpponentState] = []

        for opp in opponents:
            # Undercut vulnerability:
            # If opponent is ahead by less than 2.5s and their tyres are older,
            # we have an undercut threat against them.
            # If opponent is behind by less than 2.0s, they have an undercut threat on us.
            is_undercut_threat = False
            is_overcut_opp = False

            if opp.position == my_position - 1 and opp.gap_s <= 2.5:
                # Car ahead is vulnerable to our undercut
                is_undercut_threat = True
            elif opp.position == my_position + 1 and opp.gap_s <= 2.0:
                # Car behind can undercut us
                is_undercut_threat = True

            # Time to catch calculation
            time_to_catch = None
            if opp.gap_trend_s_per_lap < -0.05 and opp.gap_s > 0.0:
                time_to_catch = round(opp.gap_s / abs(opp.gap_trend_s_per_lap), 1)

            evaluated.append(OpponentState(
                driver=opp.driver,
                position=opp.position,
                gap_s=round(opp.gap_s, 2),
                gap_trend_s_per_lap=round(opp.gap_trend_s_per_lap, 3),
                compound=opp.compound,
                tyre_age_laps=opp.tyre_age_laps,
                undercut_threat=is_undercut_threat,
                overcut_opportunity=is_overcut_opp,
                time_to_catch_laps=time_to_catch,
            ))

        return evaluated
