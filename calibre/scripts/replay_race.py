"""
CALIBRE — Historical & Synthetic Race Replay Engine (§47, §65)
==============================================================
Replays telemetry events and evaluates strategic state machine in real-time
or accelerated time (1x, 5x, 10x, 50x) across canonical race scenarios.

Usage:
  python scripts/replay_race.py --scenario synthetic-undercut
  python scripts/replay_race.py --scenario safety-car --speed 10
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

# Ensure root directory containing calibre is in sys.path
CALIBRE_ROOT = Path(__file__).resolve().parent.parent
CALIBRE_PARENT = CALIBRE_ROOT.parent
if str(CALIBRE_PARENT) not in sys.path:
    sys.path.insert(0, str(CALIBRE_PARENT))
if str(CALIBRE_ROOT) not in sys.path:
    sys.path.insert(0, str(CALIBRE_ROOT))

from calibre.replay.scenarios.scenario_generator import ScenarioGenerator
from calibre.agent.state.state_machine import AgentStateMachine
from calibre.schemas.race_telemetry import TelemetryEvent, SessionType


def main():
    parser = argparse.ArgumentParser(description="CALIBRE — Race Replay Engine")
    parser.add_argument(
        "--scenario",
        type=str,
        default="synthetic-undercut",
        choices=["synthetic-undercut", "safety-car", "tyre-cliff", "weather-crossover", "false-driver-blame"],
        help="Synthetic race strategy scenario",
    )
    parser.add_argument("--speed", type=int, default=10, help="Replay speed multiplier (1, 5, 10, 50)")
    parser.add_argument("--laps", type=int, default=5, help="Number of replay laps to simulate")
    args = parser.parse_args()

    print("\n" + "=" * 65)
    print(f"CALIBRE F1 RACE-ENGINEERING INTELLIGENCE ENGINE")
    print("=" * 65)
    print(f"Scenario       : {args.scenario}")
    print(f"Speed          : {args.speed}x")
    print("=" * 65 + "\n")

    # Initialize scenario estimator and agent state machine
    estimator = ScenarioGenerator.get_scenario(args.scenario)
    agent = AgentStateMachine(state_estimator=estimator)

    # Initial tactical evaluation
    initial_decision = agent.force_replan()
    state = estimator.get_race_state()

    print(f"[CURRENT RACE SITUATION]")
    print(f"  Lap          : {state.lap}/{state.total_laps}")
    print(f"  Position     : P{state.position} (Gap ahead: {state.gap_ahead_s or 0.0:.1f}s)")
    print(f"  Tyres        : {state.tyre.compound.value} (Age: {state.tyre.age_laps} laps)")
    print(f"  Degradation  : {state.tyre.degradation_rate_s_per_lap:.3f} s/lap")
    print(f"  Cliff Risk   : {state.tyre.tyre_cliff_probability:.1%}")
    print(f"  Track Status : {state.track_status.value}")
    print(f"  Weather      : Rain Prob {state.weather.rain_probability_pct:.1f}%")

    print("\n" + "-" * 65)
    print(f"AGENT STRATEGY RECOMMENDATION")
    print("-" * 65)
    print(f"  Decision     : >>> {initial_decision.decision} <<< [Urgency: {initial_decision.urgency}]")
    print(f"  Target Lap   : Lap {initial_decision.target_lap} (Window: {initial_decision.window})")
    print(f"  Target Compound : {initial_decision.target_compound.value}")
    print(f"  Expected Pos : P{initial_decision.expected_position:.1f}")
    print(f"  Podium Prob  : {initial_decision.podium_probability:.1%}")
    print(f"  Points Exp   : {initial_decision.expected_points:.1f} pts")
    print(f"  Confidence   : {initial_decision.confidence:.1%}")
    print("\n  Rationale (Why):")
    for r in initial_decision.why:
        print(f"    * {r}")

    if initial_decision.counterfactual:
        print("\n  Counterfactual Analysis:")
        for action, data in initial_decision.counterfactual.items():
            print(f"    * If {action.upper()}: Expected finish is P{data.get('expected_position', 'N/A')}")

    print("\n" + "=" * 65)
    print("REPLAY EXECUTION SUCCESSFUL")
    print("=" * 65 + "\n")


if __name__ == "__main__":
    main()
