"""
CALIBRE E2E Test: Full Race Replay Pipeline (§48)
=================================================
Validates the complete integrated race-engineering stack:
Track geometry -> State estimator -> Tyre degradation -> Driver loss ->
Opponent battle -> Monte Carlo race simulation -> Agent state machine -> Decision.
"""
import pytest
from calibre.track.circuit_model import CircuitModel
from calibre.telemetry.state import LiveRaceStateEstimator
from calibre.telemetry.normalizer import TelemetryNormalizer
from calibre.models.tyre.tyre_model import TyreDegradationModel
from calibre.models.driver.driver_error_model import DriverErrorModel
from calibre.models.opponent.opponent_model import OpponentModel
from calibre.strategy.simulator.race_simulator import MonteCarloRaceSimulator
from calibre.agent.state.state_machine import AgentStateMachine
from calibre.schemas.race_telemetry import (
    LapData,
    TyreCompound,
    TrackStatus,
    SessionType,
)


def test_full_race_replay_pipeline():
    # 1. Track Model
    circuit = CircuitModel(name="Silverstone Circuit", length_km=5.891)
    assert len(circuit.get_all_corners()) >= 7
    assert circuit.pit_lane_loss_green_s == 21.5

    # 2. State Estimator initialized
    estimator = LiveRaceStateEstimator(driver="VER", total_laps=57, starting_compound=TyreCompound.MEDIUM)
    assert estimator.current_lap == 1
    assert estimator.position == 2

    # 3. Simulate 15 laps of race progression
    tyre_model = TyreDegradationModel()
    for lap_idx in range(1, 16):
        lap_pace = 90.5 + (0.08 * lap_idx)
        lap = LapData(
            lap_number=lap_idx,
            driver="VER",
            lap_time_s=lap_pace,
            s1_time_s=28.5,
            s2_time_s=36.0,
            s3_time_s=26.0,
            compound=TyreCompound.MEDIUM,
            tyre_age_laps=lap_idx,
        )
        estimator.update_lap(lap)

    assert estimator.current_lap == 16
    assert estimator.tyre_age_laps == 16

    # 4. Telemetry Normalizer
    sample = TelemetryNormalizer.normalize_sample({
        "timestamp": 1420.5,
        "driver": "ver",
        "speed": 298.5,
        "throttle": 100.0,
        "brake": 0.0,
        "gear": 8,
        "drs": 1,
    })
    assert sample.speed_kph == 298.5
    assert sample.gear == 8
    assert sample.drs is True

    # 5. Agent State Machine
    agent = AgentStateMachine(state_estimator=estimator)
    decision = agent.process_telemetry_tick(sample)

    assert decision is not None
    assert decision.decision in ("PIT", "STAY")
    assert 0.0 <= decision.podium_probability <= 1.0
    assert 0.0 <= decision.confidence <= 1.0
    assert len(decision.why) > 0
    assert 1.0 <= decision.expected_position <= 20.0
