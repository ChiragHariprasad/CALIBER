"""
CALIBRE Tests: Synthetic Strategy Scenarios (§49)
=================================================
Validates expected agent and model behaviour across the 5 canonical scenarios:
  1. Undercut
  2. Safety Car
  3. Tyre Cliff
  4. Weather Crossover
  5. False Driver Blame
"""
import pytest
from calibre.replay.scenarios.scenario_generator import ScenarioGenerator
from calibre.agent.state.state_machine import AgentStateMachine
from calibre.schemas.race_telemetry import TyreCompound, TrackStatus
from calibre.models.driver.driver_error_model import DriverErrorModel


def test_scenario_1_undercut():
    estimator = ScenarioGenerator.get_scenario("synthetic-undercut")
    agent = AgentStateMachine(state_estimator=estimator)
    decision = agent.force_replan()

    assert decision.decision == "PIT"
    assert decision.target_compound == TyreCompound.HARD
    assert any("undercut" in r.lower() for r in decision.why)


def test_scenario_2_safety_car():
    estimator = ScenarioGenerator.get_scenario("safety-car")
    assert estimator.pit_lane_loss_s == 11.5  # discounted pit loss under SC

    agent = AgentStateMachine(state_estimator=estimator)
    decision = agent.force_replan()

    assert decision.decision == "PIT"
    assert decision.urgency in ("HIGH", "CRITICAL")
    assert any("safety car" in r.lower() or "discounted" in r.lower() for r in decision.why)


def test_scenario_3_tyre_cliff():
    estimator = ScenarioGenerator.get_scenario("tyre-cliff")
    state = estimator.get_race_state()
    assert state.tyre.tyre_cliff_probability >= 0.50

    agent = AgentStateMachine(state_estimator=estimator)
    decision = agent.force_replan()

    assert decision.decision == "PIT"
    assert decision.urgency in ("HIGH", "CRITICAL")


def test_scenario_4_weather_crossover():
    estimator = ScenarioGenerator.get_scenario("weather-crossover")
    state = estimator.get_race_state()
    assert state.weather.rain_probability_pct >= 60.0

    agent = AgentStateMachine(state_estimator=estimator)
    decision = agent.force_replan()

    assert decision.decision == "PIT"
    assert decision.target_compound in (TyreCompound.INTERMEDIATE, TyreCompound.WET)


def test_scenario_5_false_driver_blame():
    model = DriverErrorModel()
    # Scenario: car is slow through T7 (Luffield) under yellow flag
    telemetry = {
        "time_loss_s": 0.45,
        "braking_point_m": 2320.0,
        "apex_speed_kph": 95.0,
    }

    # 1. Under yellow flag: must attribute to YELLOW_FLAG, NOT early braking
    insight_yellow = model.analyze_corner("T7", telemetry, track_status=TrackStatus.YELLOW)
    assert insight_yellow is not None
    assert insight_yellow.cause == "YELLOW_FLAG"
    assert insight_yellow.type == "ENVIRONMENTAL_TIME_LOSS"

    # 2. In dirty air / traffic: must attribute to TRAFFIC
    insight_traffic = model.analyze_corner("T7", telemetry, in_traffic=True)
    assert insight_traffic is not None
    assert insight_traffic.cause == "TRAFFIC"

    # 3. With tyre cliff: must attribute to TYRE_DEGRADATION
    insight_tyre = model.analyze_corner("T7", telemetry, tyre_cliff_active=True)
    assert insight_tyre is not None
    assert insight_tyre.cause == "TYRE_DEGRADATION"

    # 4. Clean green track, no traffic, fresh tyres: attributes to driver early braking
    insight_driver = model.analyze_corner("T7", telemetry)
    assert insight_driver is not None
    assert insight_driver.cause == "EARLY_BRAKING"
    assert insight_driver.type == "DRIVER_TIME_LOSS"
