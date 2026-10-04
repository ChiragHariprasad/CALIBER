"""
CALIBRE — Live Race Suite Tests (§47, §37, §40, §33-35, §65)
=============================================================
Tests for live data ingestion, error signal detection,
racing line sector analysis, tyre strategy, and race coordinator.
"""

import asyncio
import pytest
from unittest.mock import AsyncMock

from calibre.schemas.race_telemetry import TyreCompound, TrackStatus, WeatherState, OpponentState
from calibre.live.openf1_client import OpenF1Client
from calibre.live.error_signal_agent import ErrorSignalAgent
from calibre.live.line_agent import LineAgent
from calibre.live.tyre_strategy_agent import TyreStrategyAgent
from calibre.live.race_coordinator import RaceCoordinator


@pytest.fixture
def mock_openf1_client():
    client = OpenF1Client(session_key=11731, target_driver="VER")
    return client


@pytest.mark.asyncio
async def test_error_signal_agent():
    agent = ErrorSignalAgent()

    # Feed normal telemetry then telemetry with spike
    tel_normal = {"speed_kph": 300.0, "throttle_pct": 100.0, "brake_pct": 0.0, "rpm": 11000}
    tel_spike = {"speed_kph": 450.0, "throttle_pct": 100.0, "brake_pct": 0.0, "rpm": 11000}

    signals1 = agent.process_telemetry("VER", tel_normal)
    signals2 = agent.process_telemetry("VER", tel_spike)

    assert any(s["signal_type"] == "TELEMETRY_SPIKE" for s in signals2)
    assert sum(agent.get_signal_summary().values()) > 0


@pytest.mark.asyncio
async def test_line_agent():
    agent = LineAgent()

    lap_ver = {"lap_number": 5, "lap_time_s": 88.5, "s1_time_s": 28.1, "s2_time_s": 30.4, "s3_time_s": 30.0, "is_valid": True}
    lap_lec = {"lap_number": 5, "lap_time_s": 89.0, "s1_time_s": 28.3, "s2_time_s": 30.5, "s3_time_s": 30.2, "is_valid": True}

    agent.process_lap("VER", lap_ver)
    agent.process_lap("LEC", lap_lec)

    assert "VER" in agent.ewm_pace
    assert "LEC" in agent.ewm_pace
    assert agent.ewm_pace["VER"] < agent.ewm_pace["LEC"]

    # Test DRS process
    agent.process_telemetry("VER", {"drs": 12})
    assert agent.drs_activations["VER"] == 1


@pytest.mark.asyncio
async def test_tyre_strategy_agent():
    agent = TyreStrategyAgent(target_driver="VER", total_laps=56)

    weather = WeatherState(air_temp_c=29.5, track_temp_c=41.2, rain_probability=0.0, rain_intensity=0.0, is_wet=False)
    opponents = [
        OpponentState(
            driver="NOR",
            position=2,
            gap_s=3.5,
            gap_trend_s_per_lap=-0.1,
            compound=TyreCompound.MEDIUM,
            tyre_age_laps=10,
            is_in_pit=False
        ),
    ]

    lap_data = {"lap_number": 10, "lap_time_s": 89.2, "is_pit_in_lap": False, "is_pit_out_lap": False}

    decision = agent.process_lap(
        driver="VER",
        lap_data=lap_data,
        position=1,
        compound=TyreCompound.MEDIUM,
        tyre_age=10,
        gap_ahead=None,
        gap_behind=3.5,
        driver_ahead=None,
        driver_behind="NOR",
        track_status=TrackStatus.GREEN,
        weather=weather,
        opponents=opponents,
    )

    assert decision is not None
    assert decision.decision in ["STAY_OUT", "PIT", "PIT_NOW", "BOX_THIS_LAP"]
    assert len(agent.strategy_evolution) == 1


@pytest.mark.asyncio
async def test_race_coordinator_initialization(tmp_path):
    coordinator = RaceCoordinator(
        session_key=11731,
        target_driver="VER",
        total_laps=56,
        log_dir=str(tmp_path),
    )

    state = coordinator.get_live_state()
    assert state["session_key"] == 11731
    assert state["target_driver"] == "VER"
    assert state["status"] == "INITIALIZING"
    assert coordinator._running is False
