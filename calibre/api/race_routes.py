"""
CALIBRE — Race Engineering REST & WebSocket API (§52)
======================================================
Exposes real-time race state, telemetry, strategy recommendations,
tyre degradation predictions, driver loss attributions, and replay control.
"""
from __future__ import annotations

import asyncio
from typing import List, Dict, Any, Optional
from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Query
from pydantic import BaseModel

from calibre.schemas.race_telemetry import (
    RaceState,
    StrategyDecision,
    StrategyCandidate,
    TyreState,
    DriverInsight,
    OpponentState,
)
from calibre.replay.scenarios.scenario_generator import ScenarioGenerator
from calibre.agent.state.state_machine import AgentStateMachine
from calibre.track.circuit_model import CircuitModel

race_router = APIRouter(prefix="/api/v1")

# Global singleton session state
_estimator = ScenarioGenerator.get_scenario("synthetic-undercut")
_agent = AgentStateMachine(state_estimator=_estimator)
_circuit = CircuitModel()


class ScenarioRequest(BaseModel):
    scenario: str = "synthetic-undercut"


@race_router.get("/session")
async def get_session():
    """Returns current race snapshot."""
    return _estimator.get_race_state()


@race_router.get("/telemetry/live")
async def get_live_telemetry():
    """Returns live vehicle dynamic channels."""
    state = _estimator.get_race_state()
    return {
        "timestamp": state.session_time_s,
        "driver": state.driver,
        "lap": state.lap,
        "speed_kph": 288.4,
        "throttle_pct": 98.0,
        "brake_pct": 0.0,
        "gear": 7,
        "rpm": 11850,
        "drs": True,
        "lat_accel_g": 3.4,
        "lon_accel_g": 0.8,
    }


@race_router.get("/strategy/recommendation", response_model=StrategyDecision)
async def get_strategy_recommendation():
    """Returns agent strategic recommendation with numerical backing."""
    return _agent.force_replan()


@race_router.get("/strategy/candidates", response_model=List[StrategyCandidate])
async def get_strategy_candidates():
    """Returns all evaluated Monte Carlo candidate strategy branches."""
    return _agent.tools.compare_strategies()


@race_router.get("/tyres", response_model=TyreState)
async def get_tyres():
    """Returns stateful tyre condition and cliff probability."""
    state = _estimator.get_race_state()
    return state.tyre


@race_router.get("/driver/losses", response_model=List[DriverInsight])
async def get_driver_losses():
    """Returns corner-level driver loss attribution."""
    state = _estimator.get_race_state()
    losses = []

    # Check T7
    ins7 = _agent.tools.get_driver_insights("T7", {
        "time_loss_s": 0.18,
        "braking_point_m": 2338.0,  # 7m early
        "apex_speed_kph": 108.0,
        "throttle_pickup_m": 2480.0,
    })
    if ins7:
        losses.append(ins7)

    # Check T16
    ins16 = _agent.tools.get_driver_insights("T16", {
        "time_loss_s": 0.22,
        "braking_point_m": 5050.0,
        "apex_speed_kph": 95.0,
        "throttle_pickup_m": 5176.0,  # 11m late
    })
    if ins16:
        losses.append(ins16)

    return losses


@race_router.get("/opponents", response_model=List[OpponentState])
async def get_opponents():
    """Returns competitor tracking and undercut threats."""
    return _agent.tools.get_opponent_state()


@race_router.get("/insights")
async def get_insights():
    """Returns chronological tactical event feed."""
    return _agent.recent_events


@race_router.post("/replay/scenario")
async def set_scenario(req: ScenarioRequest):
    """Loads a specific synthetic scenario."""
    global _estimator, _agent
    _estimator = ScenarioGenerator.get_scenario(req.scenario)
    _agent = AgentStateMachine(state_estimator=_estimator)
    dec = _agent.force_replan()
    return {
        "status": "loaded",
        "scenario": req.scenario,
        "initial_decision": dec.decision,
        "target_lap": dec.target_lap,
    }


@race_router.websocket("/ws/telemetry")
async def websocket_telemetry(websocket: WebSocket):
    """WebSocket stream pushing live telemetry & strategy updates."""
    await websocket.accept()
    try:
        while True:
            state = _estimator.get_race_state()
            dec = _agent.current_decision or _agent.force_replan()
            await websocket.send_json({
                "type": "TELEMETRY_TICK",
                "state": state.model_dump(),
                "decision": dec.model_dump(),
            })
            await asyncio.sleep(0.5)
    except WebSocketDisconnect:
        pass
