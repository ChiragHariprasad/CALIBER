"""
CALIBRE — Live Race API Routes
================================
REST and WebSocket endpoints for live race data, error signals,
pace analysis, and real-time strategy recommendations.

These endpoints are backed by the RaceCoordinator singleton when
a live session is active.
"""
from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, WebSocket, WebSocketDisconnect, HTTPException
from pydantic import BaseModel

live_router = APIRouter(prefix="/api/v1/live", tags=["live"])

# Global coordinator reference — set by the startup hook
_coordinator = None


def set_coordinator(coordinator):
    """Set the global coordinator reference (called from main.py or race_day.py)."""
    global _coordinator
    _coordinator = coordinator


def _require_coordinator():
    if _coordinator is None:
        raise HTTPException(status_code=503, detail="No live session active. Start with: python -m calibre.live.race_day --session-key <KEY>")
    return _coordinator


class LiveStartRequest(BaseModel):
    session_key: int = 11731
    target_driver: str = "VER"
    total_laps: int = 56


# ─── REST Endpoints ──────────────────────────────────────────


@live_router.get("/status")
async def get_live_status():
    """Returns whether a live session is active and its current state."""
    if _coordinator is None:
        return {
            "active": False,
            "message": "No live session active",
            "hint": "Start with: python -m calibre.live.race_day --session-key <KEY>",
        }
    state = _coordinator.get_live_state()
    return {
        "active": True,
        "session_key": state.get("session_key"),
        "target_driver": state.get("target_driver"),
        "status": state.get("status"),
        "current_lap": state.get("current_lap"),
        "total_laps": state.get("total_laps"),
        "track_status": state.get("track_status"),
        "telemetry_samples": state.get("telemetry_samples"),
        "laps_processed": state.get("laps_processed"),
    }


@live_router.get("/errors")
async def get_live_errors():
    """Returns current active error signals."""
    coord = _require_coordinator()
    return {
        "active_signals": coord.get_error_signals(),
        "signal_summary": coord.error_agent.get_signal_summary(),
        "total_signals": len(coord.error_agent.signal_history),
    }


@live_router.get("/pace")
async def get_live_pace():
    """Returns pace model for all drivers."""
    coord = _require_coordinator()
    return coord.get_pace_model()


@live_router.get("/strategy/{driver}")
async def get_live_strategy(driver: str):
    """Returns strategy state for a specific driver."""
    coord = _require_coordinator()
    return coord.get_strategy_for_driver(driver.upper())


@live_router.get("/strategy-evolution")
async def get_strategy_evolution():
    """Returns the full strategy evolution trail for post-race analysis."""
    coord = _require_coordinator()
    return {
        "driver": coord.target_driver,
        "evolution": coord.get_strategy_evolution(),
        "total_evaluations": len(coord.strategy_agent.strategy_evolution),
    }


@live_router.get("/positions")
async def get_live_positions():
    """Returns current race positions for all drivers."""
    coord = _require_coordinator()
    return {
        "positions": dict(coord.client.latest_positions),
        "sorted": coord.client.get_sorted_drivers_by_position(),
        "track_status": coord.client.track_status.value,
    }


@live_router.get("/weather")
async def get_live_weather():
    """Returns current weather data."""
    coord = _require_coordinator()
    return {
        "weather": coord.client.get_weather_state().model_dump(),
        "raw": coord.client.latest_weather,
    }


@live_router.get("/stints")
async def get_live_stints():
    """Returns current stint data for all drivers."""
    coord = _require_coordinator()
    return {
        "stints": dict(coord.client.latest_stints),
    }


@live_router.get("/insights")
async def get_live_insights():
    """Returns the chronological feed of all insights and events."""
    coord = _require_coordinator()
    state = coord.get_live_state()
    return {
        "errors": state.get("errors", [])[-20:],
        "line_insights": state.get("line_insights", [])[-20:],
        "race_control": coord.client.race_control_messages[-20:],
    }


@live_router.get("/snapshot")
async def get_full_snapshot():
    """Returns a complete snapshot of all live data."""
    coord = _require_coordinator()
    return coord.get_live_state()


# ─── WebSocket ────────────────────────────────────────────────


@live_router.websocket("/ws")
async def websocket_live(websocket: WebSocket):
    """
    WebSocket stream pushing live race updates:
    - Strategy decisions
    - Error signals
    - Track status changes
    - Periodic state snapshots
    """
    coord = _require_coordinator()
    await websocket.accept()

    # Subscribe to coordinator broadcasts
    ws_queue = coord.subscribe_ws()

    try:
        # Send initial state
        await websocket.send_json({
            "type": "INITIAL_STATE",
            "data": coord.get_live_state(),
        })

        while True:
            try:
                # Wait for broadcast message (with timeout for keepalive)
                msg = await asyncio.wait_for(ws_queue.get(), timeout=5.0)
                await websocket.send_json(msg)
            except asyncio.TimeoutError:
                # Send keepalive with current state snapshot
                await websocket.send_json({
                    "type": "KEEPALIVE",
                    "data": {
                        "status": coord.live_state.get("status"),
                        "lap": coord.live_state.get("current_lap"),
                        "track_status": coord.live_state.get("track_status"),
                        "telemetry_samples": coord.live_state.get("telemetry_samples"),
                    },
                })
    except WebSocketDisconnect:
        pass
    finally:
        coord.unsubscribe_ws(ws_queue)
