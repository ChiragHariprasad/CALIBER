"""
CALIBRE — Race Coordinator (§65)
==================================
Master async orchestrator that runs all live agents concurrently,
processes data from the OpenF1 client, logs everything to JSONL
for post-race analysis, and pushes updates to the WebSocket bus.

This is the heart of the live race system. It:
  1. Starts the OpenF1 poller
  2. Consumes telemetry, lap, and event queues
  3. Fans out data to Error Signal, Line, and Tyre Strategy agents
  4. Logs every event, decision, and insight to JSONL
  5. Maintains a global state snapshot for API/WebSocket access
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from calibre.live.openf1_client import OpenF1Client
from calibre.live.error_signal_agent import ErrorSignalAgent
from calibre.live.line_agent import LineAgent
from calibre.live.tyre_strategy_agent import TyreStrategyAgent
from calibre.schemas.race_telemetry import TrackStatus, TyreCompound

logger = logging.getLogger(__name__)


class RaceCoordinator:
    """Master orchestrator for live race intelligence."""

    def __init__(
        self,
        session_key: int,
        target_driver: str = "VER",
        total_laps: int = 56,
        log_dir: str = "data/race_logs",
        mock_mode: bool = False,
        api_token: Optional[str] = None,
    ):
        self.session_key = session_key
        self.target_driver = target_driver.upper()
        self.total_laps = total_laps
        self.mock_mode = mock_mode

        # Initialize all components
        self.client = OpenF1Client(
            session_key=session_key,
            target_driver=target_driver,
            mock_mode=mock_mode,
            api_token=api_token,
        )
        self.error_agent = ErrorSignalAgent()
        self.line_agent = LineAgent()
        self.strategy_agent = TyreStrategyAgent(
            target_driver=target_driver,
            total_laps=total_laps,
        )

        # Logging
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        race_date = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
        self.log_file = self.log_dir / f"race_{session_key}_{race_date}.jsonl"
        self._log_handle = None

        # Global state for API consumers
        self.live_state: Dict[str, Any] = {
            "session_key": session_key,
            "target_driver": target_driver,
            "status": "INITIALIZING",
            "current_lap": 0,
            "total_laps": total_laps,
            "track_status": "GREEN",
            "last_update_ts": 0,
            "errors": [],
            "line_insights": [],
            "strategy": None,
            "strategy_candidates": [],
            "pace_summary": {},
            "weather": {},
            "positions": {},
            "laps_processed": 0,
            "telemetry_samples": 0,
        }

        # WebSocket subscribers
        self._ws_subscribers: List[asyncio.Queue] = []

        # Control
        self._running = False
        self._tasks: List[asyncio.Task] = []

    async def start(self):
        """Start all systems."""
        logger.info("=" * 65)
        logger.info("CALIBRE LIVE RACE COORDINATOR — STARTING")
        logger.info("Session: %d | Driver: %s | Laps: %d", self.session_key, self.target_driver, self.total_laps)
        logger.info("=" * 65)

        # Open log file
        self._log_handle = open(self.log_file, "a", encoding="utf-8")
        self._log_event("SYSTEM", "COORDINATOR_START", {
            "session_key": self.session_key,
            "target_driver": self.target_driver,
            "total_laps": self.total_laps,
        })

        # Start OpenF1 data client
        await self.client.start()
        self.live_state["status"] = "LIVE"

        self._running = True

        # Launch consumer tasks
        self._tasks = [
            asyncio.create_task(self._consume_telemetry()),
            asyncio.create_task(self._consume_laps()),
            asyncio.create_task(self._consume_events()),
            asyncio.create_task(self._periodic_summary()),
        ]

        logger.info("All systems live — consuming data streams")

    async def stop(self):
        """Gracefully stop all systems."""
        self._running = False
        self.live_state["status"] = "STOPPED"

        # Stop client
        await self.client.stop()

        # Cancel tasks
        for task in self._tasks:
            task.cancel()

        # Log final summary
        self._log_event("SYSTEM", "COORDINATOR_STOP", {
            "laps_processed": self.live_state["laps_processed"],
            "telemetry_samples": self.live_state["telemetry_samples"],
            "error_signals": len(self.error_agent.signal_history),
            "strategy_evaluations": len(self.strategy_agent.strategy_evolution),
        })

        # Close log file
        if self._log_handle:
            self._log_handle.close()

        logger.info("=" * 65)
        logger.info("CALIBRE RACE COORDINATOR STOPPED")
        logger.info("Log file: %s", self.log_file)
        logger.info("Total laps processed: %d", self.live_state["laps_processed"])
        logger.info("Total error signals: %d", len(self.error_agent.signal_history))
        logger.info("Strategy evaluations: %d", len(self.strategy_agent.strategy_evolution))
        logger.info("=" * 65)

    async def _consume_telemetry(self):
        """Consumer for high-frequency telemetry data."""
        while self._running:
            try:
                item = await asyncio.wait_for(self.client.telemetry_queue.get(), timeout=1.0)
                driver = item["driver"]
                data = item["data"]

                self.live_state["telemetry_samples"] += 1

                # Error signal analysis
                errors = self.error_agent.process_telemetry(driver, data)
                if errors:
                    self.live_state["errors"].extend(errors)
                    for err in errors:
                        self._log_event(driver, "ERROR_SIGNAL", err)

                # Line analysis (DRS tracking)
                self.line_agent.process_telemetry(driver, data)

                # Update positions from client cache
                pos = self.client.get_driver_position(driver)
                if pos:
                    pos_errors = self.error_agent.process_position_update(driver, pos)
                    if pos_errors:
                        self.live_state["errors"].extend(pos_errors)

            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error("Telemetry consumer error: %s", e, exc_info=True)

    async def _consume_laps(self):
        """Consumer for lap completion events — triggers strategy re-evaluation."""
        while self._running:
            try:
                item = await asyncio.wait_for(self.client.lap_queue.get(), timeout=1.0)
                driver = item["driver"]
                lap_data = item["data"]
                lap_number = lap_data.get("lap_number", 0)

                self.live_state["laps_processed"] += 1
                self.live_state["current_lap"] = max(self.live_state["current_lap"], lap_number)

                # Log the raw lap
                self._log_event(driver, "LAP_COMPLETED", lap_data)

                # Error signal analysis
                errors = self.error_agent.process_lap(driver, lap_data)
                if errors:
                    self.live_state["errors"].extend(errors)
                    for err in errors:
                        self._log_event(driver, "ERROR_SIGNAL", err)

                # Line analysis
                line_insights = self.line_agent.process_lap(driver, lap_data)
                if line_insights:
                    self.live_state["line_insights"].extend(line_insights[-5:])  # Keep latest 5
                    for insight in line_insights:
                        self._log_event(driver, "LINE_INSIGHT", insight)

                # Traffic detection
                gap_ahead = None
                if driver == self.target_driver:
                    gap_ahead = self.client.latest_intervals.get(driver)
                traffic_insights = self.line_agent.detect_traffic(driver, gap_ahead)
                if traffic_insights:
                    for ti in traffic_insights:
                        self._log_event(driver, "TRAFFIC", ti)

                # Stint tracking
                stint_info = self.client.latest_stints.get(driver, {})
                compound_str = stint_info.get("compound", "MEDIUM")
                stint_num = stint_info.get("stint_number", 1)
                stint_errors = self.error_agent.process_stint_update(driver, compound_str, stint_num)
                if stint_errors:
                    for se in stint_errors:
                        self._log_event(driver, "STINT_CHANGE", se)

                # Tyre strategy evaluation
                compound = self.client.get_driver_compound(driver)
                tyre_age = self.client.get_driver_tyre_age(driver)
                position = self.client.get_driver_position(driver)
                weather = self.client.get_weather_state()
                opponents = self.client.build_opponents(self.target_driver) if driver == self.target_driver else []

                # Determine gap ahead/behind for target driver
                sorted_drivers = self.client.get_sorted_drivers_by_position()
                target_pos = self.client.get_driver_position(self.target_driver)
                gap_a = None
                gap_b = 5.0
                driver_a = None
                driver_b = None

                for i, d in enumerate(sorted_drivers):
                    if d == self.target_driver:
                        if i > 0:
                            driver_a = sorted_drivers[i - 1]
                            # Estimate gap from intervals
                            my_interval = self.client.latest_intervals.get(self.target_driver, 0)
                            ahead_interval = self.client.latest_intervals.get(driver_a, 0)
                            if my_interval and ahead_interval:
                                gap_a = abs(my_interval - ahead_interval)
                            else:
                                gap_a = 2.0
                        if i < len(sorted_drivers) - 1:
                            driver_b = sorted_drivers[i + 1]
                            behind_interval = self.client.latest_intervals.get(driver_b, 0)
                            my_interval = self.client.latest_intervals.get(self.target_driver, 0)
                            if my_interval and behind_interval:
                                gap_b = abs(behind_interval - my_interval)
                            else:
                                gap_b = 3.0
                        break

                decision = self.strategy_agent.process_lap(
                    driver=driver,
                    lap_data=lap_data,
                    position=position,
                    compound=compound,
                    tyre_age=tyre_age,
                    gap_ahead=gap_a,
                    gap_behind=gap_b,
                    driver_ahead=driver_a,
                    driver_behind=driver_b,
                    track_status=self.client.track_status,
                    weather=weather,
                    opponents=opponents,
                )

                if decision and driver == self.target_driver:
                    self.live_state["strategy"] = decision.model_dump()
                    self.live_state["strategy_candidates"] = [
                        c.model_dump() for c in self.strategy_agent.current_candidates
                    ]
                    self._log_event(self.target_driver, "STRATEGY_DECISION", decision.model_dump())

                    # Push to WebSocket subscribers
                    await self._broadcast_ws({
                        "type": "STRATEGY_UPDATE",
                        "lap": lap_number,
                        "decision": decision.model_dump(),
                    })

                # Update pace summary every 5 laps
                if self.live_state["laps_processed"] % 5 == 0:
                    self.live_state["pace_summary"] = self.line_agent.get_pace_summary()

            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error("Lap consumer error: %s", e, exc_info=True)

    async def _consume_events(self):
        """Consumer for race control events (SC, VSC, flags, etc.)."""
        while self._running:
            try:
                item = await asyncio.wait_for(self.client.event_queue.get(), timeout=1.0)
                event_type = item.get("type", "UNKNOWN")

                self.live_state["track_status"] = item.get("new", "GREEN")
                self._log_event("RACE", event_type, item)

                logger.info("[RACE CONTROL] %s: %s → %s | %s",
                            event_type, item.get("old"), item.get("new"), item.get("message", ""))

                # Weather updates
                weather = self.client.get_weather_state()
                if self.client.latest_weather:
                    weather_errors = self.error_agent.process_weather_update(self.client.latest_weather)
                    for we in weather_errors:
                        self._log_event("WEATHER", "WEATHER_SHIFT", we)

                self.live_state["weather"] = weather.model_dump()

                # Trigger immediate strategy re-evaluation on track status changes
                if event_type == "TRACK_STATUS_CHANGE":
                    opponents = self.client.build_opponents(self.target_driver)
                    decision = self.strategy_agent.process_event(
                        event_type=event_type,
                        track_status=self.client.track_status,
                        weather=weather,
                        opponents=opponents,
                    )
                    if decision:
                        self.live_state["strategy"] = decision.model_dump()
                        self._log_event(self.target_driver, "STRATEGY_REPLAN", decision.model_dump())

                        await self._broadcast_ws({
                            "type": "STRATEGY_REPLAN",
                            "trigger": event_type,
                            "decision": decision.model_dump(),
                        })

            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error("Event consumer error: %s", e, exc_info=True)

    async def _periodic_summary(self):
        """Periodic state summary logging (every 30 seconds)."""
        while self._running:
            await asyncio.sleep(30.0)

            self.live_state["positions"] = dict(self.client.latest_positions)
            self.live_state["last_update_ts"] = time.time()

            # Prune old errors (keep last 20)
            self.live_state["errors"] = self.live_state["errors"][-20:]
            self.live_state["line_insights"] = self.live_state["line_insights"][-10:]

            self._log_event("SYSTEM", "PERIODIC_SUMMARY", {
                "lap": self.live_state["current_lap"],
                "positions": self.live_state["positions"],
                "telemetry_samples": self.live_state["telemetry_samples"],
                "error_count": len(self.error_agent.signal_history),
                "strategy_count": len(self.strategy_agent.strategy_evolution),
                "track_status": self.live_state["track_status"],
            })

    # ─── WebSocket Management ────────────────────────────────────

    def subscribe_ws(self) -> asyncio.Queue:
        """Add a WebSocket subscriber queue."""
        q: asyncio.Queue = asyncio.Queue(maxsize=100)
        self._ws_subscribers.append(q)
        return q

    def unsubscribe_ws(self, q: asyncio.Queue):
        """Remove a WebSocket subscriber."""
        if q in self._ws_subscribers:
            self._ws_subscribers.remove(q)

    async def _broadcast_ws(self, message: Dict[str, Any]):
        """Broadcast a message to all WebSocket subscribers."""
        dead = []
        for q in self._ws_subscribers:
            try:
                q.put_nowait(message)
            except asyncio.QueueFull:
                dead.append(q)
        for d in dead:
            self._ws_subscribers.remove(d)

    # ─── Logging ──────────────────────────────────────────────────

    def _log_event(self, driver: str, event_type: str, data: Any):
        """Append a structured event to the JSONL log file."""
        entry = {
            "ts": time.time(),
            "iso": datetime.now(timezone.utc).isoformat(),
            "driver": driver,
            "type": event_type,
            "data": data,
        }
        if self._log_handle:
            try:
                line = json.dumps(entry, default=str)
                self._log_handle.write(line + "\n")
                self._log_handle.flush()
            except Exception as e:
                logger.error("Log write error: %s", e)

    # ─── API Accessors ────────────────────────────────────────────

    def get_live_state(self) -> Dict[str, Any]:
        """Returns the current global state snapshot for API consumers."""
        return self.live_state.copy()

    def get_error_signals(self) -> List[Dict[str, Any]]:
        """Returns active error signals."""
        return self.error_agent.get_active_signals()

    def get_pace_model(self) -> Dict[str, Any]:
        """Returns pace analysis for all drivers."""
        return {
            "pace_summary": self.line_agent.get_pace_summary(),
            "field_ranking": self.line_agent.get_field_ranking(),
            "best_sectors": {
                str(k): {"driver": v[0], "time_s": round(v[1], 3)}
                for k, v in self.line_agent.best_sectors.items()
            },
        }

    def get_strategy_for_driver(self, driver: str) -> Dict[str, Any]:
        """Returns strategy state for a driver."""
        if driver == self.target_driver:
            return self.strategy_agent.get_strategy_summary()
        return {"message": f"Strategy not tracked for {driver} — only {self.target_driver}"}

    def get_strategy_evolution(self) -> List[Dict[str, Any]]:
        """Returns the full strategy evolution trail."""
        return self.strategy_agent.strategy_evolution
