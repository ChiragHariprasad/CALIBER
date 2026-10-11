"""
CALIBRE — Race Coordinator (§65)
==================================
Master async orchestrator that coordinates:
  1. Multi-point data stream ingestion (OpenF1, F1 Insights, Jolpica, Mock)
  2. Operating Mode State Machine (LIVE / HISTORICAL_REPLAY / SIMULATION)
  3. Sensor Integrity & Anomaly Intelligence (ErrorSignalAgent, CUSUM)
  4. Racing Line, Pace & Sector Performance (LineAgent)
  5. Predictive Tyre Degradation & Strategy Optimization (TyreStrategyAgent)
  6. Pit Lane Rule Monitoring & Verdict Engine (PitLaneMonitor)
  7. Tactical Driver Radio Synthesis (DriverRadioEngine)
  8. Streaming event fanout via WebSockets and persistent JSONL audit trail
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

from calibre.live.driver_radio import DriverRadioEngine, RadioCategory, RadioPriority
from calibre.live.error_signal_agent import ErrorSignalAgent
from calibre.live.line_agent import LineAgent
from calibre.live.multipoint_aggregator import MultiPointAggregator
from calibre.live.openf1_client import OpenF1Client
from calibre.live.operating_mode import OperatingMode, OperatingModeController
from calibre.live.pit_lane_monitor import PitLaneMonitor
from calibre.live.schemas.events import CalibreEvent, EventType
from calibre.live.tyre_strategy_agent import TyreStrategyAgent
from calibre.schemas.race_telemetry import TrackStatus, TyreCompound

logger = logging.getLogger(__name__)


class RaceCoordinator:
    """Master orchestrator for live race intelligence and multi-point streaming."""

    def __init__(
        self,
        session_key: int = 11731,
        target_driver: str = "VER",
        total_laps: int = 56,
        log_dir: str = "data/race_logs",
        mock_mode: bool = False,
        api_token: Optional[str] = None,
        use_multipoint_aggregator: bool = True,
        operating_mode: OperatingMode = OperatingMode.LIVE,
    ):
        self.session_key = session_key
        self.target_driver = target_driver.upper()
        self.total_laps = total_laps
        self.mock_mode = mock_mode
        self.use_multipoint_aggregator = use_multipoint_aggregator

        # Mode controller
        self.mode_controller = OperatingModeController()
        if mock_mode:
            self.mode_controller.transition_to(
                OperatingMode.SIMULATION,
                reason="Started with mock_mode=True",
                scenario="mock_telemetry_flow",
            )
        else:
            self.mode_controller.transition_to(
                operating_mode,
                reason=f"Coordinator initialized in {operating_mode.value} mode",
                session_key=session_key,
            )

        # Core analytical agents
        self.error_agent = ErrorSignalAgent()
        self.line_agent = LineAgent()
        self.strategy_agent = TyreStrategyAgent(
            target_driver=target_driver,
            total_laps=total_laps,
        )
        self.pit_lane_monitor = PitLaneMonitor(speed_limit_kmh=60.0)
        self.radio_engine = DriverRadioEngine(target_driver=target_driver)

        # Ingestion layer
        self.client = OpenF1Client(
            session_key=session_key,
            target_driver=target_driver,
            mock_mode=mock_mode,
            api_token=api_token,
        )
        self.aggregator: Optional[MultiPointAggregator] = None
        if self.use_multipoint_aggregator:
            self.aggregator = MultiPointAggregator.create_default_multi_point_suite(
                session_key=session_key,
                target_driver=target_driver,
                enable_insights_stream=True,
                enable_openf1=True,
                enable_jolpica=True,
                enable_mock_fallback=mock_mode,
                operating_mode=self.mode_controller.current_mode.value,
            )

        # Audit logging
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        race_date = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
        self.log_file = self.log_dir / f"race_{session_key}_{race_date}.jsonl"
        self._log_handle = None

        # Global reactive state snapshot
        self.live_state: Dict[str, Any] = {
            "session_key": session_key,
            "target_driver": target_driver,
            "operating_mode": self.mode_controller.current_mode.value,
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
            "verdicts_count": 0,
            "radio_messages_count": 0,
            "last_verdict": None,
            "last_radio_message": None,
        }

        # WebSocket subscribers
        self._ws_subscribers: List[asyncio.Queue] = []
        self._running = False
        self._tasks: List[asyncio.Task] = []

    # ── Lifecycle ────────────────────────────────────────────────────

    async def start(self) -> None:
        """Starts all streaming sources, processing loops, and logging."""
        logger.info("=" * 65)
        logger.info("CALIBRE RACE COORDINATOR — MULTI-POINT STREAMING ACTIVE")
        logger.info(
            "Session: %d | Driver: %s | Mode: %s",
            self.session_key,
            self.target_driver,
            self.mode_controller.current_mode.value,
        )
        logger.info("=" * 65)

        self._log_handle = open(self.log_file, "a", encoding="utf-8")
        self._log_event("SYSTEM", "COORDINATOR_START", {
            "session_key": self.session_key,
            "target_driver": self.target_driver,
            "total_laps": self.total_laps,
            "operating_mode": self.mode_controller.current_mode.value,
            "multipoint": self.use_multipoint_aggregator,
        })

        self.live_state["status"] = "LIVE"
        self._running = True

        # Start ingestion sources
        if self.aggregator:
            await self.aggregator.start()
            self._tasks.append(asyncio.create_task(self._consume_aggregator_events()))

        # Also start base OpenF1 client to back up data queues
        await self.client.start()
        self._tasks.extend([
            asyncio.create_task(self._consume_telemetry()),
            asyncio.create_task(self._consume_laps()),
            asyncio.create_task(self._consume_events()),
            asyncio.create_task(self._periodic_summary()),
        ])

        # Initial radio check
        radio_msg = self.radio_engine.generate_sector_advice(
            lap_number=1, sector=1, delta_s=0.0
        )
        self.live_state["last_radio_message"] = radio_msg.to_dict()

        logger.info("All CALIBRE streaming & intelligence services running.")

    async def stop(self) -> None:
        """Gracefully stops all workers and flushes logs."""
        self._running = False
        self.live_state["status"] = "STOPPED"

        if self.aggregator:
            await self.aggregator.stop()

        await self.client.stop()

        for t in self._tasks:
            t.cancel()
        self._tasks.clear()

        self._log_event("SYSTEM", "COORDINATOR_STOP", {
            "laps_processed": self.live_state["laps_processed"],
            "telemetry_samples": self.live_state["telemetry_samples"],
            "error_signals": len(self.error_agent.signal_history),
            "verdicts": len(self.pit_lane_monitor.get_recent_verdicts(100)),
            "radio_messages": len(self.radio_engine.get_recent_messages(100)),
        })

        if self._log_handle:
            self._log_handle.close()
            self._log_handle = None

        logger.info("CALIBRE Race Coordinator stopped cleanly.")

    # ── Multi-Point Stream Consumer ──────────────────────────────────

    async def _consume_aggregator_events(self) -> None:
        """Consumes prioritized, deduplicated canonical events from MultiPointAggregator."""
        while self._running:
            try:
                ev: Optional[CalibreEvent] = await self.aggregator.get_next_event(timeout=1.0)
                if ev is None:
                    continue

                # Process by canonical EventType
                if ev.event_type == EventType.TELEMETRY:
                    await self._handle_canonical_telemetry(ev)
                elif ev.event_type == EventType.LAP_COMPLETED:
                    await self._handle_canonical_lap(ev)
                elif ev.event_type == EventType.RACE_CONTROL:
                    await self._handle_canonical_race_control(ev)
                elif ev.event_type == EventType.VERDICT:
                    self.live_state["last_verdict"] = ev.data
                    self.live_state["verdicts_count"] += 1
                    await self._broadcast_ws({"type": "VERDICT", "data": ev.data})
                elif ev.event_type == EventType.WEATHER:
                    self.live_state["weather"] = ev.data

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.debug("Error processing aggregator event: %s", e)
                await asyncio.sleep(0.05)

    async def _handle_canonical_telemetry(self, ev: CalibreEvent) -> None:
        """Processes high-frequency telemetry from unified canonical stream."""
        drv = ev.driver or "VER"
        data = ev.data
        self.live_state["telemetry_samples"] += 1

        # Evaluate against pit-lane regulations & CUSUM integrity
        verdict = self.pit_lane_monitor.evaluate_telemetry(ev)
        if verdict:
            self.live_state["last_verdict"] = {
                "rule": verdict.rule_id,
                "driver": verdict.driver_number,
                "verdict": verdict.verdict.value,
                "speed": verdict.measured_value,
                "limit": verdict.threshold,
            }
            self.live_state["verdicts_count"] += 1
            if verdict.verdict.value == "BREACH":
                # Generate steward warning radio
                radio = self.radio_engine.generate_verdict_notification(
                    driver_number=str(verdict.driver_number),
                    verdict=verdict.verdict.value,
                    lap_number=self.live_state["current_lap"],
                )
                self.live_state["last_radio_message"] = radio.to_dict()
                await self._broadcast_ws({"type": "RADIO_MESSAGE", "data": radio.to_dict()})

            await self._broadcast_ws({
                "type": "VERDICT",
                "data": self.live_state["last_verdict"],
            })

    async def _handle_canonical_lap(self, ev: CalibreEvent) -> None:
        """Processes lap completed events from unified stream."""
        lap_num = int(ev.data.get("lap_number", 0))
        self.live_state["current_lap"] = max(self.live_state["current_lap"], lap_num)

    async def _handle_canonical_race_control(self, ev: CalibreEvent) -> None:
        """Processes race control flags (SC, VSC, Yellow, Red, Green)."""
        flag = ev.data.get("flag", "GREEN")
        self.live_state["track_status"] = flag
        if flag in ("SC", "SAFETY_CAR", "VSC", "YELLOW"):
            radio = self.radio_engine.generate_safety_car_directive(
                track_status=flag,
                lap_number=self.live_state["current_lap"],
            )
            self.live_state["last_radio_message"] = radio.to_dict()
            await self._broadcast_ws({"type": "RADIO_MESSAGE", "data": radio.to_dict()})

    # ── Client Queue Consumers ───────────────────────────────────────

    async def _consume_telemetry(self) -> None:
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

                # Line analysis
                self.line_agent.process_telemetry(driver, data)

                # Pit lane evaluation
                verdict = self.pit_lane_monitor.evaluate_telemetry(data)
                if verdict:
                    self.live_state["last_verdict"] = {
                        "rule": verdict.rule_id,
                        "driver": verdict.driver_number,
                        "verdict": verdict.verdict.value,
                        "speed": verdict.measured_value,
                        "limit": verdict.threshold,
                    }
                    self.live_state["verdicts_count"] += 1

                pos = self.client.get_driver_position(driver)
                if pos:
                    pos_errors = self.error_agent.process_position_update(driver, pos)
                    if pos_errors:
                        self.live_state["errors"].extend(pos_errors)

            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error("Telemetry consumer error: %s", e)

    async def _consume_laps(self) -> None:
        while self._running:
            try:
                item = await asyncio.wait_for(self.client.lap_queue.get(), timeout=1.0)
                driver = item["driver"]
                lap_data = item["data"]
                lap_number = lap_data.get("lap_number", 0)

                self.live_state["laps_processed"] += 1
                self.live_state["current_lap"] = max(self.live_state["current_lap"], lap_number)

                self._log_event(driver, "LAP_COMPLETED", lap_data)

                # Error signals
                errors = self.error_agent.process_lap(driver, lap_data)
                if errors:
                    self.live_state["errors"].extend(errors)
                    for err in errors:
                        self._log_event(driver, "ERROR_SIGNAL", err)

                # Line analysis
                line_insights = self.line_agent.process_lap(driver, lap_data)
                if line_insights:
                    self.live_state["line_insights"].extend(line_insights[-5:])
                    for insight in line_insights:
                        self._log_event(driver, "LINE_INSIGHT", insight)
                        # Check for performance loss to issue radio advice
                        if "loss" in insight.get("type", "").lower() or insight.get("delta_s", 0) > 0.25:
                            msg = self.radio_engine.generate_sector_advice(
                                lap_number=lap_number,
                                sector=insight.get("sector", 2),
                                delta_s=insight.get("delta_s", 0.3),
                            )
                            self.live_state["last_radio_message"] = msg.to_dict()
                            await self._broadcast_ws({"type": "RADIO_MESSAGE", "data": msg.to_dict()})

                # Tyre strategy evaluation
                compound = self.client.get_driver_compound(driver)
                tyre_age = self.client.get_driver_tyre_age(driver)
                position = self.client.get_driver_position(driver)
                weather = self.client.get_weather_state()
                opponents = (
                    self.client.build_opponents(self.target_driver)
                    if driver == self.target_driver
                    else []
                )

                decision = self.strategy_agent.process_lap(
                    driver=driver,
                    lap_data=lap_data,
                    position=position,
                    compound=compound,
                    tyre_age=tyre_age,
                    gap_ahead=2.0,
                    gap_behind=3.0,
                    driver_ahead=None,
                    driver_behind=None,
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

                    # Check for Box call
                    if decision.target_lap == lap_number and decision.decision == "PIT":
                        box_msg = self.radio_engine.generate_pit_call(
                            lap_number=lap_number,
                            target_compound=decision.target_compound.value if hasattr(decision.target_compound, "value") else str(decision.target_compound),
                            urgent=decision.urgency in ("HIGH", "CRITICAL"),
                        )
                        self.live_state["last_radio_message"] = box_msg.to_dict()
                        await self._broadcast_ws({"type": "RADIO_MESSAGE", "data": box_msg.to_dict()})

                    await self._broadcast_ws({
                        "type": "STRATEGY_UPDATE",
                        "lap": lap_number,
                        "decision": decision.model_dump(),
                    })

                if self.live_state["laps_processed"] % 5 == 0:
                    self.live_state["pace_summary"] = self.line_agent.get_pace_summary()

            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error("Lap consumer error: %s", e)

    async def _consume_events(self) -> None:
        while self._running:
            try:
                item = await asyncio.wait_for(self.client.event_queue.get(), timeout=1.0)
                event_type = item.get("type", "UNKNOWN")
                new_status = item.get("new", "GREEN")
                self.live_state["track_status"] = new_status
                self._log_event("RACE", event_type, item)

                if event_type == "TRACK_STATUS_CHANGE":
                    radio_msg = self.radio_engine.generate_safety_car_directive(
                        track_status=new_status,
                        lap_number=self.live_state["current_lap"],
                    )
                    self.live_state["last_radio_message"] = radio_msg.to_dict()
                    await self._broadcast_ws({"type": "RADIO_MESSAGE", "data": radio_msg.to_dict()})

                    weather = self.client.get_weather_state()
                    opponents = self.client.build_opponents(self.target_driver)
                    decision = self.strategy_agent.process_event(
                        event_type=event_type,
                        track_status=self.client.track_status,
                        weather=weather,
                        opponents=opponents,
                    )
                    if decision:
                        self.live_state["strategy"] = decision.model_dump()
                        await self._broadcast_ws({
                            "type": "STRATEGY_REPLAN",
                            "decision": decision.model_dump(),
                        })

            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error("Event consumer error: %s", e)

    async def _periodic_summary(self) -> None:
        while self._running:
            await asyncio.sleep(25.0)
            self.live_state["positions"] = dict(self.client.latest_positions)
            self.live_state["last_update_ts"] = time.time()
            self.live_state["errors"] = self.live_state["errors"][-20:]
            self.live_state["line_insights"] = self.live_state["line_insights"][-10:]

            self._log_event("SYSTEM", "PERIODIC_SUMMARY", {
                "lap": self.live_state["current_lap"],
                "positions": self.live_state["positions"],
                "telemetry_samples": self.live_state["telemetry_samples"],
                "error_count": len(self.error_agent.signal_history),
                "track_status": self.live_state["track_status"],
            })

    # ── WebSocket Management ─────────────────────────────────────────

    def subscribe_ws(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=200)
        self._ws_subscribers.append(q)
        return q

    def unsubscribe_ws(self, q: asyncio.Queue) -> None:
        if q in self._ws_subscribers:
            self._ws_subscribers.remove(q)

    async def _broadcast_ws(self, message: Dict[str, Any]) -> None:
        dead = []
        for q in self._ws_subscribers:
            try:
                q.put_nowait(message)
            except asyncio.QueueFull:
                dead.append(q)
        for d in dead:
            self._ws_subscribers.remove(d)

    # ── Logging ──────────────────────────────────────────────────────

    def _log_event(self, driver: str, event_type: str, data: Any) -> None:
        entry = {
            "ts": time.time(),
            "iso": datetime.now(timezone.utc).isoformat(),
            "driver": driver,
            "type": event_type,
            "data": data,
        }
        if self._log_handle:
            try:
                self._log_handle.write(json.dumps(entry, default=str) + "\n")
                self._log_handle.flush()
            except Exception as e:
                logger.error("Log error: %s", e)

    # ── Public API Accessors ─────────────────────────────────────────

    def get_live_state(self) -> Dict[str, Any]:
        state = self.live_state.copy()
        state["operating_mode"] = self.mode_controller.current_mode.value
        state["feed_status"] = self.mode_controller.feed_status.value
        return state

    def get_error_signals(self) -> List[Dict[str, Any]]:
        return self.error_agent.get_active_signals()

    def get_pace_model(self) -> Dict[str, Any]:
        return {
            "pace_summary": self.line_agent.get_pace_summary(),
            "field_ranking": self.line_agent.get_field_ranking(),
            "best_sectors": {
                str(k): {"driver": v[0], "time_s": round(v[1], 3)}
                for k, v in self.line_agent.best_sectors.items()
            },
        }

    def get_strategy_for_driver(self, driver: str) -> Dict[str, Any]:
        if driver.upper() == self.target_driver:
            return self.strategy_agent.get_strategy_summary()
        return {"message": f"Strategy only tracked for {self.target_driver}"}

    def get_strategy_evolution(self) -> List[Dict[str, Any]]:
        return self.strategy_agent.strategy_evolution

    def get_pit_monitor_stats(self) -> Dict[str, Any]:
        return self.pit_lane_monitor.get_stats()

    def get_recent_verdicts(self, limit: int = 20) -> List[Dict[str, Any]]:
        return self.pit_lane_monitor.get_recent_verdicts(limit=limit)

    def get_recent_radio_messages(self, limit: int = 15) -> List[Dict[str, Any]]:
        return self.radio_engine.get_recent_messages(limit=limit)

    def get_aggregator_health(self) -> Dict[str, Any]:
        if self.aggregator:
            return self.aggregator.get_health_status()
        return {"multipoint_aggregator_enabled": False}
