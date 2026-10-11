"""
CALIBRE — Enhanced Mock Data Source with Scenario Injection
=============================================================
Deterministic, configurable mock telemetry that injects scenarios
not sufficiently represented in historical recordings.

All events are marked as synthetic (SIMULATION operating mode).
Fixed random seeds ensure repeatable tests.
"""
from __future__ import annotations

import asyncio
import logging
import math
import random
import time
from typing import Any, Dict, List, Optional

from calibre.live.data_sources.base_source import (
    DataSource, SourceCapabilities, SourceStatus,
)
from calibre.live.schemas.events import (
    CalibreEvent, DataOrigin, EventType,
)

logger = logging.getLogger(__name__)


class ScenarioConfig:
    """Configuration for a simulation scenario."""

    def __init__(
        self,
        name: str = "default",
        total_laps: int = 20,
        target_driver: str = "VER",
        drivers: Optional[List[str]] = None,
        seed: int = 42,
        # Scenario flags
        inject_spike: bool = False,
        spike_lap: int = 8,
        spike_sensor: str = "speed_kph",
        spike_value: float = 430.0,

        inject_drift: bool = False,
        drift_start_lap: int = 5,
        drift_rate_per_lap: float = 0.5,
        drift_sensor: str = "speed_kph",

        inject_stuck: bool = False,
        stuck_sensor: str = "speed_kph",
        stuck_lap: int = 12,

        inject_missing: bool = False,
        missing_laps: Optional[List[int]] = None,

        inject_safety_car: bool = False,
        sc_start_lap: int = 15,
        sc_duration_laps: int = 3,

        inject_vsc: bool = False,
        vsc_lap: int = 10,

        inject_rain: bool = False,
        rain_start_lap: int = 12,

        inject_penalty: bool = False,
        penalty_lap: int = 14,
        penalty_driver: str = "VER",
        penalty_seconds: float = 5.0,

        inject_source_failure: bool = False,
        failure_lap: int = 16,
        failure_duration_laps: int = 2,

        compound_changes: Optional[Dict[int, str]] = None,
    ):
        self.name = name
        self.total_laps = total_laps
        self.target_driver = target_driver
        self.drivers = drivers or ["VER", "NOR", "LEC", "HAM", "RUS", "PIA"]
        self.seed = seed
        self.inject_spike = inject_spike
        self.spike_lap = spike_lap
        self.spike_sensor = spike_sensor
        self.spike_value = spike_value
        self.inject_drift = inject_drift
        self.drift_start_lap = drift_start_lap
        self.drift_rate_per_lap = drift_rate_per_lap
        self.drift_sensor = drift_sensor
        self.inject_stuck = inject_stuck
        self.stuck_sensor = stuck_sensor
        self.stuck_lap = stuck_lap
        self.inject_missing = inject_missing
        self.missing_laps = missing_laps or [9, 10]
        self.inject_safety_car = inject_safety_car
        self.sc_start_lap = sc_start_lap
        self.sc_duration_laps = sc_duration_laps
        self.inject_vsc = inject_vsc
        self.vsc_lap = vsc_lap
        self.inject_rain = inject_rain
        self.rain_start_lap = rain_start_lap
        self.inject_penalty = inject_penalty
        self.penalty_lap = penalty_lap
        self.penalty_driver = penalty_driver
        self.penalty_seconds = penalty_seconds
        self.inject_source_failure = inject_source_failure
        self.failure_lap = failure_lap
        self.failure_duration_laps = failure_duration_laps
        self.compound_changes = compound_changes or {1: "SOFT", 12: "HARD"}


# Pre-defined named scenarios
NAMED_SCENARIOS = {
    "default": ScenarioConfig(),
    "sensor_spike": ScenarioConfig(name="sensor_spike", inject_spike=True),
    "sensor_drift": ScenarioConfig(name="sensor_drift", inject_drift=True),
    "stuck_sensor": ScenarioConfig(name="stuck_sensor", inject_stuck=True),
    "missing_data": ScenarioConfig(name="missing_data", inject_missing=True),
    "safety_car": ScenarioConfig(name="safety_car", inject_safety_car=True),
    "vsc": ScenarioConfig(name="vsc", inject_vsc=True),
    "rain_crossover": ScenarioConfig(name="rain_crossover", inject_rain=True),
    "penalty": ScenarioConfig(name="penalty", inject_penalty=True),
    "source_failure": ScenarioConfig(name="source_failure", inject_source_failure=True),
    "full_chaos": ScenarioConfig(
        name="full_chaos",
        inject_spike=True, inject_drift=True, inject_safety_car=True,
        inject_rain=True, inject_penalty=True, inject_missing=True,
    ),
}


class MockSource(DataSource):
    """
    Deterministic mock data source that generates synthetic telemetry
    with configurable scenario injection.
    """

    def __init__(
        self,
        scenario: Optional[ScenarioConfig] = None,
        tick_interval: float = 0.5,
        source_id: str = "mock",
        target_driver: Optional[str] = None,
        tick_interval_s: Optional[float] = None,
    ):
        super().__init__(source_id=source_id)
        self.scenario = scenario or ScenarioConfig()
        if target_driver:
            self.scenario.target_driver = target_driver
        self.tick_interval = tick_interval_s if tick_interval_s is not None else tick_interval
        self._rng = random.Random(self.scenario.seed)
        self._task: Optional[asyncio.Task] = None
        self._current_lap = 0
        self._paused = False
        self._stuck_value: Optional[float] = None
        self._drift_accumulated = 0.0
        self._current_compounds: Dict[str, str] = {}
        self._track_status = "GREEN"

    def get_capabilities(self) -> SourceCapabilities:
        return SourceCapabilities(
            source_id="mock",
            supported_event_types={
                EventType.TELEMETRY, EventType.LAP_COMPLETED,
                EventType.POSITION_UPDATE, EventType.STINT_UPDATE,
                EventType.RACE_CONTROL, EventType.WEATHER,
            },
            is_live_capable=False,
            is_historical_capable=False,
            notes=f"Simulation scenario: {self.scenario.name}",
        )

    async def connect(self) -> bool:
        self._set_status(SourceStatus.CONNECTED)
        return True

    async def disconnect(self) -> None:
        self._set_status(SourceStatus.DISCONNECTED)

    async def start(self) -> None:
        self._running = True
        self._set_status(SourceStatus.CONSUMING)
        self._task = asyncio.create_task(self._generate_race())

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
        self._set_status(SourceStatus.STOPPED)

    def set_scenario(self, scenario: ScenarioConfig) -> None:
        self.scenario = scenario
        self._rng = random.Random(scenario.seed)

    async def _generate_race(self) -> None:
        """Generate a complete race simulation."""
        sc = self.scenario
        base_lap_time = 92.5  # ~1:32.5

        # Initialise compounds
        for driver in sc.drivers:
            self._current_compounds[driver] = sc.compound_changes.get(1, "SOFT")

        for lap in range(1, sc.total_laps + 1):
            if not self._running:
                break
            self._current_lap = lap

            # Check source failure injection
            if sc.inject_source_failure:
                if sc.failure_lap <= lap < sc.failure_lap + sc.failure_duration_laps:
                    logger.info("[mock] Simulated source failure at lap %d", lap)
                    self._set_status(SourceStatus.FAILED)
                    await asyncio.sleep(self.tick_interval)
                    continue
                elif lap == sc.failure_lap + sc.failure_duration_laps:
                    self._set_status(SourceStatus.CONSUMING)

            # Update track status for SC/VSC
            if sc.inject_safety_car and lap == sc.sc_start_lap:
                self._track_status = "SAFETY_CAR"
                await self._emit_race_control(lap, "SAFETY CAR DEPLOYED", "YELLOW")
            elif sc.inject_safety_car and lap == sc.sc_start_lap + sc.sc_duration_laps:
                self._track_status = "GREEN"
                await self._emit_race_control(lap, "GREEN FLAG - SAFETY CAR IN", "GREEN")

            if sc.inject_vsc and lap == sc.vsc_lap:
                self._track_status = "VIRTUAL_SAFETY_CAR"
                await self._emit_race_control(lap, "VIRTUAL SAFETY CAR DEPLOYED", "YELLOW")
            elif sc.inject_vsc and lap == sc.vsc_lap + 2:
                self._track_status = "GREEN"
                await self._emit_race_control(lap, "VSC ENDING", "GREEN")

            # Rain injection
            if sc.inject_rain and lap >= sc.rain_start_lap:
                await self._emit_weather(lap, raining=True)
            else:
                await self._emit_weather(lap, raining=False)

            # Penalty injection
            if sc.inject_penalty and lap == sc.penalty_lap:
                await self._emit_race_control(
                    lap,
                    f"{sc.penalty_seconds}s TIME PENALTY FOR {sc.penalty_driver}",
                    "PENALTY"
                )

            # Check compound changes
            for driver in sc.drivers:
                if lap in sc.compound_changes:
                    self._current_compounds[driver] = sc.compound_changes[lap]
                    await self._emit_stint(driver, lap)

            # Skip telemetry for missing data injection
            if sc.inject_missing and lap in sc.missing_laps:
                logger.info("[mock] Injecting missing data at lap %d", lap)
                await asyncio.sleep(self.tick_interval)
                continue

            # Emit per-driver telemetry and lap data
            for pos_idx, driver in enumerate(sc.drivers, 1):
                compound = self._current_compounds.get(driver, "MEDIUM")
                age = self._get_tyre_age(driver, lap)

                # Base lap time with variation
                driver_pace = base_lap_time + self._rng.gauss(0, 0.3)
                driver_pace += age * 0.05  # tyre degradation

                # SC/VSC laps are slower
                if self._track_status == "SAFETY_CAR":
                    driver_pace += 25.0
                elif self._track_status == "VIRTUAL_SAFETY_CAR":
                    driver_pace += 8.0

                # Rain penalty on dry tyres
                if sc.inject_rain and lap >= sc.rain_start_lap and compound in ("SOFT", "MEDIUM", "HARD"):
                    driver_pace += 6.0

                # Sector splits
                s1_frac = 0.30 + self._rng.gauss(0, 0.01)
                s2_frac = 0.38 + self._rng.gauss(0, 0.01)
                s3_frac = 1.0 - s1_frac - s2_frac
                s1 = driver_pace * s1_frac
                s2 = driver_pace * s2_frac
                s3 = driver_pace * s3_frac

                # Build telemetry
                speed = 280.0 + self._rng.gauss(0, 8)

                # Apply spike injection
                if sc.inject_spike and lap == sc.spike_lap and driver == sc.target_driver:
                    if sc.spike_sensor == "speed_kph":
                        speed = sc.spike_value
                        logger.info("[mock] Injected spike: %s=%f on lap %d", sc.spike_sensor, speed, lap)

                # Apply drift injection
                if sc.inject_drift and lap >= sc.drift_start_lap and driver == sc.target_driver:
                    self._drift_accumulated = (lap - sc.drift_start_lap) * sc.drift_rate_per_lap
                    if sc.drift_sensor == "speed_kph":
                        speed += self._drift_accumulated

                # Apply stuck sensor
                if sc.inject_stuck and lap >= sc.stuck_lap and driver == sc.target_driver:
                    if self._stuck_value is None:
                        self._stuck_value = speed
                    if sc.stuck_sensor == "speed_kph":
                        speed = self._stuck_value

                now = time.time()

                # Emit telemetry event
                telemetry_event = CalibreEvent(
                    event_type=EventType.TELEMETRY,
                    session_key=99999,
                    driver=driver,
                    driver_number=str(pos_idx),
                    source_timestamp=now,
                    source_id="mock",
                    operating_mode="SIMULATION",
                    data={
                        "speed_kph": round(speed, 1),
                        "throttle_pct": round(self._rng.uniform(85, 100), 1),
                        "brake_pct": round(self._rng.uniform(0, 15), 1),
                        "gear": self._rng.randint(5, 8),
                        "rpm": self._rng.randint(10500, 12000),
                        "drs": self._rng.choice([True, False]),
                        "lap_number": lap,
                    },
                    field_origins={
                        "speed_kph": "SYNTHETIC",
                        "throttle_pct": "SYNTHETIC",
                        "brake_pct": "SYNTHETIC",
                    },
                    units={"speed_kph": "km/h"},
                )
                telemetry_event.compute_dedup_key()
                await self._emit(telemetry_event)

                # Emit lap completed event
                lap_event = CalibreEvent(
                    event_type=EventType.LAP_COMPLETED,
                    session_key=99999,
                    driver=driver,
                    driver_number=str(pos_idx),
                    source_timestamp=now,
                    source_id="mock",
                    operating_mode="SIMULATION",
                    data={
                        "lap_number": lap,
                        "lap_time_s": round(driver_pace, 3),
                        "s1_time_s": round(s1, 3),
                        "s2_time_s": round(s2, 3),
                        "s3_time_s": round(s3, 3),
                        "compound": compound,
                        "tyre_age_laps": age,
                        "is_pit_out_lap": False,
                        "position": pos_idx,
                    },
                    field_origins={k: "SYNTHETIC" for k in [
                        "lap_time_s", "s1_time_s", "s2_time_s", "s3_time_s",
                    ]},
                )
                lap_event.compute_dedup_key()
                await self._emit(lap_event)

                # Emit position update
                pos_event = CalibreEvent(
                    event_type=EventType.POSITION_UPDATE,
                    session_key=99999,
                    driver=driver,
                    driver_number=str(pos_idx),
                    source_timestamp=now,
                    source_id="mock",
                    operating_mode="SIMULATION",
                    data={"position": pos_idx},
                    field_origins={"position": "SYNTHETIC"},
                )
                pos_event.compute_dedup_key()
                await self._emit(pos_event)

            await asyncio.sleep(self.tick_interval)

        # Race complete
        logger.info("[mock] Simulation complete: %d laps", sc.total_laps)
        self._running = False
        self._set_status(SourceStatus.STOPPED)

    async def _emit_weather(self, lap: int, raining: bool) -> None:
        sc = self.scenario
        rain_prob = 0.0
        if raining:
            rain_prob = min(95.0, 60.0 + (lap - sc.rain_start_lap) * 5.0)

        event = CalibreEvent(
            event_type=EventType.WEATHER,
            session_key=99999,
            source_timestamp=time.time(),
            source_id="mock",
            operating_mode="SIMULATION",
            data={
                "air_temp_c": 28.0 - (2.0 if raining else 0.0),
                "track_temp_c": 42.0 - (5.0 if raining else 0.0),
                "humidity_pct": 85.0 if raining else 55.0,
                "rainfall": raining,
                "rain_probability_pct": rain_prob,
                "wind_speed_kph": 15.0 if raining else 8.0,
            },
            field_origins={k: "SYNTHETIC" for k in [
                "air_temp_c", "track_temp_c", "rainfall",
            ]},
        )
        event.compute_dedup_key()
        await self._emit(event)

    async def _emit_race_control(self, lap: int, message: str, flag: str) -> None:
        event = CalibreEvent(
            event_type=EventType.RACE_CONTROL,
            session_key=99999,
            source_timestamp=time.time(),
            source_id="mock",
            operating_mode="SIMULATION",
            data={
                "message": message,
                "flag": flag,
                "category": "Flag" if "SAFETY" in message or "VSC" in message else "Penalty",
                "lap_number": lap,
            },
        )
        event.compute_dedup_key()
        await self._emit(event)

    async def _emit_stint(self, driver: str, lap: int) -> None:
        compound = self._current_compounds.get(driver, "MEDIUM")
        event = CalibreEvent(
            event_type=EventType.STINT_UPDATE,
            session_key=99999,
            driver=driver,
            source_timestamp=time.time(),
            source_id="mock",
            operating_mode="SIMULATION",
            data={
                "compound": compound,
                "lap_start": lap,
                "stint_number": 2 if lap > 1 else 1,
                "tyre_age_at_start": 0,
            },
        )
        event.compute_dedup_key()
        await self._emit(event)

    def _get_tyre_age(self, driver: str, current_lap: int) -> int:
        """Calculate tyre age based on compound changes."""
        last_change = 1
        for change_lap in sorted(self.scenario.compound_changes.keys()):
            if change_lap <= current_lap:
                last_change = change_lap
        return current_lap - last_change


MockDataSource = MockSource

