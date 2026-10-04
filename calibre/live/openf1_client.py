"""
CALIBRE — OpenF1 Live Data Client (§47)
========================================
Async HTTP poller that fetches real-time telemetry, lap data, positions,
stints, race control messages, weather, and intervals from the OpenF1 API.

All data is normalized into CALIBRE's internal schemas and pushed to an
asyncio.Queue for downstream agent consumption.

API Reference: https://openf1.org
- No API key required
- Rate limit: ~250 req/min (generous for our polling cadence)
"""
from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set

import httpx

from calibre.schemas.race_telemetry import (
    LapData,
    OpponentState,
    RaceState,
    TelemetryEvent,
    SessionType,
    TrackStatus,
    TyreCompound,
    WeatherState,
)

logger = logging.getLogger(__name__)

OPENF1_BASE = "https://api.openf1.org/v1"

# Map OpenF1 compound strings to our enum
COMPOUND_MAP = {
    "SOFT": TyreCompound.SOFT,
    "MEDIUM": TyreCompound.MEDIUM,
    "HARD": TyreCompound.HARD,
    "INTERMEDIATE": TyreCompound.INTERMEDIATE,
    "WET": TyreCompound.WET,
}

# Map OpenF1 flag categories to our TrackStatus
FLAG_MAP = {
    "GREEN": TrackStatus.GREEN,
    "YELLOW": TrackStatus.YELLOW,
    "DOUBLE YELLOW": TrackStatus.YELLOW,
    "RED": TrackStatus.RED,
    "SAFETY CAR": TrackStatus.SAFETY_CAR,
    "VIRTUAL SAFETY CAR": TrackStatus.VIRTUAL_SAFETY_CAR,
    "VSC": TrackStatus.VIRTUAL_SAFETY_CAR,
    "SC": TrackStatus.SAFETY_CAR,
    "CLEAR": TrackStatus.GREEN,
}


class OpenF1Client:
    """Async client that polls OpenF1 endpoints and normalizes data."""

    def __init__(
        self,
        session_key: int,
        target_driver: str = "VER",
        car_data_interval: float = 3.0,
        lap_interval: float = 5.0,
        position_interval: float = 5.0,
        stint_interval: float = 5.0,
        race_control_interval: float = 3.0,
        weather_interval: float = 10.0,
        interval_interval: float = 5.0,
    ):
        self.session_key = session_key
        self.target_driver = target_driver.upper()
        self._client: Optional[httpx.AsyncClient] = None

        # Poll intervals
        self.car_data_interval = car_data_interval
        self.lap_interval = lap_interval
        self.position_interval = position_interval
        self.stint_interval = stint_interval
        self.race_control_interval = race_control_interval
        self.weather_interval = weather_interval
        self.interval_interval = interval_interval

        # Internal state — latest data per driver
        self.drivers: Dict[str, Dict[str, Any]] = {}  # driver_number -> info
        self.driver_number_to_code: Dict[str, str] = {}
        self.latest_positions: Dict[str, int] = {}  # driver_code -> position
        self.latest_laps: Dict[str, LapData] = {}  # driver_code -> last lap
        self.latest_stints: Dict[str, Dict[str, Any]] = {}  # driver_code -> stint
        self.latest_intervals: Dict[str, float] = {}  # driver_code -> gap_to_leader
        self.latest_weather: Optional[Dict[str, Any]] = None
        self.track_status: TrackStatus = TrackStatus.GREEN
        self.race_control_messages: List[Dict[str, Any]] = []

        # Watermarks to avoid re-fetching old data
        self._last_car_data_ts: Optional[str] = None
        self._last_lap_ts: Optional[str] = None
        self._last_rc_ts: Optional[str] = None
        self._last_weather_ts: Optional[str] = None
        self._seen_lap_keys: Set[str] = set()

        # Event queues for downstream agents
        self.telemetry_queue: asyncio.Queue = asyncio.Queue(maxsize=5000)
        self.lap_queue: asyncio.Queue = asyncio.Queue(maxsize=500)
        self.event_queue: asyncio.Queue = asyncio.Queue(maxsize=500)

        # Control
        self._running = False
        self._tasks: List[asyncio.Task] = []

    async def start(self):
        """Start all polling loops."""
        self._client = httpx.AsyncClient(
            base_url=OPENF1_BASE,
            timeout=httpx.Timeout(15.0, connect=10.0),
            follow_redirects=True,
        )
        self._running = True

        # First, fetch driver list (one-time)
        await self._fetch_drivers()

        # Launch concurrent polling tasks
        self._tasks = [
            asyncio.create_task(self._poll_loop("car_data", self._fetch_car_data, self.car_data_interval)),
            asyncio.create_task(self._poll_loop("laps", self._fetch_laps, self.lap_interval)),
            asyncio.create_task(self._poll_loop("position", self._fetch_positions, self.position_interval)),
            asyncio.create_task(self._poll_loop("stints", self._fetch_stints, self.stint_interval)),
            asyncio.create_task(self._poll_loop("race_control", self._fetch_race_control, self.race_control_interval)),
            asyncio.create_task(self._poll_loop("weather", self._fetch_weather, self.weather_interval)),
            asyncio.create_task(self._poll_loop("intervals", self._fetch_intervals, self.interval_interval)),
        ]
        logger.info("OpenF1 client started — polling %d endpoints for session %d", len(self._tasks), self.session_key)

    async def stop(self):
        """Gracefully stop all polling."""
        self._running = False
        for task in self._tasks:
            task.cancel()
        if self._client:
            await self._client.aclose()
        logger.info("OpenF1 client stopped.")

    async def _poll_loop(self, name: str, fetcher, interval: float):
        """Generic polling loop with error resilience."""
        while self._running:
            try:
                await fetcher()
            except httpx.HTTPStatusError as e:
                logger.warning("[%s] HTTP error %d: %s", name, e.response.status_code, e)
            except httpx.RequestError as e:
                logger.warning("[%s] Request error: %s", name, e)
            except Exception as e:
                logger.error("[%s] Unexpected error: %s", name, e, exc_info=True)
            await asyncio.sleep(interval)

    # ─── Fetchers ─────────────────────────────────────────────────

    async def _fetch_drivers(self):
        """One-time: fetch driver list for this session."""
        resp = await self._client.get(f"/drivers?session_key={self.session_key}")
        resp.raise_for_status()
        data = resp.json()
        for d in data:
            num = str(d.get("driver_number", ""))
            code = d.get("name_acronym", d.get("last_name", "UNK"))[:3].upper()
            self.driver_number_to_code[num] = code
            self.drivers[num] = {
                "code": code,
                "full_name": d.get("full_name", ""),
                "team": d.get("team_name", ""),
                "number": num,
            }
        logger.info("Loaded %d drivers for session %d", len(self.drivers), self.session_key)

    async def _fetch_car_data(self):
        """Fetch high-frequency car telemetry."""
        params = {"session_key": self.session_key}
        if self._last_car_data_ts:
            params["date>"] = self._last_car_data_ts

        resp = await self._client.get("/car_data", params=params)
        resp.raise_for_status()
        data = resp.json()

        if not data:
            return

        for sample in data[-200:]:  # Process latest batch, cap at 200
            driver_num = str(sample.get("driver_number", ""))
            code = self.driver_number_to_code.get(driver_num, f"D{driver_num}")

            event = TelemetryEvent(
                timestamp=time.time(),
                session=SessionType.RACE,
                driver=code,
                lap=self.latest_laps.get(code, LapData(
                    lap_number=1, driver=code, lap_time_s=0, s1_time_s=0,
                    s2_time_s=0, s3_time_s=0, compound=TyreCompound.MEDIUM, tyre_age_laps=0
                )).lap_number,
                speed_kph=float(sample.get("speed", 0)),
                throttle_pct=float(sample.get("throttle", 0)),
                brake_pct=float(sample.get("brake", 0)) * 100.0 if sample.get("brake", 0) <= 1.0 else float(sample.get("brake", 0)),
                gear=int(sample.get("n_gear", 0)),
                rpm=int(sample.get("rpm", 0)),
                drs=sample.get("drs", 0) in (10, 12, 14),  # OpenF1 DRS values
            )

            try:
                self.telemetry_queue.put_nowait({
                    "type": "TELEMETRY",
                    "driver": code,
                    "data": event.model_dump(),
                    "raw": sample,
                    "ts": time.time(),
                })
            except asyncio.QueueFull:
                pass  # Drop oldest — telemetry is high frequency

        # Update watermark
        last = data[-1]
        if "date" in last:
            self._last_car_data_ts = last["date"]

    async def _fetch_laps(self):
        """Fetch lap completion data."""
        params = {"session_key": self.session_key}
        if self._last_lap_ts:
            params["date_start>"] = self._last_lap_ts

        resp = await self._client.get("/laps", params=params)
        resp.raise_for_status()
        data = resp.json()

        for lap in data:
            driver_num = str(lap.get("driver_number", ""))
            code = self.driver_number_to_code.get(driver_num, f"D{driver_num}")

            lap_key = f"{code}-L{lap.get('lap_number', 0)}"
            if lap_key in self._seen_lap_keys:
                continue
            self._seen_lap_keys.add(lap_key)

            lap_time = lap.get("lap_duration")
            if lap_time is None:
                continue

            s1 = lap.get("duration_sector_1") or 0.0
            s2 = lap.get("duration_sector_2") or 0.0
            s3 = lap.get("duration_sector_3") or 0.0

            # Get compound from stints
            stint_info = self.latest_stints.get(code, {})
            compound_str = stint_info.get("compound", "MEDIUM")
            compound = COMPOUND_MAP.get(compound_str, TyreCompound.MEDIUM)
            tyre_age = stint_info.get("tyre_age_at_start", 0) + (lap.get("lap_number", 1) - stint_info.get("lap_start", 1))

            is_pit_out = lap.get("is_pit_out_lap", False)
            pit_in_time = lap.get("pit_duration")

            lap_data = LapData(
                lap_number=int(lap.get("lap_number", 1)),
                driver=code,
                lap_time_s=float(lap_time),
                s1_time_s=float(s1),
                s2_time_s=float(s2),
                s3_time_s=float(s3),
                compound=compound,
                tyre_age_laps=max(0, int(tyre_age)),
                is_valid=not is_pit_out and pit_in_time is None,
                is_pit_in_lap=pit_in_time is not None and pit_in_time > 0,
                is_pit_out_lap=bool(is_pit_out),
            )

            self.latest_laps[code] = lap_data

            try:
                self.lap_queue.put_nowait({
                    "type": "LAP_COMPLETED",
                    "driver": code,
                    "data": lap_data.model_dump(),
                    "ts": time.time(),
                })
            except asyncio.QueueFull:
                pass

        if data:
            last = data[-1]
            if "date_start" in last and last["date_start"]:
                self._last_lap_ts = last["date_start"]

    async def _fetch_positions(self):
        """Fetch latest position data."""
        resp = await self._client.get(f"/position?session_key={self.session_key}")
        resp.raise_for_status()
        data = resp.json()

        # Only keep the latest position per driver
        latest_per_driver: Dict[str, int] = {}
        for item in data:
            driver_num = str(item.get("driver_number", ""))
            code = self.driver_number_to_code.get(driver_num, f"D{driver_num}")
            pos = item.get("position")
            if pos is not None:
                latest_per_driver[code] = int(pos)

        self.latest_positions = latest_per_driver

    async def _fetch_stints(self):
        """Fetch stint/compound data."""
        resp = await self._client.get(f"/stints?session_key={self.session_key}")
        resp.raise_for_status()
        data = resp.json()

        # Keep latest stint per driver
        for stint in data:
            driver_num = str(stint.get("driver_number", ""))
            code = self.driver_number_to_code.get(driver_num, f"D{driver_num}")
            self.latest_stints[code] = {
                "stint_number": stint.get("stint_number", 1),
                "compound": (stint.get("compound") or "MEDIUM").upper(),
                "tyre_age_at_start": stint.get("tyre_age_at_start", 0),
                "lap_start": stint.get("lap_start", 1),
                "lap_end": stint.get("lap_end"),
            }

    async def _fetch_race_control(self):
        """Fetch race control messages (flags, SC, VSC, etc.)."""
        params = {"session_key": self.session_key}
        if self._last_rc_ts:
            params["date>"] = self._last_rc_ts

        resp = await self._client.get("/race_control", params=params)
        resp.raise_for_status()
        data = resp.json()

        for msg in data:
            flag = (msg.get("flag") or "").upper().strip()
            category = (msg.get("category") or "").upper().strip()
            message_text = msg.get("message", "")

            new_status = None

            # Detect Safety Car
            if "SAFETY CAR" in category or "SAFETY CAR" in message_text.upper():
                if "VIRTUAL" in message_text.upper() or "VSC" in category:
                    new_status = TrackStatus.VIRTUAL_SAFETY_CAR
                else:
                    new_status = TrackStatus.SAFETY_CAR
            elif flag in FLAG_MAP:
                new_status = FLAG_MAP[flag]
            elif "RED FLAG" in message_text.upper():
                new_status = TrackStatus.RED

            if new_status and new_status != self.track_status:
                old_status = self.track_status
                self.track_status = new_status
                try:
                    self.event_queue.put_nowait({
                        "type": "TRACK_STATUS_CHANGE",
                        "old": old_status.value,
                        "new": new_status.value,
                        "message": message_text,
                        "ts": time.time(),
                    })
                except asyncio.QueueFull:
                    pass

            self.race_control_messages.append({
                "flag": flag,
                "category": category,
                "message": message_text,
                "ts": time.time(),
            })

        if data and "date" in data[-1]:
            self._last_rc_ts = data[-1]["date"]

    async def _fetch_weather(self):
        """Fetch weather data."""
        params = {"session_key": self.session_key}
        if self._last_weather_ts:
            params["date>"] = self._last_weather_ts

        resp = await self._client.get("/weather", params=params)
        resp.raise_for_status()
        data = resp.json()

        if data:
            latest = data[-1]
            self.latest_weather = {
                "air_temp_c": float(latest.get("air_temperature", 30.0)),
                "track_temp_c": float(latest.get("track_temperature", 45.0)),
                "humidity_pct": float(latest.get("humidity", 50.0)),
                "pressure_mbar": float(latest.get("pressure", 1013.0)),
                "wind_speed_kph": float(latest.get("wind_speed", 5.0)),
                "wind_direction_deg": float(latest.get("wind_direction", 180.0)),
                "rainfall": bool(latest.get("rainfall", 0)),
                "ts": time.time(),
            }

            if "date" in latest:
                self._last_weather_ts = latest["date"]

    async def _fetch_intervals(self):
        """Fetch interval (gap) data."""
        resp = await self._client.get(f"/intervals?session_key={self.session_key}")
        resp.raise_for_status()
        data = resp.json()

        # Keep latest interval per driver
        for item in data:
            driver_num = str(item.get("driver_number", ""))
            code = self.driver_number_to_code.get(driver_num, f"D{driver_num}")
            gap = item.get("gap_to_leader")
            interval = item.get("interval")
            if gap is not None:
                try:
                    self.latest_intervals[code] = float(gap) if gap != "" else 0.0
                except (ValueError, TypeError):
                    pass

    # ─── State Accessors ──────────────────────────────────────────

    def get_driver_position(self, driver: str) -> int:
        return self.latest_positions.get(driver, 20)

    def get_driver_compound(self, driver: str) -> TyreCompound:
        stint = self.latest_stints.get(driver, {})
        return COMPOUND_MAP.get(stint.get("compound", "MEDIUM"), TyreCompound.MEDIUM)

    def get_driver_tyre_age(self, driver: str) -> int:
        stint = self.latest_stints.get(driver, {})
        lap_start = stint.get("lap_start", 1)
        tyre_age_start = stint.get("tyre_age_at_start", 0)
        current_lap = self.latest_laps.get(driver)
        if current_lap:
            return tyre_age_start + max(0, current_lap.lap_number - lap_start)
        return tyre_age_start

    def get_weather_state(self) -> WeatherState:
        w = self.latest_weather or {}
        rainfall = w.get("rainfall", False)
        humidity = w.get("humidity_pct", 50.0)
        rain_prob = 80.0 if rainfall else min(50.0, humidity * 0.5)

        return WeatherState(
            air_temp_c=w.get("air_temp_c", 30.0),
            track_temp_c=w.get("track_temp_c", 45.0),
            rain_probability_pct=rain_prob,
            precipitation_intensity=1.0 if rainfall else 0.0,
            wind_speed_kph=w.get("wind_speed_kph", 5.0),
            crossover_approaching=rain_prob > 60.0,
            intermediate_window=rainfall,
        )

    def get_sorted_drivers_by_position(self) -> List[str]:
        """Returns driver codes sorted by current position."""
        return sorted(self.latest_positions.keys(), key=lambda d: self.latest_positions.get(d, 99))

    def build_opponents(self, for_driver: str) -> List[OpponentState]:
        """Build OpponentState list for a specific driver, excluding themselves."""
        opponents = []
        my_pos = self.get_driver_position(for_driver)

        for code, pos in self.latest_positions.items():
            if code == for_driver:
                continue

            compound = self.get_driver_compound(code)
            tyre_age = self.get_driver_tyre_age(code)
            gap = self.latest_intervals.get(code, 0.0)

            # Calculate gap relative to our driver
            relative_gap = gap - self.latest_intervals.get(for_driver, 0.0) if for_driver in self.latest_intervals else 0.0

            # Only include nearby opponents (within ~30s of track position)
            if abs(pos - my_pos) > 8:
                continue

            last_lap = self.latest_laps.get(code)
            opp_pace = last_lap.lap_time_s if last_lap else 90.0
            my_lap = self.latest_laps.get(for_driver)
            my_pace = my_lap.lap_time_s if my_lap else 90.0
            gap_trend = (opp_pace - my_pace) if last_lap and my_lap else 0.0

            undercut = pos == my_pos - 1 and abs(relative_gap) < 2.5
            overcut = pos == my_pos + 1 and abs(relative_gap) < 2.5

            opponents.append(OpponentState(
                driver=code,
                position=pos,
                gap_s=abs(relative_gap),
                gap_trend_s_per_lap=round(gap_trend, 3),
                compound=compound,
                tyre_age_laps=tyre_age,
                undercut_threat=undercut,
                overcut_opportunity=overcut,
            ))

        return sorted(opponents, key=lambda o: o.position)

    def get_total_laps(self) -> int:
        """Estimate total laps (Kuala Lumpur = 56 laps)."""
        # TODO: Could fetch from session metadata
        return 56

    def get_current_lap(self, driver: str) -> int:
        """Get the most recent completed lap for a driver."""
        last = self.latest_laps.get(driver)
        return (last.lap_number + 1) if last else 1
