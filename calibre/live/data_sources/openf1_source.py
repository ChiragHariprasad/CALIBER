"""
CALIBRE — OpenF1 Data Source Adapter
=======================================
Wraps the existing OpenF1Client as a DataSource implementing the
canonical event envelope. Supports both historical and live access.
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Any, Dict, List, Optional

import httpx

from calibre.live.data_sources.base_source import (
    DataSource, SourceCapabilities, SourceStatus,
)
from calibre.live.schemas.events import (
    CalibreEvent, DataOrigin, EventType,
)

logger = logging.getLogger(__name__)

OPENF1_BASE = "https://api.openf1.org/v1"


class OpenF1Source(DataSource):
    """
    Adapter that fetches data from the OpenF1 API and emits
    CalibreEvents. Works for historical sessions without auth;
    live sessions require a paid subscription token.
    """

    def __init__(
        self,
        session_key: int,
        operating_mode: str = "HISTORICAL_REPLAY",
        api_token: Optional[str] = None,
        poll_interval: float = 5.0,
        source_id: str = "openf1",
        target_driver: Optional[str] = None,
        poll_interval_s: Optional[float] = None,
    ):
        super().__init__(source_id=source_id)
        self.session_key = session_key
        self.operating_mode = operating_mode
        self.target_driver = target_driver
        self.api_token = api_token or os.environ.get("OPENF1_TOKEN")
        self.poll_interval = poll_interval_s if poll_interval_s is not None else poll_interval
        self._client: Optional[httpx.AsyncClient] = None
        self._tasks: List[asyncio.Task] = []
        self._driver_map: Dict[str, str] = {}  # number -> code

        # Watermarks
        self._last_car_ts: Optional[str] = None
        self._last_lap_ts: Optional[str] = None
        self._last_rc_ts: Optional[str] = None
        self._last_weather_ts: Optional[str] = None

    def get_capabilities(self) -> SourceCapabilities:
        return SourceCapabilities(
            source_id="openf1",
            supported_event_types={
                EventType.TELEMETRY, EventType.LAP_COMPLETED,
                EventType.POSITION_UPDATE, EventType.STINT_UPDATE,
                EventType.RACE_CONTROL, EventType.WEATHER,
                EventType.INTERVAL,
            },
            available_fields={
                "telemetry": ["speed", "throttle", "brake", "n_gear", "rpm", "drs"],
                "laps": ["lap_number", "lap_duration", "duration_sector_1", "duration_sector_2", "duration_sector_3"],
                "position": ["position"],
                "stints": ["compound", "tyre_age_at_start", "stint_number"],
                "weather": ["air_temperature", "track_temperature", "humidity", "pressure", "wind_speed", "rainfall"],
            },
            requires_auth=self.operating_mode == "LIVE",
            is_live_capable=bool(self.api_token),
            is_historical_capable=True,
            max_polling_rate_hz=4.0,
            notes="Free historical access; live requires €9.90/mo subscription",
        )

    async def connect(self) -> bool:
        headers = {}
        if self.api_token:
            headers["Authorization"] = f"Bearer {self.api_token}"

        self._client = httpx.AsyncClient(
            base_url=OPENF1_BASE,
            headers=headers,
            timeout=httpx.Timeout(15.0, connect=10.0),
            follow_redirects=True,
        )

        # Capability check — verify session exists
        try:
            resp = await self._client.get(f"/sessions?session_key={self.session_key}")
            resp.raise_for_status()
            sessions = resp.json()
            if not sessions:
                self._record_error(f"Session {self.session_key} not found")
                return False

            # Fetch driver map
            resp = await self._client.get(f"/drivers?session_key={self.session_key}")
            resp.raise_for_status()
            for d in resp.json():
                num = str(d.get("driver_number", ""))
                code = d.get("name_acronym", d.get("last_name", "UNK"))[:3].upper()
                self._driver_map[num] = code

            self._set_status(SourceStatus.CONNECTED)
            logger.info("[openf1] Connected — session %d, %d drivers", self.session_key, len(self._driver_map))
            return True

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 401:
                self._record_error("Auth required — live session or subscription needed")
            else:
                self._record_error(f"HTTP {e.response.status_code}")
            return False
        except Exception as e:
            self._record_error(str(e))
            return False

    async def disconnect(self) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None
        self._set_status(SourceStatus.DISCONNECTED)

    async def start(self) -> None:
        if not self._client:
            raise RuntimeError("Must connect() before start()")
        self._running = True
        self._set_status(SourceStatus.CONSUMING)
        self._tasks = [
            asyncio.create_task(self._poll_loop("laps", self._fetch_laps)),
            asyncio.create_task(self._poll_loop("positions", self._fetch_positions)),
            asyncio.create_task(self._poll_loop("stints", self._fetch_stints)),
            asyncio.create_task(self._poll_loop("weather", self._fetch_weather)),
            asyncio.create_task(self._poll_loop("race_control", self._fetch_race_control)),
        ]

    async def stop(self) -> None:
        self._running = False
        for t in self._tasks:
            t.cancel()
        self._tasks.clear()
        self._set_status(SourceStatus.STOPPED)

    # ── Fetch all session data at once (for replay) ──────────────

    async def fetch_full_session(self) -> Dict[str, List[Dict]]:
        """Fetch all data for a session at once — for replay preloading."""
        if not self._client:
            raise RuntimeError("Must connect() first")

        result: Dict[str, List[Dict]] = {}
        endpoints = {
            "laps": f"/laps?session_key={self.session_key}",
            "car_data": f"/car_data?session_key={self.session_key}",
            "position": f"/position?session_key={self.session_key}",
            "stints": f"/stints?session_key={self.session_key}",
            "weather": f"/weather?session_key={self.session_key}",
            "race_control": f"/race_control?session_key={self.session_key}",
            "intervals": f"/intervals?session_key={self.session_key}",
        }

        for name, url in endpoints.items():
            try:
                resp = await self._client.get(url)
                resp.raise_for_status()
                data = resp.json()
                result[name] = data
                logger.info("[openf1] Fetched %d records for %s", len(data), name)
            except Exception as e:
                logger.warning("[openf1] Failed to fetch %s: %s", name, e)
                result[name] = []

        return result

    # ── Polling loops ─────────────────────────────────────────────

    async def _poll_loop(self, name: str, fetcher) -> None:
        while self._running:
            try:
                await fetcher()
            except httpx.HTTPStatusError as e:
                self._record_error(f"[{name}] HTTP {e.response.status_code}")
                if e.response.status_code == 401:
                    self._set_status(SourceStatus.FAILED)
                    return
            except Exception as e:
                self._record_error(f"[{name}] {e}")
            await asyncio.sleep(self.poll_interval)

    async def _fetch_laps(self) -> None:
        params: Dict[str, Any] = {"session_key": self.session_key}
        if self._last_lap_ts:
            params["date_start>"] = self._last_lap_ts

        resp = await self._client.get("/laps", params=params)
        resp.raise_for_status()
        data = resp.json()

        for lap in data:
            driver_num = str(lap.get("driver_number", ""))
            code = self._driver_map.get(driver_num, f"D{driver_num}")
            duration = lap.get("lap_duration")
            if duration is None:
                continue

            event = CalibreEvent(
                event_type=EventType.LAP_COMPLETED,
                session_key=self.session_key,
                driver=code,
                driver_number=driver_num,
                source_timestamp=self._parse_ts(lap.get("date_start", "")),
                source_id="openf1",
                operating_mode=self.operating_mode,
                data={
                    "lap_number": lap.get("lap_number", 0),
                    "lap_time_s": float(duration),
                    "s1_time_s": float(lap.get("duration_sector_1") or 0),
                    "s2_time_s": float(lap.get("duration_sector_2") or 0),
                    "s3_time_s": float(lap.get("duration_sector_3") or 0),
                    "is_pit_out_lap": bool(lap.get("is_pit_out_lap", False)),
                    "pit_duration": lap.get("pit_duration"),
                },
                raw_payload=lap,
                field_origins={
                    "lap_time_s": "OBSERVED",
                    "s1_time_s": "OBSERVED",
                    "s2_time_s": "OBSERVED",
                    "s3_time_s": "OBSERVED",
                },
                units={"lap_time_s": "seconds", "s1_time_s": "seconds"},
            )
            event.compute_dedup_key()
            await self._emit(event)

        if data and "date_start" in data[-1] and data[-1]["date_start"]:
            self._last_lap_ts = data[-1]["date_start"]

    async def _fetch_positions(self) -> None:
        resp = await self._client.get(f"/position?session_key={self.session_key}")
        resp.raise_for_status()
        # Emit only latest position per driver
        latest: Dict[str, Dict] = {}
        for item in resp.json():
            num = str(item.get("driver_number", ""))
            latest[num] = item

        for num, item in latest.items():
            code = self._driver_map.get(num, f"D{num}")
            event = CalibreEvent(
                event_type=EventType.POSITION_UPDATE,
                session_key=self.session_key,
                driver=code,
                driver_number=num,
                source_timestamp=self._parse_ts(item.get("date", "")),
                source_id="openf1",
                operating_mode=self.operating_mode,
                data={"position": item.get("position")},
                field_origins={"position": "OBSERVED"},
            )
            event.compute_dedup_key()
            await self._emit(event)

    async def _fetch_stints(self) -> None:
        resp = await self._client.get(f"/stints?session_key={self.session_key}")
        resp.raise_for_status()
        for stint in resp.json():
            num = str(stint.get("driver_number", ""))
            code = self._driver_map.get(num, f"D{num}")
            event = CalibreEvent(
                event_type=EventType.STINT_UPDATE,
                session_key=self.session_key,
                driver=code,
                driver_number=num,
                source_timestamp=time.time(),
                source_id="openf1",
                operating_mode=self.operating_mode,
                data={
                    "stint_number": stint.get("stint_number", 1),
                    "compound": (stint.get("compound") or "MEDIUM").upper(),
                    "tyre_age_at_start": stint.get("tyre_age_at_start", 0),
                    "lap_start": stint.get("lap_start", 1),
                    "lap_end": stint.get("lap_end"),
                },
                raw_payload=stint,
                field_origins={"compound": "OBSERVED", "stint_number": "OBSERVED"},
            )
            event.compute_dedup_key()
            await self._emit(event)

    async def _fetch_weather(self) -> None:
        params: Dict[str, Any] = {"session_key": self.session_key}
        if self._last_weather_ts:
            params["date>"] = self._last_weather_ts

        resp = await self._client.get("/weather", params=params)
        resp.raise_for_status()
        data = resp.json()
        if not data:
            return
        latest = data[-1]
        event = CalibreEvent(
            event_type=EventType.WEATHER,
            session_key=self.session_key,
            source_timestamp=self._parse_ts(latest.get("date", "")),
            source_id="openf1",
            operating_mode=self.operating_mode,
            data={
                "air_temp_c": float(latest.get("air_temperature", 30.0)),
                "track_temp_c": float(latest.get("track_temperature", 45.0)),
                "humidity_pct": float(latest.get("humidity", 50.0)),
                "pressure_mbar": float(latest.get("pressure", 1013.0)),
                "wind_speed_kph": float(latest.get("wind_speed", 5.0)),
                "wind_direction_deg": float(latest.get("wind_direction", 180.0)),
                "rainfall": bool(latest.get("rainfall", 0)),
            },
            raw_payload=latest,
            field_origins={k: "OBSERVED" for k in [
                "air_temp_c", "track_temp_c", "humidity_pct",
                "pressure_mbar", "wind_speed_kph", "rainfall",
            ]},
            units={"air_temp_c": "°C", "track_temp_c": "°C", "wind_speed_kph": "km/h"},
        )
        event.compute_dedup_key()
        await self._emit(event)
        if "date" in latest:
            self._last_weather_ts = latest["date"]

    async def _fetch_race_control(self) -> None:
        params: Dict[str, Any] = {"session_key": self.session_key}
        if self._last_rc_ts:
            params["date>"] = self._last_rc_ts

        resp = await self._client.get("/race_control", params=params)
        resp.raise_for_status()
        data = resp.json()
        for msg in data:
            event = CalibreEvent(
                event_type=EventType.RACE_CONTROL,
                session_key=self.session_key,
                source_timestamp=self._parse_ts(msg.get("date", "")),
                source_id="openf1",
                operating_mode=self.operating_mode,
                data={
                    "flag": (msg.get("flag") or "").upper().strip(),
                    "category": (msg.get("category") or "").upper().strip(),
                    "message": msg.get("message", ""),
                    "scope": msg.get("scope", ""),
                    "driver_number": msg.get("driver_number"),
                },
                raw_payload=msg,
            )
            event.compute_dedup_key()
            await self._emit(event)

        if data and "date" in data[-1]:
            self._last_rc_ts = data[-1]["date"]

    @staticmethod
    def _parse_ts(ts_str: str) -> float:
        if not ts_str:
            return time.time()
        try:
            from datetime import datetime
            return datetime.fromisoformat(ts_str.replace("Z", "+00:00")).timestamp()
        except (ValueError, TypeError):
            return time.time()

    # ── Session discovery ─────────────────────────────────────────

    @staticmethod
    async def discover_sessions(year: int = 2025) -> List[Dict]:
        """Fetch available sessions for a given year."""
        async with httpx.AsyncClient(base_url=OPENF1_BASE, timeout=15.0) as client:
            resp = await client.get(f"/sessions?year={year}")
            resp.raise_for_status()
            return resp.json()
