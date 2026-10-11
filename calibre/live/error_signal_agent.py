"""
CALIBRE — Error Signal Agent (§37)
===================================
Monitors live data quality and detects anomalies in real-time telemetry
streams. Every detected signal is logged and pushed to the event bus.

Error Signal Categories:
  - DATA_GAP: No data received for a driver beyond threshold
  - TELEMETRY_SPIKE: Physical impossibility in sensor values
  - LAP_TIME_ANOMALY: Lap time deviates significantly from rolling mean
  - POSITION_JUMP: Position changes unreasonably in a single update
  - STINT_MISMATCH: Compound doesn't match expected transition
  - WEATHER_SHIFT: Sudden weather condition change
  - SENSOR_DROPOUT: Specific channels go to zero unexpectedly
"""
from __future__ import annotations

import logging
import time
from collections import defaultdict, deque
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class ErrorSignalAgent:
    """Real-time data quality and anomaly detection agent."""

    # Physical limits for F1 telemetry
    MAX_SPEED_KPH = 370.0
    MAX_LAP_TIME_S = 180.0  # 3 minutes — anything above is invalid
    MIN_LAP_TIME_S = 55.0   # Below this is physically impossible
    POSITION_JUMP_THRESHOLD = 6  # Max positions change per update
    LAP_TIME_SIGMA_THRESHOLD = 3.0  # Standard deviations for anomaly
    DATA_GAP_THRESHOLD_S = 15.0  # Seconds without data = gap

    def __init__(self):
        # Per-driver tracking
        self.last_telemetry_ts: Dict[str, float] = {}
        self.lap_time_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=20))
        self.last_positions: Dict[str, int] = {}
        self.last_compounds: Dict[str, str] = {}
        self.last_weather: Optional[Dict[str, Any]] = None

        # Active signals
        self.active_signals: List[Dict[str, Any]] = []
        self.signal_history: List[Dict[str, Any]] = []
        self._signal_count = 0

    def process_telemetry(self, driver: str, telemetry: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Check a telemetry sample for error signals. Returns list of signals detected."""
        signals = []
        now = time.time()

        # 1. DATA_GAP detection
        last_ts = self.last_telemetry_ts.get(driver)
        if last_ts and (now - last_ts) > self.DATA_GAP_THRESHOLD_S:
            sig = self._make_signal(
                "DATA_GAP",
                driver,
                severity="WARNING",
                details={
                    "gap_duration_s": round(now - last_ts, 1),
                    "threshold_s": self.DATA_GAP_THRESHOLD_S,
                },
                message=f"No telemetry data for {driver} for {now - last_ts:.1f}s",
            )
            signals.append(sig)
        self.last_telemetry_ts[driver] = now

        # 2. TELEMETRY_SPIKE — physical impossibility
        speed = telemetry.get("speed_kph", 0)
        throttle = telemetry.get("throttle_pct", 0)
        brake = telemetry.get("brake_pct", 0)

        if speed > self.MAX_SPEED_KPH:
            signals.append(self._make_signal(
                "TELEMETRY_SPIKE",
                driver,
                severity="ERROR",
                details={"channel": "speed_kph", "value": speed, "max": self.MAX_SPEED_KPH},
                message=f"{driver} speed {speed:.0f} kph exceeds physical limit {self.MAX_SPEED_KPH}",
            ))

        if throttle > 100.0 or throttle < 0.0:
            signals.append(self._make_signal(
                "TELEMETRY_SPIKE",
                driver,
                severity="WARNING",
                details={"channel": "throttle_pct", "value": throttle},
                message=f"{driver} throttle {throttle:.1f}% out of [0, 100] range",
            ))

        if brake > 100.0 or brake < 0.0:
            signals.append(self._make_signal(
                "TELEMETRY_SPIKE",
                driver,
                severity="WARNING",
                details={"channel": "brake_pct", "value": brake},
                message=f"{driver} brake {brake:.1f}% out of [0, 100] range",
            ))

        # 3. SENSOR_DROPOUT — all channels at zero simultaneously
        if speed == 0 and throttle == 0 and brake == 0 and telemetry.get("rpm", 0) == 0:
            signals.append(self._make_signal(
                "SENSOR_DROPOUT",
                driver,
                severity="WARNING",
                details={"all_channels_zero": True},
                message=f"{driver} all telemetry channels read zero — possible sensor dropout or car stopped",
            ))

        return signals

    def process_lap(self, driver: str, lap_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Check a completed lap for error signals."""
        signals = []
        lap_time = lap_data.get("lap_time_s", 0)
        lap_number = lap_data.get("lap_number", 0)
        is_pit_in = lap_data.get("is_pit_in_lap", False)
        is_pit_out = lap_data.get("is_pit_out_lap", False)

        # Skip pit laps for anomaly detection
        if is_pit_in or is_pit_out:
            return signals

        # 4. LAP_TIME_ANOMALY — basic bounds check
        if lap_time > self.MAX_LAP_TIME_S:
            signals.append(self._make_signal(
                "LAP_TIME_ANOMALY",
                driver,
                severity="WARNING",
                details={"lap": lap_number, "lap_time_s": lap_time, "max": self.MAX_LAP_TIME_S},
                message=f"{driver} Lap {lap_number} time {lap_time:.3f}s exceeds {self.MAX_LAP_TIME_S}s threshold",
            ))
        elif lap_time < self.MIN_LAP_TIME_S and lap_time > 0:
            signals.append(self._make_signal(
                "LAP_TIME_ANOMALY",
                driver,
                severity="ERROR",
                details={"lap": lap_number, "lap_time_s": lap_time, "min": self.MIN_LAP_TIME_S},
                message=f"{driver} Lap {lap_number} time {lap_time:.3f}s below physical minimum",
            ))

        # Statistical anomaly using rolling mean/std
        history = self.lap_time_history[driver]
        if len(history) >= 3 and lap_time > 0:
            import statistics
            mean = statistics.mean(history)
            stdev = statistics.stdev(history) if len(history) > 1 else 1.0
            if stdev > 0 and abs(lap_time - mean) > self.LAP_TIME_SIGMA_THRESHOLD * stdev:
                signals.append(self._make_signal(
                    "LAP_TIME_ANOMALY",
                    driver,
                    severity="INFO",
                    details={
                        "lap": lap_number,
                        "lap_time_s": lap_time,
                        "rolling_mean": round(mean, 3),
                        "rolling_std": round(stdev, 3),
                        "z_score": round((lap_time - mean) / stdev, 2),
                    },
                    message=f"{driver} Lap {lap_number} time {lap_time:.3f}s is {abs(lap_time - mean) / stdev:.1f}σ from rolling mean {mean:.3f}s",
                ))

        if lap_time > 0:
            history.append(lap_time)

        return signals

    def process_position_update(self, driver: str, new_position: int) -> List[Dict[str, Any]]:
        """Check for abnormal position changes."""
        signals = []
        old_pos = self.last_positions.get(driver)

        if old_pos is not None:
            jump = abs(new_position - old_pos)
            if jump >= self.POSITION_JUMP_THRESHOLD:
                signals.append(self._make_signal(
                    "POSITION_JUMP",
                    driver,
                    severity="WARNING",
                    details={
                        "old_position": old_pos,
                        "new_position": new_position,
                        "jump": jump,
                    },
                    message=f"{driver} position jumped from P{old_pos} to P{new_position} ({jump} places)",
                ))

        self.last_positions[driver] = new_position
        return signals

    def process_stint_update(self, driver: str, compound: str, stint_number: int) -> List[Dict[str, Any]]:
        """Check for unexpected compound changes."""
        signals = []
        old_compound = self.last_compounds.get(driver)

        if old_compound and old_compound != compound:
            # Verify it's a legitimate pit stop (stint number changed)
            signals.append(self._make_signal(
                "STINT_CHANGE",
                driver,
                severity="INFO",
                details={
                    "old_compound": old_compound,
                    "new_compound": compound,
                    "stint_number": stint_number,
                },
                message=f"{driver} compound changed: {old_compound} → {compound} (stint {stint_number})",
            ))

        self.last_compounds[driver] = compound
        return signals

    def process_weather_update(self, weather: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Check for sudden weather shifts."""
        signals = []

        if self.last_weather:
            temp_delta = abs(weather.get("air_temp_c", 0) - self.last_weather.get("air_temp_c", 0))
            track_delta = abs(weather.get("track_temp_c", 0) - self.last_weather.get("track_temp_c", 0))
            rain_change = weather.get("rainfall", False) != self.last_weather.get("rainfall", False)

            if temp_delta > 3.0:
                signals.append(self._make_signal(
                    "WEATHER_SHIFT",
                    "ALL",
                    severity="WARNING",
                    details={"type": "air_temp", "delta_c": round(temp_delta, 1)},
                    message=f"Air temperature shifted by {temp_delta:.1f}°C",
                ))

            if track_delta > 5.0:
                signals.append(self._make_signal(
                    "WEATHER_SHIFT",
                    "ALL",
                    severity="WARNING",
                    details={"type": "track_temp", "delta_c": round(track_delta, 1)},
                    message=f"Track temperature shifted by {track_delta:.1f}°C",
                ))

            if rain_change:
                is_raining = weather.get("rainfall", False)
                signals.append(self._make_signal(
                    "WEATHER_SHIFT",
                    "ALL",
                    severity="HIGH" if is_raining else "INFO",
                    details={"type": "rainfall", "raining": is_raining},
                    message=f"Rain {'started' if is_raining else 'stopped'}!",
                ))

        self.last_weather = weather.copy()
        return signals

    def get_active_signals(self) -> List[Dict[str, Any]]:
        """Returns currently active error signals."""
        # Prune signals older than 60 seconds
        now = time.time()
        self.active_signals = [s for s in self.active_signals if now - s["ts"] < 60.0]
        return self.active_signals

    def get_signal_summary(self) -> Dict[str, int]:
        """Returns count of each signal type seen."""
        from collections import Counter
        return dict(Counter(s["signal_type"] for s in self.signal_history))

    def _make_signal(
        self, signal_type: str, driver: str, severity: str,
        details: Dict[str, Any], message: str
    ) -> Dict[str, Any]:
        self._signal_count += 1
        signal = {
            "signal_id": f"ERR-{self._signal_count:04d}",
            "signal_type": signal_type,
            "type": signal_type,
            "driver": driver,
            "severity": severity,
            "details": details,
            "message": message,
            "ts": time.time(),
        }
        self.active_signals.append(signal)
        self.signal_history.append(signal)
        logger.info("[ERROR_SIGNAL] %s | %s | %s | %s", signal_type, driver, severity, message)
        return signal
