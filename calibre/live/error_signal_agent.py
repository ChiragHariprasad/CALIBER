"""
CALIBRE — Sensor Integrity Intelligence (§6 FR-SENSOR-001 to FR-SENSOR-004)
=============================================================================
Detects isolated spikes, sustained drift, stuck readings, data gaps, and
sensor dropouts across live and replayed telemetry channels.

Provides evidence-rich findings linking observed values, baselines,
detection methods, confidence, and downstream data quality impact.
"""

from __future__ import annotations

import logging
import time
from collections import defaultdict, deque
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class ErrorSignalAgent:
    """Real-time sensor integrity, anomaly, and drift detection agent."""

    # Physical limits for F1 telemetry
    MAX_SPEED_KPH = 370.0
    MIN_SPEED_KPH = 0.0
    MAX_LAP_TIME_S = 180.0
    MIN_LAP_TIME_S = 55.0
    DATA_GAP_THRESHOLD_S = 15.0
    SPIKE_SPEED_DELTA_THRESHOLD_KMH = 50.0  # Max realistic speed jump in 0.5s (~2.8g)
    DRIFT_SAMPLE_WINDOW = 6
    STUCK_SAMPLE_WINDOW = 6

    def __init__(self):
        # Per-driver telemetry tracking
        self.last_telemetry_ts: Dict[str, float] = {}
        self.last_telemetry_sample: Dict[str, Dict[str, Any]] = {}
        self.lap_time_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=20))
        self.last_positions: Dict[str, int] = {}
        self.last_compounds: Dict[str, str] = {}
        self.last_weather: Optional[Dict[str, Any]] = None

        # Rolling buffers for drift & stuck detection
        self._speed_window: Dict[str, deque] = defaultdict(lambda: deque(maxlen=self.DRIFT_SAMPLE_WINDOW))
        self._stuck_tracker: Dict[str, deque] = defaultdict(lambda: deque(maxlen=self.STUCK_SAMPLE_WINDOW))

        # Active findings and chronological history
        self.active_signals: List[Dict[str, Any]] = []
        self.signal_history: List[Dict[str, Any]] = []
        self._signal_count = 0

    def process_telemetry(self, driver: str, telemetry: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Evaluates a telemetry sample for spikes, sustained drift, stuck values, and dropouts."""
        findings = []
        now = time.time()
        driver = driver.upper()

        speed = float(telemetry.get("speed_kph", telemetry.get("speed", 0.0)))
        throttle = float(telemetry.get("throttle_pct", telemetry.get("throttle", 0.0)))
        brake = float(telemetry.get("brake_pct", telemetry.get("brake", 0.0)))

        # 1. DATA_GAP detection
        last_ts = self.last_telemetry_ts.get(driver)
        if last_ts and (now - last_ts) > self.DATA_GAP_THRESHOLD_S:
            findings.append(self._make_finding(
                category="DATA_GAP",
                signal="telemetry_stream",
                driver=driver,
                severity="WARNING",
                observed_value=round(now - last_ts, 1),
                reference_value=0.5,
                method="heartbeat_interval",
                confidence=0.95,
                details={"gap_duration_s": round(now - last_ts, 1), "threshold_s": self.DATA_GAP_THRESHOLD_S},
                message=f"No telemetry data received for {driver} for {now - last_ts:.1f}s",
                downstream_impact="Downstream agents operating in stale data mode",
            ))
        self.last_telemetry_ts[driver] = now

        prev = self.last_telemetry_sample.get(driver)

        # 2. SPIKE_ANOMALY detection (Outliers & Implausible jump)
        if speed > self.MAX_SPEED_KPH:
            findings.append(self._make_finding(
                category="SPIKE_ANOMALY",
                signal="speed_kph",
                driver=driver,
                severity="CRITICAL",
                observed_value=speed,
                reference_value=self.MAX_SPEED_KPH,
                method="physical_bounds_check",
                confidence=0.99,
                details={"channel": "speed_kph", "value": speed, "max_limit": self.MAX_SPEED_KPH},
                message=f"{driver} speed {speed:.1f} km/h exceeds maximum physical envelope ({self.MAX_SPEED_KPH} km/h)",
                downstream_impact="PitLaneMonitor & VerdictEngine: speed measurement flagged INVALID; verdicts DEFERRED",
            ))
        elif prev and "speed" in prev:
            prev_speed = prev["speed"]
            speed_delta = abs(speed - prev_speed)
            if speed_delta > self.SPIKE_SPEED_DELTA_THRESHOLD_KMH:
                findings.append(self._make_finding(
                    category="SPIKE_ANOMALY",
                    signal="speed_kph",
                    driver=driver,
                    severity="ERROR",
                    observed_value=speed,
                    reference_value=prev_speed,
                    method="rate_of_change_outlier",
                    confidence=0.92,
                    details={"speed_delta": speed_delta, "previous_speed": prev_speed, "current_speed": speed},
                    message=f"{driver} sudden speed jump of {speed_delta:.1f} km/h within single tick ({prev_speed:.1f} -> {speed:.1f})",
                    downstream_impact="Telemetry marked noisy; speed readings quarantined for window",
                ))

        if throttle > 100.0 or throttle < 0.0:
            findings.append(self._make_finding(
                category="SPIKE_ANOMALY",
                signal="throttle_pct",
                driver=driver,
                severity="WARNING",
                observed_value=throttle,
                reference_value=100.0 if throttle > 100 else 0.0,
                method="range_validation",
                confidence=0.98,
                details={"channel": "throttle_pct", "value": throttle},
                message=f"{driver} throttle {throttle:.1f}% out of valid physical range [0, 100]",
                downstream_impact="Throttle telemetry ignored in acceleration modeling",
            ))

        if brake > 100.0 or brake < 0.0:
            findings.append(self._make_finding(
                category="SPIKE_ANOMALY",
                signal="brake_pct",
                driver=driver,
                severity="WARNING",
                observed_value=brake,
                reference_value=100.0 if brake > 100 else 0.0,
                method="range_validation",
                confidence=0.98,
                details={"channel": "brake_pct", "value": brake},
                message=f"{driver} brake pressure {brake:.1f}% out of valid physical range [0, 100]",
                downstream_impact="Braking point analysis deferred",
            ))

        # 3. SUSTAINED_DRIFT detection
        window = self._speed_window[driver]
        window.append(speed)
        if len(window) >= self.DRIFT_SAMPLE_WINDOW:
            deltas = [window[i] - window[i - 1] for i in range(1, len(window))]
            # Check for monotonic continuous positive drift or negative drift with steady throttle
            is_monotonic_drift = all(d > 1.5 for d in deltas) or all(d < -1.5 for d in deltas)
            mean_drift = sum(deltas) / len(deltas)
            if is_monotonic_drift and abs(mean_drift) >= 2.0 and throttle > 0:
                cumulative_drift = window[-1] - window[0]
                findings.append(self._make_finding(
                    category="SUSTAINED_DRIFT",
                    signal="speed_kph",
                    driver=driver,
                    severity="WARNING",
                    observed_value=round(speed, 1),
                    reference_value=round(window[0], 1),
                    method="cumulative_drift_analysis",
                    confidence=0.88,
                    details={
                        "window_size": len(window),
                        "cumulative_drift_kmh": round(cumulative_drift, 1),
                        "mean_step_kmh": round(mean_drift, 2),
                    },
                    message=f"{driver} sustained speed drift detected (+{cumulative_drift:.1f} km/h over {len(window)} ticks)",
                    downstream_impact="Speed sensor calibration bias suspected; secondary timing loop prioritized",
                ))

        # 4. STUCK_SENSOR detection (repeated identical non-zero readings)
        stuck_buf = self._stuck_tracker[driver]
        stuck_buf.append(speed)
        if len(stuck_buf) >= self.STUCK_SAMPLE_WINDOW and speed > 10.0:
            if len(set(stuck_buf)) == 1:
                findings.append(self._make_finding(
                    category="STUCK_SENSOR",
                    signal="speed_kph",
                    driver=driver,
                    severity="ERROR",
                    observed_value=speed,
                    reference_value=speed,
                    method="frozen_channel_detector",
                    confidence=0.96,
                    details={"frozen_value": speed, "consecutive_samples": len(stuck_buf)},
                    message=f"{driver} speed sensor stuck at {speed:.1f} km/h across {len(stuck_buf)} consecutive updates",
                    downstream_impact="Speed channel marked frozen; all rule evaluations deferred",
                ))

        # 5. SENSOR_DROPOUT detection
        if speed == 0 and throttle == 0 and brake == 0 and telemetry.get("rpm", 0) == 0:
            if prev and prev.get("speed", 0) > 30.0:
                findings.append(self._make_finding(
                    category="SENSOR_DROPOUT",
                    signal="all_channels",
                    driver=driver,
                    severity="WARNING",
                    observed_value=0.0,
                    reference_value=prev.get("speed", 0),
                    method="multi_signal_zero_correlation",
                    confidence=0.85,
                    details={"all_channels_zero": True, "last_speed": prev.get("speed")},
                    message=f"{driver} all primary dynamic channels dropped to zero simultaneously",
                    downstream_impact="Vehicle telemetry stream unverified",
                ))

        self.last_telemetry_sample[driver] = {"speed": speed, "throttle": throttle, "brake": brake}
        return findings

    def process_lap(self, driver: str, lap_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Evaluates completed lap times against physical bounds and statistical history."""
        findings = []
        driver = driver.upper()
        lap_time = float(lap_data.get("lap_time_s", 0.0))
        lap_number = int(lap_data.get("lap_number", 0))
        is_pit_in = lap_data.get("is_pit_in_lap", False)
        is_pit_out = lap_data.get("is_pit_out_lap", False)

        if is_pit_in or is_pit_out or lap_time <= 0:
            return findings

        if lap_time > self.MAX_LAP_TIME_S:
            findings.append(self._make_finding(
                category="LAP_TIME_ANOMALY",
                signal="lap_timing",
                driver=driver,
                severity="WARNING",
                observed_value=lap_time,
                reference_value=self.MAX_LAP_TIME_S,
                method="timing_envelope_check",
                confidence=0.97,
                details={"lap": lap_number, "lap_time_s": lap_time, "max_allowed": self.MAX_LAP_TIME_S},
                message=f"{driver} Lap {lap_number} time {lap_time:.3f}s exceeds maximum envelope ({self.MAX_LAP_TIME_S}s)",
                downstream_impact="Lap excluded from tyre degradation and pace models",
            ))
        elif lap_time < self.MIN_LAP_TIME_S and lap_time > 0:
            findings.append(self._make_finding(
                category="LAP_TIME_ANOMALY",
                signal="lap_timing",
                driver=driver,
                severity="ERROR",
                observed_value=lap_time,
                reference_value=self.MIN_LAP_TIME_S,
                method="physical_minimum_check",
                confidence=0.99,
                details={"lap": lap_number, "lap_time_s": lap_time, "min_allowed": self.MIN_LAP_TIME_S},
                message=f"{driver} Lap {lap_number} time {lap_time:.3f}s is below physically achievable minimum ({self.MIN_LAP_TIME_S}s)",
                downstream_impact="Lap timing invalidated and quarantined",
            ))

        history = self.lap_time_history[driver]
        if len(history) >= 3 and lap_time > 0:
            import statistics
            mean = statistics.mean(history)
            stdev = statistics.stdev(history) if len(history) > 1 else 1.0
            if stdev > 0 and abs(lap_time - mean) > 3.0 * stdev:
                findings.append(self._make_finding(
                    category="LAP_TIME_ANOMALY",
                    signal="lap_timing",
                    driver=driver,
                    severity="INFO",
                    observed_value=lap_time,
                    reference_value=round(mean, 3),
                    method="gaussian_3sigma_residual",
                    confidence=0.85,
                    details={"lap": lap_number, "rolling_mean": round(mean, 3), "rolling_std": round(stdev, 3), "z_score": round((lap_time - mean) / stdev, 2)},
                    message=f"{driver} Lap {lap_number} time {lap_time:.3f}s deviates >3σ from rolling pace baseline",
                    downstream_impact="Pace smoothing weighting temporarily reduced",
                ))

        if lap_time > 0:
            history.append(lap_time)

        return findings

    def process_position_update(self, driver: str, new_position: int) -> List[Dict[str, Any]]:
        """Check for abnormal position changes."""
        findings = []
        old_pos = self.last_positions.get(driver)
        if old_pos is not None:
            jump = abs(new_position - old_pos)
            if jump >= 6:
                findings.append(self._make_finding(
                    category="POSITION_JUMP",
                    signal="timing_position",
                    driver=driver,
                    severity="WARNING",
                    observed_value=new_position,
                    reference_value=old_pos,
                    method="discrete_step_check",
                    confidence=0.91,
                    details={"old_position": old_pos, "new_position": new_position, "jump": jump},
                    message=f"{driver} position jumped from P{old_pos} to P{new_position} ({jump} places in single update)",
                    downstream_impact="Position tracking flagged discontinuous",
                ))
        self.last_positions[driver] = new_position
        return findings

    def process_stint_update(self, driver: str, compound: str, stint_number: int) -> List[Dict[str, Any]]:
        """Check for unexpected compound transitions."""
        findings = []
        old_compound = self.last_compounds.get(driver)
        if old_compound and old_compound != compound:
            findings.append(self._make_finding(
                category="STINT_CHANGE",
                signal="tyre_compound",
                driver=driver,
                severity="INFO",
                observed_value=compound,
                reference_value=old_compound,
                method="state_transition_audit",
                confidence=0.99,
                details={"old_compound": old_compound, "new_compound": compound, "stint_number": stint_number},
                message=f"{driver} compound transition verified: {old_compound} -> {compound} (stint {stint_number})",
                downstream_impact="Tyre degradation model reset for new compound",
            ))
        self.last_compounds[driver] = compound
        return findings

    def process_weather_update(self, weather: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Check for significant meteorological shifts."""
        findings = []
        if self.last_weather:
            temp_delta = abs(weather.get("air_temp_c", 0) - self.last_weather.get("air_temp_c", 0))
            if temp_delta > 3.0:
                findings.append(self._make_finding(
                    category="WEATHER_SHIFT",
                    signal="air_temperature",
                    driver="ALL",
                    severity="WARNING",
                    observed_value=weather.get("air_temp_c"),
                    reference_value=self.last_weather.get("air_temp_c"),
                    method="thermal_gradient_check",
                    confidence=0.88,
                    details={"delta_c": round(temp_delta, 1)},
                    message=f"Air temperature shifted by {temp_delta:.1f}°C",
                    downstream_impact="Track grip evolution and cooling rates adjusted",
                ))
        self.last_weather = weather.copy()
        return findings

    def inject_synthetic_fault(
        self,
        fault_type: str,
        driver: str = "VER",
        signal: str = "speed_kph",
        magnitude: float = 120.0,
    ) -> Dict[str, Any]:
        """Injects an explicitly labelled synthetic anomaly for testing and fault verification."""
        fault_type = fault_type.lower()
        if "spike" in fault_type:
            category = "SPIKE_ANOMALY"
            obs = 410.0
            ref = 290.0
            msg = f"SIMULATED: {driver} artificial speed spike to {obs} km/h (+{magnitude} km/h anomaly)"
        elif "drift" in fault_type:
            category = "SUSTAINED_DRIFT"
            obs = 315.0
            ref = 290.0
            msg = f"SIMULATED: {driver} artificial sensor drift (+{magnitude:.1f} km/h positive offset bias)"
        elif "stuck" in fault_type:
            category = "STUCK_SENSOR"
            obs = 180.0
            ref = 180.0
            msg = f"SIMULATED: {driver} artificial stuck sensor at constant {obs} km/h"
        else:
            category = "SPIKE_ANOMALY"
            obs = 395.0
            ref = 290.0
            msg = f"SIMULATED: Generic injected anomaly on {signal}"

        finding = self._make_finding(
            category=category,
            signal=signal,
            driver=driver,
            severity="ERROR",
            observed_value=obs,
            reference_value=ref,
            method="synthetic_fault_injection",
            confidence=1.0,
            details={"is_injected_scenario": True, "magnitude": magnitude, "fault_type": fault_type},
            message=msg,
            downstream_impact="Controlled fault injection: downstream rule evaluation downgraded to DEFER",
            is_synthetic=True,
        )
        return finding

    def get_active_signals(self) -> List[Dict[str, Any]]:
        """Returns currently active error signals (within 90s window)."""
        now = time.time()
        self.active_signals = [s for s in self.active_signals if now - s["ts"] < 90.0]
        return self.active_signals

    def get_signal_summary(self) -> Dict[str, int]:
        from collections import Counter
        return dict(Counter(s["category"] for s in self.signal_history))

    def _make_finding(
        self,
        category: str,
        signal: str,
        driver: str,
        severity: str,
        observed_value: Any,
        reference_value: Any,
        method: str,
        confidence: float,
        details: Dict[str, Any],
        message: str,
        downstream_impact: str,
        is_synthetic: bool = False,
    ) -> Dict[str, Any]:
        self._signal_count += 1
        finding = {
            "finding_id": f"ERR-{self._signal_count:04d}",
            "anomaly_id": f"ANOM-{self._signal_count:04d}",
            "signal_id": f"ERR-{self._signal_count:04d}",
            "category": category,
            "signal_type": "TELEMETRY_SPIKE" if category == "SPIKE_ANOMALY" else category,
            "type": category,
            "signal": signal,
            "driver": driver,
            "severity": severity,
            "observed_value": observed_value,
            "reference_value": reference_value,
            "detection_method": method,
            "confidence": round(confidence, 2),
            "evidence_window": [
                {"timestamp": time.time() - 1.0, "value": reference_value},
                {"timestamp": time.time(), "value": observed_value},
            ],
            "details": details,
            "message": message,
            "downstream_impact": downstream_impact,
            "is_synthetic": is_synthetic,
            "data_quality": {
                "is_valid": False if severity in ("ERROR", "CRITICAL") else True,
                "is_stale": False,
                "provenance": "simulated" if is_synthetic else "observed",
            },
            "ts": time.time(),
            "iso": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
        self.active_signals.append(finding)
        self.signal_history.append(finding)
        logger.info("[INTEGRITY_FINDING] %s | %s | %s | %s", category, driver, severity, message)
        return finding
