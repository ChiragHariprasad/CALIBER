"""
CALIBRE — Real-Time Pit Lane Monitor & Sensor Integrity Arbiter
===============================================================
Enforces the FIA pit-lane speed limit (default 60 km/h or 80 km/h) by coupling:
  1. PitLaneSpeedMonitor (speed breach detection)
  2. UncertaintyEngine (statistical classification with sensor error margins)
  3. CUSUMDetector (sensor drift & calibration failure detection)
  4. VerdictEngine (BREACH / CLEAR / DEFER decision support)

Ensures that no false penalty recommendation is issued if on-car telemetry
has drifted or timing-loop sensors are degraded.
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Union

from calibre.integrity.detector import CUSUMDetector, IntegrityAlert
from calibre.live.schemas.events import (
    CalibreEvent,
    DataOrigin,
    DataQuality,
    EventType,
    SensorIntegrityFinding,
    ValidationState,
)
from calibre.rules.pit_lane_speed import PitLaneSpeedMonitor
from calibre.schemas.telemetry import TelemetryEvent
from calibre.schemas.verdict import Verdict, VerdictValue
from calibre.uncertainty.engine import UncertaintyEngine
from calibre.verdict.engine import VerdictEngine

logger = logging.getLogger(__name__)


class PitLaneMonitor:
    """
    Live monitoring subsystem for pit lane entry, transit, and exit.
    Integrates rule monitoring, sensor uncertainty, and CUSUM integrity detection.
    """

    def __init__(
        self,
        speed_limit_kmh: float = 60.0,
        cusum_threshold: float = 4.0,
        cusum_drift: float = 0.5,
    ):
        self.speed_limit_kmh = speed_limit_kmh
        self.rule_monitor = PitLaneSpeedMonitor(limit_kmh=speed_limit_kmh)
        self.uncertainty_engine = UncertaintyEngine()
        self.integrity_detector = CUSUMDetector(
            threshold=cusum_threshold,
            drift=cusum_drift,
        )
        self.verdict_engine = VerdictEngine(
            uncertainty_engine=self.uncertainty_engine,
            integrity_detector=self.integrity_detector,
        )

        self._verdict_history: List[Verdict] = []
        self._alerts_history: List[IntegrityAlert] = []
        self._active_breaches: List[Verdict] = []
        self._total_evaluated = 0

    def feed_timing_cross_check(
        self,
        sector: str,
        timing_speed: float,
        oncar_speed: float,
        timestamp: Optional[datetime] = None,
    ) -> Optional[IntegrityAlert]:
        """
        Feeds timing-loop speed vs on-car speed to detect sensor bias or calibration drift.
        Returns IntegrityAlert if CUSUM boundary is exceeded.
        """
        alert = self.integrity_detector.update(
            sector=sector,
            timing_speed=timing_speed,
            oncar_speed=oncar_speed,
            timestamp=timestamp,
        )
        if alert:
            self._alerts_history.append(alert)
            logger.warning(
                "PitLaneMonitor: CUSUM Integrity Alert in %s! Residual=%.2f, CUSUM=%.2f, Severity=%s",
                sector,
                alert.residual,
                alert.cusum,
                alert.severity,
            )
        return alert

    def evaluate_telemetry(
        self,
        sample: Union[Dict[str, Any], TelemetryEvent, CalibreEvent],
    ) -> Optional[Verdict]:
        """
        Evaluates a telemetry sample against pit-lane regulations.
        Returns a Verdict (BREACH, CLEAR, or DEFER) if within the pit lane, or None.
        """
        self._total_evaluated += 1
        telem_event = self._normalize_to_telemetry_event(sample)
        if telem_event is None:
            return None

        # Check if car is in pit lane / pit entry / pit exit
        zone = telem_event.zone
        if not zone or zone not in ("pit_lane", "pit_entry", "pit_exit"):
            # Check if sample dict has in_pit flag
            if isinstance(sample, dict) and sample.get("in_pit"):
                telem_event.zone = "pit_lane"
            elif isinstance(sample, CalibreEvent) and sample.data.get("in_pit"):
                telem_event.zone = "pit_lane"
            else:
                return None

        rule_result = self.rule_monitor.evaluate(telem_event)
        if rule_result is None:
            return None

        verdict = self.verdict_engine.decide(
            rule_result=rule_result,
            event=telem_event,
        )

        self._verdict_history.append(verdict)
        if verdict.verdict == VerdictValue.BREACH:
            self._active_breaches.append(verdict)
            logger.warning(
                "PIT LANE SPEED BREACH: Driver %s measured %.1f km/h (limit %.1f km/h)",
                verdict.driver_number,
                verdict.measured_value,
                verdict.threshold,
            )
        elif verdict.verdict == VerdictValue.DEFER:
            logger.info(
                "PIT LANE SPEED DEFERRED (Sensor Integrity Concern): Driver %s measured %.1f km/h",
                verdict.driver_number,
                verdict.measured_value,
            )

        return verdict

    def evaluate_and_wrap(
        self,
        sample: Union[Dict[str, Any], TelemetryEvent, CalibreEvent],
        source_id: str = "pit_lane_monitor",
    ) -> Optional[CalibreEvent]:
        """Evaluates sample and returns a canonical CalibreEvent if a verdict was generated."""
        verdict = self.evaluate_telemetry(sample)
        if verdict is None:
            return None

        now = time.time()
        ev = CalibreEvent(
            event_type=EventType.VERDICT,
            source_id=source_id,
            operating_mode="LIVE",
            source_timestamp=now,
            driver_number=str(verdict.driver_number),
            data_quality=DataQuality.HIGH if verdict.verdict != VerdictValue.DEFER else DataQuality.LOW,
            data={
                "rule_id": verdict.rule_id,
                "verdict": verdict.verdict.value,
                "measured_speed_kmh": verdict.measured_value,
                "limit_kmh": verdict.threshold,
                "uncertainty": verdict.uncertainty,
                "confidence_margin": verdict.confidence_margin,
                "driver_number": verdict.driver_number,
                "timestamp": verdict.timestamp.isoformat(),
            },
            field_origins={
                "verdict": DataOrigin.DERIVED.value,
                "measured_speed_kmh": DataOrigin.OBSERVED.value,
            },
        )
        ev.compute_dedup_key()
        return ev

    def _normalize_to_telemetry_event(
        self, sample: Union[Dict[str, Any], TelemetryEvent, CalibreEvent]
    ) -> Optional[TelemetryEvent]:
        """Converts diverse sample representations into a TelemetryEvent."""
        if isinstance(sample, TelemetryEvent):
            return sample

        now_dt = datetime.now(timezone.utc)

        if isinstance(sample, CalibreEvent):
            d = sample.data
            speed = float(d.get("speed_kph") or d.get("speed", 0.0))
            driver_num_str = sample.driver_number or str(d.get("driver_number", "1"))
            try:
                driver_num = int("".join(filter(str.isdigit, driver_num_str)) or "1")
            except Exception:
                driver_num = 1

            zone = d.get("zone") or ("pit_lane" if d.get("in_pit") else None)
            return TelemetryEvent(
                timestamp=datetime.fromtimestamp(sample.source_timestamp, tz=timezone.utc),
                session_key=sample.session_key or 11731,
                driver_number=driver_num,
                speed=speed,
                throttle=float(d.get("throttle_pct", 0.0)),
                brake=float(d.get("brake_pct", 0.0)),
                gear=int(d.get("gear", 0)),
                rpm=float(d.get("rpm", 0.0)),
                source=sample.source_id,
                zone=zone,
            )

        if isinstance(sample, dict):
            speed = float(sample.get("speed_kph") or sample.get("speed", 0.0))
            driver_num_raw = str(sample.get("driver_number", sample.get("driver", "1")))
            try:
                driver_num = int("".join(filter(str.isdigit, driver_num_raw)) or "1")
            except Exception:
                driver_num = 1

            ts = sample.get("timestamp")
            if isinstance(ts, (int, float)):
                ts_dt = datetime.fromtimestamp(ts, tz=timezone.utc)
            elif isinstance(ts, str):
                try:
                    ts_dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                except Exception:
                    ts_dt = now_dt
            elif isinstance(ts, datetime):
                ts_dt = ts
            else:
                ts_dt = now_dt

            zone = sample.get("zone") or ("pit_lane" if sample.get("in_pit") else None)
            return TelemetryEvent(
                timestamp=ts_dt,
                session_key=int(sample.get("session_key", 11731)),
                driver_number=driver_num,
                speed=speed,
                throttle=float(sample.get("throttle_pct", sample.get("throttle", 0.0))),
                brake=float(sample.get("brake_pct", sample.get("brake", 0.0))),
                gear=int(sample.get("gear", 0)),
                rpm=float(sample.get("rpm", 0.0)),
                source=str(sample.get("source", "raw")),
                zone=zone,
            )

        return None

    def get_stats(self) -> Dict[str, Any]:
        """Summary metrics of pit lane monitoring."""
        breach_count = sum(1 for v in self._verdict_history if v.verdict == VerdictValue.BREACH)
        clear_count = sum(1 for v in self._verdict_history if v.verdict == VerdictValue.CLEAR)
        defer_count = sum(1 for v in self._verdict_history if v.verdict == VerdictValue.DEFER)
        return {
            "speed_limit_kmh": self.speed_limit_kmh,
            "total_evaluated": self._total_evaluated,
            "total_verdicts": len(self._verdict_history),
            "breaches": breach_count,
            "clears": clear_count,
            "defers": defer_count,
            "integrity_alerts": len(self._alerts_history),
            "recent_alerts": [
                {
                    "sector": a.sector,
                    "residual": round(a.residual, 2),
                    "cusum": round(a.cusum, 2),
                    "severity": a.severity,
                }
                for a in self._alerts_history[-5:]
            ],
        }

    def get_recent_verdicts(self, limit: int = 20) -> List[Dict[str, Any]]:
        return [
            {
                "timestamp": v.timestamp.isoformat(),
                "driver_number": v.driver_number,
                "rule_id": v.rule_id,
                "verdict": v.verdict.value,
                "measured_speed": v.measured_value,
                "threshold": v.threshold,
                "uncertainty": round(v.uncertainty, 3),
            }
            for v in self._verdict_history[-limit:]
        ]
