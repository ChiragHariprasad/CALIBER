"""
CALIBRE — Racing Line Agent (§40)
==================================
Analyses sector performance, pace models, DRS effectiveness, and
traffic detection for all drivers in real time.

Outputs:
  - Per-driver sector deltas vs field median
  - Rolling exponential weighted pace estimate
  - DRS zone usage tracking
  - Traffic/dirty air detection
  - Fastest driver per sector
"""
from __future__ import annotations

import logging
import time
from collections import defaultdict, deque
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


class LineAgent:
    """Real-time racing line and pace analysis agent."""

    # Exponential weight factor for pace smoothing (higher = more weight on recent)
    EWM_ALPHA = 0.3

    def __init__(self):
        # Per-driver sector time histories
        self.sector_times: Dict[str, Dict[int, deque]] = defaultdict(
            lambda: {1: deque(maxlen=30), 2: deque(maxlen=30), 3: deque(maxlen=30)}
        )
        # Per-driver lap time history (valid laps only)
        self.pace_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=30))
        # Exponentially weighted pace per driver
        self.ewm_pace: Dict[str, float] = {}

        # Field-wide sector medians (updated each lap)
        self.sector_medians: Dict[int, float] = {1: 0, 2: 0, 3: 0}

        # DRS tracking
        self.drs_activations: Dict[str, int] = defaultdict(int)
        self.drs_total_samples: Dict[str, int] = defaultdict(int)

        # Traffic tracking
        self.traffic_laps: Dict[str, int] = defaultdict(int)

        # Best sectors
        self.best_sectors: Dict[int, Tuple[str, float]] = {}  # sector -> (driver, time)
        self.best_laps: Dict[str, float] = {}  # driver -> personal best

        # Insights log
        self.insights: List[Dict[str, Any]] = []

    def process_lap(self, driver: str, lap_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Process a completed lap and generate line analysis insights."""
        insights = []
        s1 = lap_data.get("s1_time_s", 0)
        s2 = lap_data.get("s2_time_s", 0)
        s3 = lap_data.get("s3_time_s", 0)
        lap_time = lap_data.get("lap_time_s", 0)
        lap_number = lap_data.get("lap_number", 0)
        is_pit_in = lap_data.get("is_pit_in_lap", False)
        is_pit_out = lap_data.get("is_pit_out_lap", False)
        is_valid = lap_data.get("is_valid", True)

        if not is_valid or is_pit_in or is_pit_out or lap_time <= 0:
            return insights

        # Store sector times
        if s1 > 0:
            self.sector_times[driver][1].append(s1)
        if s2 > 0:
            self.sector_times[driver][2].append(s2)
        if s3 > 0:
            self.sector_times[driver][3].append(s3)

        # Store pace
        self.pace_history[driver].append(lap_time)

        # Update exponentially weighted pace
        if driver not in self.ewm_pace:
            self.ewm_pace[driver] = lap_time
        else:
            self.ewm_pace[driver] = self.EWM_ALPHA * lap_time + (1 - self.EWM_ALPHA) * self.ewm_pace[driver]

        # Update personal best
        if driver not in self.best_laps or lap_time < self.best_laps[driver]:
            self.best_laps[driver] = lap_time
            insights.append(self._make_insight(
                "PERSONAL_BEST", driver,
                details={"lap": lap_number, "time_s": round(lap_time, 3)},
                message=f"{driver} set personal best: {lap_time:.3f}s on Lap {lap_number}",
            ))

        # Update best sectors
        for sec_num, sec_time in [(1, s1), (2, s2), (3, s3)]:
            if sec_time <= 0:
                continue
            if sec_num not in self.best_sectors or sec_time < self.best_sectors[sec_num][1]:
                old = self.best_sectors.get(sec_num)
                self.best_sectors[sec_num] = (driver, sec_time)
                if old and old[0] != driver:
                    insights.append(self._make_insight(
                        "FASTEST_SECTOR", driver,
                        details={"sector": sec_num, "time_s": round(sec_time, 3), "previous_holder": old[0]},
                        message=f"{driver} takes fastest S{sec_num}: {sec_time:.3f}s (from {old[0]})",
                    ))

        # Sector delta vs field median
        self._update_field_medians()
        sector_deltas = {}
        for sec_num, sec_time in [(1, s1), (2, s2), (3, s3)]:
            if sec_time > 0 and self.sector_medians.get(sec_num, 0) > 0:
                delta = sec_time - self.sector_medians[sec_num]
                sector_deltas[f"s{sec_num}_delta"] = round(delta, 3)

                # Flag if significantly slower (>0.3s)
                if delta > 0.3:
                    insights.append(self._make_insight(
                        "SECTOR_LOSS", driver,
                        details={
                            "sector": sec_num,
                            "time_s": round(sec_time, 3),
                            "median_s": round(self.sector_medians[sec_num], 3),
                            "delta_s": round(delta, 3),
                            "lap": lap_number,
                        },
                        message=f"{driver} lost {delta:.3f}s in S{sec_num} vs field median (Lap {lap_number})",
                    ))

        return insights

    def process_telemetry(self, driver: str, telemetry: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Process telemetry sample for DRS and traffic analysis."""
        insights = []

        # DRS tracking
        drs = telemetry.get("drs", False)
        self.drs_total_samples[driver] += 1
        if drs:
            self.drs_activations[driver] += 1

        return insights

    def detect_traffic(self, driver: str, gap_ahead_s: Optional[float]) -> List[Dict[str, Any]]:
        """Detect if a driver is stuck in dirty air/traffic."""
        insights = []

        if gap_ahead_s is not None and 0 < gap_ahead_s < 1.5:
            self.traffic_laps[driver] += 1
            if self.traffic_laps[driver] >= 3:
                insights.append(self._make_insight(
                    "TRAFFIC_DETECTED", driver,
                    details={
                        "gap_ahead_s": gap_ahead_s,
                        "consecutive_laps": self.traffic_laps[driver],
                    },
                    message=f"{driver} in dirty air for {self.traffic_laps[driver]} consecutive laps (gap: {gap_ahead_s:.1f}s)",
                ))
        else:
            self.traffic_laps[driver] = 0

        return insights

    def get_pace_summary(self) -> Dict[str, Dict[str, Any]]:
        """Returns pace summary for all drivers."""
        summary = {}
        for driver, pace in self.ewm_pace.items():
            history = list(self.pace_history.get(driver, []))
            last_3 = history[-3:] if len(history) >= 3 else history
            avg_last_3 = sum(last_3) / len(last_3) if last_3 else 0

            summary[driver] = {
                "ewm_pace_s": round(pace, 3),
                "last_3_avg_s": round(avg_last_3, 3),
                "personal_best_s": round(self.best_laps.get(driver, 0), 3),
                "laps_recorded": len(history),
                "drs_usage_pct": round(
                    100.0 * self.drs_activations.get(driver, 0) / max(1, self.drs_total_samples.get(driver, 1)),
                    1
                ),
            }

        return summary

    def get_sector_analysis(self, driver: str) -> Dict[str, Any]:
        """Returns detailed sector analysis for a specific driver."""
        analysis = {}
        for sec_num in [1, 2, 3]:
            times = list(self.sector_times[driver][sec_num])
            if times:
                import statistics
                analysis[f"s{sec_num}"] = {
                    "mean_s": round(statistics.mean(times), 3),
                    "best_s": round(min(times), 3),
                    "latest_s": round(times[-1], 3),
                    "std_s": round(statistics.stdev(times), 3) if len(times) > 1 else 0,
                    "count": len(times),
                }
                if self.sector_medians.get(sec_num, 0) > 0:
                    analysis[f"s{sec_num}"]["vs_median_s"] = round(
                        statistics.mean(times) - self.sector_medians[sec_num], 3
                    )
        return analysis

    def get_field_ranking(self) -> List[Dict[str, Any]]:
        """Returns drivers ranked by current EWM pace."""
        ranked = sorted(self.ewm_pace.items(), key=lambda x: x[1])
        return [
            {
                "rank": i + 1,
                "driver": driver,
                "ewm_pace_s": round(pace, 3),
                "delta_to_fastest_s": round(pace - ranked[0][1], 3) if ranked else 0,
            }
            for i, (driver, pace) in enumerate(ranked)
        ]

    def _update_field_medians(self):
        """Recalculate field-wide sector medians from all drivers."""
        import statistics
        for sec_num in [1, 2, 3]:
            all_times = []
            for driver_sectors in self.sector_times.values():
                times = list(driver_sectors[sec_num])
                if times:
                    all_times.append(times[-1])  # Use most recent
            if len(all_times) >= 3:
                self.sector_medians[sec_num] = statistics.median(all_times)

    def _make_insight(self, insight_type: str, driver: str,
                      details: Dict[str, Any], message: str) -> Dict[str, Any]:
        insight = {
            "type": insight_type,
            "agent": "LINE",
            "driver": driver,
            "details": details,
            "message": message,
            "ts": time.time(),
        }
        self.insights.append(insight)
        logger.info("[LINE] %s | %s | %s", insight_type, driver, message)
        return insight
