"""
CALIBRE – Sensor Integrity Detector (CUSUM)
=============================================
Detects common-cause sensor faults by tracking the residual between an
independent timing-system speed reading and the on-car telemetry speed.

    residual = timing_system_speed − on_car_speed

A one-sided upper CUSUM is maintained **per sector** to flag sustained
positive bias (the sensor reads faster than reality), which is the
safety-critical direction for pit-lane speed enforcement.

Extension point (future):
    residual = sector_effect + car_effect + noise

    The full mixed-effects decomposition will replace the simple
    residual once the timing-system twin module is available.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

from config.settings import settings

logger = logging.getLogger(__name__)


@dataclass
class IntegrityAlert:
    """Emitted when CUSUM exceeds the configured threshold."""

    sector: str
    residual: float
    cusum: float
    threshold: float
    severity: str  # "warning" | "critical"
    timestamp: datetime = field(default_factory=datetime.utcnow)


class CUSUMDetector:
    """
    Per-sector CUSUM change-point detector.

    Parameters
    ----------
    threshold : float
        CUSUM decision boundary (h).
    drift : float
        Allowable drift / slack (k in the CUSUM literature).
    """

    def __init__(
        self,
        threshold: float | None = None,
        drift: float | None = None,
    ) -> None:
        self.threshold = threshold or settings.cusum_threshold
        self.drift = drift or settings.cusum_drift
        # State: per-sector running CUSUM and history
        self._cusum: dict[str, float] = defaultdict(float)
        self._history: dict[str, list[float]] = defaultdict(list)
        self._alerts: list[IntegrityAlert] = []

    def update(
        self,
        sector: str,
        timing_speed: float,
        oncar_speed: float,
        timestamp: Optional[datetime] = None,
    ) -> Optional[IntegrityAlert]:
        """
        Feed a new observation.  Returns an ``IntegrityAlert`` if the
        CUSUM statistic exceeds the threshold, else ``None``.
        """
        residual = timing_speed - oncar_speed
        self._history[sector].append(residual)

        # One-sided upper CUSUM: detects positive mean shift
        self._cusum[sector] = max(
            0.0,
            self._cusum[sector] + residual - self.drift,
        )

        if self._cusum[sector] >= self.threshold:
            severity = "critical" if self._cusum[sector] >= 2 * self.threshold else "warning"
            alert = IntegrityAlert(
                sector=sector,
                residual=residual,
                cusum=self._cusum[sector],
                threshold=self.threshold,
                severity=severity,
                timestamp=timestamp or datetime.now(timezone.utc),
            )
            self._alerts.append(alert)
            logger.warning(
                "IntegrityAlert sector=%s cusum=%.2f severity=%s",
                sector, self._cusum[sector], severity,
            )
            # Reset CUSUM after alert (Page's restart rule)
            self._cusum[sector] = 0.0
            return alert

        return None

    def has_recent_alert(self, sector: str, within_n: int = 5) -> bool:
        """Check if the given sector had an alert in the last *n* updates."""
        recent = [a for a in self._alerts[-within_n:] if a.sector == sector]
        return len(recent) > 0

    def get_alerts(self) -> list[IntegrityAlert]:
        """Return all alerts emitted so far."""
        return list(self._alerts)

    def get_cusum(self, sector: str) -> float:
        """Current CUSUM value for a sector."""
        return self._cusum[sector]
