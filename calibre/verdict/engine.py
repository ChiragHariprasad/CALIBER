"""
CALIBRE – Verdict Engine
=========================
Combines rule evaluation, uncertainty quantification, and sensor
integrity information to produce a final BREACH / CLEAR / DEFER verdict.

Key decision logic:
  1. Run the uncertainty engine on the rule result.
  2. If the uncertainty engine says BREACH but a recent integrity alert
     exists for this sector, downgrade to DEFER.
  3. The system never auto-executes penalties – it is decision-support.
"""

from __future__ import annotations

from datetime import datetime, timezone

from integrity.detector import CUSUMDetector
from rules.base import RuleResult
from schemas.telemetry import TelemetryEvent
from schemas.verdict import Verdict, VerdictValue
from uncertainty.engine import UncertaintyEngine


class VerdictEngine:
    """
    Final decision layer.

    Combines:
      - :class:`UncertaintyEngine`  → statistical classification
      - :class:`CUSUMDetector`      → sensor trustworthiness
    """

    def __init__(
        self,
        uncertainty_engine: UncertaintyEngine,
        integrity_detector: CUSUMDetector,
    ) -> None:
        self.uncertainty = uncertainty_engine
        self.integrity = integrity_detector

    def decide(
        self,
        rule_result: RuleResult,
        event: TelemetryEvent,
    ) -> Verdict:
        """
        Produce a verdict for a single rule evaluation.

        Parameters
        ----------
        rule_result : RuleResult
            Output from a :class:`RuleMonitor`.
        event : TelemetryEvent
            The telemetry event that triggered the evaluation.
        """
        # 1. Uncertainty classification
        unc = self.uncertainty.evaluate(
            measured_value=rule_result.measured_value,
            threshold=rule_result.threshold,
        )

        verdict_value = unc.verdict

        # 2. Integrity override: if a BREACH would be issued but the
        #    sensor sector is flagged, downgrade to DEFER.
        sector = event.zone or "unknown"
        if verdict_value == VerdictValue.BREACH:
            if self.integrity.has_recent_alert(sector):
                verdict_value = VerdictValue.DEFER

        # 3. Assemble the verdict record
        return Verdict(
            timestamp=event.timestamp,
            session_key=event.session_key,
            driver_number=event.driver_number,
            rule_id=rule_result.rule_id,
            measured_value=rule_result.measured_value,
            threshold=rule_result.threshold,
            uncertainty=unc.combined_uncertainty,
            confidence_margin=unc.normalised_margin,
            verdict=verdict_value,
            source_event_ids=[],
            parameter_version="v1.0",
            valid_from=event.timestamp,
            recorded_at=datetime.now(timezone.utc),
        )
