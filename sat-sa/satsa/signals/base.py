"""
SAT-SA — Signal Base Class (§5.2)
====================================
All signal detectors inherit from :class:`SignalDetector`.
Each signal has a YAML signal card and emits Finding objects.
"""
from __future__ import annotations

import abc
from typing import List, Optional

import polars as pl

from satsa.evidence.models import (
    BaselineType, Dimension, Finding, FindingStatus, SignalType,
)


class SignalDetector(abc.ABC):
    """
    Abstract base class for all signal detectors.

    Each concrete detector implements ``detect()`` which receives
    the data for a single CSE and returns a list of findings.
    """

    signal_id: str = ""
    signal_name: str = ""
    dimension: Dimension = Dimension.SECURITY_OPERATIONS
    signal_type: SignalType = SignalType.EXECUTION_GAP
    required_tables: list[str] = []
    required_fields: list[str] = []

    @abc.abstractmethod
    def detect(self, cse_id: str, data: dict[str, pl.DataFrame],
               baselines: Optional[dict] = None) -> List[Finding]:
        """
        Run detection for a single CSE.

        Parameters
        ----------
        cse_id : str
            The entity being assessed.
        data : dict
            Table name → Polars DataFrame.
        baselines : dict, optional
            Pre-computed baseline values.

        Returns
        -------
        list[Finding]
            Findings detected, may be empty.
        """
        ...

    def is_assessable(self, data: dict[str, pl.DataFrame]) -> bool:
        """Check if this signal has enough data to run."""
        for table in self.required_tables:
            if table not in data or data[table].is_empty():
                return False
        return True

    def _make_finding(self, cse_id: str, stratum: str, metric: str,
                      observed: float, expected: float,
                      evidence_records: list[str],
                      severity: float = 0.5, confidence: float = 0.5,
                      baseline_type: BaselineType = BaselineType.PEER,
                      p_value: float | None = None,
                      q_value: float | None = None,
                      rationale: str = "") -> Finding:
        """Helper to construct a Finding with signal metadata pre-filled."""
        import hashlib
        from datetime import datetime, timezone

        # Deterministic finding ID from content
        id_content = f"{self.signal_id}|{cse_id}|{stratum}|{metric}|{observed}|{expected}"
        finding_hash = hashlib.sha256(id_content.encode()).hexdigest()[:12].upper()

        return Finding(
            finding_id=f"FG-{finding_hash}",
            cse_id=cse_id,
            dimension=self.dimension,
            signal_type=self.signal_type,
            signal_card_id=self.signal_id,
            baseline_type=baseline_type,
            stratum=stratum,
            metric=metric,
            observed=observed,
            expected=expected,
            p_value=p_value,
            q_value=q_value,
            severity=min(1.0, max(0.0, severity)),
            confidence=min(1.0, max(0.0, confidence)),
            evidence_count=len(evidence_records),
            data_completeness=1.0,
            rationale=rationale or self._default_rationale(
                cse_id, metric, observed, expected, len(evidence_records)),
            evidence_records=evidence_records[:50],
            created_at=None,
        )

    def _default_rationale(self, cse_id: str, metric: str,
                           observed: float, expected: float,
                           evidence_count: int) -> str:
        direction = "below" if observed < expected else "above"
        return (f"{self.signal_name} at {cse_id}: {metric} is {observed:.1f} "
                f"({direction} expected {expected:.1f}); "
                f"{evidence_count} records examined.")
