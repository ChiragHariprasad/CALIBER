"""
CALIBRE – Rule Monitor Base Class
===================================
All FIA rule monitors inherit from :class:`RuleMonitor`.

To add a new rule:
  1. Create a file in ``rules/`` (e.g. ``track_limits.py``).
  2. Subclass :class:`RuleMonitor`.
  3. Implement :meth:`evaluate`.
  4. Register the new monitor in ``StreamProcessor.__init__``.

Future monitors planned:
  - Track limits
  - Overtake Mode (DRS / energy deployment)
  - Safety-Car / VSC compliance
  - Reprimands & penalty-points accumulation
"""

from __future__ import annotations

import abc
from typing import Optional

from pydantic import BaseModel

from schemas.telemetry import TelemetryEvent


class RuleResult(BaseModel):
    """Structured output of a single rule evaluation."""

    rule_id: str
    measured_value: float
    threshold: float
    exceeded: bool
    metadata: dict = {}


class RuleMonitor(abc.ABC):
    """Abstract interface for all FIA rule monitors."""

    rule_id: str

    @abc.abstractmethod
    def evaluate(self, event: TelemetryEvent) -> Optional[RuleResult]:
        """
        Evaluate *event* against this rule.

        Returns
        -------
        RuleResult if the rule is applicable, else ``None``.
        """
        ...
