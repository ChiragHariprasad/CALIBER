"""
CALIBRE – Uncertainty Engine
==============================
Combines measurement and parameter uncertainties, then classifies the
normalised margin into BREACH / CLEAR / DEFER.

The model is intentionally simple for the initial scaffold:

    combined_uncertainty = √(σ_measurement² + σ_parameter²)
    margin = threshold − measured_value
    normalised_margin = margin / combined_uncertainty

    if normalised_margin ≤ −k  →  BREACH
    if normalised_margin ≥  k  →  CLEAR
    else                       →  DEFER
"""

from __future__ import annotations

import math
from dataclasses import dataclass

from config.settings import settings
from schemas.verdict import VerdictValue


@dataclass
class UncertaintyResult:
    """Output of the uncertainty calculation."""

    combined_uncertainty: float
    margin: float
    normalised_margin: float
    verdict: VerdictValue


class UncertaintyEngine:
    """
    Stateless uncertainty calculator.

    Parameters are sourced from :mod:`config.settings` but can be
    overridden per-call for testing or scenario analysis.
    """

    def __init__(
        self,
        measurement_uncertainty: float | None = None,
        parameter_uncertainty: float | None = None,
        k: float | None = None,
    ) -> None:
        self.measurement_uncertainty = (
            measurement_uncertainty
            if measurement_uncertainty is not None
            else settings.measurement_uncertainty
        )
        self.parameter_uncertainty = (
            parameter_uncertainty
            if parameter_uncertainty is not None
            else settings.parameter_uncertainty
        )
        self.k = k if k is not None else settings.uncertainty_k

    def evaluate(
        self,
        measured_value: float,
        threshold: float,
        *,
        measurement_uncertainty: float | None = None,
        parameter_uncertainty: float | None = None,
    ) -> UncertaintyResult:
        """
        Compute combined uncertainty and classify the result.

        Parameters
        ----------
        measured_value : float
            The value actually measured (e.g. speed in km/h).
        threshold : float
            The regulatory limit.
        measurement_uncertainty : float, optional
            Override default σ_measurement for this call.
        parameter_uncertainty : float, optional
            Override default σ_parameter for this call.
        """
        sigma_m = measurement_uncertainty or self.measurement_uncertainty
        sigma_p = parameter_uncertainty or self.parameter_uncertainty

        combined = math.sqrt(sigma_m**2 + sigma_p**2)

        margin = threshold - measured_value
        normalised = margin / combined if combined > 0 else (
            float("inf") if margin > 0 else float("-inf")
        )

        if normalised <= -self.k:
            verdict = VerdictValue.BREACH
        elif normalised >= self.k:
            verdict = VerdictValue.CLEAR
        else:
            verdict = VerdictValue.DEFER

        return UncertaintyResult(
            combined_uncertainty=combined,
            margin=margin,
            normalised_margin=normalised,
            verdict=verdict,
        )
