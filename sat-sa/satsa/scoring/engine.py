"""
SAT-SA — Scoring Engine (§5.4)
==============================
Aggregates signal findings into entity-level scores using Noisy-OR probability,
calculates the Supervisory Action Index (SAI 0–100), and assigns Priority Tiers (T1–T4).
"""
from __future__ import annotations

from typing import Dict, List, Any, Optional
import math
from pydantic import BaseModel, Field

from satsa.evidence.models import Finding


class EntityScoreResult(BaseModel):
    """Combined score summary for an assessed entity."""
    entity_id: str
    entity_name: str
    sector: str
    sai_score: float = Field(..., description="Supervisory Action Index (0-100)")
    priority_tier: str = Field(..., description="T1 (Critical), T2 (High), T3 (Medium), T4 (Low)")
    noisy_or_p_failure: float = Field(..., description="Noisy-OR failure probability [0, 1]")
    total_findings: int
    critical_findings_count: int
    high_findings_count: int
    coverage_penalty: float = Field(default=0.0, description="Penalty for missing mandatory Tier 1 schemas")
    top_finding_titles: List[str] = Field(default_factory=list)
    findings: List[Finding] = Field(default_factory=list)


class ScoringEngine:
    """
    Scoring Engine executing Noisy-OR finding aggregation and SAI calculation.
    """

    def __init__(self, weights: Optional[Dict[str, float]] = None):
        self.weights = weights or {
            "noisy_or": 0.70,       # Evidence-backed signal findings
            "severity_peak": 0.15,  # Maximum single signal severity
            "coverage_gap": 0.15,   # Missing schemas / unsubmitted required data
        }

    def aggregate_noisy_or(self, findings: List[Finding]) -> float:
        """
        Noisy-OR combination of signal probabilities:
        P(Failure) = 1 - PROD_i (1 - (confidence_i * severity_i))
        """
        if not findings:
            return 0.0

        prod_prob_not_fail = 1.0
        for f in findings:
            sev_w = max(0.0, min(1.0, f.severity))
            c = max(0.0, min(1.0, f.confidence))
            p_component = c * sev_w
            prod_prob_not_fail *= (1.0 - p_component)

        return float(1.0 - prod_prob_not_fail)

    def calculate_sai(
        self,
        entity_id: str,
        entity_name: str,
        sector: str,
        findings: List[Finding],
        missing_mandatory_tables: List[str] = None
    ) -> EntityScoreResult:
        """
        Calculate Supervisory Action Index (SAI) on scale 0-100.
        """
        missing_tables = missing_mandatory_tables or []
        
        # 1. Noisy-OR Failure Probability
        p_fail = self.aggregate_noisy_or(findings)
        
        # 2. Maximum Severity Peak
        sev_values = [f.severity * f.confidence for f in findings]
        max_sev = max(sev_values) if sev_values else 0.0

        # 3. Coverage Penalty (15 pts per missing mandatory schema table, cap at 1.0)
        coverage_penalty = min(1.0, len(missing_tables) * 0.15)

        # Composite raw score [0, 1]
        w = self.weights
        raw_score = (
            w["noisy_or"] * p_fail +
            w["severity_peak"] * max_sev +
            w["coverage_gap"] * coverage_penalty
        )

        # SAI on 0-100 scale
        sai = min(100.0, max(0.0, raw_score * 100.0))

        # Count findings by severity
        crit_count = sum(1 for f in findings if f.severity >= 0.8)
        high_count = sum(1 for f in findings if 0.5 <= f.severity < 0.8)

        if sai >= 70.0 or crit_count > 0:
            tier = "Tier 1 (Critical Triage)"
        elif sai >= 45.0:
            tier = "Tier 2 (High Priority)"
        elif sai >= 20.0:
            tier = "Tier 3 (Medium Priority)"
        else:
            tier = "Tier 4 (Low Risk / Clean Claim)"

        top_titles = [f.signal_card_id + ": " + f.rationale[:60] for f in sorted(findings, key=lambda x: (x.severity, x.confidence), reverse=True)[:5]]

        return EntityScoreResult(
            entity_id=entity_id,
            entity_name=entity_name,
            sector=sector,
            sai_score=round(sai, 2),
            priority_tier=tier,
            noisy_or_p_failure=round(p_fail, 4),
            total_findings=len(findings),
            critical_findings_count=crit_count,
            high_findings_count=high_count,
            coverage_penalty=round(coverage_penalty, 2),
            top_finding_titles=top_titles,
            findings=findings
        )
