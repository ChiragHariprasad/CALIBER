"""
SAT-SA — Examiner Record Sampling Engine (§6.2)
==============================================
Selects high-priority evidence records for examiner inspection.
Guarantees hold-out seed lock reproducibility for auditing.
"""
from __future__ import annotations

import random
from typing import Dict, List, Any, Optional
from pydantic import BaseModel, Field

from satsa.evidence.models import Finding


class SampledRecord(BaseModel):
    """A record selected for targeted examiner review with justifying evidence context."""
    finding_id: str
    signal_code: str
    finding_title: str
    severity: float
    record_id: str
    sampling_rationale: str
    priority_score: float


class ExaminerSampleBatch(BaseModel):
    """Container for a reproducible batch of sampled records for an entity."""
    entity_id: str
    seed: int
    sample_size: int
    sampled_records: List[SampledRecord]


class PriorityRecordSampler:
    """
    Priority Record Sampler selecting the highest-yield evidence records for examiners.
    """

    def __init__(self, default_seed: int = 42):
        self.default_seed = default_seed

    def draw_sample(
        self,
        entity_id: str,
        findings: List[Finding],
        sample_size: int = 15,
        seed: Optional[int] = None
    ) -> ExaminerSampleBatch:
        """
        Draw a deterministic, prioritized sample of records from entity findings.
        """
        chosen_seed = seed if seed is not None else self.default_seed
        rng = random.Random(chosen_seed)

        candidates: List[SampledRecord] = []

        for f in findings:
            w = f.severity * f.confidence
            for rec_id in f.evidence_records:
                rec_score = w + rng.uniform(0.001, 0.05)
                candidates.append(SampledRecord(
                    finding_id=f.finding_id,
                    signal_code=f.signal_card_id,
                    finding_title=f.metric,
                    severity=f.severity,
                    record_id=str(rec_id),
                    sampling_rationale=f"Triggered by {f.signal_card_id} (sev={f.severity:.2f}): {f.rationale}",
                    priority_score=round(rec_score, 4)
                ))

        # Sort candidates by priority score descending
        candidates.sort(key=lambda x: x.priority_score, reverse=True)

        # Deduplicate by record_id
        seen_keys = set()
        deduped: List[SampledRecord] = []
        for cand in candidates:
            if cand.record_id not in seen_keys:
                seen_keys.add(cand.record_id)
                deduped.append(cand)

        # Pick top sample_size
        selected = deduped[:sample_size]

        return ExaminerSampleBatch(
            entity_id=entity_id,
            seed=chosen_seed,
            sample_size=len(selected),
            sampled_records=selected
        )
