"""
Tests for Core Signals, Scoring Engine, and Priority Sampler (§5, §6)
"""
from __future__ import annotations

import pytest
from satsa.scoring.engine import ScoringEngine
from satsa.sampling.sampler import PriorityRecordSampler
from satsa.evidence.models import Finding, Dimension, SignalType, BaselineType


def test_noisy_or_aggregation():
    engine = ScoringEngine()

    # Case 1: No findings -> P(Failure) = 0
    assert engine.aggregate_noisy_or([]) == 0.0

    # Case 2: One Critical finding (c=1.0, sev=1.0) -> P(Failure) = 1.0
    f1 = Finding(
        finding_id="F1",
        cse_id="CSE-1",
        dimension=Dimension.SECURITY_OPERATIONS,
        signal_type=SignalType.EXECUTION_GAP,
        signal_card_id="S01",
        baseline_type=BaselineType.PEER,
        stratum="test",
        metric="discrepancy",
        observed=1.0,
        expected=0.0,
        severity=1.0,
        confidence=1.0,
        evidence_count=1,
        data_completeness=1.0,
        rationale="Discrepancy detected"
    )
    assert engine.aggregate_noisy_or([f1]) == 1.0

    # Case 3: Medium severity finding
    f2 = Finding(
        finding_id="F2",
        cse_id="CSE-1",
        dimension=Dimension.INVESTIGATION,
        signal_type=SignalType.EXECUTION_GAP,
        signal_card_id="S02",
        baseline_type=BaselineType.PEER,
        stratum="test",
        metric="downtime",
        observed=0.5,
        expected=0.0,
        severity=0.5,
        confidence=0.5,
        evidence_count=1,
        data_completeness=1.0,
        rationale="Downtime mismatch"
    )
    p = engine.aggregate_noisy_or([f2])
    assert 0.0 < p < 1.0


def test_sai_score_calculation():
    engine = ScoringEngine()
    f1 = Finding(
        finding_id="F1",
        cse_id="CSE-1",
        dimension=Dimension.ESCALATION,
        signal_type=SignalType.EXECUTION_GAP,
        signal_card_id="ES-01",
        baseline_type=BaselineType.PEER,
        stratum="test",
        metric="missing_escalations",
        observed=0.8,
        expected=0.0,
        severity=0.8,
        confidence=0.9,
        evidence_count=5,
        data_completeness=1.0,
        rationale="Missing escalations"
    )
    result = engine.calculate_sai(
        entity_id="CSE-1",
        entity_name="Bank A",
        sector="Banking",
        findings=[f1],
        missing_mandatory_tables=["soc_incidents"]
    )

    assert result.sai_score > 0
    assert "Tier" in result.priority_tier
    assert result.coverage_penalty == 0.15


def test_examiner_sampler_seed_determinism():
    sampler = PriorityRecordSampler()
    f = Finding(
        finding_id="F1",
        cse_id="CSE-1",
        dimension=Dimension.SECURITY_OPERATIONS,
        signal_type=SignalType.EXECUTION_GAP,
        signal_card_id="S01",
        baseline_type=BaselineType.PEER,
        stratum="test",
        metric="test_metric",
        observed=1.0,
        expected=0.0,
        severity=0.8,
        confidence=0.9,
        evidence_count=20,
        data_completeness=1.0,
        rationale="Test rationale",
        evidence_records=[f"LOG-{i}" for i in range(20)]
    )

    sample1 = sampler.draw_sample("CSE-1", [f], sample_size=5, seed=12345)
    sample2 = sampler.draw_sample("CSE-1", [f], sample_size=5, seed=12345)
    sample_diff_seed = sampler.draw_sample("CSE-1", [f], sample_size=5, seed=99999)

    # Identical seed produces identical sample ordering
    assert [r.record_id for r in sample1.sampled_records] == [r.record_id for r in sample2.sampled_records]
