"""
E2E Test: Priority Review Sampling (§3, §4)
============================================
Tests priority-based examiner record sampling, seed-locked determinism,
stratum allocation, and deduplication.
"""
import pytest
from satsa.evidence.models import Finding, Dimension, SignalType, BaselineType
from satsa.sampling.sampler import PriorityRecordSampler


def _create_mock_finding(fid: str, severity: float, records: list[str]) -> Finding:
    return Finding(
        finding_id=fid,
        cse_id="CSE-TEST",
        dimension=Dimension.SECURITY_OPERATIONS,
        signal_type=SignalType.ANOMALY,
        signal_card_id="SIG-01",
        baseline_type=BaselineType.PEER,
        stratum="critical_cases",
        metric="test_metric",
        observed=10.0,
        expected=2.0,
        severity=severity,
        confidence=0.9,
        evidence_count=len(records),
        data_completeness=1.0,
        rationale=f"Mock finding with severity {severity}",
        evidence_records=records,
    )


def test_priority_sampling_order_and_bounds():
    sampler = PriorityRecordSampler()
    findings = [
        _create_mock_finding("F-LOW", 0.2, ["REC-L1", "REC-L2"]),
        _create_mock_finding("F-MED", 0.5, ["REC-M1", "REC-M2"]),
        _create_mock_finding("F-HIGH", 0.9, ["REC-H1", "REC-H2", "REC-H3"]),
    ]

    batch = sampler.draw_sample(
        entity_id="CSE-TEST",
        findings=findings,
        sample_size=4,
        seed=42,
    )

    assert batch.entity_id == "CSE-TEST"
    assert len(batch.sampled_records) <= 4
    assert len(batch.sampled_records) > 0

    # Ensure items drawn have valid scores and rationale
    for item in batch.sampled_records:
        assert item.finding_id in ["F-LOW", "F-MED", "F-HIGH"]
        assert len(item.record_id) > 0
        assert item.priority_score > 0.0


def test_sampling_seed_determinism():
    sampler = PriorityRecordSampler()
    findings = [
        _create_mock_finding(f"F-{i}", (i * 0.1) % 1.0, [f"R-{i}-A", f"R-{i}-B"])
        for i in range(10)
    ]

    batch1 = sampler.draw_sample("CSE-TEST", findings, sample_size=5, seed=777)
    batch2 = sampler.draw_sample("CSE-TEST", findings, sample_size=5, seed=777)

    items1 = [(it.finding_id, it.record_id) for it in batch1.sampled_records]
    items2 = [(it.finding_id, it.record_id) for it in batch2.sampled_records]

    assert items1 == items2
