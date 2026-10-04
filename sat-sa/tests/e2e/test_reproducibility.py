"""
E2E Test: Pipeline Determinism & Reproducibility (§3 Step 4)
=============================================================
Proves that repeating the assessment on the same input data
with the same seed produces identical scores, finding IDs,
findings count, and Merkle tree roots.
"""
import pytest
from pathlib import Path

from generator.generate import SyntheticDataGenerator
from satsa.api.runner import AssessmentRunner
from satsa.ledger.trust import AssessmentLedger


def test_assessment_reproducibility(tmp_path):
    data_dir = tmp_path / "repro_data"
    gen = SyntheticDataGenerator(n_cses=2, months=1, seed=999, output_dir=data_dir)
    gen.generate()

    cse_dir = data_dir / "CSE-001"

    # Run 1
    ledger1 = AssessmentLedger(storage_path=tmp_path / "ledger1.db")
    runner1 = AssessmentRunner(ledger=ledger1)
    res1 = runner1.run_assessment(
        entity_id="CSE-001",
        entity_name="Reproducibility Bank",
        sector="Banking",
        submission_dir=cse_dir,
        seed=42,
    )

    # Run 2 (Independent run on same input)
    ledger2 = AssessmentLedger(storage_path=tmp_path / "ledger2.db")
    runner2 = AssessmentRunner(ledger=ledger2)
    res2 = runner2.run_assessment(
        entity_id="CSE-001",
        entity_name="Reproducibility Bank",
        sector="Banking",
        submission_dir=cse_dir,
        seed=42,
    )

    # Assert exact score identity
    assert res1["score"].sai_score == res2["score"].sai_score
    assert res1["score"].priority_tier == res2["score"].priority_tier
    assert res1["score"].noisy_or_p_failure == res2["score"].noisy_or_p_failure

    # Assert exact finding identity
    findings1 = res1["findings"]
    findings2 = res2["findings"]
    assert len(findings1) == len(findings2)

    ids1 = [f.finding_id for f in findings1]
    ids2 = [f.finding_id for f in findings2]
    assert ids1 == ids2

    # Assert exact Merkle root identity
    assert res1["merkle_root"] == res2["merkle_root"]

    ledger1.close()
    ledger2.close()
