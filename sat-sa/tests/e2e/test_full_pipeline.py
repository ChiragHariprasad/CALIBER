"""
E2E Test: Full SAT-SA Pipeline Execution (§3, §4)
==================================================
Tests end-to-end ingestion, signal detection, scoring, sampling,
Merkle tree generation, and TRUST ledger recording.
"""
import pytest
from pathlib import Path
import json

from generator.generate import SyntheticDataGenerator
from satsa.api.runner import AssessmentRunner
from satsa.ledger.trust import AssessmentLedger


def test_full_pipeline_single_and_batch(tmp_path):
    # 1. Generate 3 CSEs in temporary directory
    data_dir = tmp_path / "pipeline_test"
    gen = SyntheticDataGenerator(n_cses=3, months=1, seed=123, output_dir=data_dir)
    gen.generate()

    assert (data_dir / "profiles.json").exists()
    assert (data_dir / "ground_truth.json").exists()

    # 2. Run assessment runner on CSE-001
    cse1_dir = data_dir / "CSE-001"
    assert cse1_dir.exists()
    assert (cse1_dir / "manifest.json").exists()

    ledger_path = data_dir / "trust_ledger.db"
    ledger = AssessmentLedger(storage_path=ledger_path)
    runner = AssessmentRunner(ledger=ledger)

    result = runner.run_assessment(
        entity_id="CSE-001",
        entity_name="Test Bank",
        sector="Banking",
        submission_dir=cse1_dir,
        sample_size=10,
        seed=123,
    )

    # 3. Assert scores and bounds
    score = result["score"]
    assert 0.0 <= score.sai_score <= 100.0
    assert 0.0 <= score.noisy_or_p_failure <= 1.0
    assert score.priority_tier in [
        "Tier 1 (Critical Triage)",
        "Tier 2 (High Priority)",
        "Tier 3 (Medium Priority)",
        "Tier 4 (Low Risk / Clean Claim)"
    ]

    # 4. Assert findings and Merkle root
    findings = result["findings"]
    assert isinstance(findings, list)
    for f in findings:
        assert 0.0 <= f.severity <= 1.0
        assert 0.0 <= f.confidence <= 1.0
        assert len(f.rationale) > 0
        assert f.signal_card_id is not None

    merkle_root = result["merkle_root"]
    assert isinstance(merkle_root, str)
    assert len(merkle_root) == 64  # SHA256 hex string

    # 5. Assert TRUST ledger validation
    report = ledger.verify_chain()
    assert report["chain_valid"] is True
    assert report["signatures_valid"] is True
    assert len(report["tampered_blocks"]) == 0
    assert report["total_blocks"] >= 2  # submission block + run block

    ledger.close()
