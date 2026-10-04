"""
E2E Test: Ledger Tamper Detection (§3 Step 5)
==============================================
Proves that deliberately tampering with any persisted ledger block
causes cryptographic verification to fail immediately.
"""
import sqlite3
import pytest
from pathlib import Path

from satsa.ledger.trust import AssessmentLedger


def test_ledger_tamper_detection(tmp_path):
    ledger_path = tmp_path / "tamper_test.db"
    ledger = AssessmentLedger(storage_path=ledger_path)

    # Append test submission block
    b1 = ledger.record_submission(
        entity_id="CSE-TEST",
        entity_name="Tamper CSE",
        sector="Telecom",
        submission_path="/tmp/test",
        file_hashes={"alerts.parquet": "abcdef123456"}
    )

    # Append test run block
    b2 = ledger.record_assessment_run(
        submission_block_hash=b1.header.block_hash,
        findings=[{"finding_id": "FIND-001", "severity": 0.8}],
        sai_score=78.5,
        priority_tier="Tier 1 (Critical Triage)"
    )

    # 1. Verify before tamper
    clean_report = ledger.verify_chain()
    assert clean_report["chain_valid"] is True
    assert clean_report["signatures_valid"] is True
    assert len(clean_report["tampered_blocks"]) == 0
    ledger.close()

    # 2. Deliberately tamper with block 2 payload in SQLite directly
    conn = sqlite3.connect(ledger_path)
    cur = conn.cursor()
    cur.execute("SELECT payload FROM blocks WHERE block_id = 2")
    original_payload = cur.fetchone()[0]

    # Tamper: change sai_score from 78.5 to 10.0 (fraudulent score reduction)
    tampered_payload = original_payload.replace("78.5", "10.0")
    cur.execute("UPDATE blocks SET payload = ? WHERE block_id = 2", (tampered_payload,))
    conn.commit()
    conn.close()

    # 3. Re-verify ledger with tampered block
    tampered_ledger = AssessmentLedger(storage_path=ledger_path)
    tampered_report = tampered_ledger.verify_chain()

    # Must detect tamper!
    assert tampered_report["chain_valid"] is False
    assert 2 in tampered_report["tampered_blocks"]
    assert any("payload tampered" in err or "signature invalid" in err for err in tampered_report["errors"])

    # 4. Restore original payload and re-verify passes
    conn = sqlite3.connect(ledger_path)
    cur = conn.cursor()
    cur.execute("UPDATE blocks SET payload = ? WHERE block_id = 2", (original_payload,))
    conn.commit()
    conn.close()

    restored_report = tampered_ledger.verify_chain()
    assert restored_report["chain_valid"] is True
    assert restored_report["signatures_valid"] is True
    assert len(restored_report["tampered_blocks"]) == 0
    tampered_ledger.close()
