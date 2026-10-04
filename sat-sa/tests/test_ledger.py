"""
Tests for Merkle Tree and TRUST Ledger (§7)
"""
from __future__ import annotations

from pathlib import Path
import tempfile
import pytest

from satsa.ledger.merkle import MerkleTree, hash_leaf
from satsa.ledger.trust import AssessmentLedger


def test_merkle_tree_root_determinism():
    items = ['{"finding": 1}', '{"finding": 2}', '{"finding": 3}']
    t1 = MerkleTree.build(items)
    t2 = MerkleTree.build(items)
    assert t1.root == t2.root
    assert len(t1.root) == 64  # SHA-256 hex length


def test_merkle_tree_empty():
    t = MerkleTree.build([])
    assert t.root != ""


def test_merkle_inclusion_proof():
    items = ["finding_a", "finding_b", "finding_c", "finding_d"]
    tree = MerkleTree.build(items)

    proof = tree.inclusion_proof(1)  # "finding_b"
    b_hash = hash_leaf("finding_b")
    assert tree.verify_proof(b_hash, proof, tree.root)
    assert not tree.verify_proof(hash_leaf("tampered"), proof, tree.root)


def test_trust_ledger_creation_and_tampering():
    with tempfile.TemporaryDirectory() as tmpdir:
        ledger_file = Path(tmpdir) / "trust_ledger.db"
        ledger = AssessmentLedger(storage_path=ledger_file)

        b1 = ledger.record_submission(
            entity_id="CSE-1",
            entity_name="Bank A",
            sector="Banking",
            submission_path="/tmp/sub",
            file_hashes={"test.parquet": "abc123hash"}
        )

        b2 = ledger.record_assessment_run(
            submission_block_hash=b1.header.block_hash,
            findings=[],
            sai_score=85.0,
            priority_tier="Tier 1"
        )

        report = ledger.verify_chain()
        assert report["chain_valid"] is True
        assert report["signatures_valid"] is True

        # Simulate tampering directly in SQLite store
        ledger.conn.execute("UPDATE blocks SET payload = ? WHERE block_id = 1", ('{"tampered": true}',))
        ledger.conn.commit()

        report_tampered = ledger.verify_chain()
        assert report_tampered["chain_valid"] is False

        ledger.close()
