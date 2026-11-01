"""CALIBRE SIA v2.2 - Phase 6 Persistence, Bitemporal Queries, Leases, and Replay Determinism Suite.

Verifies:
1. Persistence durability and idempotency across restarts.
2. Revision chaining (previous_verdict_hash pointers and forward traversal).
3. Bitemporal as-of-K queries (querying verdict state as known at historical knowledge time K).
4. Downstream consumer lease expiry (fail-closed UNKNOWN when lease elapsed).
5. Replay determinism R1 (bit-for-bit hash equality on pure evaluation).
6. Replay determinism R2 (admission log playback reproducing FINAL verdicts).
"""
from __future__ import annotations

import json
from pathlib import Path
import tempfile
import pytest
from typing import Any, Dict

from calibre_core.contracts import utc_now
from persistence.service import PersistenceService
from sia_core.contracts.parser import parse_contract_dict
from sia_core.evaluation.contract_evaluator import ContractEvaluator


@pytest.fixture
def temp_db_path(tmp_path: Path) -> Path:
    return tmp_path / "test_persisted_sia.db"


@pytest.fixture
def persistence_service(temp_db_path: Path) -> PersistenceService:
    service = PersistenceService(db_path=temp_db_path)
    return service


def test_01_idempotent_verdict_persistence(persistence_service: PersistenceService):
    """Verify verdicts are stored idempotently with no corruption on duplicate delivery."""
    verdict_1 = {
        "verdict_hash": "hash_v1_abc123",
        "decision_instance_id": "session_01|VER|dec_timing",
        "contract": {"id": "dec_timing", "version": "1.0", "epoch": 1},
        "integrity_state": "HEALTHY",
        "resolution_maturity": "STATE_RESOLVED",
        "finality_status": "OPEN",
        "revision_number": 1,
        "previous_verdict_hash": None,
        "knowledge_frontier": {"watermark": "2026-11-01T14:00:00.000Z", "run_clock": "2026-11-01T14:00:05.000Z"},
        "validity_timeline": [{"from": "2026-11-01T14:00:00.000Z", "state": "HEALTHY"}],
        "lease_until": "2026-11-01T14:01:00.000Z",
        "lease_clock_domain": "LIVE_UTC",
        "primary_binding_witness": "WITNESS_OK",
        "witness_set": ["WITNESS_OK"],
    }
    # Initial persist
    ok1 = persistence_service.persist_verdict(verdict_1)
    assert ok1 is True

    # Duplicate delivery (must be idempotent without error)
    ok2 = persistence_service.persist_verdict(verdict_1)
    assert ok2 is True

    records = persistence_service.query_verdicts_by_decision("session_01|VER|dec_timing")
    assert len(records) == 1
    assert records[0]["verdict_hash"] == "hash_v1_abc123"


def test_02_revision_chaining_traversal(persistence_service: PersistenceService):
    """Verify linked revisions H1 -> H2 -> H3 can be traversed back and forward."""
    h1 = {
        "verdict_hash": "hash_h1",
        "decision_instance_id": "session_01|driver_1|contract_1",
        "contract": {"id": "contract_1", "version": "1.0"},
        "integrity_state": "UNKNOWN",
        "revision_number": 1,
        "previous_verdict_hash": None,
        "knowledge_frontier": {"run_clock": "2026-11-01T14:00:05.000Z"},
    }
    h2 = {
        "verdict_hash": "hash_h2",
        "decision_instance_id": "session_01|driver_1|contract_1",
        "contract": {"id": "contract_1", "version": "1.0"},
        "integrity_state": "SUSPECT",
        "revision_number": 2,
        "previous_verdict_hash": "hash_h1",
        "knowledge_frontier": {"run_clock": "2026-11-01T14:00:15.000Z"},
    }
    h3 = {
        "verdict_hash": "hash_h3",
        "decision_instance_id": "session_01|driver_1|contract_1",
        "contract": {"id": "contract_1", "version": "1.0"},
        "integrity_state": "HEALTHY",
        "revision_number": 3,
        "previous_verdict_hash": "hash_h2",
        "knowledge_frontier": {"run_clock": "2026-11-01T14:00:25.000Z"},
    }
    persistence_service.persist_verdict(h1)
    persistence_service.persist_verdict(h2)
    persistence_service.persist_verdict(h3)

    # Query chain from leaf H3
    chain = persistence_service.query_revision_chain("hash_h3")
    assert len(chain) == 3
    assert [v["verdict_hash"] for v in chain] == ["hash_h1", "hash_h2", "hash_h3"]
    assert chain[0]["revision_number"] == 1
    assert chain[1]["revision_number"] == 2
    assert chain[2]["revision_number"] == 3


def test_03_bitemporal_as_of_knowledge_query(persistence_service: PersistenceService):
    """Verify bitemporal As-Of-K query returns the exact state known at historical time K."""
    dec_id = "session_01|driver_44|lap_time"
    v_10s = {
        "verdict_hash": "hash_at_10s",
        "decision_instance_id": dec_id,
        "contract": {"id": "lap_time"},
        "integrity_state": "UNKNOWN",
        "revision_number": 1,
        "knowledge_frontier": {"run_clock": "2026-11-01T14:00:10.000Z"},
    }
    v_20s = {
        "verdict_hash": "hash_at_20s",
        "decision_instance_id": dec_id,
        "contract": {"id": "lap_time"},
        "integrity_state": "HEALTHY",
        "revision_number": 2,
        "previous_verdict_hash": "hash_at_10s",
        "knowledge_frontier": {"run_clock": "2026-11-01T14:00:20.000Z"},
    }
    persistence_service.persist_verdict(v_10s)
    persistence_service.persist_verdict(v_20s)

    # Query as of 14:00:15 (before v_20s was known)
    as_of_15s = persistence_service.query_verdict_as_of_knowledge_time(dec_id, "2026-11-01T14:00:15.000Z")
    assert as_of_15s is not None
    assert as_of_15s["verdict_hash"] == "hash_at_10s"
    assert as_of_15s["integrity_state"] == "UNKNOWN"

    # Query as of 14:00:25 (after v_20s was known)
    as_of_25s = persistence_service.query_verdict_as_of_knowledge_time(dec_id, "2026-11-01T14:00:25.000Z")
    assert as_of_25s is not None
    assert as_of_25s["verdict_hash"] == "hash_at_20s"
    assert as_of_25s["integrity_state"] == "HEALTHY"


def test_04_consumer_lease_validity_check(persistence_service: PersistenceService):
    """Verify consumer lease validation accurately detects active vs expired leases."""
    verdict = {
        "verdict_hash": "hash_lease_test",
        "integrity_state": "HEALTHY",
        "lease_until": "2026-11-01T14:05:00.000Z",
        "lease_clock_domain": "LIVE_UTC",
    }
    # Before lease expires: Valid
    assert PersistenceService.is_verdict_lease_valid(verdict, "2026-11-01T14:04:59.000Z") is True
    # At exact boundary: Valid
    assert PersistenceService.is_verdict_lease_valid(verdict, "2026-11-01T14:05:00.000Z") is True
    # After lease expires: Stale / Expired
    assert PersistenceService.is_verdict_lease_valid(verdict, "2026-11-01T14:05:01.000Z") is False


def test_05_replay_determinism_r1_hash_equality():
    """Verify Replay Determinism R1: identical canonical inputs produce identical verdict hash."""
    with open("tests/fixtures/FX-14_late_revision.json", "r", encoding="utf-8") as f:
        fx = json.load(f)
    contract = parse_contract_dict(fx["target_contract"])
    input_stream = fx["input_stream"]
    sched0 = fx["evaluation_schedule"][0]

    # Run evaluation 10 times in isolation
    hashes = set()
    for _ in range(10):
        v = ContractEvaluator.evaluate(
            contract=contract,
            decision_instance_id=sched0["decision_instance_id"],
            input_stream=[input_stream[0]],
            t_eval=sched0["t_eval"],
        )
        hashes.add(v["verdict_hash"])

    # Must be strictly 1 unique hash across all 10 independent executions
    assert len(hashes) == 1
    assert list(hashes)[0] == fx["expected_verdicts"][0]["verdict_hash"]


def test_06_replay_determinism_r2_admission_log():
    """Verify Replay Determinism R2: replaying logged admission stream reproduces FINAL verdict."""
    with open("tests/fixtures/FX-15_post_finality_violation.json", "r", encoding="utf-8") as f:
        fx = json.load(f)
    contract = parse_contract_dict(fx["target_contract"])
    input_stream = fx["input_stream"]
    sched0 = fx["evaluation_schedule"][0]
    expected_final_hash = fx["expected_verdicts"][0]["verdict_hash"]

    # Replay admission log stream
    replayed_v = ContractEvaluator.evaluate(
        contract=contract,
        decision_instance_id=sched0["decision_instance_id"],
        input_stream=input_stream,
        t_eval=sched0["t_eval"],
    )
    assert replayed_v["verdict_hash"] == expected_final_hash
    assert replayed_v["finality_status"] == "FINAL"
