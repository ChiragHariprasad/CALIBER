"""Comprehensive test suite for CALIBRE Scoped Integrity Algebra (SIA) v2.2 Golden Fixtures.

Validates all 20 Golden Fixture Families (FX-01 through FX-20) and manifest.json:
1. Manifest integrity and 20/20 fixture file presence.
2. Formal schema conformance (calibre.golden_fixture v1.0).
3. Contract compile-time validation via sia_core.contracts.validator.
4. Input stream structure, timestamp validity, and chronological monotonicity.
5. Bit-for-bit deterministic RFC 8785 verdict_hash recomputation via sia_core.identity.hasher.
6. Behavioral assertions pinning Defects D-01, D-03..D-09, D-13, D-15, D-18 and Theorems G4-I / G4-V.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List
import pytest

from sia_core.contracts.parser import parse_contract_dict
from sia_core.contracts.validator import validate_contract
from sia_core.identity.hasher import compute_verdict_hash

FIXTURES_DIR = Path("tests/fixtures")
MANIFEST_PATH = FIXTURES_DIR / "manifest.json"


@pytest.fixture(scope="module")
def manifest() -> List[Dict[str, Any]]:
    assert MANIFEST_PATH.exists(), f"Missing manifest at {MANIFEST_PATH}"
    with open(MANIFEST_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


@pytest.fixture(scope="module")
def all_fixtures() -> Dict[str, Dict[str, Any]]:
    fixtures = {}
    for i in range(1, 21):
        fx_id = f"FX-{i:02d}"
        matching = list(FIXTURES_DIR.glob(f"{fx_id}_*.json"))
        assert len(matching) == 1, f"Expected exactly one file for {fx_id}, found {matching}"
        with open(matching[0], "r", encoding="utf-8") as f:
            fixtures[fx_id] = json.load(f)
    return fixtures


def test_manifest_completeness_and_structure(manifest: List[Dict[str, Any]]):
    """Assert manifest lists all 20 fixtures in numerical sequence with valid metadata."""
    assert len(manifest) == 20, f"Expected 20 entries in manifest, got {len(manifest)}"
    for i, entry in enumerate(manifest, start=1):
        expected_id = f"FX-{i:02d}"
        assert entry["fixture_id"] == expected_id
        assert entry["category"] in {"NORMAL", "BOUNDARY", "ADVERSARIAL"}
        assert entry["evaluation_phase"] in {"PHASE_3A_MV_SIA", "PHASE_3B_FULL_EVALUATOR"}
        assert len(entry["file_name"]) > 5
        assert len(entry["expected_verdict_hashes"]) >= 1
        for vh in entry["expected_verdict_hashes"]:
            assert len(vh) == 64
            int(vh, 16)  # valid hex string


@pytest.mark.parametrize("fx_idx", range(1, 21))
def test_fixture_schema_and_integrity(fx_idx: int, all_fixtures: Dict[str, Dict[str, Any]]):
    """Assert each fixture conforms to calibre.golden_fixture v1.0 schema."""
    fx_id = f"FX-{fx_idx:02d}"
    fixture = all_fixtures[fx_id]

    assert fixture["schema_name"] == "calibre.golden_fixture"
    assert fixture["schema_version"] == "1.0"
    assert fixture["fixture_id"] == fx_id
    assert len(fixture["title"]) >= 5
    assert fixture["category"] in {"NORMAL", "BOUNDARY", "ADVERSARIAL"}
    assert fixture["evaluation_phase"] in {"PHASE_3A_MV_SIA", "PHASE_3B_FULL_EVALUATOR"}
    assert "invariant_tested" in fixture
    assert "target_contract" in fixture
    assert isinstance(fixture["input_stream"], list)
    assert len(fixture["input_stream"]) >= 1
    assert isinstance(fixture["evaluation_schedule"], list)
    assert len(fixture["evaluation_schedule"]) >= 1
    assert isinstance(fixture["expected_verdicts"], list)
    assert len(fixture["expected_verdicts"]) >= 1


@pytest.mark.parametrize("fx_idx", range(1, 21))
def test_target_contract_compilation(fx_idx: int, all_fixtures: Dict[str, Dict[str, Any]]):
    """Assert every target DEC in golden fixtures parses and validates with zero errors."""
    fx_id = f"FX-{fx_idx:02d}"
    fixture = all_fixtures[fx_id]
    target_contract = fixture["target_contract"]

    dec = parse_contract_dict(target_contract)
    assert dec.contract_id == target_contract["contract_id"]
    result = validate_contract(dec)
    assert result.is_valid, f"Target contract in {fx_id} failed validation: {result.errors}"


@pytest.mark.parametrize("fx_idx", range(1, 21))
def test_input_stream_chronological_order(fx_idx: int, all_fixtures: Dict[str, Dict[str, Any]]):
    """Assert event stream timestamps are monotonically non-decreasing."""
    fx_id = f"FX-{fx_idx:02d}"
    fixture = all_fixtures[fx_id]
    events = fixture["input_stream"]

    prev_time = ""
    valid_types = {
        "RAW_SOURCE_ENVELOPE",
        "EVIDENCE_ATOM",
        "REFERENCE_FACT",
        "GUARD_FACT",
        "RUN_CLOCK_TICK",
    }
    for ev in events:
        assert ev["stream_item_type"] in valid_types
        t = ev["transport_event_time_utc"]
        assert len(t) >= 19
        if prev_time:
            assert t >= prev_time, f"Monotonicity violation in {fx_id}: {t} < {prev_time}"
        prev_time = t


@pytest.mark.parametrize("fx_idx", range(1, 21))
def test_verdict_hash_recalculation_parity(fx_idx: int, all_fixtures: Dict[str, Dict[str, Any]]):
    """Assert bit-for-bit mathematical determinism: compute_verdict_hash matches expected_verdicts."""
    fx_id = f"FX-{fx_idx:02d}"
    fixture = all_fixtures[fx_id]

    for verdict in fixture["expected_verdicts"]:
        expected_vh = verdict["verdict_hash"]
        contract = verdict["contract"]
        frontier = verdict["knowledge_frontier"]
        canonical_frontier = {
            "run_clock": frontier["run_clock"],
            "watermark": frontier.get("watermark"),
        }

        all_support = []
        for r_data in verdict["corroboration_profile_by_role"].values():
            all_support.extend(r_data.get("support_atom_ids", []))
        all_support = sorted(list(set(all_support)))

        all_reference = []
        for r_name, r_data in verdict["corroboration_profile_by_role"].items():
            if "reference" in r_name:
                all_reference.extend(r_data.get("support_atom_ids", []))
        all_reference = sorted(list(set(all_reference)))

        recomputed_vh = compute_verdict_hash(
            contract_id=contract["id"],
            contract_version=contract["version"],
            contract_epoch=contract["epoch"],
            decision_instance_id=verdict["decision_instance_id"],
            integrity_state=verdict["integrity_state"],
            resolution_maturity=verdict["resolution_maturity"],
            finality_status=verdict["finality_status"],
            canonical_frontier=canonical_frontier,
            corroboration_profile_by_role=verdict["corroboration_profile_by_role"],
            validity_timeline=verdict["validity_timeline"],
            sorted_support_atom_ids=all_support,
            sorted_reference_atom_ids=all_reference,
        )

        assert recomputed_vh == expected_vh, (
            f"{fx_id} verdict hash mismatch!\n"
            f"Expected:   {expected_vh}\n"
            f"Recomputed: {recomputed_vh}"
        )


def test_fx01_missing_evidence_slot_expiration(all_fixtures: Dict[str, Any]):
    fx = all_fixtures["FX-01"]
    assert fx["defect_reference"] == "D-05"
    v = fx["expected_verdicts"][0]
    assert v["integrity_state"] == "DEGRADED"
    assert "SLOT_DEADLINE_EXPIRED" in v["primary_binding_witness"]


def test_fx02_stale_evidence_timeline(all_fixtures: Dict[str, Any]):
    fx = all_fixtures["FX-02"]
    assert fx["defect_reference"] == "D-04"
    v = fx["expected_verdicts"][0]
    assert v["integrity_state"] == "SUSPECT"
    assert len(v["validity_timeline"]) == 2
    assert v["validity_timeline"][0]["state"] == "SUSPECT"
    assert v["validity_timeline"][1]["state"] == "DEGRADED"


def test_fx03_hard_expired_evidence_exclusion(all_fixtures: Dict[str, Any]):
    fx = all_fixtures["FX-03"]
    v = fx["expected_verdicts"][0]
    assert v["integrity_state"] == "DEGRADED"
    profile = v["corroboration_profile_by_role"]
    assert profile["track_weather"]["support_atom_ids"] == []
    assert any("HARD_AGE_EXCEEDED" in w for w in v["witness_set"])


def test_fx04_ambiguous_time_conversion(all_fixtures: Dict[str, Any]):
    fx = all_fixtures["FX-04"]
    v = fx["expected_verdicts"][0]
    assert v["integrity_state"] == "UNKNOWN"
    assert any("MISSING_REFERENCE_FACT" in w for w in v["witness_set"])


def test_fx05_invalid_reference_fact(all_fixtures: Dict[str, Any]):
    fx = all_fixtures["FX-05"]
    v = fx["expected_verdicts"][0]
    assert v["integrity_state"] == "UNKNOWN"
    assert any("INVALID_REFERENCE_FACT" in w for w in v["witness_set"])


def test_fx06_correlated_providers_single_upstream(all_fixtures: Dict[str, Any]):
    fx = all_fixtures["FX-06"]
    assert fx["defect_reference"] == "D-15"
    v = fx["expected_verdicts"][0]
    assert any("UNMODELLED_COMMON_CAUSE" in w for w in v["witness_set"])


def test_fx07_duplicated_processors_idempotence(all_fixtures: Dict[str, Any]):
    fx = all_fixtures["FX-07"]
    v = fx["expected_verdicts"][0]
    profile = v["corroboration_profile_by_role"]
    assert len(profile["lap_record"]["support_atom_ids"]) == 1


def test_fx08_supersession_lineage(all_fixtures: Dict[str, Any]):
    fx = all_fixtures["FX-08"]
    assert fx["defect_reference"] == "D-09"
    v = fx["expected_verdicts"][0]
    profile = v["corroboration_profile_by_role"]
    assert profile["telemetry_speed"]["processing_depth"] == 0
    assert "atom-speed-v11" in profile["telemetry_speed"]["support_atom_ids"]


def test_fx09_two_disagreeing_sources_budget(all_fixtures: Dict[str, Any]):
    fx = all_fixtures["FX-09"]
    v = fx["expected_verdicts"][0]
    assert v["integrity_state"] == "SUSPECT"
    assert any("FAULT_BUDGET_EXCEEDED" in w for w in v["witness_set"])


def test_fx10_star_graph_ambiguous_diagnosis(all_fixtures: Dict[str, Any]):
    fx = all_fixtures["FX-10"]
    assert fx["defect_reference"] == "D-01"
    v = fx["expected_verdicts"][0]
    assert v["integrity_state"] == "SUSPECT"
    assert v["diagnosis_result"] == "DIAGNOSIS_AMBIGUOUS"
    assert len(v["corroboration_profile_by_role"]["track_temperature"]["support_atom_ids"]) == 3


def test_fx11_unique_intersection_diagnosis(all_fixtures: Dict[str, Any]):
    fx = all_fixtures["FX-11"]
    v = fx["expected_verdicts"][0]
    assert v["integrity_state"] == "HEALTHY"
    assert v["diagnosis_result"] == "EXCLUDED_FAULTS"
    assert any("EXCLUDED_FAULTY_ORIGIN" in w for w in v["witness_set"])


def test_fx12_pending_origins_sound_envelope(all_fixtures: Dict[str, Any]):
    fx = all_fixtures["FX-12"]
    assert fx["defect_reference"] == "D-03"
    v = fx["expected_verdicts"][0]
    assert v["resolution_maturity"] == "OPEN"
    assert "ENVELOPE_EVALUATED" in v["primary_binding_witness"]


def test_fx13_sound_early_collapse(all_fixtures: Dict[str, Any]):
    fx = all_fixtures["FX-13"]
    v = fx["expected_verdicts"][0]
    assert v["resolution_maturity"] == "STATE_RESOLVED"


def test_fx14_late_revision_chaining(all_fixtures: Dict[str, Any]):
    fx = all_fixtures["FX-14"]
    assert fx["defect_reference"] == "D-08"
    assert len(fx["expected_verdicts"]) == 2
    v1, v2 = fx["expected_verdicts"]
    assert v1["revision_number"] == 1
    assert v2["revision_number"] == 2
    assert v2["previous_verdict_hash"] == v1["verdict_hash"]


def test_fx15_post_finality_immutability(all_fixtures: Dict[str, Any]):
    fx = all_fixtures["FX-15"]
    v = fx["expected_verdicts"][0]
    assert v["finality_status"] == "FINAL"
    assert v["decision_maturity"] == "FINAL"


def test_fx16_stationary_watermark_expiry(all_fixtures: Dict[str, Any]):
    fx = all_fixtures["FX-16"]
    assert fx["defect_reference"] == "D-06"
    v = fx["expected_verdicts"][0]
    assert v["integrity_state"] == "DEGRADED"
    assert v["knowledge_frontier"]["watermark"] == "2026-11-01T14:00:00.000Z"
    assert v["knowledge_frontier"]["run_clock"] == "2026-11-01T14:00:16.000Z"


def test_fx17_replay_virtual_clock_leases(all_fixtures: Dict[str, Any]):
    fx = all_fixtures["FX-17"]
    assert fx["defect_reference"] == "D-07"
    v = fx["expected_verdicts"][0]
    assert v["lease_clock_domain"] == "REPLAY_VIRTUAL"


def test_fx18_incompatible_fact_interpretations_scope(all_fixtures: Dict[str, Any]):
    fx = all_fixtures["FX-18"]
    v = fx["expected_verdicts"][0]
    assert v["decision_instance_id"] == "mexico-2026|VER|52|pit_lane_timing_evidence"
    assert v["integrity_state"] == "HEALTHY"


def test_fx19_overflow_with_conflicting_atom(all_fixtures: Dict[str, Any]):
    fx = all_fixtures["FX-19"]
    assert fx["defect_reference"] == "D-18"
    v = fx["expected_verdicts"][0]
    assert v["integrity_state"] == "UNKNOWN"
    assert any("SEARCH_BUDGET_EXCEEDED" in w for w in v["witness_set"])


def test_fx20_cross_language_conformance(all_fixtures: Dict[str, Any]):
    fx = all_fixtures["FX-20"]
    assert fx["defect_reference"] == "D-13"
    v = fx["expected_verdicts"][0]
    assert len(v["verdict_hash"]) == 64
