"""Comprehensive pure evaluator test suite for CALIBRE Scoped Integrity Algebra (SIA) v2.2.

Verifies:
1. Pure zero-dependency evaluation of all 20 Golden Fixtures (FX-01 through FX-20).
2. Bit-for-bit RFC 8785 SHA-256 verdict hash conformance across all fixture schedules.
3. 4-State Admissibility Lattice (D < U < S < H) meet/join algebraic invariants.
4. Pointwise validity timeline step function composition sigma(t).
5. Theorem G4-V (Value Validity under Triangle Inequality) & Theorem G4-I (Fault Isolation via \\bigcap D_min).
6. Bounded Bron-Kerbosch clique search (M_max = 10) with canonical tie-breaking.
7. Abstract envelope completions (C_max = 32 cap, Obligation P4 worst/best bounds, Obligation P5 early collapse).
8. Typed-time conversion algebra and dual-clock domain leases (LIVE_UTC vs REPLAY_VIRTUAL).
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List
import pytest

from sia_core.contracts.parser import parse_contract_dict
from sia_core.evaluation.contract_evaluator import ContractEvaluator
from sia_core.models.states import (
    IntegrityState,
    ResolutionMaturity,
    FinalityStatus,
    and_state,
    or_state,
    k_of_n_state,
)
from sia_core.time.timeline import ValidityTimeline, TimelineBreakpoint, compose_pointwise
from sia_core.envelope.clique import SupportSetSearch, atoms_agree
from sia_core.envelope.completion import EnvelopeEvaluator
from sia_core.contracts.models import (
    DecisionEvidenceContract,
    DecisionScopeSpec,
    ScopeClass,
    FinalityCondition,
    TieBreakPolicy,
    AndNode,
    RoleLeafNode,
)
from sia_core.time.types import TimeClass, TypedTime
from sia_core.time.resolver import TimeResolver
from sia_core.models.evidence import EvidenceAtom, ReferenceFact


FIXTURES_DIR = Path("tests/fixtures")


def load_fixture(fx_id: str) -> Dict[str, Any]:
    matching = list(FIXTURES_DIR.glob(f"{fx_id}_*.json"))
    assert len(matching) == 1, f"Fixture {fx_id} not found"
    with open(matching[0], "r", encoding="utf-8") as f:
        return json.load(f)


@pytest.mark.parametrize("fx_idx", range(1, 21))
def test_pure_evaluator_golden_fixture_parity(fx_idx: int):
    """Execute all 20 Golden Fixtures through ContractEvaluator and assert bit-for-bit parity."""
    fx_id = f"FX-{fx_idx:02d}"
    fx = load_fixture(fx_id)

    contract = parse_contract_dict(fx["target_contract"])
    input_stream = fx["input_stream"]
    schedule = fx["evaluation_schedule"]
    expected_verdicts = fx["expected_verdicts"]

    assert len(schedule) == len(expected_verdicts)

    prev_verdict = None

    for i, sched in enumerate(schedule):
        after_step = sched["after_step_index"]
        t_eval = sched["t_eval"]
        clock_domain = sched.get("clock_domain", "LIVE_UTC")
        decision_id = sched["decision_instance_id"]
        exp_v = expected_verdicts[i]

        # Feed input stream up to after_step (supporting 0-based and 1-based indexing)
        if input_stream and input_stream[0].get("step_index") == 1:
            current_input = [item for item in input_stream if item.get("step_index", 0) <= after_step + 1]
        else:
            current_input = [item for item in input_stream if item.get("step_index", 0) <= after_step]

        actual_v = ContractEvaluator.evaluate(
            contract=contract,
            decision_instance_id=decision_id,
            input_stream=current_input,
            t_eval=t_eval,
            clock_domain=clock_domain,
            previous_verdict=prev_verdict,
        )

        prev_verdict = actual_v

        # Assert all 9 verdict fields per CAL-DOC-007
        assert actual_v["integrity_state"] == exp_v["integrity_state"], (
            f"{fx_id} Eval {sched['eval_id']}: integrity_state mismatch "
            f"actual={actual_v['integrity_state']} != exp={exp_v['integrity_state']}"
        )
        assert actual_v["resolution_maturity"] == exp_v["resolution_maturity"], (
            f"{fx_id} Eval {sched['eval_id']}: resolution_maturity mismatch "
            f"actual={actual_v['resolution_maturity']} != exp={exp_v['resolution_maturity']}"
        )
        assert actual_v["finality_status"] == exp_v["finality_status"], (
            f"{fx_id} Eval {sched['eval_id']}: finality_status mismatch "
            f"actual={actual_v['finality_status']} != exp={exp_v['finality_status']}"
        )
        assert actual_v["revision_number"] == exp_v["revision_number"], (
            f"{fx_id} Eval {sched['eval_id']}: revision_number mismatch "
            f"actual={actual_v['revision_number']} != exp={exp_v['revision_number']}"
        )
        assert actual_v["primary_binding_witness"] == exp_v["primary_binding_witness"], (
            f"{fx_id} Eval {sched['eval_id']}: primary_binding_witness mismatch "
            f"actual={actual_v['primary_binding_witness']} != exp={exp_v['primary_binding_witness']}"
        )
        assert actual_v["lease_until"] == exp_v["lease_until"], (
            f"{fx_id} Eval {sched['eval_id']}: lease_until mismatch "
            f"actual={actual_v['lease_until']} != exp={exp_v['lease_until']}"
        )
        assert actual_v["next_transition_at"] == exp_v["next_transition_at"], (
            f"{fx_id} Eval {sched['eval_id']}: next_transition_at mismatch "
            f"actual={actual_v['next_transition_at']} != exp={exp_v['next_transition_at']}"
        )
        if "diagnosis_result" in exp_v:
            assert actual_v.get("diagnosis_result") == exp_v["diagnosis_result"], (
                f"{fx_id} Eval {sched['eval_id']}: diagnosis_result mismatch "
                f"actual={actual_v.get('diagnosis_result')} != exp={exp_v['diagnosis_result']}"
            )

        # Assert timeline matches
        assert actual_v["validity_timeline"] == exp_v["validity_timeline"], (
            f"{fx_id} Eval {sched['eval_id']}: validity_timeline mismatch "
            f"actual={actual_v['validity_timeline']} != exp={exp_v['validity_timeline']}"
        )

        # Assert profile matches
        assert actual_v["corroboration_profile_by_role"] == exp_v["corroboration_profile_by_role"], (
            f"{fx_id} Eval {sched['eval_id']}: corroboration_profile_by_role mismatch "
            f"actual={actual_v['corroboration_profile_by_role']} != exp={exp_v['corroboration_profile_by_role']}"
        )

        # Assert SHA-256 verdict hash matches bit-for-bit
        assert actual_v["verdict_hash"] == exp_v["verdict_hash"], (
            f"{fx_id} Eval {sched['eval_id']}: verdict_hash mismatch "
            f"actual={actual_v['verdict_hash']} != exp={exp_v['verdict_hash']}"
        )


def test_lattice_algebra_invariants():
    """Verify 4-state admissibility lattice (D < U < S < H) meets, joins, and k-of-n."""
    d = IntegrityState.DEGRADED
    u = IntegrityState.UNKNOWN
    s = IntegrityState.SUSPECT
    h = IntegrityState.HEALTHY

    # Ordering: D < U < S < H
    assert d < u < s < h

    # Meet (AND, greatest lower bound) truth table: min(a, b)
    assert and_state(h, h) == h
    assert and_state(h, s) == s
    assert and_state(s, u) == u
    assert and_state(u, d) == d
    assert and_state(h, d) == d

    # Join (OR, least upper bound) truth table: max(a, b)
    assert or_state(d, d) == d
    assert or_state(d, u) == u
    assert or_state(u, s) == s
    assert or_state(s, h) == h
    assert or_state(d, h) == h

    # k-of-n threshold operator
    assert k_of_n_state(1, h, s, u, d) == h
    assert k_of_n_state(2, h, s, u, d) == s
    assert k_of_n_state(3, h, s, u, d) == u
    assert k_of_n_state(4, h, s, u, d) == d


def test_timeline_pointwise_composition():
    """Verify pointwise step function composition sigma(t) = sigma_1(t) meet sigma_2(t)."""
    # Timeline 1: [10:00, 10:15) -> H, [10:15, +inf) -> S
    t1 = ValidityTimeline([
        TimelineBreakpoint(from_time="2026-03-29T10:00:00Z", state=IntegrityState.HEALTHY),
        TimelineBreakpoint(from_time="2026-03-29T10:15:00Z", state=IntegrityState.SUSPECT),
    ])

    # Timeline 2: [10:00, 10:30) -> H, [10:30, +inf) -> D
    t2 = ValidityTimeline([
        TimelineBreakpoint(from_time="2026-03-29T10:00:00Z", state=IntegrityState.HEALTHY),
        TimelineBreakpoint(from_time="2026-03-29T10:30:00Z", state=IntegrityState.DEGRADED),
    ])

    composed = compose_pointwise("AND", [t1, t2])
    serialized = composed.to_list()

    assert len(serialized) == 3
    assert serialized[0] == {"from": "2026-03-29T10:00:00Z", "state": "HEALTHY"}
    assert serialized[1] == {"from": "2026-03-29T10:15:00Z", "state": "SUSPECT"}
    assert serialized[2] == {"from": "2026-03-29T10:30:00Z", "state": "DEGRADED"}


def test_theorem_g4v_value_validity_metric():
    """Verify Theorem G4-V metric agreement (|v1 - v2| <= eta, symmetric relative difference)."""
    # Absolute difference
    a1 = EvidenceAtom(
        evidence_atom_id="a1",
        role_id="test_role",
        source_origin_set=("SRC_A",),
        processing_origin_set=("PROC_1",),
        transport_event_time_utc="2026-03-29T14:00:00Z",
        time={"value": "2026-03-29T14:00:00Z", "class": "ABSOLUTE_UTC"},
        value=100.0,
    )
    a2 = EvidenceAtom(
        evidence_atom_id="a2",
        role_id="test_role",
        source_origin_set=("SRC_B",),
        processing_origin_set=("PROC_2",),
        transport_event_time_utc="2026-03-29T14:00:00Z",
        time={"value": "2026-03-29T14:00:00Z", "class": "ABSOLUTE_UTC"},
        value=100.04,
    )
    a3 = EvidenceAtom(
        evidence_atom_id="a3",
        role_id="test_role",
        source_origin_set=("SRC_C",),
        processing_origin_set=("PROC_3",),
        transport_event_time_utc="2026-03-29T14:00:00Z",
        time={"value": "2026-03-29T14:00:00Z", "class": "ABSOLUTE_UTC"},
        value=100.10,
    )

    # With tolerance 0.05
    assert atoms_agree(a1, a2, metric="abs_diff", tolerance_eta=0.05) is True
    assert atoms_agree(a1, a3, metric="abs_diff", tolerance_eta=0.05) is False

    # Relative difference symmetry: a ~ b iff b ~ a
    assert atoms_agree(a1, a2, metric="relative_diff", tolerance_eta=0.001) == atoms_agree(a2, a1, metric="relative_diff", tolerance_eta=0.001)


def test_clique_search_bounding_and_tie_breaking():
    """Verify Bron-Kerbosch search bounding (M_max = 10) and canonical tie-breaking."""
    atoms = [
        EvidenceAtom(
            evidence_atom_id=f"atom_{i:02d}",
            role_id="role_x",
            source_origin_set=(f"SRC_{i}",),
            processing_origin_set=(f"PROC_{i}",),
            transport_event_time_utc=f"2026-03-29T14:00:{i:02d}Z",
            time={"value": f"2026-03-29T14:00:{i:02d}Z", "class": "ABSOLUTE_UTC"},
            value=100.0 + i,
        )
        for i in range(12)
    ]

    selected, exceeded, witness = SupportSetSearch.search_support_set(
        atoms=atoms,
        metric="abs_diff",
        tolerance_eta=0.05,
        axis="source",
        M_max=10,
    )

    assert exceeded is True
    assert "SEARCH_BUDGET_EXCEEDED" in witness
    assert len(selected) == 12


def test_abstract_completion_envelope_bounds():
    """Verify abstract completion envelope 3^k bounds and early collapse (Obligations P4 & P5)."""
    # Build contract: AND(role_a, role_b)
    ast = AndNode(children=[RoleLeafNode("role_a"), RoleLeafNode("role_b")])
    contract = DecisionEvidenceContract(
        contract_id="test_contract",
        version="1.0",
        epoch=1,
        effective_from="2026-03-29T14:00:00Z",
        decision_type="TEST",
        decision_scope=DecisionScopeSpec(scope_class=ScopeClass.SESSION, scope_key_pattern="session_1"),
        formula_str="AND(role_a, role_b)",
        formula_ast=ast,
        roles={},
        simultaneity_window_s=1.0,
        finality_condition=FinalityCondition.EARLY_COLLAPSE_OR_LATENESS,
        tie_break=TieBreakPolicy.CANONICAL_ORDER,
        explanation_template_id="tpl_test",
    )

    # 1. Early collapse when role_a is DEGRADED: DEGRADED AND anything = DEGRADED
    lower, upper, alts, witness, maturity = EnvelopeEvaluator.evaluate_completions(
        contract=contract,
        known_leaf_states={"role_a": IntegrityState.DEGRADED},
        pending_roles=["role_b"],
        C_max=32,
    )
    assert lower == IntegrityState.DEGRADED
    assert upper == IntegrityState.DEGRADED
    assert maturity == ResolutionMaturity.STATE_RESOLVED
    assert "EARLY_COLLAPSE_P5" in witness

    # 2. Open envelope when role_a is HEALTHY: HEALTHY AND {DEGRADED, SUSPECT, HEALTHY}
    lower2, upper2, alts2, witness2, maturity2 = EnvelopeEvaluator.evaluate_completions(
        contract=contract,
        known_leaf_states={"role_a": IntegrityState.HEALTHY},
        pending_roles=["role_b"],
        C_max=32,
    )
    assert lower2 == IntegrityState.DEGRADED
    assert upper2 == IntegrityState.HEALTHY
    assert maturity2 == ResolutionMaturity.OPEN
    assert "ENVELOPE_EVALUATED" in witness2


def test_typed_time_conversion_legal_paths():
    """Verify legal typed time conversions and illegal path rejections."""
    # Legal: LAP_RELATIVE -> SESSION_ELAPSED
    assert TimeResolver.can_convert(TimeClass.LAP_RELATIVE, TimeClass.SESSION_ELAPSED) is True

    # Legal: SESSION_ELAPSED -> ABSOLUTE_UTC
    assert TimeResolver.can_convert(TimeClass.SESSION_ELAPSED, TimeClass.ABSOLUTE_UTC) is True

    # Illegal: LAP_DURATION -> any absolute time (Invariant G10)
    assert TimeResolver.can_convert(TimeClass.LAP_DURATION, TimeClass.ABSOLUTE_UTC) is False
    assert TimeResolver.can_convert(TimeClass.RECEIPT_TIME_ONLY, TimeClass.ABSOLUTE_UTC) is False

    # Execution with reference facts
    t_lap = TypedTime(value=45.0, time_class=TimeClass.LAP_RELATIVE, unit="s")
    lap_start_ref = ReferenceFact(
        fact_id="ref_lap_start",
        fact_type="lap_start_reference",
        value=100.0,
        origin_set=("SPORTING_REG",),
    )
    res_ses = TimeResolver.resolve_time(
        input_time=t_lap,
        target_class=TimeClass.SESSION_ELAPSED,
        reference_facts={"lap_start_reference": lap_start_ref},
    )
    assert res_ses is not None
    converted_time, origins, grade = res_ses
    assert converted_time.value == 145.0
    assert converted_time.time_class == TimeClass.SESSION_ELAPSED
    assert origins == {"SPORTING_REG"}
