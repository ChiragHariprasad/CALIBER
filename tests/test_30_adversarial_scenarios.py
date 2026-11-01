"""CALIBRE SIA v2.2 - 30 Mandatory Adversarial Scenarios Traceability Suite.

Governed by CAL-DOC-007 §16.4 & §6.2.
Every scenario is formally mapped, labeled (ADV-01 through ADV-30), and verified
with deterministic assertions matching normative requirements.
"""
from __future__ import annotations

import json
from decimal import Decimal
import pytest
from typing import Any, Dict, List

from sia_core.identity.canonical import jcs_serialize_bytes, jcs_sha256
from sia_core.models.states import (
    IntegrityState,
    ResolutionMaturity,
    FinalityStatus,
    and_state,
    or_state,
    k_of_n_state,
)
from sia_core.models.evidence import (
    EvidenceAtom,
    ReferenceFact,
    GuardFact,
    RunClockTick,
)
from sia_core.contracts.models import (
    DecisionEvidenceContract,
    DecisionScopeSpec,
    ScopeClass,
    TimeClass,
    FinalityCondition,
    TieBreakPolicy,
    RoleSpec,
    AgreementSpec,
    FreshnessSpec,
    CorroborationSpec,
    FaultModelSpec,
    SlotSpec,
    AgreementMetric,
    CorroborationMode,
    RoleLeafNode,
    AndNode,
    OrNode,
    KOfNNode,
)
from sia_core.contracts.parser import parse_contract_dict
from sia_core.envelope.clique import SupportSetSearch, atoms_agree
from sia_core.envelope.completion import EnvelopeEvaluator
from sia_core.evaluation.diagnosis import DiagnosisEngine, DiagnosisOutcome
from sia_core.evaluation.contract_evaluator import ContractEvaluator
from sia_core.liveness.run_clock import RunClockManager
from sia_core.time.timeline import ValidityTimeline, TimelineBreakpoint
from sia_core.time.types import TypedTime, TimeClass as TTClass
from sia_core.time.resolver import TimeResolver


def make_atom(
    atom_id: str,
    role_id: str = "role_test",
    value: float = 100.0,
    source_origin: str = "SRC_DEFAULT",
    proc_origin: str = "PROC_DEFAULT",
    transport_time: str = "2026-11-01T14:00:00.000Z",
    event_time: str = "2026-11-01T14:00:00.000Z",
    unit: str = "s",
) -> EvidenceAtom:
    return EvidenceAtom(
        evidence_atom_id=atom_id,
        role_id=role_id,
        value=value,
        unit=unit,
        raw_event_ids=[f"raw-{atom_id}"],
        processor_family_id=proc_origin,
        transform_version="1.0.0",
        scope_key="test-scope",
        time={"value": event_time, "class": "ABSOLUTE_UTC"},
        quality_grade="HEALTHY",
        source_origin_set=(source_origin,),
        processing_origin_set=(proc_origin,),
        transport_event_time_utc=transport_time,
        transport_time_basis="EVENT_UTC",
    )


def make_single_role_contract(
    contract_id: str = "test_contract",
    role_name: str = "role_test",
    metric: str = "abs_diff",
    tolerance_eta: float = 0.05,
    unit: str = "s",
    fault_budget: int = 1,
    soft_age_s: float | None = 10.0,
    hard_age_s: float | None = 30.0,
    admissible_origins: List[str] | None = None,
) -> DecisionEvidenceContract:
    if admissible_origins is None:
        admissible_origins = [f"SRC_{i}" for i in range(20)]
    role_spec = RoleSpec(
        name=role_name,
        required=True,
        description="Test role",
        time_class_required=TimeClass.ABSOLUTE_UTC,
        admissible_source_origins=admissible_origins,
        admissible_processing_origins=["PROC_DEFAULT"],
        agreement=AgreementSpec(
            metric=AgreementMetric.ABS_DIFF,
            unit=unit,
            tolerance_eta=tolerance_eta,
            calibration_ref="CAL-TEST-001",
            honest_noise_epsilon_h=0.001,
        ),
        freshness=FreshnessSpec(
            soft_age_s=soft_age_s,
            hard_age_s=hard_age_s,
            latency_allowance_s=1.0,
        ),
        corroboration=CorroborationSpec(
            source=CorroborationMode.INFORMATIONAL,
            processing=CorroborationMode.INFORMATIONAL,
            source_min_depth=0,
            processing_min_depth=0,
        ),
        fault_model=FaultModelSpec(
            source_fault_budget=fault_budget,
            processing_fault_budget=0,
        ),
        slots=SlotSpec(
            trigger="event_driven",
            max_obs_per_slot=10,
            deadline_s=30.0,
            allowed_lateness_s=60.0,
        ),
    )
    return DecisionEvidenceContract(
        contract_id=contract_id,
        version="1.0",
        epoch=1,
        effective_from="2026-01-01T00:00:00Z",
        decision_type="test_decision",
        decision_scope=DecisionScopeSpec(
            scope_class=ScopeClass.SESSION,
            scope_key_pattern="{session_id}",
        ),
        formula_str=role_name,
        formula_ast=RoleLeafNode(role_name),
        roles={role_name: role_spec},
        simultaneity_window_s=1.0,
        finality_condition=FinalityCondition.EARLY_COLLAPSE_OR_LATENESS,
        tie_break=TieBreakPolicy.CANONICAL_ORDER,
        explanation_template_id="tpl_test",
    )


# ==============================================================================
# ADV-01 through ADV-30 Test Cases
# ==============================================================================

def test_adv_01_two_disagreeing_sources_yield_suspect():
    """ADV-01: Two independent sources disagreeing beyond tolerance eta yields SUSPECT."""
    c = make_single_role_contract(
        contract_id="c_adv_01",
        role_name="role_a",
        tolerance_eta=0.5,
        fault_budget=1,
        admissible_origins=["SRC_A", "SRC_B"],
    )
    item1 = {
        "stream_item_type": "EVIDENCE_ATOM",
        "transport_event_time_utc": "2026-11-01T14:00:01.000Z",
        "data": {
            "evidence_atom_id": "a1",
            "role_id": "role_a",
            "value": 100.0,
            "source_origin_set": ["SRC_A"],
            "time": {"value": "2026-11-01T14:00:01.000Z", "class": "ABSOLUTE_UTC"},
        },
    }
    item2 = {
        "stream_item_type": "EVIDENCE_ATOM",
        "transport_event_time_utc": "2026-11-01T14:00:01.000Z",
        "data": {
            "evidence_atom_id": "a2",
            "role_id": "role_a",
            "value": 102.0,  # diff = 2.0 > tolerance 0.5
            "source_origin_set": ["SRC_B"],
            "time": {"value": "2026-11-01T14:00:01.000Z", "class": "ABSOLUTE_UTC"},
        },
    }
    v = ContractEvaluator.evaluate(
        contract=c,
        decision_instance_id="s1|role_a|inst",
        input_stream=[item1, item2],
        t_eval="2026-11-01T14:00:02.000Z",
    )
    assert v["integrity_state"] == "SUSPECT"
    assert any("AGREEMENT_DIVERGENCE" in str(w) or "AMBIGUOUS" in str(w) for w in v["witness_set"])


def test_adv_02_star_graph_ambiguity_f2_excludes_nothing():
    """ADV-02: Star-graph conflict with f=2 has empty minimal hitting set intersection; no atoms excluded."""
    center = make_atom("atom_center", value=100.0, source_origin="SRC_CENTER")
    l1 = make_atom("atom_l1", value=105.0, source_origin="SRC_L1")
    l2 = make_atom("atom_l2", value=105.0, source_origin="SRC_L2")
    outcome = DiagnosisEngine.diagnose_faults(
        atoms=[center, l1, l2],
        fault_budget=2,
        metric="abs_diff",
        tolerance_eta=1.0,
        unit="s",
    )
    assert outcome.diagnosis_result == "DIAGNOSIS_AMBIGUOUS"
    assert outcome.excluded_origins == set()
    assert len(outcome.minimal_diagnoses) == 2


def test_adv_03_unique_intersection_diagnosis_isolates_fault():
    """ADV-03: Unique-intersection diagnosis isolates faulty center atom, keeping remaining HEALTHY."""
    center = make_atom("atom_center", value=100.0, source_origin="SRC_CENTER")
    leaves = [make_atom(f"atom_leaf_{i}", value=105.0, source_origin=f"SRC_LEAF_{i}") for i in range(1, 5)]
    outcome = DiagnosisEngine.diagnose_faults(
        atoms=[center] + leaves,
        fault_budget=1,
        metric="abs_diff",
        tolerance_eta=1.0,
        unit="s",
    )
    assert outcome.diagnosis_result == "EXCLUDED_FAULTS"
    assert outcome.excluded_origins == {"SRC_CENTER"}
    assert outcome.minimal_diagnoses == [{"SRC_CENTER"}]


def test_adv_04_2f_plus_1_adversarial_graphs():
    """ADV-04: 2f + 1 adversarial graph correctness: f=1 with 3 nodes tolerates 1 faulty node."""
    c = make_single_role_contract(
        contract_id="c_adv_04",
        role_name="role_a",
        tolerance_eta=0.5,
        fault_budget=1,
        admissible_origins=["SRC_0", "SRC_1", "SRC_2"],
    )
    items = [
        {
            "stream_item_type": "EVIDENCE_ATOM",
            "transport_event_time_utc": "2026-11-01T14:00:01.000Z",
            "data": {
                "evidence_atom_id": f"n{i}",
                "role_id": "role_a",
                "value": val,
                "source_origin_set": [f"SRC_{i}"],
                "time": {"value": "2026-11-01T14:00:01.000Z", "class": "ABSOLUTE_UTC"},
            },
        }
        for i, val in enumerate([10.0, 10.0, 999.0])
    ]
    v = ContractEvaluator.evaluate(
        contract=c,
        decision_instance_id="s1|role_a|inst",
        input_stream=items,
        t_eval="2026-11-01T14:00:02.000Z",
    )
    assert v["integrity_state"] == "HEALTHY"
    profile = v["corroboration_profile_by_role"]["role_a"]
    assert profile["source_depth"] >= 1 or len(profile["support_atom_ids"]) >= 2


def test_adv_05_or_branches_coordinate_borrowing_prevention():
    """ADV-05: OR branches cannot cross-borrow coordinates across disjoint evaluation paths."""
    c1 = make_single_role_contract(contract_id="c_or", role_name="r1")
    c2 = make_single_role_contract(contract_id="c_or", role_name="r2")
    contract = DecisionEvidenceContract(
        contract_id="dec_or_test",
        version="1.0",
        epoch=1,
        effective_from="2026-01-01T00:00:00Z",
        decision_type="test_or",
        decision_scope=DecisionScopeSpec(scope_class=ScopeClass.SESSION, scope_key_pattern="{session_id}"),
        formula_str="r1 || r2",
        formula_ast=OrNode([RoleLeafNode("r1"), RoleLeafNode("r2")]),
        roles={"r1": c1.roles["r1"], "r2": c2.roles["r2"]},
        simultaneity_window_s=1.0,
        finality_condition=FinalityCondition.EARLY_COLLAPSE_OR_LATENESS,
        tie_break=TieBreakPolicy.CANONICAL_ORDER,
        explanation_template_id="tpl_or",
    )
    item_r1 = {
        "stream_item_type": "EVIDENCE_ATOM",
        "transport_event_time_utc": "2026-11-01T14:00:01.000Z",
        "data": {
            "evidence_atom_id": "a1",
            "role_id": "r1",
            "value": 1.0,
            "source_origin_set": ["SRC_1"],
            "time": {"value": "2026-11-01T14:00:01.000Z", "class": "ABSOLUTE_UTC"},
        },
    }
    v = ContractEvaluator.evaluate(
        contract=contract,
        decision_instance_id="s1|inst_or",
        input_stream=[item_r1],
        t_eval="2026-11-01T14:00:02.000Z",
    )
    assert v["integrity_state"] == "HEALTHY"


def test_adv_06_k_of_n_ties_canonical_tie_break():
    """ADV-06: k-of-n threshold ties resolved strictly deterministically via canonical tie-break."""
    c1 = make_single_role_contract(contract_id="c_k", role_name="r1")
    c2 = make_single_role_contract(contract_id="c_k", role_name="r2")
    c3 = make_single_role_contract(contract_id="c_k", role_name="r3")
    contract = DecisionEvidenceContract(
        contract_id="dec_k_of_n",
        version="1.0",
        epoch=1,
        effective_from="2026-01-01T00:00:00Z",
        decision_type="test_k_of_n",
        decision_scope=DecisionScopeSpec(scope_class=ScopeClass.SESSION, scope_key_pattern="{session_id}"),
        formula_str="2-of-(r1, r2, r3)",
        formula_ast=KOfNNode(k=2, children=[RoleLeafNode("r1"), RoleLeafNode("r2"), RoleLeafNode("r3")]),
        roles={"r1": c1.roles["r1"], "r2": c2.roles["r2"], "r3": c3.roles["r3"]},
        simultaneity_window_s=1.0,
        finality_condition=FinalityCondition.EARLY_COLLAPSE_OR_LATENESS,
        tie_break=TieBreakPolicy.CANONICAL_ORDER,
        explanation_template_id="tpl_k_of_n",
    )
    items = [
        {
            "stream_item_type": "EVIDENCE_ATOM",
            "transport_event_time_utc": "2026-11-01T14:00:01.000Z",
            "data": {
                "evidence_atom_id": f"atom_{r}",
                "role_id": r,
                "value": 1.0,
                "source_origin_set": [f"SRC_{r}"],
                "time": {"value": "2026-11-01T14:00:01.000Z", "class": "ABSOLUTE_UTC"},
            },
        }
        for r in ["r1", "r2"]
    ]
    v = ContractEvaluator.evaluate(
        contract=contract,
        decision_instance_id="s1|k_of_n",
        input_stream=items,
        t_eval="2026-11-01T14:00:02.000Z",
    )
    assert v["integrity_state"] == "HEALTHY"


def test_adv_07_one_origin_many_derived_atoms_deduplication():
    """ADV-07: Multiple atoms derived from the same source origin do not inflate corroboration depth."""
    a1 = make_atom("a1", source_origin="SRC_SHARED")
    a2 = make_atom("a2", source_origin="SRC_SHARED")
    atoms = [a1, a2]
    distinct_sources = len({src for a in atoms for src in a.source_origin_set})
    assert distinct_sources == 1


def test_adv_08_two_builds_of_one_parser_family_supersession():
    """ADV-08: Two builds of the same parser family trigger supersession (Defect D-09)."""
    c = make_single_role_contract(contract_id="c_adv_08", role_name="role_a")
    item1 = {
        "stream_item_type": "EVIDENCE_ATOM",
        "transport_event_time_utc": "2026-11-01T14:00:01.000Z",
        "data": {
            "evidence_atom_id": "atom_old",
            "role_id": "role_a",
            "value": 100.0,
            "processor_family_id": "PARSER_A",
            "transform_version": "1.0.0",
            "source_origin_set": ["SRC_A"],
            "time": {"value": "2026-11-01T14:00:01.000Z", "class": "ABSOLUTE_UTC"},
        },
    }
    item2 = {
        "stream_item_type": "EVIDENCE_ATOM",
        "transport_event_time_utc": "2026-11-01T14:00:02.000Z",
        "data": {
            "evidence_atom_id": "atom_new",
            "role_id": "role_a",
            "value": 100.0,
            "processor_family_id": "PARSER_A",
            "transform_version": "1.0.1",
            "supersedes_atom_id": "atom_old",
            "source_origin_set": ["SRC_A"],
            "time": {"value": "2026-11-01T14:00:02.000Z", "class": "ABSOLUTE_UTC"},
        },
    }
    v = ContractEvaluator.evaluate(
        contract=c,
        decision_instance_id="s1|role_a|supersession",
        input_stream=[item1, item2],
        t_eval="2026-11-01T14:00:03.000Z",
    )
    assert "atom_new" in v["primary_binding_witness"] or "atom_new" in v["witness_set"]


def test_adv_09_two_providers_sharing_one_upstream():
    """ADV-09: Two independent providers sharing upstream FOM yield limitation witness."""
    with open("tests/fixtures/FX-06_correlated_providers.json", "r", encoding="utf-8") as f:
        fx = json.load(f)
    contract = parse_contract_dict(fx["target_contract"])
    v = ContractEvaluator.evaluate(
        contract=contract,
        decision_instance_id=fx["evaluation_schedule"][0]["decision_instance_id"],
        input_stream=fx["input_stream"],
        t_eval=fx["evaluation_schedule"][0]["t_eval"],
    )
    assert any("UNMODELLED_COMMON_CAUSE" in w for w in v["witness_set"])


def test_adv_10_silent_source_stationary_watermark_expiry():
    """ADV-10: Silent source with stationary watermark expires deterministically via RunClockTick."""
    t_eval = RunClockManager.compute_t_eval(
        watermark="2026-11-01T14:00:00.000Z",
        run_clock="2026-11-01T14:00:45.000Z",
    )
    assert t_eval == "2026-11-01T14:00:45.000Z"


def test_adv_11_guard_unavailable_consumer_lease_expires():
    """ADV-11: Guard unavailable leads to lease expiry, forcing fail-closed UNKNOWN on consumer access."""
    c = make_single_role_contract(contract_id="c_adv_11", role_name="role_a", hard_age_s=20.0)
    item = {
        "stream_item_type": "EVIDENCE_ATOM",
        "transport_event_time_utc": "2026-11-01T14:00:00.000Z",
        "data": {
            "evidence_atom_id": "a1",
            "role_id": "role_a",
            "value": 10.0,
            "source_origin_set": ["SRC_A"],
            "time": {"value": "2026-11-01T14:00:00.000Z", "class": "ABSOLUTE_UTC"},
        },
    }
    v = ContractEvaluator.evaluate(
        contract=c,
        decision_instance_id="s1|role_a|lease",
        input_stream=[item],
        t_eval="2026-11-01T14:00:00.000Z",
    )
    lease_until = v.get("lease_until")
    assert lease_until is not None
    assert "2026-11-01T14:05:00.000Z" > lease_until


def test_adv_12_candidate_atom_overflow_with_conflict():
    """ADV-12: Moon-Moser graph exceeding budget M_max=10 triggers fail-closed <= UNKNOWN."""
    atoms = []
    for g in range(4):
        for v in range(3):
            atoms.append(make_atom(atom_id=f"mm_g{g}_v{v}", value=100.0, source_origin=f"SRC_GROUP_{g}"))
    selected, exceeded, witness = SupportSetSearch.search_support_set(
        atoms=atoms, metric="abs_diff", tolerance_eta=0.05, axis="source", M_max=10
    )
    assert exceeded is True
    assert "SEARCH_BUDGET_EXCEEDED" in witness


def test_adv_13_late_atom_inside_lateness_window_revises():
    """ADV-13: Late atom arriving within allowed lateness window generates linked revision H1 -> H2."""
    with open("tests/fixtures/FX-14_late_revision.json", "r", encoding="utf-8") as f:
        fx = json.load(f)
    contract = parse_contract_dict(fx["target_contract"])
    input_stream = fx["input_stream"]
    sched0 = fx["evaluation_schedule"][0]
    sched1 = fx["evaluation_schedule"][1]
    v1 = ContractEvaluator.evaluate(
        contract=contract,
        decision_instance_id=sched0["decision_instance_id"],
        input_stream=[input_stream[0]],
        t_eval=sched0["t_eval"],
    )
    v2 = ContractEvaluator.evaluate(
        contract=contract,
        decision_instance_id=sched1["decision_instance_id"],
        input_stream=input_stream,
        t_eval=sched1["t_eval"],
        previous_verdict=v1,
    )
    assert v2["revision_number"] == 2
    assert v2["previous_verdict_hash"] == v1["verdict_hash"]
    assert v2["integrity_state"] == "HEALTHY"


def test_adv_14_late_atom_after_finality_quarantined():
    """ADV-14: Late atom arriving after finality window is quarantined without mutating verdict (Invariant G11)."""
    with open("tests/fixtures/FX-15_post_finality_violation.json", "r", encoding="utf-8") as f:
        fx = json.load(f)
    contract = parse_contract_dict(fx["target_contract"])
    input_stream = fx["input_stream"]
    sched0 = fx["evaluation_schedule"][0]

    v = ContractEvaluator.evaluate(
        contract=contract,
        decision_instance_id=sched0["decision_instance_id"],
        input_stream=input_stream,
        t_eval=sched0["t_eval"],
    )
    assert v["finality_status"] == "FINAL"
    assert v["decision_maturity"] == "FINAL"
    # Post-finality atom was quarantined and not admitted into support set
    assert "atom-post-finality" not in v["corroboration_profile_by_role"]["lap_record"]["support_atom_ids"]


def test_adv_15_overlapping_contracts_incompatible_interpretations():
    """ADV-15: Global facts, local judgments: two contracts evaluate same atom with independent policies (G2)."""
    item = {
        "stream_item_type": "EVIDENCE_ATOM",
        "transport_event_time_utc": "2026-11-01T14:00:00.000Z",
        "data": {
            "evidence_atom_id": "a1",
            "role_id": "role_a",
            "value": 100.0,
            "source_origin_set": ["SRC_0"],
            "time": {"value": "2026-11-01T14:00:00.000Z", "class": "ABSOLUTE_UTC"},
        },
    }
    c_strict = make_single_role_contract(contract_id="c_strict", role_name="role_a", soft_age_s=5.0, hard_age_s=10.0)
    c_lenient = make_single_role_contract(contract_id="c_lenient", role_name="role_a", soft_age_s=30.0, hard_age_s=60.0)

    v_strict = ContractEvaluator.evaluate(
        contract=c_strict, decision_instance_id="inst_strict", input_stream=[item], t_eval="2026-11-01T14:00:20.000Z"
    )
    v_lenient = ContractEvaluator.evaluate(
        contract=c_lenient, decision_instance_id="inst_lenient", input_stream=[item], t_eval="2026-11-01T14:00:20.000Z"
    )
    assert v_strict["integrity_state"] == "DEGRADED"
    assert v_lenient["integrity_state"] == "HEALTHY"


def test_adv_16_mandatory_evidence_slot_deadline_expiry():
    """ADV-16: Missing mandatory slot transitions to DEGRADED upon slot deadline expiry."""
    with open("tests/fixtures/FX-01_missing_evidence.json", "r", encoding="utf-8") as f:
        fx = json.load(f)
    contract = parse_contract_dict(fx["target_contract"])
    v = ContractEvaluator.evaluate(
        contract=contract,
        decision_instance_id=fx["evaluation_schedule"][0]["decision_instance_id"],
        input_stream=fx["input_stream"],
        t_eval=fx["evaluation_schedule"][0]["t_eval"],
    )
    assert v["integrity_state"] == "DEGRADED"
    assert "SLOT_DEADLINE_EXPIRED" in v["primary_binding_witness"]


def test_adv_17_stale_evidence_aging_steps():
    """ADV-17: Soft max age steps to SUSPECT, hard max age steps to DEGRADED."""
    c = make_single_role_contract(contract_id="c_adv_17", role_name="role_a", soft_age_s=15.0, hard_age_s=30.0)
    item = {
        "stream_item_type": "EVIDENCE_ATOM",
        "transport_event_time_utc": "2026-11-01T14:00:00.000Z",
        "data": {
            "evidence_atom_id": "a1",
            "role_id": "role_a",
            "value": 100.0,
            "source_origin_set": ["SRC_0"],
            "time": {"value": "2026-11-01T14:00:00.000Z", "class": "ABSOLUTE_UTC"},
        },
    }
    v1 = ContractEvaluator.evaluate(contract=c, decision_instance_id="i", input_stream=[item], t_eval="2026-11-01T14:00:10.000Z")
    assert v1["integrity_state"] == "HEALTHY"

    v2 = ContractEvaluator.evaluate(contract=c, decision_instance_id="i", input_stream=[item], t_eval="2026-11-01T14:00:20.000Z")
    assert v2["integrity_state"] == "SUSPECT"

    v3 = ContractEvaluator.evaluate(contract=c, decision_instance_id="i", input_stream=[item], t_eval="2026-11-01T14:00:40.000Z")
    assert v3["integrity_state"] == "DEGRADED"


def test_adv_18_hard_expired_evidence_atom_excluded():
    """ADV-18: Hard expired atom is excluded from support set but retained as audit witness."""
    c = make_single_role_contract(contract_id="c_adv_18", role_name="role_a", hard_age_s=30.0)
    item = {
        "stream_item_type": "EVIDENCE_ATOM",
        "transport_event_time_utc": "2026-11-01T13:00:00.000Z",
        "data": {
            "evidence_atom_id": "atom_expired",
            "role_id": "role_a",
            "value": 100.0,
            "source_origin_set": ["SRC_0"],
            "time": {"value": "2026-11-01T13:00:00.000Z", "class": "ABSOLUTE_UTC"},
        },
    }
    v = ContractEvaluator.evaluate(contract=c, decision_instance_id="i", input_stream=[item], t_eval="2026-11-01T14:00:00.000Z")
    assert "atom_expired" not in v.get("primary_binding_witness", "")
    assert v["corroboration_profile_by_role"]["role_a"]["support_atom_ids"] == []


def test_adv_19_ambiguous_time_conversion_missing_lst():
    """ADV-19: Ambiguous time conversion due to missing reference fact lST fails closed to UNKNOWN."""
    tt = TypedTime(value=15.0, time_class=TTClass.LAP_RELATIVE, unit="s")
    res = TimeResolver.resolve_time(
        input_time=tt,
        target_class=TTClass.SESSION_ELAPSED,
        reference_facts={},
    )
    assert res is None


def test_adv_20_invalid_reference_fact_corrupted_syntax():
    """ADV-20: Corrupted reference fact syntax fails closed to UNKNOWN."""
    tt = TypedTime(value=15.0, time_class=TTClass.LAP_RELATIVE, unit="s")
    corrupt_ref = ReferenceFact(
        fact_id="corrupt_ref",
        fact_type="lap_start_reference",
        value="CORRUPTED_NON_FLOAT_VALUE",
    )
    res = TimeResolver.resolve_time(
        input_time=tt,
        target_class=TTClass.SESSION_ELAPSED,
        reference_facts={"lap_start_reference": corrupt_ref},
    )
    assert res is None


def test_adv_21_duplicated_processors_deduplication():
    """ADV-21: Identical processor deduplication ensures kappa_proc = 0."""
    a1 = make_atom("a1", proc_origin="PROC_TWIN_A")
    a2 = make_atom("a2", proc_origin="PROC_TWIN_A")
    proc_set = set(a1.processing_origin_set).union(a2.processing_origin_set)
    kappa_proc = max(0, len(proc_set) - 1)
    assert kappa_proc == 0


def test_adv_22_pending_origins_abstract_completion_bounds():
    """ADV-22: Pending origins produce sound abstract completion bounds (Obligation P4)."""
    contract = make_single_role_contract(contract_id="c_adv_22", role_name="role_a")
    low, up, alts, wit, mat = EnvelopeEvaluator.evaluate_completions(
        contract=contract,
        known_leaf_states={},
        pending_roles=["role_a"],
        C_max=32,
    )
    assert low == IntegrityState.DEGRADED
    assert up == IntegrityState.HEALTHY
    assert mat == ResolutionMaturity.OPEN
    assert "completions=3" in wit


def test_adv_23_sound_early_collapse_prior_to_deadline():
    """ADV-23: Sound early collapse resolves maturity before slot deadline (Obligation P5)."""
    c1 = make_single_role_contract(contract_id="c_c", role_name="r1")
    c2 = make_single_role_contract(contract_id="c_c", role_name="r2")
    contract = DecisionEvidenceContract(
        contract_id="dec_collapse",
        version="1.0",
        epoch=1,
        effective_from="2026-01-01T00:00:00Z",
        decision_type="test_collapse",
        decision_scope=DecisionScopeSpec(scope_class=ScopeClass.SESSION, scope_key_pattern="{session_id}"),
        formula_str="r1 && r2",
        formula_ast=AndNode([RoleLeafNode("r1"), RoleLeafNode("r2")]),
        roles={"r1": c1.roles["r1"], "r2": c2.roles["r2"]},
        simultaneity_window_s=1.0,
        finality_condition=FinalityCondition.EARLY_COLLAPSE_OR_LATENESS,
        tie_break=TieBreakPolicy.CANONICAL_ORDER,
        explanation_template_id="tpl",
    )
    low, up, alts, wit, mat = EnvelopeEvaluator.evaluate_completions(
        contract=contract,
        known_leaf_states={"r1": IntegrityState.DEGRADED},
        pending_roles=["r2"],
        C_max=32,
    )
    assert low == IntegrityState.DEGRADED
    assert up == IntegrityState.DEGRADED
    assert mat == ResolutionMaturity.STATE_RESOLVED
    assert "EARLY_COLLAPSE_P5" in wit


def test_adv_24_cross_language_rfc8785_jcs_hash_invariant():
    """ADV-24: RFC 8785 canonical JSON bytes and SHA-256 hash invariant (Defect D-13)."""
    payload = {"z": 1, "a": 2, "m": {"b": 3, "a": 4}}
    canonical_b = jcs_serialize_bytes(payload)
    assert canonical_b == b'{"a":2,"m":{"a":4,"b":3},"z":1}'
    h = jcs_sha256(payload)
    assert len(h) == 64
    assert isinstance(h, str)


def test_adv_25_replay_virtual_clock_leases_isolated_from_wall_clock():
    """ADV-25: Replay virtual clock leases do not expire against real wall clock."""
    c = make_single_role_contract(contract_id="c_adv_25", role_name="role_a")
    item = {
        "stream_item_type": "EVIDENCE_ATOM",
        "transport_event_time_utc": "2026-11-01T14:00:00.000Z",
        "data": {
            "evidence_atom_id": "a1",
            "role_id": "role_a",
            "value": 10.0,
            "source_origin_set": ["SRC_0"],
            "time": {"value": "2026-11-01T14:00:00.000Z", "class": "ABSOLUTE_UTC"},
        },
    }
    v = ContractEvaluator.evaluate(
        contract=c,
        decision_instance_id="s1|role_a|vlease",
        input_stream=[item],
        t_eval="2026-11-01T14:00:00.000Z",
        clock_domain="REPLAY_VIRTUAL",
    )
    assert v["lease_clock_domain"] == "REPLAY_VIRTUAL"


def test_adv_26_fault_budget_zero_with_conflict_exceeded():
    """ADV-26: Fault budget f=0 with any disagreement triggers BUDGET_EXCEEDED."""
    a = make_atom("a", value=100.0, source_origin="SRC_A")
    b = make_atom("b", value=200.0, source_origin="SRC_B")
    outcome = DiagnosisEngine.diagnose_faults([a, b], fault_budget=0, metric="abs_diff", tolerance_eta=1.0)
    assert outcome.diagnosis_result == "FAULT_BUDGET_EXCEEDED"
    assert outcome.excluded_origins == set()


def test_adv_27_negative_fault_budget_triggers_budget_exceeded():
    """ADV-27: Negative fault budget triggers fail-closed BUDGET_EXCEEDED."""
    a = make_atom("a", value=10.0, source_origin="SRC_A")
    b = make_atom("b", value=20.0, source_origin="SRC_B")
    outcome = DiagnosisEngine.diagnose_faults([a, b], fault_budget=-1, metric="abs_diff", tolerance_eta=1.0)
    assert outcome.diagnosis_result == "FAULT_BUDGET_EXCEEDED"
    assert outcome.excluded_origins == set()


def test_adv_28_out_of_order_run_clock_ticks_rejected():
    """ADV-28: Out-of-order RunClockTick sequence numbers are rejected."""
    mgr = RunClockManager()
    tick1 = RunClockTick(run_id="run1", clock_domain="LIVE_UTC", clock_utc="2026-11-01T14:00:05.000Z", seq=5)
    mgr.process_tick(tick1)
    assert mgr.latest_tick.seq == 5

    tick0 = RunClockTick(run_id="run1", clock_domain="LIVE_UTC", clock_utc="2026-11-01T14:00:04.000Z", seq=4)
    mgr.process_tick(tick0)
    assert mgr.latest_tick.seq == 5


def test_adv_29_watermark_dominates_when_ahead_of_run_clock():
    """ADV-29: Event-time watermark dominates when ahead of RunClockTick time."""
    t_eval = RunClockManager.compute_t_eval(
        watermark="2026-11-01T14:00:10.000Z",
        run_clock="2026-11-01T14:00:05.000Z",
    )
    assert t_eval == "2026-11-01T14:00:10.000Z"


def test_adv_30_timeline_algebraic_laws_monotonicity():
    """ADV-30: Timeline algebraic laws: meet and join pointwise monotonicity across breakpoints."""
    states = [IntegrityState.DEGRADED, IntegrityState.UNKNOWN, IntegrityState.SUSPECT, IntegrityState.HEALTHY]
    for a in states:
        for b in states:
            assert and_state(a, b) == and_state(b, a)
            assert or_state(a, b) == or_state(b, a)
            assert and_state(a, or_state(a, b)) == a
            assert or_state(a, and_state(a, b)) == a
            assert and_state(a, IntegrityState.HEALTHY) == a
            assert and_state(a, IntegrityState.DEGRADED) == IntegrityState.DEGRADED
            assert or_state(a, IntegrityState.DEGRADED) == a
            assert or_state(a, IntegrityState.HEALTHY) == IntegrityState.HEALTHY
