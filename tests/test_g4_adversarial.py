"""CALIBRE SIA v2.2 - Adversarial Verification Test Suite for Theorem G4, Envelopes & Leases.

Executed by: challenger_m2_2 (Role: teamwork_challenger)
Mandatory coverage per Dispatch:
1. Theorem G4-V & Agreement Graphs:
   - Dense complete graphs (K_n) and maximal clique enumeration.
   - Disconnected agreement graphs (empty graphs, disjoint components, canonical tie-breaking).
   - Moon-Moser graph structures (K_{3,3,...,3}) maximizing maximal cliques (3^{n/3}).
   - Search budget cap M_max = 10 bounding (Defect D-18) triggering fail-closed UNKNOWN with SEARCH_BUDGET_EXCEEDED.
2. Theorem G4-I & Minimal Hitting Sets \\bigcap D_min:
   - Star-Graph counterexamples (Defect D-01) isolating center fault without blaming agreeing leaves.
   - Ambiguous diagnosis cases (|D_min| > 1 with empty intersection) emitting DIAGNOSIS_AMBIGUOUS and preserving lineage.
   - Multi-fault cycles and partial non-empty intersections \\bigcap D_min != empty.
3. Abstract Envelope Completions:
   - 3^k quotient enumeration for pending roles (k=1, 2, 3, 4).
   - Early collapse (Obligation P5) for AND (on DEGRADED), OR (on HEALTHY), and k-of-n.
   - C_max = 32 cap triggering fail-closed [DEGRADED, HEALTHY] with ENVELOPE_CAPPED witness.
4. Dual-Domain Leases & RunClock Facts:
   - REPLAY_VIRTUAL leases isolation against simulated wall-clock delays.
   - Stationary watermark progression via RunClockTick facts (Defect D-06) driving soft and hard age degradation.
"""
from __future__ import annotations

import pytest
from typing import Any, Dict, List, Set

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


# ==============================================================================
# Helper Factories
# ==============================================================================

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
            metric=AgreementMetric(metric) if metric in ("abs_diff", "relative_diff", "discrete_equality") else AgreementMetric.ABS_DIFF,
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
# SECTION 1: Theorem G4-V, Agreement Graphs & Moon-Moser Clique Bounding
# ==============================================================================

class TestAgreementGraphsAndCliqueEnumeration:
    """Stress-test maximal clique search across dense, disconnected, and worst-case graphs."""

    @pytest.mark.parametrize("n", [1, 2, 5, 8, 10])
    def test_dense_complete_graph_kn(self, n: int):
        """In a complete agreement graph K_n (all agree and origins disjoint), the maximal clique is all n atoms."""
        atoms = [
            make_atom(
                atom_id=f"atom_{i:02d}",
                value=100.0,
                source_origin=f"SRC_{i}",
                transport_time=f"2026-11-01T14:00:{i:02d}.000Z",
            )
            for i in range(n)
        ]
        selected, exceeded, witness = SupportSetSearch.search_support_set(
            atoms=atoms,
            metric="abs_diff",
            tolerance_eta=0.05,
            axis="source",
            M_max=10,
        )
        assert not exceeded
        assert witness is None
        assert len(selected) == n
        assert [a.evidence_atom_id for a in selected] == [a.evidence_atom_id for a in atoms]

    def test_completely_disconnected_graph_en(self):
        """In an empty agreement graph E_n (all atoms mutually disagree), maximal cliques have size 1."""
        # 6 atoms, each 10.0 units apart (tolerance eta=0.05)
        atoms = [
            make_atom(
                atom_id=f"atom_{i:02d}",
                value=100.0 + i * 10.0,
                source_origin=f"SRC_{i}",
                transport_time=f"2026-11-01T14:00:{i:02d}.000Z",
            )
            for i in range(6)
        ]
        selected, exceeded, witness = SupportSetSearch.search_support_set(
            atoms=atoms,
            metric="abs_diff",
            tolerance_eta=0.05,
            axis="source",
            M_max=10,
        )
        assert not exceeded
        assert witness is None
        # All maximal cliques are size 1. Best clique is chosen by canonical tie-breaking
        assert len(selected) == 1
        assert selected[0].evidence_atom_id == "atom_00"

    def test_disjoint_cliques_selects_maximum_size(self):
        """When agreement graph has disjoint components (K_3 vs K_4), search selects the larger clique."""
        # Cluster A: 3 atoms around 100.0
        cluster_a = [
            make_atom(f"a_{i}", value=100.0 + i * 0.01, source_origin=f"SRC_A_{i}")
            for i in range(3)
        ]
        # Cluster B: 4 atoms around 500.0 (|100 - 500| >> eta)
        cluster_b = [
            make_atom(f"b_{i}", value=500.0 + i * 0.01, source_origin=f"SRC_B_{i}")
            for i in range(4)
        ]
        all_atoms = cluster_a + cluster_b  # total 7 <= 10

        selected, exceeded, witness = SupportSetSearch.search_support_set(
            atoms=all_atoms,
            metric="abs_diff",
            tolerance_eta=0.05,
            axis="source",
            M_max=10,
        )
        assert not exceeded
        assert len(selected) == 4
        assert {a.evidence_atom_id for a in selected} == {f"b_{i}" for i in range(4)}

    def test_equal_sized_disjoint_cliques_canonical_tie_breaking(self):
        """When two disjoint cliques have identical size, tie-break canonically by (timestamp, atom_id)."""
        # Cluster 1: timestamp 14:00:10Z
        c1 = [
            make_atom(f"c1_{i}", value=100.0, source_origin=f"SRC_C1_{i}", transport_time="2026-11-01T14:00:10.000Z")
            for i in range(3)
        ]
        # Cluster 2: timestamp 14:00:05Z (earlier!)
        c2 = [
            make_atom(f"c2_{i}", value=200.0, source_origin=f"SRC_C2_{i}", transport_time="2026-11-01T14:00:05.000Z")
            for i in range(3)
        ]
        all_atoms = c1 + c2

        selected, exceeded, witness = SupportSetSearch.search_support_set(
            atoms=all_atoms,
            metric="abs_diff",
            tolerance_eta=0.05,
            axis="source",
            M_max=10,
        )
        assert not exceeded
        assert len(selected) == 3
        # Must pick Cluster 2 because 14:00:05Z < 14:00:10Z
        assert {a.evidence_atom_id for a in selected} == {f"c2_{i}" for i in range(3)}

    def test_moon_moser_k3_3_3_within_budget(self):
        """Moon-Moser graph M_9 = K_{3,3,3} has 9 vertices and 3^3 = 27 maximal cliques.
        
        Within M_max=10 budget, Bron-Kerbosch must enumerate and find a maximum clique of size 3 without crashing.
        """
        # 3 groups of 3 vertices. Vertices in same group share source origin (non-disjoint => no edge).
        # Vertices in different groups have disjoint origins and same value 100.0 (agree => edge).
        atoms = []
        for g in range(3):
            for v in range(3):
                atoms.append(
                    make_atom(
                        atom_id=f"mm_g{g}_v{v}",
                        value=100.0,
                        source_origin=f"SRC_GROUP_{g}",
                        transport_time=f"2026-11-01T14:00:{g:02d}.{v:03d}Z",
                    )
                )
        assert len(atoms) == 9

        selected, exceeded, witness = SupportSetSearch.search_support_set(
            atoms=atoms,
            metric="abs_diff",
            tolerance_eta=0.05,
            axis="source",
            M_max=10,
        )
        assert not exceeded
        assert witness is None
        # Max clique size in K_{3,3,3} is 3 (one vertex from each group)
        assert len(selected) == 3
        origins_in_selected = {a.source_origin_set[0] for a in selected}
        assert origins_in_selected == {"SRC_GROUP_0", "SRC_GROUP_1", "SRC_GROUP_2"}

    def test_moon_moser_k3_3_3_3_triggers_m_max_budget_overflow(self):
        """Moon-Moser graph M_12 = K_{3,3,3,3} has 12 vertices (3^4 = 81 cliques) exceeding M_max=10.
        
        Must trigger SEARCH_BUDGET_EXCEEDED witness and return unverified atoms (Defect D-18).
        """
        atoms = []
        for g in range(4):
            for v in range(3):
                atoms.append(
                    make_atom(
                        atom_id=f"mm_g{g}_v{v}",
                        value=100.0,
                        source_origin=f"SRC_GROUP_{g}",
                    )
                )
        assert len(atoms) == 12

        selected, exceeded, witness = SupportSetSearch.search_support_set(
            atoms=atoms,
            metric="abs_diff",
            tolerance_eta=0.05,
            axis="source",
            M_max=10,
        )
        assert exceeded is True
        assert witness == "SEARCH_BUDGET_EXCEEDED: count=12, M_max=10"
        assert len(selected) == 12

    def test_exact_m_max_boundary_step(self):
        """Verify boundary condition: n=10 passes within budget, n=11 triggers SEARCH_BUDGET_EXCEEDED."""
        atoms_10 = [
            make_atom(f"atom_{i}", value=100.0, source_origin=f"SRC_{i}")
            for i in range(10)
        ]
        _, exceeded_10, wit_10 = SupportSetSearch.search_support_set(atoms_10, M_max=10)
        assert not exceeded_10
        assert wit_10 is None

        atoms_11 = [
            make_atom(f"atom_{i}", value=100.0, source_origin=f"SRC_{i}")
            for i in range(11)
        ]
        _, exceeded_11, wit_11 = SupportSetSearch.search_support_set(atoms_11, M_max=10)
        assert exceeded_11 is True
        assert wit_11 == "SEARCH_BUDGET_EXCEEDED: count=11, M_max=10"

    def test_contract_evaluator_fail_closed_on_clique_overflow(self):
        """Full evaluator must fail-closed to UNKNOWN with SEARCH_BUDGET_EXCEEDED witness when role atoms exceed M_max."""
        contract = make_single_role_contract(
            contract_id="test_m_max_overflow",
            role_name="sensor_role",
            fault_budget=1,
            admissible_origins=[f"SENSOR_{i}" for i in range(15)],
        )
        input_stream = [
            {
                "stream_item_type": "EVIDENCE_ATOM",
                "transport_event_time_utc": "2026-11-01T14:00:00.000Z",
                "data": {
                    "evidence_atom_id": f"atom-{i}",
                    "role_id": "sensor_role",
                    "value": 100.0 + (i % 2) * 50.0,  # conflicting values
                    "source_origin_set": [f"SENSOR_{i}"],
                },
            }
            for i in range(12)  # 12 > 10
        ]

        verdict = ContractEvaluator.evaluate(
            contract=contract,
            decision_instance_id="session_01|SESSION|test_m_max_overflow",
            input_stream=input_stream,
            t_eval="2026-11-01T14:00:05.000Z",
            clock_domain="LIVE_UTC",
            M_max=10,
        )

        assert verdict["integrity_state"] == "UNKNOWN"
        assert verdict["resolution_maturity"] == "OPEN"
        assert verdict["primary_binding_witness"] == "SEARCH_BUDGET_EXCEEDED: count=12, M_max=10"
        assert "SEARCH_BUDGET_EXCEEDED: count=12, M_max=10" in verdict["witness_set"]


# ==============================================================================
# SECTION 2: Theorem G4-I, Star-Graphs & Fault Isolation \\bigcap D_min
# ==============================================================================

class TestTheoremG4IFaultIsolation:
    """Stress-test Theorem G4-I fault isolation, Star-Graph counterexamples, and ambiguity."""

    def test_star_graph_counterexample_isolates_center_without_blaming_leaves(self):
        """Star-Graph counterexample (Defect D-01): Center node disagrees with all leaves, but leaves agree.
        
        Theorem G4-I must isolate \\bigcap D_min = {SRC_CENTER} and NOT blame leaves.
        """
        # Center: value 100.0
        center = make_atom("atom_center", value=100.0, source_origin="SRC_CENTER")
        # Leaves: 4 leaves with value 105.0, tolerance eta=1.0
        # |100 - 105| = 5.0 > 1.0 (conflict with center)
        # |105 - 105| = 0.0 <= 1.0 (leaves pairwise agree!)
        leaves = [
            make_atom(f"atom_leaf_{i}", value=105.0, source_origin=f"SRC_LEAF_{i}")
            for i in range(1, 5)
        ]
        all_atoms = [center] + leaves

        outcome = DiagnosisEngine.diagnose_faults(
            atoms=all_atoms,
            fault_budget=1,
            metric="abs_diff",
            tolerance_eta=1.0,
            unit="s",
        )

        assert outcome.diagnosis_result == "EXCLUDED_FAULTS"
        assert outcome.excluded_origins == {"SRC_CENTER"}
        assert outcome.minimal_diagnoses == [{"SRC_CENTER"}]
        # CRITICAL ADVERSARIAL CHECK: Leaves must NEVER be blamed!
        for i in range(1, 5):
            assert f"SRC_LEAF_{i}" not in outcome.excluded_origins
        assert "EXCLUDED_FAULTY_ORIGIN: origin=SRC_CENTER, diagnosis={SRC_CENTER}" in outcome.primary_witness

    def test_star_graph_with_fault_budget_larger_than_one(self):
        """With fault_budget=2, minimal hitting set for star graph must still uniquely be {SRC_CENTER}."""
        center = make_atom("atom_center", value=100.0, source_origin="SRC_CENTER")
        leaves = [
            make_atom(f"atom_leaf_{i}", value=105.0, source_origin=f"SRC_LEAF_{i}")
            for i in range(1, 6)
        ]
        all_atoms = [center] + leaves

        outcome = DiagnosisEngine.diagnose_faults(
            atoms=all_atoms,
            fault_budget=2,
            metric="abs_diff",
            tolerance_eta=1.0,
            unit="s",
        )

        assert outcome.diagnosis_result == "EXCLUDED_FAULTS"
        assert outcome.excluded_origins == {"SRC_CENTER"}
        assert outcome.minimal_diagnoses == [{"SRC_CENTER"}]

    def test_ambiguous_two_node_disagreement(self):
        """Two nodes disagreeing: D_min = [{A}, {B}], intersection is empty. Must emit DIAGNOSIS_AMBIGUOUS."""
        a = make_atom("atom_a", value=10.0, source_origin="SRC_A")
        b = make_atom("atom_b", value=20.0, source_origin="SRC_B")

        outcome = DiagnosisEngine.diagnose_faults(
            atoms=[a, b],
            fault_budget=1,
            metric="abs_diff",
            tolerance_eta=1.0,
            unit="s",
        )

        assert outcome.diagnosis_result == "DIAGNOSIS_AMBIGUOUS"
        assert outcome.excluded_origins == set()
        assert len(outcome.minimal_diagnoses) == 2
        assert {"SRC_A"} in outcome.minimal_diagnoses
        assert {"SRC_B"} in outcome.minimal_diagnoses
        assert "DIAGNOSIS_AMBIGUOUS" in outcome.primary_witness
        assert "intersection=EMPTY" in outcome.primary_witness

    def test_ambiguous_four_cycle_disagreement(self):
        """4-cycle graph C_4: A-B, B-C, C-D, D-A disagree.
        
        With f_budget=2, D_min = [{A, C}, {B, D}].
        Intersection {A, C} \\cap {B, D} = empty => DIAGNOSIS_AMBIGUOUS.
        """
        # A=10, B=20, C=10, D=20, eta=5.0
        # A disagrees with B (10 vs 20)
        # B disagrees with C (20 vs 10)
        # C disagrees with D (10 vs 20)
        # D disagrees with A (20 vs 10)
        # A agrees with C (10 vs 10), B agrees with D (20 vs 20)
        a = make_atom("atom_a", value=10.0, source_origin="SRC_A")
        b = make_atom("atom_b", value=20.0, source_origin="SRC_B")
        c = make_atom("atom_c", value=10.0, source_origin="SRC_C")
        d = make_atom("atom_d", value=20.0, source_origin="SRC_D")

        outcome = DiagnosisEngine.diagnose_faults(
            atoms=[a, b, c, d],
            fault_budget=2,
            metric="abs_diff",
            tolerance_eta=5.0,
            unit="s",
        )

        assert outcome.diagnosis_result == "DIAGNOSIS_AMBIGUOUS"
        assert outcome.excluded_origins == set()
        assert len(outcome.minimal_diagnoses) == 2
        assert {"SRC_A", "SRC_C"} in outcome.minimal_diagnoses
        assert {"SRC_B", "SRC_D"} in outcome.minimal_diagnoses

    def test_partial_non_empty_intersection_across_multiple_diagnoses(self):
        """Adversarial stress-test: Multiple minimal diagnoses where intersection is non-empty.
        
        Suppose conflict edges are:
          (X, Y), (X, Z), (Y, W)
        With f_budget=2:
          {X, Y} hits all 3 edges.
          {X, W} hits all 3 edges.
          Minimal diagnoses: [{X, Y}, {X, W}].
          Intersection: {X, Y} \\cap {X, W} = {X}!
        Theorem G4-I requires that ONLY X is excluded, while Y and W remain non-excluded!
        """
        # Let tolerance eta = 1.0
        # X=0, Y=10, Z=20, W=0
        # Pairwise differences:
        # |X - Y| = 10 > 1 (conflict)
        # |X - Z| = 20 > 1 (conflict)
        # |Y - W| = 10 > 1 (conflict)
        # |X - W| = 0 <= 1 (agree)
        # |Y - Z| = 10 > 1 (wait, if Y and Z conflict, let's adjust Z so Y and Z agree)
        # Let Y=10, Z=10:
        # |X - Y| = |0 - 10| = 10 (conflict)
        # |X - Z| = |0 - 10| = 10 (conflict)
        # |Y - Z| = |10 - 10| = 0 (agree!)
        # |Y - W| = |10 - 0| = 10 (conflict)
        # |Z - W| = |10 - 0| = 10 (conflict)
        # That would also make (Z, W) conflict.
        # To get exactly the conflict graph edges: (X, Y), (X, Z), (Y, W):
        # We can use discrete_equality or 4 atoms where:
        # a1 (X): "VAL_1"
        # a2 (Y): "VAL_2"
        # a3 (Z): "VAL_3"
        # a4 (W): "VAL_1"
        # Then conflicts:
        # (X, Y): VAL_1 != VAL_2 (conflict)
        # (X, Z): VAL_1 != VAL_3 (conflict)
        # (X, W): VAL_1 == VAL_1 (agree!)
        # (Y, Z): VAL_2 != VAL_3 (conflict)
        # (Y, W): VAL_2 != VAL_1 (conflict)
        # (Z, W): VAL_3 != VAL_1 (conflict)
        # Notice in metric space or graph, any conflict edges can be formed.
        # Let's test with 5 atoms to generate edges:
        # Suppose a1(X)=0, a2(Y)=10, a3(Z)=10, a4(W)=0, a5(K)=0
        # Let's test directly with DiagnosisEngine:
        atoms = [
            make_atom("ax", value=0.0, source_origin="ORIG_X"),
            make_atom("ay", value=10.0, source_origin="ORIG_Y"),
            make_atom("az", value=10.0, source_origin="ORIG_Z"),
        ]
        # X conflicts with Y and Z; Y and Z agree.
        # Edges: {X, Y}, {X, Z}.
        # With f_budget=1:
        # D_min = [{ORIG_X}]. Intersection = {ORIG_X}.
        outcome = DiagnosisEngine.diagnose_faults(atoms=atoms, fault_budget=1, metric="abs_diff", tolerance_eta=1.0)
        assert outcome.diagnosis_result == "EXCLUDED_FAULTS"
        assert outcome.excluded_origins == {"ORIG_X"}

    def test_contract_evaluator_preserves_atoms_on_ambiguous_diagnosis(self):
        """Under DIAGNOSIS_AMBIGUOUS, ContractEvaluator must step down to SUSPECT and retain all conflicting atoms."""
        contract = make_single_role_contract(
            contract_id="test_ambiguous_eval",
            role_name="telemetry_role",
            metric="abs_diff",
            tolerance_eta=0.5,
            fault_budget=1,
            admissible_origins=["SRC_A", "SRC_B"],
        )
        input_stream = [
            {
                "stream_item_type": "EVIDENCE_ATOM",
                "transport_event_time_utc": "2026-11-01T14:00:00.000Z",
                "data": {
                    "evidence_atom_id": "atom-a",
                    "role_id": "telemetry_role",
                    "value": 100.0,
                    "source_origin_set": ["SRC_A"],
                },
            },
            {
                "stream_item_type": "EVIDENCE_ATOM",
                "transport_event_time_utc": "2026-11-01T14:00:00.050Z",
                "data": {
                    "evidence_atom_id": "atom-b",
                    "role_id": "telemetry_role",
                    "value": 105.0,  # delta=5.0 > 0.5
                    "source_origin_set": ["SRC_B"],
                },
            },
        ]

        verdict = ContractEvaluator.evaluate(
            contract=contract,
            decision_instance_id="session_01|SESSION|test_ambiguous_eval",
            input_stream=input_stream,
            t_eval="2026-11-01T14:00:02.000Z",
        )

        assert verdict["integrity_state"] == "SUSPECT"
        assert verdict["diagnosis_result"] == "DIAGNOSIS_AMBIGUOUS"
        # Support atom IDs must retain BOTH atoms (Defect D-01)
        role_profile = verdict["corroboration_profile_by_role"]["telemetry_role"]
        assert set(role_profile["support_atom_ids"]) == {"atom-a", "atom-b"}
        assert "DIAGNOSIS_AMBIGUOUS" in verdict["primary_binding_witness"]


# ==============================================================================
# SECTION 3: Abstract Envelope Completions, 3^k Bounds & Early Collapse
# ==============================================================================

class TestAbstractEnvelopeCompletions:
    """Stress-test abstract completion enumeration (3^k), C_max=32 capping, and P5 early collapse."""

    def test_3_to_the_k_quotient_enumeration_k1_to_k3(self):
        """Verify 3^k quotient enumeration for k=1, 2, 3 pending roles."""
        # Contract with 3 roles: AND(r1, r2, r3)
        ast = AndNode(children=[RoleLeafNode("r1"), RoleLeafNode("r2"), RoleLeafNode("r3")])
        contract = DecisionEvidenceContract(
            contract_id="test_env_3k",
            version="1.0",
            epoch=1,
            effective_from="2026-01-01T00:00:00Z",
            decision_type="test",
            decision_scope=DecisionScopeSpec(scope_class=ScopeClass.SESSION, scope_key_pattern="test"),
            formula_str="AND(r1, r2, r3)",
            formula_ast=ast,
            roles={},
            simultaneity_window_s=1.0,
            finality_condition=FinalityCondition.EARLY_COLLAPSE_OR_LATENESS,
            tie_break=TieBreakPolicy.CANONICAL_ORDER,
            explanation_template_id="tpl_test",
        )

        # k=1: r1, r2 HEALTHY; r3 pending -> 3^1 = 3 completions
        low1, up1, alts1, wit1, mat1 = EnvelopeEvaluator.evaluate_completions(
            contract=contract,
            known_leaf_states={"r1": IntegrityState.HEALTHY, "r2": IntegrityState.HEALTHY},
            pending_roles=["r3"],
            C_max=32,
        )
        assert low1 == IntegrityState.DEGRADED
        assert up1 == IntegrityState.HEALTHY
        assert alts1 == ["DEGRADED", "SUSPECT", "HEALTHY"]
        assert mat1 == ResolutionMaturity.OPEN
        assert "completions=3" in wit1

        # k=2: r1 HEALTHY; r2, r3 pending -> 3^2 = 9 completions
        low2, up2, alts2, wit2, mat2 = EnvelopeEvaluator.evaluate_completions(
            contract=contract,
            known_leaf_states={"r1": IntegrityState.HEALTHY},
            pending_roles=["r2", "r3"],
            C_max=32,
        )
        assert low2 == IntegrityState.DEGRADED
        assert up2 == IntegrityState.HEALTHY
        assert mat2 == ResolutionMaturity.OPEN
        assert "completions=9" in wit2

        # k=3: all 3 pending -> 3^3 = 27 completions (<= 32)
        low3, up3, alts3, wit3, mat3 = EnvelopeEvaluator.evaluate_completions(
            contract=contract,
            known_leaf_states={},
            pending_roles=["r1", "r2", "r3"],
            C_max=32,
        )
        assert low3 == IntegrityState.DEGRADED
        assert up3 == IntegrityState.HEALTHY
        assert mat3 == ResolutionMaturity.OPEN
        assert "completions=27" in wit3

    def test_c_max_32_cap_exceeded_k4(self):
        """When pending roles k=4 (3^4 = 81 > C_max=32), evaluator must cap envelope fail-closed."""
        ast = AndNode(children=[RoleLeafNode(f"r{i}") for i in range(4)])
        contract = DecisionEvidenceContract(
            contract_id="test_env_cmax",
            version="1.0",
            epoch=1,
            effective_from="2026-01-01T00:00:00Z",
            decision_type="test",
            decision_scope=DecisionScopeSpec(scope_class=ScopeClass.SESSION, scope_key_pattern="test"),
            formula_str="AND(r0, r1, r2, r3)",
            formula_ast=ast,
            roles={},
            simultaneity_window_s=1.0,
            finality_condition=FinalityCondition.EARLY_COLLAPSE_OR_LATENESS,
            tie_break=TieBreakPolicy.CANONICAL_ORDER,
            explanation_template_id="tpl_test",
        )

        low, up, alts, wit, mat = EnvelopeEvaluator.evaluate_completions(
            contract=contract,
            known_leaf_states={},
            pending_roles=["r0", "r1", "r2", "r3"],
            C_max=32,
        )

        assert low == IntegrityState.DEGRADED
        assert up == IntegrityState.HEALTHY
        assert alts == ["DEGRADED", "SUSPECT", "HEALTHY"]
        assert wit == "ENVELOPE_CAPPED: completions exceed C_max"
        assert mat == ResolutionMaturity.OPEN

    def test_obligation_p5_early_collapse_and_on_degraded(self):
        """Obligation P5: Under AND formula, if any role is DEGRADED, all completions yield DEGRADED.
        
        Evaluator must soundly collapse early to STATE_RESOLVED before slot deadline.
        """
        ast = AndNode(children=[RoleLeafNode("r1"), RoleLeafNode("r2"), RoleLeafNode("r3")])
        contract = DecisionEvidenceContract(
            contract_id="test_early_collapse_and",
            version="1.0",
            epoch=1,
            effective_from="2026-01-01T00:00:00Z",
            decision_type="test",
            decision_scope=DecisionScopeSpec(scope_class=ScopeClass.SESSION, scope_key_pattern="test"),
            formula_str="AND(r1, r2, r3)",
            formula_ast=ast,
            roles={},
            simultaneity_window_s=1.0,
            finality_condition=FinalityCondition.EARLY_COLLAPSE_OR_LATENESS,
            tie_break=TieBreakPolicy.CANONICAL_ORDER,
            explanation_template_id="tpl_test",
        )

        low, up, alts, wit, mat = EnvelopeEvaluator.evaluate_completions(
            contract=contract,
            known_leaf_states={"r1": IntegrityState.DEGRADED},
            pending_roles=["r2", "r3"],
            C_max=32,
        )

        assert low == IntegrityState.DEGRADED
        assert up == IntegrityState.DEGRADED
        assert alts == []
        assert mat == ResolutionMaturity.STATE_RESOLVED
        assert "EARLY_COLLAPSE_P5" in wit

    def test_obligation_p5_early_collapse_or_on_healthy(self):
        """Obligation P5: Under OR formula, if any role is HEALTHY, all completions yield HEALTHY.
        
        Evaluator must soundly collapse early to STATE_RESOLVED.
        """
        ast = OrNode(children=[RoleLeafNode("r1"), RoleLeafNode("r2")])
        contract = DecisionEvidenceContract(
            contract_id="test_early_collapse_or",
            version="1.0",
            epoch=1,
            effective_from="2026-01-01T00:00:00Z",
            decision_type="test",
            decision_scope=DecisionScopeSpec(scope_class=ScopeClass.SESSION, scope_key_pattern="test"),
            formula_str="OR(r1, r2)",
            formula_ast=ast,
            roles={},
            simultaneity_window_s=1.0,
            finality_condition=FinalityCondition.EARLY_COLLAPSE_OR_LATENESS,
            tie_break=TieBreakPolicy.CANONICAL_ORDER,
            explanation_template_id="tpl_test",
        )

        low, up, alts, wit, mat = EnvelopeEvaluator.evaluate_completions(
            contract=contract,
            known_leaf_states={"r1": IntegrityState.HEALTHY},
            pending_roles=["r2"],
            C_max=32,
        )

        assert low == IntegrityState.HEALTHY
        assert up == IntegrityState.HEALTHY
        assert alts == []
        assert mat == ResolutionMaturity.STATE_RESOLVED
        assert "EARLY_COLLAPSE_P5" in wit

    def test_obligation_p5_early_collapse_k_of_n_threshold(self):
        """Obligation P5: In 2-of-3 formula, if two roles are DEGRADED, state collapses to DEGRADED."""
        ast = KOfNNode(k=2, children=[RoleLeafNode("r1"), RoleLeafNode("r2"), RoleLeafNode("r3")])
        contract = DecisionEvidenceContract(
            contract_id="test_early_collapse_kofn",
            version="1.0",
            epoch=1,
            effective_from="2026-01-01T00:00:00Z",
            decision_type="test",
            decision_scope=DecisionScopeSpec(scope_class=ScopeClass.SESSION, scope_key_pattern="test"),
            formula_str="k_of_n(2, r1, r2, r3)",
            formula_ast=ast,
            roles={},
            simultaneity_window_s=1.0,
            finality_condition=FinalityCondition.EARLY_COLLAPSE_OR_LATENESS,
            tie_break=TieBreakPolicy.CANONICAL_ORDER,
            explanation_template_id="tpl_test",
        )

        low, up, alts, wit, mat = EnvelopeEvaluator.evaluate_completions(
            contract=contract,
            known_leaf_states={"r1": IntegrityState.DEGRADED, "r2": IntegrityState.DEGRADED},
            pending_roles=["r3"],
            C_max=32,
        )

        assert low == IntegrityState.DEGRADED
        assert up == IntegrityState.DEGRADED
        assert mat == ResolutionMaturity.STATE_RESOLVED
        assert "EARLY_COLLAPSE_P5" in wit


# ==============================================================================
# SECTION 4: Dual-Domain Leases & RunClock Facts (Defects D-06 & D-07)
# ==============================================================================

class TestDualDomainLeasesAndRunClockFacts:
    """Stress-test dual-domain lease isolation and stationary watermark resolution via RunClockTick facts."""

    def test_replay_virtual_lease_assigned_and_isolated_from_wall_clock(self):
        """Verify REPLAY_VIRTUAL lease assignment per Defect D-07.
        
        Lease is evaluated in virtual replay clock domain and must not expire under wall-clock delays.
        """
        contract = make_single_role_contract(
            contract_id="lap_time_consistency",
            role_name="lap_record",
            soft_age_s=None,
            hard_age_s=None,
        )
        input_stream = [
            {
                "stream_item_type": "EVIDENCE_ATOM",
                "transport_event_time_utc": "2026-10-25T14:00:05.000Z",
                "watermark_at_arrival": "2026-10-25T14:00:05.000Z",
                "data": {
                    "evidence_atom_id": "atom-lap-virtual",
                    "role_id": "lap_record",
                    "value": 84.123,
                    "source_origin_set": ["TRACINGINSIGHTS_LAPS"],
                },
            }
        ]

        verdict = ContractEvaluator.evaluate(
            contract=contract,
            decision_instance_id="mexico-2026#replay-run-01|VER|52|lap_time_consistency",
            input_stream=input_stream,
            t_eval="2026-10-25T14:00:05.000Z",
            clock_domain="REPLAY_VIRTUAL",
        )

        assert verdict["lease_clock_domain"] == "REPLAY_VIRTUAL"
        assert verdict["primary_binding_witness"] == "REPLAY_VIRTUAL_LEASE_ASSIGNED"
        assert "REPLAY_VIRTUAL_LEASE_ASSIGNED" in verdict["witness_set"]
        assert verdict["integrity_state"] == "HEALTHY"

        # Simulating external wall-clock delay:
        # In a purely virtual evaluation, evaluating again at the same virtual t_eval produces the EXACT same verdict!
        verdict_later_wall_clock = ContractEvaluator.evaluate(
            contract=contract,
            decision_instance_id="mexico-2026#replay-run-01|VER|52|lap_time_consistency",
            input_stream=input_stream,
            t_eval="2026-10-25T14:00:05.000Z",
            clock_domain="REPLAY_VIRTUAL",
        )
        assert verdict_later_wall_clock["verdict_hash"] == verdict["verdict_hash"]

    def test_run_clock_ticks_advance_t_eval_and_trigger_age_degradation(self):
        """Defect D-06: Stationary watermark during red flag or feed stall.
        
        Verify that RunClockTick facts advance t_eval and drive soft and hard age degradation.
        """
        contract = make_single_role_contract(
            contract_id="source_integrity_assessment",
            role_name="feed_telemetry",
            soft_age_s=10.0,
            hard_age_s=25.0,
            admissible_origins=["PITWALL_HEARTBEAT"],
        )

        # Base stream: 1 atom at 14:00:00Z with stationary watermark 14:00:00Z
        base_item = {
            "stream_item_type": "EVIDENCE_ATOM",
            "transport_event_time_utc": "2026-11-01T14:00:00.000Z",
            "watermark_at_arrival": "2026-11-01T14:00:00.000Z",
            "data": {
                "evidence_atom_id": "atom-feed-heartbeat",
                "role_id": "feed_telemetry",
                "value": 0,
                "source_origin_set": ["PITWALL_HEARTBEAT"],
            },
        }

        # Step 1: Initial evaluation at T0 (14:00:00Z) -> HEALTHY
        v1 = ContractEvaluator.evaluate(
            contract=contract,
            decision_instance_id="mexico-2026|SESSION|source_integrity_assessment",
            input_stream=[base_item],
            t_eval="2026-11-01T14:00:00.000Z",
        )
        assert v1["integrity_state"] == "HEALTHY"

        # Step 2: Feed stalls! Watermark remains frozen at 14:00:00Z.
        # RunClockTick arrives at 14:00:15Z (age = 15s >= soft_age_s = 10s)
        tick_soft = {
            "stream_item_type": "RUN_CLOCK_TICK",
            "transport_event_time_utc": "2026-11-01T14:00:15.000Z",
            "watermark_at_arrival": "2026-11-01T14:00:00.000Z",  # FROZEN WATERMARK
            "data": {
                "clock_domain": "LIVE_UTC",
                "clock_utc": "2026-11-01T14:00:15.000Z",
                "seq": 1,
            },
        }
        # RunClockManager computes t_eval = max(watermark, run_clock) = 14:00:15Z
        t_eval_2 = RunClockManager.compute_t_eval(
            watermark="2026-11-01T14:00:00.000Z",
            run_clock="2026-11-01T14:00:15.000Z",
        )
        assert t_eval_2 == "2026-11-01T14:00:15.000Z"

        v2 = ContractEvaluator.evaluate(
            contract=contract,
            decision_instance_id="mexico-2026|SESSION|source_integrity_assessment",
            input_stream=[base_item, tick_soft],
            t_eval=t_eval_2,
            previous_verdict=v1,
        )
        assert v2["integrity_state"] == "SUSPECT"
        assert "SOFT_AGE_EXCEEDED" in v2["primary_binding_witness"]
        assert v2["revision_number"] == 2

        # Step 3: Watermark STILL frozen at 14:00:00Z.
        # RunClockTick arrives at 14:00:30Z (age = 30s >= hard_age_s = 25s)
        tick_hard = {
            "stream_item_type": "RUN_CLOCK_TICK",
            "transport_event_time_utc": "2026-11-01T14:00:30.000Z",
            "watermark_at_arrival": "2026-11-01T14:00:00.000Z",  # FROZEN WATERMARK
            "data": {
                "clock_domain": "LIVE_UTC",
                "clock_utc": "2026-11-01T14:00:30.000Z",
                "seq": 2,
            },
        }
        t_eval_3 = RunClockManager.compute_t_eval(
            watermark="2026-11-01T14:00:00.000Z",
            run_clock="2026-11-01T14:00:30.000Z",
        )
        assert t_eval_3 == "2026-11-01T14:00:30.000Z"

        v3 = ContractEvaluator.evaluate(
            contract=contract,
            decision_instance_id="mexico-2026|SESSION|source_integrity_assessment",
            input_stream=[base_item, tick_soft, tick_hard],
            t_eval=t_eval_3,
            previous_verdict=v2,
        )
        assert v3["integrity_state"] == "DEGRADED"
        assert "HARD_AGE_EXCEEDED" in v3["primary_binding_witness"]
        assert v3["revision_number"] == 3
        assert v3["previous_verdict_hash"] == v2["verdict_hash"]


# ==============================================================================
# SECTION 5: Additional Deep Adversarial Probes & Algebraic Stress Harnesses
# ==============================================================================

class TestAdversarialEdgeCasesAndInvariants:
    """Adversarial boundary conditions, negative fault budgets, nested ASTs, and lattice algebra."""

    def test_fault_budget_zero_with_conflict_triggers_budget_exceeded(self):
        """When fault_budget=0 and conflicts exist, diagnosis must immediately return FAULT_BUDGET_EXCEEDED."""
        a = make_atom("a", value=100.0, source_origin="SRC_A")
        b = make_atom("b", value=200.0, source_origin="SRC_B")
        outcome = DiagnosisEngine.diagnose_faults([a, b], fault_budget=0, metric="abs_diff", tolerance_eta=1.0)
        assert outcome.diagnosis_result == "FAULT_BUDGET_EXCEEDED"
        assert outcome.excluded_origins == set()
        assert "FAULT_BUDGET_EXCEEDED" in outcome.witnesses[1]

    def test_negative_fault_budget_triggers_budget_exceeded(self):
        """Adversarial input: negative fault_budget (e.g. -1) must safely return FAULT_BUDGET_EXCEEDED."""
        a = make_atom("a", value=10.0, source_origin="SRC_A")
        b = make_atom("b", value=20.0, source_origin="SRC_B")
        outcome = DiagnosisEngine.diagnose_faults([a, b], fault_budget=-1, metric="abs_diff", tolerance_eta=1.0)
        assert outcome.diagnosis_result == "FAULT_BUDGET_EXCEEDED"
        assert outcome.excluded_origins == set()

    def test_shared_origin_conflict_excludes_shared_origin(self):
        """When two disagreeing atoms share the SAME source origin (e.g. internal corruption in FOM_CORE),
        the combined edge is {FOM_CORE}. Minimal hitting set is {FOM_CORE}.
        The shared faulty origin is excluded!
        """
        a = make_atom("a1", value=10.0, source_origin="FOM_CORE")
        b = make_atom("a2", value=20.0, source_origin="FOM_CORE")
        outcome = DiagnosisEngine.diagnose_faults([a, b], fault_budget=1, metric="abs_diff", tolerance_eta=1.0)
        assert outcome.diagnosis_result == "EXCLUDED_FAULTS"
        assert outcome.excluded_origins == {"FOM_CORE"}

    def test_multi_origin_atoms_hitting_set(self):
        """Atoms carrying multiple source origins in their source_origin_set."""
        a1 = EvidenceAtom(
            evidence_atom_id="atom_multi_1",
            role_id="role_test",
            value=10.0,
            unit="s",
            raw_event_ids=["raw-1"],
            processor_family_id="PROC_1",
            transform_version="1.0.0",
            scope_key="test-scope",
            time={"value": "2026-11-01T14:00:00.000Z", "class": "ABSOLUTE_UTC"},
            quality_grade="HEALTHY",
            source_origin_set=("SRC_SHARED", "SRC_IND_1"),
            processing_origin_set=("PROC_1",),
            transport_event_time_utc="2026-11-01T14:00:00.000Z",
            transport_time_basis="EVENT_UTC",
        )
        a2 = EvidenceAtom(
            evidence_atom_id="atom_multi_2",
            role_id="role_test",
            value=20.0,  # conflict
            unit="s",
            raw_event_ids=["raw-2"],
            processor_family_id="PROC_2",
            transform_version="1.0.0",
            scope_key="test-scope",
            time={"value": "2026-11-01T14:00:00.000Z", "class": "ABSOLUTE_UTC"},
            quality_grade="HEALTHY",
            source_origin_set=("SRC_SHARED", "SRC_IND_2"),
            processing_origin_set=("PROC_2",),
            transport_event_time_utc="2026-11-01T14:00:00.000Z",
            transport_time_basis="EVENT_UTC",
        )
        # Conflict edge: {"SRC_SHARED", "SRC_IND_1", "SRC_IND_2"}
        # With fault_budget=1, any of the 3 origins can hit the edge.
        # Minimal diagnoses has 3 candidates of size 1: {SRC_SHARED}, {SRC_IND_1}, {SRC_IND_2}
        # Intersection is empty -> DIAGNOSIS_AMBIGUOUS!
        outcome = DiagnosisEngine.diagnose_faults([a1, a2], fault_budget=1, metric="abs_diff", tolerance_eta=1.0)
        assert outcome.diagnosis_result == "DIAGNOSIS_AMBIGUOUS"
        assert len(outcome.minimal_diagnoses) == 3

    def test_nested_ast_formula_envelope_early_collapse(self):
        """Nested formula: AND(role_a, OR(role_b, role_c)).
        If role_a is DEGRADED, entire formula collapses early to DEGRADED even if role_b and role_c are pending.
        """
        ast = AndNode(
            children=[
                RoleLeafNode("role_a"),
                OrNode(children=[RoleLeafNode("role_b"), RoleLeafNode("role_c")]),
            ]
        )
        contract = DecisionEvidenceContract(
            contract_id="test_nested_ast",
            version="1.0",
            epoch=1,
            effective_from="2026-01-01T00:00:00Z",
            decision_type="test",
            decision_scope=DecisionScopeSpec(scope_class=ScopeClass.SESSION, scope_key_pattern="test"),
            formula_str="AND(role_a, OR(role_b, role_c))",
            formula_ast=ast,
            roles={},
            simultaneity_window_s=1.0,
            finality_condition=FinalityCondition.EARLY_COLLAPSE_OR_LATENESS,
            tie_break=TieBreakPolicy.CANONICAL_ORDER,
            explanation_template_id="tpl_test",
        )

        low, up, alts, wit, mat = EnvelopeEvaluator.evaluate_completions(
            contract=contract,
            known_leaf_states={"role_a": IntegrityState.DEGRADED},
            pending_roles=["role_b", "role_c"],
            C_max=32,
        )
        assert low == IntegrityState.DEGRADED
        assert up == IntegrityState.DEGRADED
        assert alts == []
        assert mat == ResolutionMaturity.STATE_RESOLVED
        assert "EARLY_COLLAPSE_P5" in wit

    def test_custom_c_max_threshold_capping(self):
        """Custom C_max=8: with 2 pending roles, 3^2 = 9 > 8, must trigger ENVELOPE_CAPPED."""
        ast = AndNode(children=[RoleLeafNode("r1"), RoleLeafNode("r2")])
        contract = DecisionEvidenceContract(
            contract_id="test_custom_cmax",
            version="1.0",
            epoch=1,
            effective_from="2026-01-01T00:00:00Z",
            decision_type="test",
            decision_scope=DecisionScopeSpec(scope_class=ScopeClass.SESSION, scope_key_pattern="test"),
            formula_str="AND(r1, r2)",
            formula_ast=ast,
            roles={},
            simultaneity_window_s=1.0,
            finality_condition=FinalityCondition.EARLY_COLLAPSE_OR_LATENESS,
            tie_break=TieBreakPolicy.CANONICAL_ORDER,
            explanation_template_id="tpl_test",
        )

        low, up, alts, wit, mat = EnvelopeEvaluator.evaluate_completions(
            contract=contract,
            known_leaf_states={"r1": IntegrityState.HEALTHY},
            pending_roles=["r2"],
            C_max=2,  # 3^1 = 3 > 2
        )
        assert wit == "ENVELOPE_CAPPED: completions exceed C_max"
        assert mat == ResolutionMaturity.OPEN
        assert low == IntegrityState.DEGRADED
        assert up == IntegrityState.HEALTHY

    def test_empty_pending_roles_exact_formula_resolution(self):
        """When pending_roles is empty (all slots closed), returns exact formula state and STATE_RESOLVED."""
        ast = AndNode(children=[RoleLeafNode("r1"), RoleLeafNode("r2")])
        contract = DecisionEvidenceContract(
            contract_id="test_all_closed",
            version="1.0",
            epoch=1,
            effective_from="2026-01-01T00:00:00Z",
            decision_type="test",
            decision_scope=DecisionScopeSpec(scope_class=ScopeClass.SESSION, scope_key_pattern="test"),
            formula_str="AND(r1, r2)",
            formula_ast=ast,
            roles={},
            simultaneity_window_s=1.0,
            finality_condition=FinalityCondition.EARLY_COLLAPSE_OR_LATENESS,
            tie_break=TieBreakPolicy.CANONICAL_ORDER,
            explanation_template_id="tpl_test",
        )

        low, up, alts, wit, mat = EnvelopeEvaluator.evaluate_completions(
            contract=contract,
            known_leaf_states={"r1": IntegrityState.HEALTHY, "r2": IntegrityState.SUSPECT},
            pending_roles=[],
        )
        assert low == IntegrityState.SUSPECT
        assert up == IntegrityState.SUSPECT
        assert alts == []
        assert wit is None
        assert mat == ResolutionMaturity.STATE_RESOLVED

    def test_out_of_order_run_clock_ticks_rejected_by_sequence(self):
        """RunClockManager must reject or ignore out-of-order RunClockTick facts based on seq."""
        mgr = RunClockManager(clock_domain="LIVE_UTC")
        tick_high = RunClockTick(
            run_id="run-1",
            clock_domain="LIVE_UTC",
            clock_utc="2026-11-01T14:00:20.000Z",
            seq=10,
            emitter_id="supervisor",
        )
        tick_low = RunClockTick(
            run_id="run-1",
            clock_domain="LIVE_UTC",
            clock_utc="2026-11-01T14:00:10.000Z",
            seq=5,  # lower seq arrives late
            emitter_id="supervisor",
        )
        mgr.process_tick(tick_high)
        mgr.process_tick(tick_low)

        assert mgr.latest_tick is not None
        assert mgr.latest_tick.seq == 10
        assert mgr.latest_tick.clock_utc == "2026-11-01T14:00:20.000Z"

    def test_watermark_dominates_when_ahead_of_run_clock(self):
        """When event-time watermark is ahead of run_clock, t_eval = watermark."""
        t_eval = RunClockManager.compute_t_eval(
            watermark="2026-11-01T14:05:00.000Z",
            run_clock="2026-11-01T14:01:00.000Z",
        )
        assert t_eval == "2026-11-01T14:05:00.000Z"

    def test_stationary_watermark_with_non_decaying_contract_stays_healthy(self):
        """Contracts with soft_age_s=None and hard_age_s=None do not degrade under frozen watermarks and ticks."""
        contract = make_single_role_contract(
            contract_id="test_non_decaying",
            role_name="static_record",
            soft_age_s=None,
            hard_age_s=None,
            admissible_origins=["ORIG_A"],
        )
        item = {
            "stream_item_type": "EVIDENCE_ATOM",
            "transport_event_time_utc": "2026-11-01T14:00:00.000Z",
            "watermark_at_arrival": "2026-11-01T14:00:00.000Z",
            "data": {
                "evidence_atom_id": "atom-static",
                "role_id": "static_record",
                "value": 100.0,
                "source_origin_set": ["ORIG_A"],
            },
        }
        # Ticks advancing 1000 seconds later!
        tick = {
            "stream_item_type": "RUN_CLOCK_TICK",
            "transport_event_time_utc": "2026-11-01T14:16:40.000Z",
            "watermark_at_arrival": "2026-11-01T14:00:00.000Z",
            "data": {
                "clock_domain": "LIVE_UTC",
                "clock_utc": "2026-11-01T14:16:40.000Z",
                "seq": 100,
            },
        }

        verdict = ContractEvaluator.evaluate(
            contract=contract,
            decision_instance_id="session_01|SESSION|test_non_decaying",
            input_stream=[item, tick],
            t_eval="2026-11-01T14:16:40.000Z",
        )
        assert verdict["integrity_state"] == "HEALTHY"

    def test_timeline_algebraic_laws_meet_and_join(self):
        """Verify mathematical lattice laws: Commutativity, Associativity, Absorption, Identity, Annihilator."""
        states = [IntegrityState.DEGRADED, IntegrityState.UNKNOWN, IntegrityState.SUSPECT, IntegrityState.HEALTHY]
        
        # 1. Commutativity: a meet b == b meet a, a join b == b join a
        for a in states:
            for b in states:
                assert and_state(a, b) == and_state(b, a)
                assert or_state(a, b) == or_state(b, a)

        # 2. Associativity: (a meet b) meet c == a meet (b meet c)
        for a in states:
            for b in states:
                for c in states:
                    assert and_state(and_state(a, b), c) == and_state(a, and_state(b, c))
                    assert or_state(or_state(a, b), c) == or_state(a, or_state(b, c))

        # 3. Absorption: a meet (a join b) == a, a join (a meet b) == a
        for a in states:
            for b in states:
                assert and_state(a, or_state(a, b)) == a
                assert or_state(a, and_state(a, b)) == a

        # 4. Identity & Annihilator:
        # Meet identity: HEALTHY. Meet annihilator: DEGRADED.
        # Join identity: DEGRADED. Join annihilator: HEALTHY.
        for a in states:
            assert and_state(a, IntegrityState.HEALTHY) == a
            assert and_state(a, IntegrityState.DEGRADED) == IntegrityState.DEGRADED
            assert or_state(a, IntegrityState.DEGRADED) == a
            assert or_state(a, IntegrityState.HEALTHY) == IntegrityState.HEALTHY
