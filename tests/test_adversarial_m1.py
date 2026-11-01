"""Adversarial Challenge & Stress Verification Suite for Milestone 1.

Authors: challenger_m1_2 (EMPIRICAL CHALLENGER - critic, specialist)
Scope: DEC Schema v1, AST Formula Parser, Compile-Time Validator, and 20 Golden Fixture Families.

Tests adversarial stress scenarios:
- Malformed/illegal formulas (syntax, arity, bounds, nesting depth, recursion limits)
- Malformed/illegal DEC definitions (orphan roles, impossible fault budgets, unit mismatches, negative freshness)
- Defect D-15 single upstream origin bypasses and overclaims
- Golden fixture event streams, monotonic timestamps, JSON schema conformance, and RFC 8785 hash vectors
- Discrepancy checks against worker claims
"""
from __future__ import annotations

import copy
import json
from pathlib import Path
import pytest
import yaml

from sia_core.contracts.models import (
    AndNode,
    CorroborationMode,
    DecisionEvidenceContract,
    KOfNNode,
    MetricType,
    OrNode,
    RoleLeafNode,
    ScopeClass,
    TimeClass,
)
from sia_core.contracts.parser import (
    ContractParseError,
    FormulaParseError,
    load_contract_file,
    parse_contract_dict,
    parse_formula,
)
from sia_core.contracts.validator import (
    ValidationError,
    validate_contract,
)
from sia_core.identity.hasher import compute_verdict_hash

DEFINITIONS_DIR = Path("sia_core/contracts/definitions")
FIXTURES_DIR = Path("tests/fixtures")


@pytest.fixture
def base_contract_dict():
    """Load a valid base contract dict for mutation testing."""
    with open(DEFINITIONS_DIR / "dec_01_lap_time_consistency.yaml", "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# ==============================================================================
# Suite 1: Formula Parser Adversarial Stress & Edge Cases
# ==============================================================================

class TestFormulaParserAdversarial:
    """Stress tests and boundary condition exploration for formula tokenizer & parser."""

    @pytest.mark.parametrize("malformed_expr", [
        "",
        "   ",
        "\t\n",
        "AND",
        "OR",
        "k_of_n",
        "AND()",
        "OR()",
        "k_of_n()",
        "k_of_n(2)",
        "AND(role_a",
        "AND(role_a, role_b))",
        "AND(role_a, , role_b)",
        "AND(role_a, role_b, )",
        "AND(role_a role_b)",
        "AND(role_a, 123)",
        "123",
        "(role_a AND role_b)",
        "AND((role_a, role_b))",
        "AND(role_a, role_b) ; DROP TABLE decs;",
        "k_of_n(role_a, role_b, role_c)",
        "k_of_n(-1, role_a, role_b)",
        "k_of_n(2.5, role_a, role_b)",
        "AND(role_a, \x00)",
        "AND(role_a, role_b) extra_token",
    ])
    def test_parser_rejects_malformed_syntax(self, malformed_expr: str):
        """Verify parser raises FormulaParseError on corrupt or non-conformant grammar."""
        with pytest.raises(FormulaParseError):
            parse_formula(malformed_expr)

    def test_parser_nested_formula_success(self):
        """Verify deeply nested but valid formula parses up to moderate depth."""
        # 50 levels of AND
        expr = "role_0"
        for i in range(1, 51):
            expr = f"AND({expr}, role_{i})"
        ast = parse_formula(expr)
        assert isinstance(ast, AndNode)
        assert len(ast.get_referenced_roles()) == 51

    def test_parser_recursion_limit_handled_gracefully(self):
        """Deep nesting (depth=500) should raise FormulaParseError or ContractParseError, not unhandled RecursionError."""
        expr = "r"
        for _ in range(500):
            expr = f"AND({expr}, r)"
        try:
            parse_formula(expr)
        except RecursionError:
            pytest.fail("Parser crashed with unhandled RecursionError")
        except FormulaParseError:
            pass  # Expected graceful failure


# ==============================================================================
# Suite 2: Contract Validator Adversarial Challenge
# ==============================================================================

class TestContractValidatorAdversarial:
    """Adversarial stress-testing of Rules V-01 through V-10."""

    # --------------------------------------------------------------------------
    # V-01: Role Identifier Bijective Consistency
    # --------------------------------------------------------------------------
    def test_multiple_orphan_and_undefined_roles(self, base_contract_dict):
        """Simultaneous multiple orphan roles in 'roles' and undefined roles in 'formula'."""
        data = copy.deepcopy(base_contract_dict)
        data["formula"] = "AND(lap_record, undefined_role_x, undefined_role_y)"
        data["roles"]["orphan_role_1"] = copy.deepcopy(data["roles"]["lap_record"])
        data["roles"]["orphan_role_2"] = copy.deepcopy(data["roles"]["lap_record"])

        contract = parse_contract_dict(data)
        res = validate_contract(contract)
        assert not res.is_valid
        orphan_codes = [e.role for e in res.errors if e.code == "E_V01_ORPHAN_ROLE"]
        undef_codes = [e.role for e in res.errors if e.code == "E_V01_UNDEFINED_ROLE"]
        assert set(orphan_codes) == {"orphan_role_1", "orphan_role_2", "lap_start_reference"}
        assert set(undef_codes) == {"undefined_role_x", "undefined_role_y"}

    # --------------------------------------------------------------------------
    # V-02: Formula Arity and Thresholds
    # --------------------------------------------------------------------------
    def test_k_of_n_threshold_zero_rejected(self, base_contract_dict):
        """k=0 in k_of_n must be rejected by Rule V-02."""
        data = copy.deepcopy(base_contract_dict)
        data["formula"] = "k_of_n(0, lap_record, lap_start_reference)"
        contract = parse_contract_dict(data)
        res = validate_contract(contract)
        assert not res.is_valid
        assert any(e.code == "E_V02_INVALID_K_THRESHOLD" for e in res.errors)

    def test_k_of_n_threshold_exceeding_n_rejected(self, base_contract_dict):
        """k > n in k_of_n must be rejected by Rule V-02."""
        data = copy.deepcopy(base_contract_dict)
        data["formula"] = "k_of_n(4, lap_record, lap_start_reference)"
        contract = parse_contract_dict(data)
        res = validate_contract(contract)
        assert not res.is_valid
        assert any(e.code == "E_V02_INVALID_K_THRESHOLD" for e in res.errors)

    def test_and_operator_single_child_rejected(self, base_contract_dict):
        """AND with arity 1 must be rejected by Rule V-02."""
        data = copy.deepcopy(base_contract_dict)
        data["formula"] = "AND(lap_record)"
        data["roles"].pop("lap_start_reference")
        contract = parse_contract_dict(data)
        res = validate_contract(contract)
        assert not res.is_valid
        assert any(e.code == "E_V02_INVALID_ARITY" for e in res.errors)

    def test_or_operator_single_child_rejected(self, base_contract_dict):
        """OR with arity 1 must be rejected by Rule V-02."""
        data = copy.deepcopy(base_contract_dict)
        data["formula"] = "OR(lap_record)"
        data["roles"].pop("lap_start_reference")
        contract = parse_contract_dict(data)
        res = validate_contract(contract)
        assert not res.is_valid
        assert any(e.code == "E_V02_INVALID_ARITY" for e in res.errors)

    # --------------------------------------------------------------------------
    # V-03: Physical Unit & Metric Compatibility
    # --------------------------------------------------------------------------
    @pytest.mark.parametrize("metric,unit", [
        ("abs_diff", "boolean"),
        ("abs_diff", "enum"),
        ("abs_diff", "string"),
        ("abs_diff", "flags"),
        ("relative_diff", "boolean"),
        ("relative_diff", "enum"),
        ("temporal_delta", "enum"),
        ("temporal_delta", "boolean"),
    ])
    def test_continuous_metric_discrete_unit_rejected(self, base_contract_dict, metric, unit):
        """Continuous metrics with discrete units must be rejected by Rule V-03."""
        data = copy.deepcopy(base_contract_dict)
        data["roles"]["lap_record"]["agreement"]["metric"] = metric
        data["roles"]["lap_record"]["agreement"]["unit"] = unit
        contract = parse_contract_dict(data)
        res = validate_contract(contract)
        assert not res.is_valid
        assert any(e.code == "E_V03_INCOMPATIBLE_UNIT" for e in res.errors)

    @pytest.mark.parametrize("eta", [-0.001, -1.0, -100.0])
    def test_negative_tolerance_rejected(self, base_contract_dict, eta):
        """Negative agreement tolerance eta must be rejected by Rule V-03."""
        data = copy.deepcopy(base_contract_dict)
        data["roles"]["lap_record"]["agreement"]["tolerance_eta"] = eta
        contract = parse_contract_dict(data)
        res = validate_contract(contract)
        assert not res.is_valid
        assert any(e.code == "E_V03_INVALID_TOLERANCE" for e in res.errors)

    def test_discrete_equality_non_zero_tolerance_rejected(self, base_contract_dict):
        """Discrete equality with non-zero tolerance eta violates Rule V-03 specification."""
        data = copy.deepcopy(base_contract_dict)
        data["roles"]["lap_record"]["agreement"]["metric"] = "discrete_equality"
        data["roles"]["lap_record"]["agreement"]["unit"] = "enum"
        data["roles"]["lap_record"]["agreement"]["tolerance_eta"] = 5.0
        contract = parse_contract_dict(data)
        res = validate_contract(contract)
        assert not res.is_valid, "Validator should reject non-zero tolerance for discrete equality"

    # --------------------------------------------------------------------------
    # V-04 & V-05: Corroboration & Fault Model Admissibility
    # --------------------------------------------------------------------------
    def test_impossible_source_fault_budget(self, base_contract_dict):
        """f_src >= n_src must be rejected."""
        data = copy.deepcopy(base_contract_dict)
        data["roles"]["lap_record"]["admissible_source_origins"] = ["SRC_A", "SRC_B"]
        data["roles"]["lap_record"]["fault_model"]["source_fault_budget"] = 2  # N=2, f=2
        contract = parse_contract_dict(data)
        res = validate_contract(contract)
        assert not res.is_valid
        assert any(e.code == "E_V04_IMPOSSIBLE_SOURCE_FAULT_BUDGET" for e in res.errors)

    def test_zero_source_origins_with_positive_fault_budget(self, base_contract_dict):
        """An empty source origins list with positive fault budget is impossible and must fail."""
        data = copy.deepcopy(base_contract_dict)
        data["roles"]["lap_record"]["admissible_source_origins"] = []
        data["roles"]["lap_record"]["fault_model"]["source_fault_budget"] = 1
        contract = parse_contract_dict(data)
        res = validate_contract(contract)
        assert not res.is_valid, "Empty source origins with positive fault budget should be rejected"

    def test_zero_processing_origins_with_positive_fault_budget(self, base_contract_dict):
        """An empty processing origins list with positive fault budget is impossible and must fail."""
        data = copy.deepcopy(base_contract_dict)
        data["roles"]["lap_record"]["admissible_processing_origins"] = []
        data["roles"]["lap_record"]["fault_model"]["processing_fault_budget"] = 1
        contract = parse_contract_dict(data)
        res = validate_contract(contract)
        assert not res.is_valid, "Empty processing origins with positive fault budget should be rejected"

    def test_worker_claim_discrepancy_lamport_vs_pigeonhole(self, base_contract_dict):
        """Audit test checking worker claim in handoff.md (N >= 2f + 1) vs code (N >= f + 1).

        Worker handoff.md claims: 'V-04 (Source fault model admissibility N_src >= 2 f_src + 1)'.
        If N=2 and f=1, 2 < 2(1)+1, so under worker's claimed rule this would fail.
        In reality, the code enforces pigeonhole N >= f + 1, so N=2, f=1 passes.
        """
        data = copy.deepcopy(base_contract_dict)
        data["roles"]["lap_record"]["admissible_source_origins"] = ["SRC_A", "SRC_B"]
        data["roles"]["lap_record"]["fault_model"]["source_fault_budget"] = 1
        contract = parse_contract_dict(data)
        res = validate_contract(contract)
        # Empirical fact: it passes, demonstrating worker's handoff claim is inaccurate
        assert res.is_valid

    # --------------------------------------------------------------------------
    # V-08: Freshness Ordering
    # --------------------------------------------------------------------------
    def test_soft_age_greater_than_hard_age_rejected(self, base_contract_dict):
        """soft_age_s > hard_age_s must be rejected by Rule V-08."""
        data = copy.deepcopy(base_contract_dict)
        data["roles"]["lap_record"]["freshness"]["soft_age_s"] = 120.0
        data["roles"]["lap_record"]["freshness"]["hard_age_s"] = 60.0
        contract = parse_contract_dict(data)
        res = validate_contract(contract)
        assert not res.is_valid
        assert any(e.code == "E_V08_INVALID_FRESHNESS_ORDER" for e in res.errors)

    def test_negative_latency_allowance_rejected(self, base_contract_dict):
        """Negative latency_allowance_s must be rejected."""
        data = copy.deepcopy(base_contract_dict)
        data["roles"]["lap_record"]["freshness"]["latency_allowance_s"] = -1.0
        contract = parse_contract_dict(data)
        res = validate_contract(contract)
        assert not res.is_valid
        assert any(e.code == "E_V08_INVALID_FRESHNESS_ORDER" for e in res.errors)

    def test_negative_soft_age_alone_rejected(self, base_contract_dict):
        """Negative soft_age_s with hard_age_s=None must be rejected."""
        data = copy.deepcopy(base_contract_dict)
        data["roles"]["lap_record"]["freshness"]["soft_age_s"] = -10.0
        data["roles"]["lap_record"]["freshness"]["hard_age_s"] = None
        contract = parse_contract_dict(data)
        res = validate_contract(contract)
        assert not res.is_valid, "Negative soft_age_s must be rejected even when hard_age_s is None"

    # --------------------------------------------------------------------------
    # V-09: Slot Boundedness
    # --------------------------------------------------------------------------
    @pytest.mark.parametrize("deadline,lateness,obs", [
        (0.0, 10.0, 1),
        (-5.0, 10.0, 1),
        (10.0, -1.0, 1),
        (10.0, 10.0, 0),
        (10.0, 10.0, -1),
    ])
    def test_invalid_slot_parameters_rejected(self, base_contract_dict, deadline, lateness, obs):
        """Non-positive deadline, negative lateness, or obs < 1 must be rejected by Rule V-09."""
        data = copy.deepcopy(base_contract_dict)
        data["roles"]["lap_record"]["slots"]["deadline_s"] = deadline
        data["roles"]["lap_record"]["slots"]["allowed_lateness_s"] = lateness
        data["roles"]["lap_record"]["slots"]["max_obs_per_slot"] = obs
        contract = parse_contract_dict(data)
        res = validate_contract(contract)
        assert not res.is_valid
        assert any(e.code == "E_V09_INVALID_SLOT_BOUNDS" for e in res.errors)

    # --------------------------------------------------------------------------
    # V-10: Single Upstream F1 Timing Origin (Defect D-15 Compliance)
    # --------------------------------------------------------------------------
    def test_free_feed_mandatory_source_overclaim_rejected(self, base_contract_dict):
        """Free feed contract claiming source: MANDATORY must be rejected by Rule V-10."""
        data = copy.deepcopy(base_contract_dict)
        data["roles"]["lap_record"]["admissible_source_origins"] = [
            "TRACINGINSIGHTS_LAPS_A",
            "TRACINGINSIGHTS_LAPS_B",
        ]
        data["roles"]["lap_record"]["corroboration"]["source"] = "MANDATORY"
        data["roles"]["lap_record"]["corroboration"]["source_min_depth"] = 1
        contract = parse_contract_dict(data)
        res = validate_contract(contract)
        assert not res.is_valid
        assert any(e.code == "E_V10_D15_FREE_FEED_INDEPENDENCE_OVERCLAIM" for e in res.errors)

    def test_free_feed_source_fault_budget_overclaim_rejected(self, base_contract_dict):
        """Free feed contract claiming source_fault_budget > 0 must be rejected by Rule V-10."""
        data = copy.deepcopy(base_contract_dict)
        data["roles"]["lap_record"]["admissible_source_origins"] = [
            "PITWALL_A",
            "PITWALL_B",
        ]
        data["roles"]["lap_record"]["fault_model"]["source_fault_budget"] = 1
        contract = parse_contract_dict(data)
        res = validate_contract(contract)
        assert not res.is_valid
        assert any(e.code == "E_V10_D15_FREE_FEED_INDEPENDENCE_OVERCLAIM" for e in res.errors)

    def test_free_feed_missing_d15_witness_rejected(self, base_contract_dict):
        """Free feed contract omitting mandatory limitation witness must be rejected."""
        data = copy.deepcopy(base_contract_dict)
        data["limitations"] = ["SOME_OTHER_LIMITATION"]
        contract = parse_contract_dict(data)
        res = validate_contract(contract)
        assert not res.is_valid
        assert any(e.code == "E_V10_D15_MISSING_COMMON_CAUSE_LIMITATION" for e in res.errors)

    def test_free_feed_case_sensitivity_bypass_prevented(self, base_contract_dict):
        """Lowercase or mixed-case origin prefixes like 'pitwall_' must NOT bypass Rule V-10."""
        data = copy.deepcopy(base_contract_dict)
        data["roles"]["lap_record"]["admissible_source_origins"] = [
            "pitwall_timing_a",
            "pitwall_timing_b",
        ]
        data["roles"]["lap_record"]["corroboration"]["source"] = "MANDATORY"
        data["roles"]["lap_record"]["corroboration"]["source_min_depth"] = 1
        data["roles"]["lap_record"]["fault_model"]["source_fault_budget"] = 1

        data["roles"]["lap_start_reference"]["admissible_source_origins"] = [
            "tracinginsights_laps_a",
            "tracinginsights_laps_b",
        ]
        data["roles"]["lap_start_reference"]["corroboration"]["source"] = "MANDATORY"
        data["roles"]["lap_start_reference"]["corroboration"]["source_min_depth"] = 1
        data["roles"]["lap_start_reference"]["fault_model"]["source_fault_budget"] = 1

        data["limitations"] = []  # Omit D-15 witness too
        contract = parse_contract_dict(data)
        res = validate_contract(contract)
        assert not res.is_valid, "Lowercase free feed origin should still trigger D-15 check"


# ==============================================================================
# Suite 3: Golden Fixture Invariants & Boundary Stress
# ==============================================================================

@pytest.fixture(scope="module")
def all_fixtures():
    fixtures = {}
    for i in range(1, 21):
        fx_id = f"FX-{i:02d}"
        matches = list(FIXTURES_DIR.glob(f"{fx_id}_*.json"))
        assert len(matches) == 1, f"Missing or duplicate fixture file for {fx_id}"
        with open(matches[0], "r", encoding="utf-8") as f:
            fixtures[fx_id] = json.load(f)
    return fixtures


class TestGoldenFixturesAdversarial:
    """Invariant stress testing across all 20 Golden Fixture Families (FX-01 through FX-20)."""

    def test_fixture_event_sequence_timestamp_strict_monotonicity(self, all_fixtures):
        """Assert every event in input_stream has non-decreasing timestamp."""
        for fx_id, fixture in all_fixtures.items():
            prev_time = ""
            for idx, ev in enumerate(fixture["input_stream"]):
                t = ev["transport_event_time_utc"]
                assert len(t) >= 19, f"{fx_id} event {idx} has invalid timestamp string: {t}"
                if prev_time:
                    assert t >= prev_time, f"{fx_id} monotonicity regression at step {idx}: {t} < {prev_time}"
                prev_time = t

    def test_fixture_evaluation_schedule_chronological_validity(self, all_fixtures):
        """Assert evaluation schedule t_eval >= event transport time at after_step_index."""
        for fx_id, fixture in all_fixtures.items():
            stream = fixture["input_stream"]
            schedule = fixture["evaluation_schedule"]
            for s in schedule:
                step_idx = s["after_step_index"]
                assert step_idx < len(stream), f"{fx_id} after_step_index {step_idx} exceeds stream size"
                ev_time = stream[step_idx]["transport_event_time_utc"]
                t_eval = s["t_eval"]
                assert t_eval >= ev_time, f"{fx_id} t_eval {t_eval} precedes step event time {ev_time}"

    def test_fixture_support_atom_ids_exist_in_input_stream(self, all_fixtures):
        """Assert every atom ID claimed in expected_verdicts support_atom_ids exists in input_stream."""
        for fx_id, fixture in all_fixtures.items():
            stream_atom_ids = set()
            for ev in fixture["input_stream"]:
                ev_data = ev.get("data", {})
                for k in ("evidence_atom_id", "fact_id", "raw_event_id"):
                    if k in ev_data:
                        stream_atom_ids.add(ev_data[k])

            for v_idx, v in enumerate(fixture["expected_verdicts"]):
                for role_name, profile in v.get("corroboration_profile_by_role", {}).items():
                    for atom_id in profile.get("support_atom_ids", []):
                        assert atom_id in stream_atom_ids, (
                            f"{fx_id} verdict[{v_idx}] role '{role_name}' references support_atom_id "
                            f"'{atom_id}' not found in input_stream!"
                        )

    def test_fixture_verdict_hashes_reproducible_and_unique(self, all_fixtures):
        """Recompute verdict hashes using sia_core.identity.hasher and verify bit-for-bit parity."""
        all_computed_hashes = []
        for fx_id, fixture in all_fixtures.items():
            for v in fixture["expected_verdicts"]:
                contract = v["contract"]
                frontier = v["knowledge_frontier"]
                canonical_frontier = {
                    "run_clock": frontier["run_clock"],
                    "watermark": frontier.get("watermark"),
                }
                all_support = sorted(list(set(
                    aid for r in v["corroboration_profile_by_role"].values() for aid in r.get("support_atom_ids", [])
                )))
                all_reference = sorted(list(set(
                    aid for r_name, r in v["corroboration_profile_by_role"].items()
                    if "reference" in r_name for aid in r.get("support_atom_ids", [])
                )))

                h = compute_verdict_hash(
                    contract_id=contract["id"],
                    contract_version=contract["version"],
                    contract_epoch=contract["epoch"],
                    decision_instance_id=v["decision_instance_id"],
                    integrity_state=v["integrity_state"],
                    resolution_maturity=v["resolution_maturity"],
                    finality_status=v["finality_status"],
                    canonical_frontier=canonical_frontier,
                    corroboration_profile_by_role=v["corroboration_profile_by_role"],
                    validity_timeline=v["validity_timeline"],
                    sorted_support_atom_ids=all_support,
                    sorted_reference_atom_ids=all_reference,
                )
                assert h == v["verdict_hash"], f"{fx_id} verdict hash mismatch!"
                all_computed_hashes.append(h)

        # 21 verdicts total (FX-14 has 2 verdicts)
        assert len(all_computed_hashes) == 21
        # All computed hashes must be 64-character lowercase hex
        for h in all_computed_hashes:
            assert len(h) == 64
            int(h, 16)

    def test_fx14_backward_linked_revision_chain(self, all_fixtures):
        """Verify FX-14 late revision forms a cryptographic chain: prev_hash(v2) == hash(v1)."""
        fx14 = all_fixtures["FX-14"]
        v1, v2 = fx14["expected_verdicts"]
        assert v1["revision_number"] == 1
        assert v1["previous_verdict_hash"] is None
        assert v2["revision_number"] == 2
        assert v2["previous_verdict_hash"] == v1["verdict_hash"]
        assert v2["verdict_hash"] != v1["verdict_hash"]

    def test_fx19_clique_overflow_scale(self, all_fixtures):
        """Verify FX-19 pins M_max=10 bound with 12 conflicting evidence atoms."""
        fx19 = all_fixtures["FX-19"]
        assert fx19["defect_reference"] == "D-18"
        assert len(fx19["input_stream"]) == 12
        v = fx19["expected_verdicts"][0]
        assert v["integrity_state"] == "UNKNOWN"
        assert "SEARCH_BUDGET_EXCEEDED" in v["primary_binding_witness"]
