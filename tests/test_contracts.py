"""Test Suite for DEC Schema v1, AST Models, Parser, and Compile-Time Validator."""
from __future__ import annotations

import copy
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
    FormulaParseError,
    load_contract_file,
    load_contract_yaml,
    parse_contract_dict,
    parse_formula,
)
from sia_core.contracts.validator import (
    ContractValidationError,
    validate_contract,
)

DEFINITIONS_DIR = Path("e:/CALIBER/sia_core/contracts/definitions")
CONTRACT_FILES = sorted(list(DEFINITIONS_DIR.glob("dec_*.yaml")))


def test_definitions_directory_has_11_contracts():
    """Verify that all 11 required contract files exist."""
    assert len(CONTRACT_FILES) == 11
    expected_ids = [
        "dec_01_lap_time_consistency",
        "dec_02_telemetry_session_alignment",
        "dec_03_race_control_incident_timing",
        "dec_04_session_status_transition",
        "dec_05_pit_lane_timing_evidence",
        "dec_06_weather_strategy_advisory",
        "dec_07_tyre_condition_advisory",
        "dec_08_source_integrity_assessment",
        "dec_09_pit_entry_speed_evidence",
        "dec_10_vehicle_sensor_anomaly",
        "dec_11_track_limit_crossing",
    ]
    actual_stems = [f.stem for f in CONTRACT_FILES]
    assert actual_stems == expected_ids


@pytest.mark.parametrize("contract_path", CONTRACT_FILES, ids=[f.stem for f in CONTRACT_FILES])
def test_all_contracts_load_and_validate_cleanly(contract_path: Path):
    """Assert all 11 production contracts parse and validate with zero errors."""
    contract = load_contract_file(contract_path)
    assert isinstance(contract, DecisionEvidenceContract)

    result = validate_contract(contract, raise_on_error=False)
    assert result.is_valid, f"Contract {contract_path.name} failed validation: {result.errors}"
    assert len(result.errors) == 0


def test_grounded_contract_count_exceeds_gate():
    """Verify that at least 7 contracts are grounded in Class A or Class H feeds."""
    grounded_count = 0
    for contract_path in CONTRACT_FILES:
        contract = load_contract_file(contract_path)
        is_synthetic = any("SYNTHETIC" in lim for lim in contract.limitations)
        if not is_synthetic:
            grounded_count += 1

    # Exactly 10 base contracts are grounded, 1 synthetic (track_limit_crossing)
    assert grounded_count >= 7
    assert grounded_count == 10


class TestFormulaParser:
    """Unit tests for recursive descent formula parser."""

    def test_parse_simple_and(self):
        ast = parse_formula("AND(lap_record, lap_start_reference)")
        assert isinstance(ast, AndNode)
        assert len(ast.children) == 2
        assert isinstance(ast.children[0], RoleLeafNode)
        assert ast.children[0].role_name == "lap_record"
        assert ast.children[1].role_name == "lap_start_reference"
        assert ast.get_referenced_roles() == {"lap_record", "lap_start_reference"}

    def test_parse_nested_or(self):
        ast = parse_formula("OR(role_a, AND(role_b, role_c))")
        assert isinstance(ast, OrNode)
        assert len(ast.children) == 2
        assert ast.get_referenced_roles() == {"role_a", "role_b", "role_c"}

    def test_parse_k_of_n(self):
        ast = parse_formula("k_of_n(2, sensor_1, sensor_2, sensor_3)")
        assert isinstance(ast, KOfNNode)
        assert ast.k == 2
        assert len(ast.children) == 3
        assert ast.get_referenced_roles() == {"sensor_1", "sensor_2", "sensor_3"}

    def test_parse_empty_formula_raises(self):
        with pytest.raises(FormulaParseError):
            parse_formula("")

    def test_parse_unbalanced_parentheses_raises(self):
        with pytest.raises(FormulaParseError):
            parse_formula("AND(role_a, role_b")


class TestValidatorRules:
    """Negative and boundary tests for compile-time validator rules V-01 through V-10."""

    @pytest.fixture
    def base_contract_dict(self):
        with open(DEFINITIONS_DIR / "dec_01_lap_time_consistency.yaml", "r", encoding="utf-8") as f:
            return yaml.safe_load(f)

    def test_v01_orphan_role(self, base_contract_dict):
        # Add extra role to roles that is not in formula
        data = copy.deepcopy(base_contract_dict)
        data["roles"]["orphan_role"] = copy.deepcopy(data["roles"]["lap_record"])
        contract = parse_contract_dict(data)
        res = validate_contract(contract)
        assert not res.is_valid
        assert any(e.code == "E_V01_ORPHAN_ROLE" and e.role == "orphan_role" for e in res.errors)

    def test_v01_undefined_role(self, base_contract_dict):
        # Add role to formula that is not in roles
        data = copy.deepcopy(base_contract_dict)
        data["formula"] = "AND(lap_record, undefined_role)"
        contract = parse_contract_dict(data)
        res = validate_contract(contract)
        assert not res.is_valid
        assert any(e.code == "E_V01_UNDEFINED_ROLE" and e.role == "undefined_role" for e in res.errors)

    def test_v02_invalid_operator_arity(self, base_contract_dict):
        # AND with 1 argument
        data = copy.deepcopy(base_contract_dict)
        data["formula"] = "AND(lap_record)"
        data["roles"].pop("lap_start_reference")
        contract = parse_contract_dict(data)
        res = validate_contract(contract)
        assert not res.is_valid
        assert any(e.code == "E_V02_INVALID_ARITY" for e in res.errors)

    def test_v02_invalid_k_threshold(self, base_contract_dict):
        # k=3 with 2 children
        data = copy.deepcopy(base_contract_dict)
        data["formula"] = "k_of_n(3, lap_record, lap_start_reference)"
        contract = parse_contract_dict(data)
        res = validate_contract(contract)
        assert not res.is_valid
        assert any(e.code == "E_V02_INVALID_K_THRESHOLD" for e in res.errors)

    def test_v03_incompatible_unit(self, base_contract_dict):
        # Continuous metric on discrete unit
        data = copy.deepcopy(base_contract_dict)
        data["roles"]["lap_record"]["agreement"]["metric"] = "abs_diff"
        data["roles"]["lap_record"]["agreement"]["unit"] = "boolean"
        contract = parse_contract_dict(data)
        res = validate_contract(contract)
        assert not res.is_valid
        assert any(e.code == "E_V03_INCOMPATIBLE_UNIT" for e in res.errors)

    def test_v03_negative_tolerance(self, base_contract_dict):
        data = copy.deepcopy(base_contract_dict)
        data["roles"]["lap_record"]["agreement"]["tolerance_eta"] = -0.05
        contract = parse_contract_dict(data)
        res = validate_contract(contract)
        assert not res.is_valid
        assert any(e.code == "E_V03_INVALID_TOLERANCE" for e in res.errors)

    def test_v04_impossible_source_fault_budget(self, base_contract_dict):
        data = copy.deepcopy(base_contract_dict)
        # 1 origin configured, budget = 1 (requires >= 2)
        data["roles"]["lap_record"]["fault_model"]["source_fault_budget"] = 1
        contract = parse_contract_dict(data)
        res = validate_contract(contract)
        assert not res.is_valid
        assert any(e.code == "E_V04_IMPOSSIBLE_SOURCE_FAULT_BUDGET" for e in res.errors)

    def test_v04_impossible_source_depth(self, base_contract_dict):
        data = copy.deepcopy(base_contract_dict)
        data["roles"]["lap_record"]["corroboration"]["source"] = "MANDATORY"
        data["roles"]["lap_record"]["corroboration"]["source_min_depth"] = 1  # 1 origin, requires >= 2
        contract = parse_contract_dict(data)
        res = validate_contract(contract)
        assert not res.is_valid
        assert any(e.code == "E_V04_IMPOSSIBLE_SOURCE_DEPTH" for e in res.errors)

    def test_v05_impossible_processing_depth(self, base_contract_dict):
        data = copy.deepcopy(base_contract_dict)
        data["roles"]["lap_record"]["corroboration"]["processing"] = "MANDATORY"
        data["roles"]["lap_record"]["corroboration"]["processing_min_depth"] = 1  # 1 parser, requires >= 2
        contract = parse_contract_dict(data)
        res = validate_contract(contract)
        assert not res.is_valid
        assert any(e.code == "E_V05_IMPOSSIBLE_PROCESSING_DEPTH" for e in res.errors)

    def test_v07_tolerance_below_completeness_warning(self, base_contract_dict):
        data = copy.deepcopy(base_contract_dict)
        # eps_h = 0.05, eta = 0.05 < 2 * 0.05 (0.10)
        data["roles"]["lap_record"]["agreement"]["honest_noise_epsilon_h"] = 0.05
        data["roles"]["lap_record"]["agreement"]["tolerance_eta"] = 0.05
        contract = parse_contract_dict(data)
        res = validate_contract(contract)
        assert any(w.code == "W_V07_TOLERANCE_BELOW_COMPLETENESS" for w in res.warnings)

    def test_v08_invalid_freshness_order(self, base_contract_dict):
        data = copy.deepcopy(base_contract_dict)
        data["roles"]["lap_record"]["freshness"]["soft_age_s"] = 60.0
        data["roles"]["lap_record"]["freshness"]["hard_age_s"] = 30.0  # soft > hard!
        contract = parse_contract_dict(data)
        res = validate_contract(contract)
        assert not res.is_valid
        assert any(e.code == "E_V08_INVALID_FRESHNESS_ORDER" for e in res.errors)

    def test_v09_invalid_slot_deadline(self, base_contract_dict):
        data = copy.deepcopy(base_contract_dict)
        data["roles"]["lap_record"]["slots"]["deadline_s"] = 0.0
        contract = parse_contract_dict(data)
        res = validate_contract(contract)
        assert not res.is_valid
        assert any(e.code == "E_V09_INVALID_SLOT_BOUNDS" for e in res.errors)

    def test_v10_free_feed_overclaim(self, base_contract_dict):
        data = copy.deepcopy(base_contract_dict)
        # Free feed TRACINGINSIGHTS claiming MANDATORY source depth
        data["roles"]["lap_record"]["admissible_source_origins"] = ["TRACINGINSIGHTS_LAPS", "TRACINGINSIGHTS_BACKUP"]
        data["roles"]["lap_record"]["corroboration"]["source"] = "MANDATORY"
        data["roles"]["lap_record"]["corroboration"]["source_min_depth"] = 1
        contract = parse_contract_dict(data)
        res = validate_contract(contract)
        assert not res.is_valid
        assert any(e.code == "E_V10_D15_FREE_FEED_INDEPENDENCE_OVERCLAIM" for e in res.errors)

    def test_v10_missing_common_cause_witness(self, base_contract_dict):
        data = copy.deepcopy(base_contract_dict)
        # Remove common cause witness from free feed contract
        data["limitations"] = ["SOME_OTHER_LIMITATION"]
        contract = parse_contract_dict(data)
        res = validate_contract(contract)
        assert not res.is_valid
        assert any(e.code == "E_V10_D15_MISSING_COMMON_CAUSE_LIMITATION" for e in res.errors)

    def test_raise_on_error(self, base_contract_dict):
        data = copy.deepcopy(base_contract_dict)
        data["roles"]["lap_record"]["slots"]["deadline_s"] = -1.0
        contract = parse_contract_dict(data)
        with pytest.raises(ContractValidationError):
            validate_contract(contract, raise_on_error=True)
