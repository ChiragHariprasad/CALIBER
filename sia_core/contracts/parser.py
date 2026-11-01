"""DEC Schema v1 Parser and AST Builder.

Parses YAML / JSON contract definitions and boolean formula grammar into
immutable DecisionEvidenceContract AST representations.
"""
from __future__ import annotations

import json
from pathlib import Path
import re
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

import yaml

from .models import (
    AgreementMetric,
    AgreementSpec,
    AndNode,
    CorroborationMode,
    CorroborationSpec,
    DecisionEvidenceContract,
    DecisionScopeSpec,
    FaultModelSpec,
    FinalityCondition,
    FormulaNode,
    FreshnessSpec,
    KOfNNode,
    MetricType,
    OrNode,
    RoleLeafNode,
    RoleSpec,
    ScopeClass,
    SlotSpec,
    TieBreakPolicy,
    TimeClass,
)


class FormulaParseError(ValueError):
    """Raised when formula string parsing encounters malformed grammar or syntax."""
    pass


class ContractParseError(ValueError):
    """Raised when raw contract data dictionary is missing required fields or has invalid structure."""
    pass


# ------------------------------------------------------------------------------
# Formula Lexer and Recursive Descent Parser
# ------------------------------------------------------------------------------

def _tokenize_formula(s: str) -> List[Tuple[str, str, int]]:
    """Tokenize boolean formula string into (TYPE, VALUE, POS) tuples."""
    tokens: List[Tuple[str, str, int]] = []
    pos = 0
    n = len(s)
    while pos < n:
        c = s[pos]
        if c.isspace():
            pos += 1
            continue
        if c in "(),":
            tokens.append((c, c, pos))
            pos += 1
            continue
        m_ident = re.match(r"[a-zA-Z_][a-zA-Z0-9_]*", s[pos:])
        if m_ident:
            val = m_ident.group(0)
            tokens.append(("IDENT", val, pos))
            pos += m_ident.end()
            continue
        m_int = re.match(r"[0-9]+", s[pos:])
        if m_int:
            val = m_int.group(0)
            tokens.append(("INT", val, pos))
            pos += m_int.end()
            continue
        raise FormulaParseError(f"Unexpected character in formula at position {pos}: '{c}'")
    tokens.append(("EOF", "", n))
    return tokens


class _FormulaParser:
    def __init__(self, formula_str: str):
        self.formula_str = formula_str
        self.tokens = _tokenize_formula(formula_str)
        self.index = 0
        self.depth = 0

    def cur(self) -> Tuple[str, str, int]:
        return self.tokens[self.index]

    def peek(self) -> str:
        return self.tokens[self.index][0]

    def eat(self, expected_type: str) -> Tuple[str, str, int]:
        tok = self.cur()
        if tok[0] != expected_type:
            raise FormulaParseError(
                f"Expected '{expected_type}' but found '{tok[1]}' at position {tok[2]} in formula: '{self.formula_str}'"
            )
        self.index += 1
        return tok

    def parse_expr(self) -> FormulaNode:
        if self.depth >= 100:
            raise FormulaParseError("Formula expression exceeds maximum allowable recursion depth (100)")
        self.depth += 1
        try:
            tok = self.cur()
            if tok[0] != "IDENT":
                raise FormulaParseError(
                    f"Expected operator or role name but found '{tok[1]}' at position {tok[2]}"
                )

            name = tok[1]
            if name == "AND":
                self.eat("IDENT")
                self.eat("(")
                children = self.parse_arg_list()
                self.eat(")")
                return AndNode(children=children)
            elif name == "OR":
                self.eat("IDENT")
                self.eat("(")
                children = self.parse_arg_list()
                self.eat(")")
                return OrNode(children=children)
            elif name == "k_of_n":
                self.eat("IDENT")
                self.eat("(")
                int_tok = self.eat("INT")
                k_val = int(int_tok[1])
                self.eat(",")
                children = self.parse_arg_list()
                self.eat(")")
                return KOfNNode(k=k_val, children=children)
            else:
                self.eat("IDENT")
                return RoleLeafNode(role_name=name)
        finally:
            self.depth -= 1

    def parse_arg_list(self) -> List[FormulaNode]:
        args = [self.parse_expr()]
        while self.peek() == ",":
            self.eat(",")
            args.append(self.parse_expr())
        return args

    def parse(self) -> FormulaNode:
        ast = self.parse_expr()
        if self.peek() != "EOF":
            tok = self.cur()
            raise FormulaParseError(
                f"Unexpected token '{tok[1]}' at position {tok[2]} after complete formula expression"
            )
        return ast


def parse_formula(formula_str: str) -> FormulaNode:
    """Parse boolean formula string into FormulaNode AST."""
    if not formula_str or not formula_str.strip():
        raise FormulaParseError("Empty formula expression")
    try:
        return _FormulaParser(formula_str.strip()).parse()
    except RecursionError as e:
        raise FormulaParseError("Formula expression exceeds maximum allowable recursion depth") from e


# ------------------------------------------------------------------------------
# DEC Dictionary and File Loader
# ------------------------------------------------------------------------------

def parse_contract_dict(data: Dict[str, Any]) -> DecisionEvidenceContract:
    """Parse raw contract dictionary (e.g. from YAML or JSON) into DecisionEvidenceContract AST."""
    required_top = [
        "contract_id",
        "version",
        "epoch",
        "decision_type",
        "decision_scope",
        "formula",
        "roles",
    ]
    for req in required_top:
        if req not in data:
            raise ContractParseError(f"Missing required contract field: '{req}'")

    # Scope
    scope_data = data["decision_scope"]
    if "scope_class" not in scope_data:
        raise ContractParseError("Missing 'scope_class' in 'decision_scope'")
    try:
        scope_class = ScopeClass(scope_data["scope_class"])
    except ValueError as e:
        raise ContractParseError(f"Invalid scope_class: '{scope_data['scope_class']}'") from e

    decision_scope = DecisionScopeSpec(
        scope_class=scope_class,
        scope_key_pattern=scope_data.get("scope_key_pattern", ""),
    )

    # Formula AST
    formula_str = str(data["formula"])
    formula_ast = parse_formula(formula_str)

    # Roles
    roles_data = data["roles"]
    if not isinstance(roles_data, dict):
        raise ContractParseError("'roles' must be a dictionary")

    roles: Dict[str, RoleSpec] = {}
    for r_name, r_dict in roles_data.items():
        if not isinstance(r_dict, dict):
            raise ContractParseError(f"Role '{r_name}' definition must be a dictionary")

        try:
            time_class = TimeClass(r_dict["time_class_required"])
        except (KeyError, ValueError) as e:
            raise ContractParseError(f"Invalid time_class_required in role '{r_name}'") from e

        # Agreement
        agr_dict = r_dict.get("agreement", {}) or {}
        metric_str = agr_dict.get("metric", "discrete_equality")
        try:
            metric = MetricType(metric_str)
        except ValueError as e:
            raise ContractParseError(f"Invalid agreement metric '{metric_str}' in role '{r_name}'") from e

        agreement = AgreementSpec(
            metric=metric,
            unit=agr_dict.get("unit", "s"),
            tolerance_eta=float(agr_dict.get("tolerance_eta", 0.0)),
            calibration_ref=agr_dict.get("calibration_ref", "UNAVAILABLE"),
            honest_noise_epsilon_h=(
                float(agr_dict["honest_noise_epsilon_h"])
                if agr_dict.get("honest_noise_epsilon_h") is not None
                else None
            ),
        )

        # Freshness
        fresh_dict = r_dict.get("freshness", {}) or {}
        freshness = FreshnessSpec(
            soft_age_s=(
                float(fresh_dict["soft_age_s"])
                if fresh_dict.get("soft_age_s") is not None
                else None
            ),
            hard_age_s=(
                float(fresh_dict["hard_age_s"])
                if fresh_dict.get("hard_age_s") is not None
                else None
            ),
            latency_allowance_s=float(fresh_dict.get("latency_allowance_s", 0.0)),
        )

        # Corroboration
        corrob_dict = r_dict.get("corroboration", {}) or {}
        try:
            corrob_source = CorroborationMode(corrob_dict.get("source", "INFORMATIONAL"))
            corrob_proc = CorroborationMode(corrob_dict.get("processing", "INFORMATIONAL"))
        except ValueError as e:
            raise ContractParseError(f"Invalid corroboration mode in role '{r_name}'") from e

        corroboration = CorroborationSpec(
            source=corrob_source,
            processing=corrob_proc,
            source_min_depth=int(corrob_dict.get("source_min_depth", 0)),
            processing_min_depth=int(corrob_dict.get("processing_min_depth", 0)),
        )

        # Fault Model
        fault_dict = r_dict.get("fault_model", {}) or {}
        fault_model = FaultModelSpec(
            source_fault_budget=int(fault_dict.get("source_fault_budget", 0)),
            processing_fault_budget=int(fault_dict.get("processing_fault_budget", 0)),
        )

        # Slots
        slot_dict = r_dict.get("slots", {}) or {}
        slots = SlotSpec(
            trigger=slot_dict.get("trigger", "event_driven"),
            max_obs_per_slot=int(slot_dict.get("max_obs_per_slot", 1)),
            deadline_s=float(slot_dict.get("deadline_s", 30.0)),
            allowed_lateness_s=float(slot_dict.get("allowed_lateness_s", 60.0)),
        )

        roles[r_name] = RoleSpec(
            name=r_name,
            required=bool(r_dict.get("required", True)),
            description=r_dict.get("description", ""),
            time_class_required=time_class,
            admissible_source_origins=list(r_dict.get("admissible_source_origins", [])),
            admissible_processing_origins=list(r_dict.get("admissible_processing_origins", [])),
            agreement=agreement,
            freshness=freshness,
            corroboration=corroboration,
            fault_model=fault_model,
            slots=slots,
        )

    # Finality
    finality_val = data.get("finality", {})
    if isinstance(finality_val, dict):
        finality_cond_str = finality_val.get("condition", "all_slots_closed_and_frontier_past_lateness")
    else:
        finality_cond_str = str(finality_val)
    try:
        finality_condition = FinalityCondition(finality_cond_str)
    except ValueError as e:
        raise ContractParseError(f"Invalid finality condition: '{finality_cond_str}'") from e

    # Tie Break
    tie_break_str = data.get("tie_break", "canonical_order")
    try:
        tie_break = TieBreakPolicy(tie_break_str)
    except ValueError as e:
        raise ContractParseError(f"Invalid tie_break: '{tie_break_str}'") from e

    # Explanation template ID
    expl_data = data.get("explanation", {})
    if isinstance(expl_data, dict):
        template_id = expl_data.get("template_id", "")
    else:
        template_id = str(expl_data)

    limitations = tuple(data.get("limitations", []))

    return DecisionEvidenceContract(
        contract_id=str(data["contract_id"]),
        version=str(data["version"]),
        epoch=int(data["epoch"]),
        effective_from=str(data.get("effective_from", "2026-01-01T00:00:00Z")),
        decision_type=str(data["decision_type"]),
        decision_scope=decision_scope,
        formula_str=formula_str,
        formula_ast=formula_ast,
        roles=roles,
        simultaneity_window_s=float(data.get("simultaneity_window_s", 1.0)),
        finality_condition=finality_condition,
        tie_break=tie_break,
        explanation_template_id=template_id,
        limitations=limitations,
    )


def load_contract_yaml(yaml_text: str) -> DecisionEvidenceContract:
    """Load and parse contract from YAML text."""
    raw = yaml.safe_load(yaml_text)
    if not isinstance(raw, dict):
        raise ContractParseError("YAML document root must be a mapping")
    return parse_contract_dict(raw)


def load_contract_file(file_path: Union[str, Path]) -> DecisionEvidenceContract:
    """Load and parse contract from YAML or JSON file."""
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"Contract file not found: {path}")

    with open(path, "r", encoding="utf-8") as f:
        content = f.read()

    if path.suffix in (".yaml", ".yml"):
        return load_contract_yaml(content)
    elif path.suffix == ".json":
        raw = json.loads(content)
        return parse_contract_dict(raw)
    else:
        # Default try YAML then JSON
        try:
            return load_contract_yaml(content)
        except Exception:
            raw = json.loads(content)
            return parse_contract_dict(raw)
