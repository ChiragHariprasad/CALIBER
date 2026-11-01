"""DEC Schema v1 Compile-Time Contract Validator.

Enforces rules V-01 through V-10 per CALIBRE Scoped Integrity Algebra (SIA) v2.2
contracts specification plan (CAL-PLAN-DEC-001 §3).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Sequence, Set

from .models import (
    AndNode,
    CorroborationMode,
    DecisionEvidenceContract,
    FormulaNode,
    KOfNNode,
    MetricType,
    OrNode,
    RoleLeafNode,
    TimeClass,
)


@dataclass(frozen=True, slots=True)
class ValidationError:
    code: str
    message: str
    role: Optional[str] = None
    is_warning: bool = False

    def __str__(self) -> str:
        role_ctx = f" (Role: {self.role})" if self.role else ""
        prefix = "WARN" if self.is_warning else "ERROR"
        return f"[{prefix}] [{self.code}]{role_ctx}: {self.message}"


class ContractValidationError(ValueError):
    """Raised when compile-time contract validation fails."""

    def __init__(self, errors: Sequence[ValidationError]):
        self.errors = list(errors)
        err_msg = "; ".join(str(e) for e in self.errors)
        super().__init__(f"Contract validation failed with {len(self.errors)} error(s): {err_msg}")


@dataclass
class ValidationResult:
    is_valid: bool
    errors: List[ValidationError]
    warnings: List[ValidationError]


DISCRETE_UNITS = {"enum", "boolean", "bool", "string", "flags"}
CONTINUOUS_METRICS = {MetricType.ABS_DIFF, MetricType.RELATIVE_DIFF, MetricType.TEMPORAL_DELTA}
FREE_FEED_ORIGIN_PREFIXES = ("PITWALL_", "TRACINGINSIGHTS_")
D15_WITNESS = "UNMODELLED_COMMON_CAUSE: shared F1 timing upstream"


def _check_formula_nodes(node: FormulaNode, errors: List[ValidationError]):
    """Recursively validate operator arity and threshold constraints (V-02)."""
    if isinstance(node, AndNode):
        if len(node.children) < 2:
            errors.append(
                ValidationError(
                    code="E_V02_INVALID_ARITY",
                    message=f"AND operator requires arity >= 2, got {len(node.children)}",
                )
            )
        for child in node.children:
            _check_formula_nodes(child, errors)
    elif isinstance(node, OrNode):
        if len(node.children) < 2:
            errors.append(
                ValidationError(
                    code="E_V02_INVALID_ARITY",
                    message=f"OR operator requires arity >= 2, got {len(node.children)}",
                )
            )
        for child in node.children:
            _check_formula_nodes(child, errors)
    elif isinstance(node, KOfNNode):
        n = len(node.children)
        if n < 2:
            errors.append(
                ValidationError(
                    code="E_V02_INVALID_ARITY",
                    message=f"k_of_n operator requires n >= 2, got {n}",
                )
            )
        if node.k < 1 or node.k > n:
            errors.append(
                ValidationError(
                    code="E_V02_INVALID_K_THRESHOLD",
                    message=f"k_of_n threshold k={node.k} is invalid for n={n} children (must satisfy 1 <= k <= n)",
                )
            )
        for child in node.children:
            _check_formula_nodes(child, errors)
    elif isinstance(node, RoleLeafNode):
        pass


def validate_contract(
    contract: DecisionEvidenceContract,
    raise_on_error: bool = False,
) -> ValidationResult:
    """Validate a DecisionEvidenceContract AST against compile-time rules V-01 through V-10."""
    errors: List[ValidationError] = []
    warnings: List[ValidationError] = []

    # --------------------------------------------------------------------------
    # Rule V-01: Role Identifier & Formula Bijective Consistency
    # --------------------------------------------------------------------------
    formula_roles = contract.formula_ast.get_referenced_roles()
    declared_roles = set(contract.roles.keys())

    orphan_roles = declared_roles - formula_roles
    for role_name in sorted(orphan_roles):
        errors.append(
            ValidationError(
                code="E_V01_ORPHAN_ROLE",
                message=f"Role '{role_name}' is declared in 'roles' but never referenced in 'formula'",
                role=role_name,
            )
        )

    undefined_roles = formula_roles - declared_roles
    for role_name in sorted(undefined_roles):
        errors.append(
            ValidationError(
                code="E_V01_UNDEFINED_ROLE",
                message=f"Role '{role_name}' is referenced in 'formula' but not defined in 'roles'",
                role=role_name,
            )
        )

    # --------------------------------------------------------------------------
    # Rule V-02: Formula Syntax, Arity, and Threshold Validity
    # --------------------------------------------------------------------------
    _check_formula_nodes(contract.formula_ast, errors)

    # Global slot / contract bounds check
    if contract.simultaneity_window_s < 0.0:
        errors.append(
            ValidationError(
                code="E_V09_INVALID_SLOT_BOUNDS",
                message=f"Negative simultaneity_window_s: {contract.simultaneity_window_s}",
            )
        )

    has_free_feed_origins = False

    # Role-level validation
    for role_name, role in contract.roles.items():
        # ----------------------------------------------------------------------
        # Rule V-03: Physical Unit & Dimension Compatibility
        # ----------------------------------------------------------------------
        agr = role.agreement
        unit_lower = agr.unit.lower()
        if agr.metric in CONTINUOUS_METRICS and unit_lower in DISCRETE_UNITS:
            errors.append(
                ValidationError(
                    code="E_V03_INCOMPATIBLE_UNIT",
                    message=(
                        f"Continuous agreement metric '{agr.metric.value}' cannot be used with "
                        f"discrete unit '{agr.unit}'"
                    ),
                    role=role_name,
                )
            )

        if agr.tolerance_eta < 0.0:
            errors.append(
                ValidationError(
                    code="E_V03_INVALID_TOLERANCE",
                    message=f"Negative agreement tolerance eta: {agr.tolerance_eta}",
                    role=role_name,
                )
            )

        if agr.metric == MetricType.DISCRETE_EQUALITY and agr.tolerance_eta != 0.0:
            errors.append(
                ValidationError(
                    code="E_V03_INVALID_TOLERANCE",
                    message=f"Discrete equality requires tolerance_eta == 0.0, got {agr.tolerance_eta}",
                    role=role_name,
                )
            )

        # ----------------------------------------------------------------------
        # Rule V-04: Corroboration Feasibility on Source Axis (Pigeonhole Check)
        # ----------------------------------------------------------------------
        n_src = len(role.admissible_source_origins)
        f_src = role.fault_model.source_fault_budget
        k_src = role.corroboration.source_min_depth

        if (f_src > 0 and n_src == 0) or (f_src >= n_src and n_src > 0):
            errors.append(
                ValidationError(
                    code="E_V04_IMPOSSIBLE_SOURCE_FAULT_BUDGET",
                    message=(
                        f"source_fault_budget={f_src} requires at least {f_src + 1} admissible "
                        f"source origins, but only {n_src} configured"
                    ),
                    role=role_name,
                )
            )
        if role.corroboration.source == CorroborationMode.MANDATORY and k_src >= n_src:
            errors.append(
                ValidationError(
                    code="E_V04_IMPOSSIBLE_SOURCE_DEPTH",
                    message=(
                        f"Mandatory source_min_depth={k_src} requires at least {k_src + 1} admissible "
                        f"source origins, but only {n_src} configured"
                    ),
                    role=role_name,
                )
            )

        # ----------------------------------------------------------------------
        # Rule V-05: Corroboration Feasibility on Processing Axis (Anti-Churn D-09)
        # ----------------------------------------------------------------------
        n_proc = len(role.admissible_processing_origins)
        f_proc = role.fault_model.processing_fault_budget
        k_proc = role.corroboration.processing_min_depth

        if (f_proc > 0 and n_proc == 0) or (f_proc >= n_proc and n_proc > 0):
            errors.append(
                ValidationError(
                    code="E_V05_IMPOSSIBLE_PROCESSING_FAULT_BUDGET",
                    message=(
                        f"processing_fault_budget={f_proc} requires at least {f_proc + 1} admissible "
                        f"processing origins, but only {n_proc} configured"
                    ),
                    role=role_name,
                )
            )
        if role.corroboration.processing == CorroborationMode.MANDATORY and k_proc >= n_proc:
            errors.append(
                ValidationError(
                    code="E_V05_IMPOSSIBLE_PROCESSING_DEPTH",
                    message=(
                        f"Mandatory processing_min_depth={k_proc} requires at least {k_proc + 1} admissible "
                        f"processing origins, but only {n_proc} configured"
                    ),
                    role=role_name,
                )
            )

        # ----------------------------------------------------------------------
        # Rule V-06: Typed-Time Legal Conversion Path Existence
        # ----------------------------------------------------------------------
        # time_class_required must be valid enum member (already enforced by model)
        if role.time_class_required not in (
            TimeClass.ABSOLUTE_UTC,
            TimeClass.SESSION_ELAPSED,
            TimeClass.LAP_RELATIVE,
        ):
            errors.append(
                ValidationError(
                    code="E_V06_ILLEGAL_TIME_CLASS",
                    message=f"Illegal time_class_required: '{role.time_class_required}'",
                    role=role_name,
                )
            )

        # ----------------------------------------------------------------------
        # Rule V-07: Agreement Tolerance & Completeness Bound (Lemma C)
        # ----------------------------------------------------------------------
        eps_h = agr.honest_noise_epsilon_h
        if eps_h is not None and eps_h > 0.0:
            if agr.tolerance_eta < 2.0 * eps_h:
                warnings.append(
                    ValidationError(
                        code="W_V07_TOLERANCE_BELOW_COMPLETENESS",
                        message=(
                            f"Agreement tolerance eta={agr.tolerance_eta} is below the 2*eps_h completeness "
                            f"bound (2 * {eps_h} = {2.0 * eps_h}). Honest atoms may conflict."
                        ),
                        role=role_name,
                        is_warning=True,
                    )
                )

        # ----------------------------------------------------------------------
        # Rule V-08: Freshness Horizon Monotonicity
        # ----------------------------------------------------------------------
        fresh = role.freshness
        if fresh.latency_allowance_s < 0.0:
            errors.append(
                ValidationError(
                    code="E_V08_INVALID_FRESHNESS_ORDER",
                    message=f"Negative latency_allowance_s: {fresh.latency_allowance_s}",
                    role=role_name,
                )
            )
        if fresh.soft_age_s is not None and fresh.soft_age_s <= 0.0:
            errors.append(
                ValidationError(
                    code="E_V08_INVALID_FRESHNESS_ORDER",
                    message=f"Non-positive soft_age_s: {fresh.soft_age_s}",
                    role=role_name,
                )
            )
        if fresh.hard_age_s is not None and fresh.hard_age_s <= 0.0:
            errors.append(
                ValidationError(
                    code="E_V08_INVALID_FRESHNESS_ORDER",
                    message=f"Non-positive hard_age_s: {fresh.hard_age_s}",
                    role=role_name,
                )
            )
        if (
            fresh.soft_age_s is not None
            and fresh.hard_age_s is not None
            and fresh.soft_age_s > 0.0
            and fresh.hard_age_s > 0.0
            and fresh.soft_age_s > fresh.hard_age_s
        ):
            errors.append(
                ValidationError(
                    code="E_V08_INVALID_FRESHNESS_ORDER",
                    message=(
                        f"Freshness horizon violation: soft_age_s ({fresh.soft_age_s}) > "
                        f"hard_age_s ({fresh.hard_age_s})"
                    ),
                    role=role_name,
                )
            )

        # ----------------------------------------------------------------------
        # Rule V-09: Slot Parameter Boundedness
        # ----------------------------------------------------------------------
        slot = role.slots
        if slot.deadline_s <= 0.0:
            errors.append(
                ValidationError(
                    code="E_V09_INVALID_SLOT_BOUNDS",
                    message=f"Slot deadline_s must be > 0, got {slot.deadline_s}",
                    role=role_name,
                )
            )
        if slot.allowed_lateness_s < 0.0:
            errors.append(
                ValidationError(
                    code="E_V09_INVALID_SLOT_BOUNDS",
                    message=f"Slot allowed_lateness_s must be >= 0, got {slot.allowed_lateness_s}",
                    role=role_name,
                )
            )
        if slot.max_obs_per_slot < 1:
            errors.append(
                ValidationError(
                    code="E_V09_INVALID_SLOT_BOUNDS",
                    message=f"Slot max_obs_per_slot must be >= 1, got {slot.max_obs_per_slot}",
                    role=role_name,
                )
            )

        # ----------------------------------------------------------------------
        # Track Free Feed Origins for Rule V-10
        # ----------------------------------------------------------------------
        is_free_feed_role = any(
            any(src.upper().startswith(prefix) for prefix in FREE_FEED_ORIGIN_PREFIXES)
            for src in role.admissible_source_origins
        )
        if is_free_feed_role:
            has_free_feed_origins = True
            # Overclaim check: free feeds cannot claim MANDATORY source corroboration or budget > 0
            if role.corroboration.source == CorroborationMode.MANDATORY or role.corroboration.source_min_depth > 0:
                errors.append(
                    ValidationError(
                        code="E_V10_D15_FREE_FEED_INDEPENDENCE_OVERCLAIM",
                        message=(
                            f"Role '{role_name}' uses free F1 feeds with shared upstream FOM timing "
                            f"and cannot claim mandatory source corroboration (source_min_depth > 0)"
                        ),
                        role=role_name,
                    )
                )
            if role.fault_model.source_fault_budget > 0:
                errors.append(
                    ValidationError(
                        code="E_V10_D15_FREE_FEED_INDEPENDENCE_OVERCLAIM",
                        message=(
                            f"Role '{role_name}' uses free F1 feeds and cannot declare source_fault_budget > 0"
                        ),
                        role=role_name,
                    )
                )

    # --------------------------------------------------------------------------
    # Rule V-10: Single Upstream F1 Timing Origin Witness (Defect D-15 Compliance)
    # --------------------------------------------------------------------------
    if has_free_feed_origins:
        if not any(D15_WITNESS in lim for lim in contract.limitations):
            errors.append(
                ValidationError(
                    code="E_V10_D15_MISSING_COMMON_CAUSE_LIMITATION",
                    message=(
                        f"Free feed contract must attach mandatory limitation witness: '{D15_WITNESS}'"
                    ),
                )
            )

    result = ValidationResult(
        is_valid=(len(errors) == 0),
        errors=errors,
        warnings=warnings,
    )

    if raise_on_error and not result.is_valid:
        raise ContractValidationError(errors)

    return result
