"""CALIBRE SIA v2.2 - Decision Evidence Contract (DEC) AST Models."""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Sequence, Set


class ScopeClass(str, Enum):
    DRIVER_LAP = "DRIVER_LAP"
    DRIVER_SESSION = "DRIVER_SESSION"
    SESSION = "SESSION"
    SECTOR = "SECTOR"


class TimeClass(str, Enum):
    ABSOLUTE_UTC = "ABSOLUTE_UTC"
    SESSION_ELAPSED = "SESSION_ELAPSED"
    LAP_RELATIVE = "LAP_RELATIVE"


class MetricType(str, Enum):
    ABS_DIFF = "abs_diff"
    RELATIVE_DIFF = "relative_diff"
    DISCRETE_EQUALITY = "discrete_equality"
    TEMPORAL_DELTA = "temporal_delta"
    ENVELOPE_CONTAINMENT = "envelope_containment"


# Alias for MetricType
AgreementMetric = MetricType


class CorroborationMode(str, Enum):
    INFORMATIONAL = "INFORMATIONAL"
    MANDATORY = "MANDATORY"
    OPTIONAL = "OPTIONAL"


class FinalityCondition(str, Enum):
    ALL_SLOTS_CLOSED_AND_FRONTIER_PAST_LATENESS = "all_slots_closed_and_frontier_past_lateness"
    EARLY_COLLAPSE_OR_LATENESS = "early_collapse_or_lateness"


class DecisionMaturity(str, Enum):
    PROVISIONAL = "PROVISIONAL"
    FINAL = "FINAL"
    REVISED = "REVISED"


class TieBreakPolicy(str, Enum):
    CANONICAL_ORDER = "canonical_order"


# ------------------------------------------------------------------------------
# Formula AST Hierarchy
# ------------------------------------------------------------------------------

class FormulaNode(ABC):
    """Abstract syntax tree node representing a boolean contract expression."""

    @abstractmethod
    def get_referenced_roles(self) -> Set[str]:
        """Collect all role leaf identifiers in this subtree."""
        pass


@dataclass(frozen=True, slots=True)
class RoleLeafNode(FormulaNode):
    role_name: str

    def get_referenced_roles(self) -> Set[str]:
        return {self.role_name}


# Alias for RoleLeafNode
RoleNode = RoleLeafNode


@dataclass(frozen=True, slots=True)
class AndNode(FormulaNode):
    children: Sequence[FormulaNode]

    def get_referenced_roles(self) -> Set[str]:
        roles: Set[str] = set()
        for child in self.children:
            roles.update(child.get_referenced_roles())
        return roles


@dataclass(frozen=True, slots=True)
class OrNode(FormulaNode):
    children: Sequence[FormulaNode]

    def get_referenced_roles(self) -> Set[str]:
        roles: Set[str] = set()
        for child in self.children:
            roles.update(child.get_referenced_roles())
        return roles


@dataclass(frozen=True, slots=True)
class KOfNNode(FormulaNode):
    k: int
    children: Sequence[FormulaNode]

    def get_referenced_roles(self) -> Set[str]:
        roles: Set[str] = set()
        for child in self.children:
            roles.update(child.get_referenced_roles())
        return roles


# ------------------------------------------------------------------------------
# Contract Component Specifications
# ------------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class AgreementSpec:
    metric: MetricType
    unit: str
    tolerance_eta: float
    calibration_ref: str
    honest_noise_epsilon_h: Optional[float] = None


@dataclass(frozen=True, slots=True)
class FreshnessSpec:
    soft_age_s: Optional[float]
    hard_age_s: Optional[float]
    latency_allowance_s: float


@dataclass(frozen=True, slots=True)
class CorroborationSpec:
    source: CorroborationMode
    processing: CorroborationMode
    source_min_depth: int = 0
    processing_min_depth: int = 0


@dataclass(frozen=True, slots=True)
class FaultModelSpec:
    source_fault_budget: int = 0
    processing_fault_budget: int = 0


@dataclass(frozen=True, slots=True)
class SlotSpec:
    trigger: str
    max_obs_per_slot: int
    deadline_s: float
    allowed_lateness_s: float


@dataclass(frozen=True, slots=True)
class RoleSpec:
    name: str
    required: bool
    description: str
    time_class_required: TimeClass
    admissible_source_origins: Sequence[str]
    admissible_processing_origins: Sequence[str]
    agreement: AgreementSpec
    freshness: FreshnessSpec
    corroboration: CorroborationSpec
    fault_model: FaultModelSpec
    slots: SlotSpec


# Alias for RoleSpec
RoleDefinition = RoleSpec


@dataclass(frozen=True, slots=True)
class DecisionScopeSpec:
    scope_class: ScopeClass
    scope_key_pattern: str


# Alias for DecisionScopeSpec
DecisionScope = DecisionScopeSpec


@dataclass(frozen=True, slots=True)
class DecisionEvidenceContract:
    contract_id: str
    version: str
    epoch: int
    effective_from: str
    decision_type: str
    decision_scope: DecisionScopeSpec
    formula_str: str
    formula_ast: FormulaNode
    roles: Dict[str, RoleSpec]
    simultaneity_window_s: float
    finality_condition: FinalityCondition
    tie_break: TieBreakPolicy
    explanation_template_id: str
    limitations: Sequence[str] = field(default_factory=tuple)
