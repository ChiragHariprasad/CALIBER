"""CALIBRE SIA v2.2 - Abstract Completion Model and Verdict Envelopes per CAL-DOC-007 §2.7."""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence, Tuple
from ..models.states import IntegrityState, ResolutionMaturity, and_state, or_state, k_of_n_state
from ..contracts.models import DecisionEvidenceContract, FormulaNode, RoleLeafNode, AndNode, OrNode, KOfNNode


class EnvelopeEvaluator:
    """Evaluates abstract completion bounds over open evidence slots."""

    @staticmethod
    def evaluate_completions(
        contract: DecisionEvidenceContract,
        known_leaf_states: Dict[str, IntegrityState],
        pending_roles: Sequence[str],
        C_max: int = 32,
    ) -> Tuple[IntegrityState, IntegrityState, List[str], Optional[str], ResolutionMaturity]:
        """Evaluate completion bounds over pending slots.
        
        Returns:
            Tuple of:
            - lower_bound: IntegrityState
            - upper_bound: IntegrityState
            - alternatives: List[str]
            - witness: Optional[str]
            - maturity: ResolutionMaturity (OPEN or STATE_RESOLVED)
        """
        if not pending_roles:
            # All slots closed, evaluate exact formula
            exact_state = EnvelopeEvaluator._eval_formula(contract.formula_ast, known_leaf_states)
            return exact_state, exact_state, [], None, ResolutionMaturity.STATE_RESOLVED

        # Possible outcomes for any pending role: DEGRADED (absent/failed), SUSPECT (conflict/soft), HEALTHY (agreeing)
        possible_outcomes = [
            IntegrityState.DEGRADED,
            IntegrityState.SUSPECT,
            IntegrityState.HEALTHY,
        ]

        # Calculate number of combinations
        total_completions = len(possible_outcomes) ** len(pending_roles)
        if total_completions > C_max:
            witness = "ENVELOPE_CAPPED: completions exceed C_max"
            return (
                IntegrityState.DEGRADED,
                IntegrityState.HEALTHY,
                ["DEGRADED", "SUSPECT", "HEALTHY"],
                witness,
                ResolutionMaturity.OPEN,
            )

        evaluated_outcomes: List[IntegrityState] = []
        import itertools
        for combo in itertools.product(possible_outcomes, repeat=len(pending_roles)):
            hypothetical_states = dict(known_leaf_states)
            for role_name, hyp_state in zip(pending_roles, combo):
                hypothetical_states[role_name] = hyp_state
            res_state = EnvelopeEvaluator._eval_formula(contract.formula_ast, hypothetical_states)
            evaluated_outcomes.append(res_state)

        lower_bound = min(evaluated_outcomes)
        upper_bound = max(evaluated_outcomes)

        unique_outcomes = [s.value for s in sorted(set(evaluated_outcomes), key=lambda x: x.rank)]

        # Obligation P5: Sound early collapse check
        if lower_bound == upper_bound:
            witness = f"EARLY_COLLAPSE_P5: lower={lower_bound.value}, upper={upper_bound.value}"
            return lower_bound, upper_bound, [], witness, ResolutionMaturity.STATE_RESOLVED
        else:
            witness = f"ENVELOPE_EVALUATED: lower={lower_bound.value}, upper={upper_bound.value}, completions={total_completions}"
            return lower_bound, upper_bound, unique_outcomes, witness, ResolutionMaturity.OPEN

    @staticmethod
    def _eval_formula(node: FormulaNode, states: Dict[str, IntegrityState]) -> IntegrityState:
        if isinstance(node, RoleLeafNode):
            return states.get(node.role_name, IntegrityState.UNKNOWN)
        elif isinstance(node, AndNode):
            child_states = [EnvelopeEvaluator._eval_formula(c, states) for c in node.children]
            return and_state(*child_states)
        elif isinstance(node, OrNode):
            child_states = [EnvelopeEvaluator._eval_formula(c, states) for c in node.children]
            return or_state(*child_states)
        elif isinstance(node, KOfNNode):
            child_states = [EnvelopeEvaluator._eval_formula(c, states) for c in node.children]
            return k_of_n_state(node.k, *child_states)
        else:
            raise TypeError(f"Unknown formula node type: {type(node)}")
