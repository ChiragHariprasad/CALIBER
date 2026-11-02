"""CALIBRE SIA v2.2 - Leaf Role Evaluation per CAL-DOC-007 §2.5.2."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

from ..contracts.models import RoleSpec, CorroborationMode
from ..models.evidence import EvidenceAtom, ReferenceFact, GuardFact
from ..models.states import IntegrityState
from ..origin.manager import OriginManager
from ..envelope.clique import SupportSetSearch, atoms_agree
from ..time.resolver import diff_seconds_iso, add_seconds_to_iso, parse_iso_utc
from ..time.timeline import ValidityTimeline, TimelineBreakpoint
from .diagnosis import DiagnosisEngine, DiagnosisOutcome


@dataclass
class LeafEvaluationResult:
    """Outcome of single role leaf evaluation."""
    role_name: str
    state: IntegrityState
    support_atom_ids: List[str]
    origin_refs: List[str]
    source_depth: int
    processing_depth: int
    timeline: ValidityTimeline
    next_transition_at: Optional[str]
    lease_until: Optional[str]
    primary_witness: Optional[str]
    witnesses: List[str]
    diagnosis_result: str = "NONE"
    is_pending_slot: bool = False
    slot_expired_at: Optional[str] = None


class LeafEvaluator:
    """Evaluates a single role leaf according to §2.5.2 normative rules."""

    @staticmethod
    def evaluate_leaf(
        role_name: str,
        role_spec: RoleSpec,
        atoms: Sequence[EvidenceAtom],
        reference_facts: Dict[str, ReferenceFact],
        t_eval: str,
        guard_facts: Dict[str, GuardFact],
        decision_instance_id: str,
        slot_number: Optional[Any] = None,
        anchor_time: Optional[str] = None,
        M_max: int = 10,
        simultaneity_window_s: Optional[float] = None,
    ) -> LeafEvaluationResult:
        # Check if this role is satisfied by a reference fact directly
        if "reference" in role_name or role_name in reference_facts:
            ref_fact = reference_facts.get(role_name)
            if not ref_fact:
                scope_key = decision_instance_id.rsplit("|", 1)[0]
                witness = f"MISSING_REFERENCE_FACT: role={role_name}, key={scope_key}"
                tl = ValidityTimeline([TimelineBreakpoint(t_eval, IntegrityState.UNKNOWN)])
                return LeafEvaluationResult(
                    role_name=role_name,
                    state=IntegrityState.UNKNOWN,
                    support_atom_ids=[],
                    origin_refs=[],
                    source_depth=0,
                    processing_depth=0,
                    timeline=tl,
                    next_transition_at=None,
                    lease_until=add_seconds_to_iso(t_eval, 30.0),
                    primary_witness=witness,
                    witnesses=[witness],
                )

            # Check if reference fact has malformed / invalid syntax generically
            is_malformed = False
            syntax_reason = ref_fact.rejection_reason
            if ref_fact.grade == "MALFORMED":
                is_malformed = True
                syntax_reason = syntax_reason or "MALFORMED_REFERENCE"
            elif isinstance(ref_fact.value, str) and ("T" in ref_fact.value or "-" in ref_fact.value):
                try:
                    from datetime import datetime
                    datetime.fromisoformat(ref_fact.value.replace("Z", "+00:00"))
                except (ValueError, TypeError):
                    is_malformed = True
                    syntax_reason = syntax_reason or "MALFORMED_TIMEZONE_OFFSET"

            if ref_fact.rejection_reason or is_malformed:
                reason = syntax_reason or "MALFORMED_REFERENCE_FACT"
                witness = f"INVALID_REFERENCE_FACT: {role_name}, reason={reason}"
                tl = ValidityTimeline([TimelineBreakpoint(t_eval, IntegrityState.UNKNOWN)])
                return LeafEvaluationResult(
                    role_name=role_name,
                    state=IntegrityState.UNKNOWN,
                    support_atom_ids=[],
                    origin_refs=[],
                    source_depth=0,
                    processing_depth=0,
                    timeline=tl,
                    next_transition_at=None,
                    lease_until=add_seconds_to_iso(t_eval, 30.0),
                    primary_witness=witness,
                    witnesses=[witness],
                )

            # Valid reference fact
            fact_grade = IntegrityState(ref_fact.grade) if ref_fact.grade in IntegrityState._value2member_map_ else IntegrityState.HEALTHY
            tl = ValidityTimeline([], default_state=fact_grade)
            admissible = list(role_spec.admissible_source_origins)
            ref_origins = list(ref_fact.origin_set)
            matched_ref_origins = [orig for orig in admissible if orig in ref_origins]
            if not matched_ref_origins and admissible:
                matched_ref_origins = admissible
            final_ref_origins = matched_ref_origins if matched_ref_origins else ref_origins

            return LeafEvaluationResult(
                role_name=role_name,
                state=fact_grade,
                support_atom_ids=[ref_fact.fact_id],
                origin_refs=final_ref_origins,
                source_depth=0,
                processing_depth=0,
                timeline=tl,
                next_transition_at=None,
                lease_until=None,
                primary_witness=None,
                witnesses=[],
            )

        # Filter atoms for this role
        role_atoms = [a for a in atoms if a.role_id == role_name]

        # Supersession (Defect D-09)
        superseded_wit = None
        for a in role_atoms:
            if a.supersedes_atom_id:
                superseded_wit = f"SUPERSEDED_ATOM: original={a.supersedes_atom_id}, superseded_by={a.evidence_atom_id}"
        active_atoms = OriginManager.apply_supersession(role_atoms)

        # Keep the full input history for audit and revision, but bound support
        # search to simultaneous observations. Periodic roles also obey hard
        # freshness. These semantic windows do not alter M_max.
        if active_atoms and (simultaneity_window_s is not None or role_spec.slots.trigger == "periodic"):
            def semantic_time(atom: EvidenceAtom) -> str:
                if isinstance(atom.time, dict):
                    value = atom.time.get("value")
                    if isinstance(value, str) and "T" in value:
                        return value
                return atom.transport_event_time_utc or t_eval

            latest_search_time = max(semantic_time(atom) for atom in active_atoms)
            horizons = [float(value) for value in (
                simultaneity_window_s,
                role_spec.freshness.hard_age_s if role_spec.slots.trigger == "periodic" else None,
            ) if value is not None and float(value) >= 0]
            if horizons:
                search_floor = add_seconds_to_iso(latest_search_time, -min(horizons))
                search_floor_dt = parse_iso_utc(search_floor)
                active_atoms = [atom for atom in active_atoms if parse_iso_utc(semantic_time(atom)) >= search_floor_dt]

        # Check origin STALLED in guard facts
        stalled_origin = None
        for a in active_atoms:
            for orig in a.source_origin_set:
                gf = guard_facts.get(orig)
                if gf and gf.status == "STALLED":
                    stalled_origin = orig
                    break

        # Check if slot is empty
        if not active_atoms:
            start_t = anchor_time or t_eval
            deadline_s = role_spec.slots.deadline_s
            deadline_t = add_seconds_to_iso(start_t, deadline_s)
            allowed_lateness_s = role_spec.slots.allowed_lateness_s
            lateness_t = add_seconds_to_iso(deadline_t, allowed_lateness_s)

            if t_eval >= deadline_t or stalled_origin is not None:
                # Slot deadline expired or stalled -> DEGRADED
                slot_id_str = str(slot_number) if slot_number is not None else "0"
                witness = f"SLOT_DEADLINE_EXPIRED: role={role_name}, slot={slot_id_str}"
                tl = ValidityTimeline([TimelineBreakpoint(deadline_t, IntegrityState.DEGRADED)])
                return LeafEvaluationResult(
                    role_name=role_name,
                    state=IntegrityState.DEGRADED,
                    support_atom_ids=[],
                    origin_refs=[],
                    source_depth=0,
                    processing_depth=0,
                    timeline=tl,
                    next_transition_at=None,
                    lease_until=lateness_t,
                    primary_witness=witness,
                    witnesses=[witness],
                    is_pending_slot=False,
                    slot_expired_at=deadline_t,
                )
            else:
                # Slot is still open -> UNKNOWN
                tl = ValidityTimeline([TimelineBreakpoint(start_t, IntegrityState.UNKNOWN)])
                return LeafEvaluationResult(
                    role_name=role_name,
                    state=IntegrityState.UNKNOWN,
                    support_atom_ids=[],
                    origin_refs=[],
                    source_depth=0,
                    processing_depth=0,
                    timeline=tl,
                    next_transition_at=deadline_t,
                    lease_until=deadline_t,
                    primary_witness=None,
                    witnesses=[],
                    is_pending_slot=True,
                )

        # Check search budget M_max overflow before diagnosis per §2.4.1 / FX-19
        if len(active_atoms) > M_max:
            witness = f"SEARCH_BUDGET_EXCEEDED: count={len(active_atoms)}, M_max={M_max}"
            all_ids = [a.evidence_atom_id for a in active_atoms]
            raw_origins = set(orig for a in active_atoms for orig in a.source_origin_set)
            admissible = list(role_spec.admissible_source_origins)
            matched_origins = [orig for orig in admissible if orig in raw_origins]
            all_origins = matched_origins if matched_origins else sorted(list(raw_origins))
            base_time = active_atoms[0].transport_event_time_utc or t_eval
            soft_age_s = role_spec.freshness.soft_age_s
            next_trans = add_seconds_to_iso(base_time, soft_age_s) if soft_age_s is not None else None
            tl = ValidityTimeline([TimelineBreakpoint(base_time, IntegrityState.UNKNOWN)])
            return LeafEvaluationResult(
                role_name=role_name,
                state=IntegrityState.UNKNOWN,
                support_atom_ids=all_ids,
                origin_refs=all_origins,
                source_depth=0,
                processing_depth=0,
                timeline=tl,
                next_transition_at=next_trans,
                lease_until=next_trans,
                primary_witness=witness,
                witnesses=[witness],
                diagnosis_result="NONE",
            )

        # Active atoms exist: evaluate freshness and aging
        soft_age_s = role_spec.freshness.soft_age_s
        hard_age_s = role_spec.freshness.hard_age_s

        def get_atom_semantic_time(a: EvidenceAtom) -> str:
            if isinstance(a.time, dict):
                v = a.time.get("value")
                if isinstance(v, str) and "T" in v:
                    return v
            return a.transport_event_time_utc or t_eval

        latest_atom_time = max(
            get_atom_semantic_time(a) for a in active_atoms
        )

        age = diff_seconds_iso(t_eval, latest_atom_time)

        # Check hard age expiry
        if hard_age_s is not None and age >= hard_age_s:
            hard_t = add_seconds_to_iso(latest_atom_time, hard_age_s)
            witness = f"HARD_AGE_EXCEEDED: role={role_name}, age={age:.1f}s, hard_age_s={hard_age_s:.1f}s"
            tl = ValidityTimeline([TimelineBreakpoint(hard_t, IntegrityState.DEGRADED)])
            # Lease calculation for hard-expired feed
            if role_spec.slots and role_spec.slots.allowed_lateness_s is not None:
                lease_dur = role_spec.slots.allowed_lateness_s
            elif role_spec.freshness and role_spec.freshness.latency_allowance_s is not None:
                lease_dur = role_spec.freshness.latency_allowance_s
            else:
                lease_dur = 30.0
            lease_until = add_seconds_to_iso(t_eval, lease_dur)
            return LeafEvaluationResult(
                role_name=role_name,
                state=IntegrityState.DEGRADED,
                support_atom_ids=[],
                origin_refs=[],
                source_depth=0,
                processing_depth=0,
                timeline=tl,
                next_transition_at=None,
                lease_until=lease_until,
                primary_witness=witness,
                witnesses=[witness],
            )

        # Check soft age expiry
        is_soft_expired = False
        soft_witness = None
        next_trans: Optional[str] = None
        if soft_age_s is not None and age >= soft_age_s:
            is_soft_expired = True
            soft_witness = f"SOFT_AGE_EXCEEDED: role={role_name}, age={age:.1f}s, soft_age_s={soft_age_s:.1f}s"
            if hard_age_s is not None:
                next_trans = add_seconds_to_iso(latest_atom_time, hard_age_s)
        elif soft_age_s is not None:
            if role_spec.slots and (role_spec.slots.trigger == "periodic" or role_spec.slots.max_obs_per_slot > 2):
                start_slot = active_atoms[0].transport_event_time_utc if active_atoms else (anchor_time or t_eval)
                next_trans = add_seconds_to_iso(start_slot, soft_age_s)
            else:
                next_trans = add_seconds_to_iso(latest_atom_time, soft_age_s)

        # Run fault diagnosis over active atoms
        f_budget = role_spec.fault_model.source_fault_budget
        metric_name = role_spec.agreement.metric.value if hasattr(role_spec.agreement.metric, "value") else str(role_spec.agreement.metric)
        diag = DiagnosisEngine.diagnose_faults(
            atoms=active_atoms,
            fault_budget=f_budget,
            metric=metric_name,
            tolerance_eta=role_spec.agreement.tolerance_eta,
            unit=role_spec.agreement.unit,
        )

        # Support set search
        surviving_atoms = [
            a for a in active_atoms if set(a.source_origin_set).isdisjoint(diag.excluded_origins)
        ]

        if diag.diagnosis_result in ("DIAGNOSIS_AMBIGUOUS", "FAULT_BUDGET_EXCEEDED"):
            # Retain all active atoms in profile without exclusion
            support_set = list(active_atoms)
        else:
            support_set, _, _ = SupportSetSearch.search_support_set(
                atoms=surviving_atoms,
                metric=metric_name,
                tolerance_eta=role_spec.agreement.tolerance_eta,
                axis="source",
                M_max=M_max,
            )

        witnesses: List[str] = []
        if superseded_wit:
            witnesses.append(superseded_wit)

        witnesses.extend(diag.witnesses)

        # Determine state
        if diag.diagnosis_result == "FAULT_BUDGET_EXCEEDED":
            state = IntegrityState.SUSPECT
            primary_wit = diag.primary_witness
        elif diag.diagnosis_result == "DIAGNOSIS_AMBIGUOUS":
            state = IntegrityState.SUSPECT
            primary_wit = diag.primary_witness
        elif is_soft_expired:
            state = IntegrityState.SUSPECT
            primary_wit = soft_witness
            if soft_witness and soft_witness not in witnesses:
                witnesses.insert(0, soft_witness)
        else:
            state = IntegrityState.HEALTHY
            primary_wit = superseded_wit or (diag.primary_witness if diag.diagnosis_result == "EXCLUDED_FAULTS" else None)

        # Calculate corroboration depth
        unique_src_origins = sorted(list(set(orig for a in support_set for orig in a.source_origin_set)))
        unique_proc_origins = sorted(list(set(orig for a in support_set for orig in a.processing_origin_set)))
        
        # If axis is INFORMATIONAL, depth defaults to len - 1 if > 1 and disjoint
        if state == IntegrityState.HEALTHY and diag.diagnosis_result in ("EXCLUDED_FAULTS", "NONE"):
            src_depth = max(0, len(unique_src_origins) - 1)
        else:
            src_depth = 0
        proc_depth = 0

        # Build timeline
        breakpoints: List[TimelineBreakpoint] = []
        if is_soft_expired:
            start_from = t_eval
            breakpoints.append(TimelineBreakpoint(start_from, state))
            if hard_age_s is not None:
                hard_t = add_seconds_to_iso(latest_atom_time, hard_age_s)
                breakpoints.append(TimelineBreakpoint(hard_t, IntegrityState.DEGRADED))
        else:
            start_from = latest_atom_time
            breakpoints.append(TimelineBreakpoint(start_from, state))

        tl = ValidityTimeline(breakpoints)

        # Lease calculation
        lease_until: Optional[str] = next_trans
        if lease_until is None:
            lease_until = add_seconds_to_iso(t_eval, 30.0)

        # Atom IDs
        support_atom_ids = [a.evidence_atom_id for a in support_set]
        admissible = list(role_spec.admissible_source_origins)
        matched_origins = [orig for orig in admissible if orig in unique_src_origins]
        if not matched_origins and unique_src_origins and admissible:
            matched_origins = admissible
        origin_refs = matched_origins if matched_origins else unique_src_origins

        return LeafEvaluationResult(
            role_name=role_name,
            state=state,
            support_atom_ids=support_atom_ids,
            origin_refs=origin_refs,
            source_depth=src_depth,
            processing_depth=proc_depth,
            timeline=tl,
            next_transition_at=next_trans,
            lease_until=lease_until,
            primary_witness=primary_wit,
            witnesses=witnesses,
            diagnosis_result=diag.diagnosis_result,
        )
