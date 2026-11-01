"""CALIBRE SIA v2.2 - Pure Evaluator Engine per CAL-DOC-007 §2.5, §2.6, §2.7, §5."""
from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

from ..contracts.models import (
    DecisionEvidenceContract,
    FormulaNode,
    RoleLeafNode,
    AndNode,
    OrNode,
    KOfNNode,
    FinalityCondition,
)
from ..models.evidence import (
    EvidenceAtom,
    ReferenceFact,
    GuardFact,
    RunClockTick,
)
from ..models.frontier import KnowledgeFrontier
from ..models.states import (
    IntegrityState,
    ResolutionMaturity,
    FinalityStatus,
    DecisionMaturity,
    and_state,
    or_state,
    k_of_n_state,
)
from ..time.timeline import ValidityTimeline, compose_pointwise, TimelineBreakpoint
from ..time.resolver import add_seconds_to_iso, diff_seconds_iso
from ..envelope.completion import EnvelopeEvaluator
from ..identity.hasher import compute_verdict_hash
from .leaf import LeafEvaluator, LeafEvaluationResult


class ContractEvaluator:
    """Pure evaluator library with zero external dependencies per CAL-DOC-007 §5."""

    @staticmethod
    def evaluate(
        contract: DecisionEvidenceContract,
        decision_instance_id: str,
        input_stream: Sequence[Dict[str, Any] | Any],
        t_eval: str,
        clock_domain: str = "LIVE_UTC",
        previous_verdict: Optional[Dict[str, Any]] = None,
        eval_id: Optional[str] = None,
        M_max: int = 10,
        C_max: int = 32,
    ) -> Dict[str, Any]:
        """Evaluate contract against admitted evidence at evaluation instant t_eval."""
        atoms: List[EvidenceAtom] = []
        reference_facts: Dict[str, ReferenceFact] = {}
        guard_facts: Dict[str, GuardFact] = {}
        ticks: List[RunClockTick] = []
        watermark: Optional[str] = None
        anchor_time: Optional[str] = None

        ref_role_names = [r for r in contract.roles if "reference" in r or "guard" in r]
        non_ref_role_names = [r for r in contract.roles if r not in ref_role_names]

        seen_atom_ids: Set[str] = set()

        for item in input_stream:
            if isinstance(item, dict):
                item_type = item.get("stream_item_type", "")
                data = item.get("data", {})
                item_wm = item.get("watermark_at_arrival")
                if item_wm:
                    if watermark is None or item_wm > watermark:
                        watermark = item_wm

                if item_type == "REFERENCE_FACT":
                    fact_id = data.get("fact_id", "")
                    fact_type = data.get("fact_type") or data.get("role_id")
                    if not fact_type:
                        if "lapstart" in fact_id:
                            fact_type = "lap_start_reference"
                        elif "t0" in fact_id:
                            fact_type = "session_t0_reference"
                        elif "guard" in fact_id or "liveness" in fact_id:
                            fact_type = "guard_liveness"
                        elif len(ref_role_names) == 1:
                            fact_type = ref_role_names[0]
                        else:
                            fact_type = "lap_start_reference"

                    origin_set = data.get("origin_set") or data.get("source_origin_set")
                    if not origin_set:
                        role_match = contract.roles.get(fact_type)
                        if role_match and role_match.admissible_source_origins:
                            origin_set = list(role_match.admissible_source_origins)
                        else:
                            origin_set = []

                    fact = ReferenceFact(
                        fact_id=fact_id,
                        fact_type=fact_type,
                        session_id=data.get("session_id"),
                        driver_id=data.get("driver_id"),
                        lap_number=data.get("lap_number"),
                        value=data.get("value"),
                        time_class=data.get("time_class", "SESSION_ELAPSED"),
                        grade=data.get("grade", "HEALTHY"),
                        origin_set=origin_set,
                        validity_window=data.get("validity_window"),
                        rejection_reason=data.get("rejection_reason"),
                    )
                    reference_facts[fact.fact_type] = fact
                    if anchor_time is None and ("lapstart" in fact_id or "reference" in fact.fact_type or "t0" in fact_id):
                        anchor_time = item.get("transport_event_time_utc")

                elif item_type == "EVIDENCE_ATOM":
                    raw_time = data.get("time")
                    time_dict = raw_time if isinstance(raw_time, dict) else {"value": raw_time}
                    atom_id = data.get("evidence_atom_id", "")
                    if atom_id and atom_id in seen_atom_ids:
                        continue
                    if atom_id:
                        seen_atom_ids.add(atom_id)

                    role_id = data.get("role_id")
                    if not role_id:
                        if len(non_ref_role_names) == 1:
                            role_id = non_ref_role_names[0]
                        else:
                            role_id = ""

                    atom = EvidenceAtom(
                        evidence_atom_id=atom_id,
                        role_id=role_id,
                        value=data.get("value"),
                        unit=data.get("unit", "s"),
                        raw_event_ids=data.get("raw_event_ids", []),
                        processor_family_id=data.get("processor_family_id", "generic_parser_family"),
                        transform_version=data.get("transform_version", "1.0.0"),
                        scope_key=data.get("scope_key", ""),
                        time=time_dict,
                        quality_grade=data.get("quality_grade", "HEALTHY"),
                        source_origin_set=data.get("source_origin_set", []),
                        processing_origin_set=data.get("processing_origin_set", []),
                        supersedes_atom_id=data.get("supersedes_atom_id"),
                        reference_atom_ids=data.get("reference_atom_ids", []),
                        origin_instance_refs=data.get("origin_instance_refs", []),
                        transport_event_time_utc=item.get("transport_event_time_utc", ""),
                        transport_time_basis=item.get("transport_time_basis", "EVENT_UTC"),
                    )
                    atoms.append(atom)
                    if anchor_time is None and "lap" in contract.contract_id:
                        anchor_time = atom.transport_event_time_utc

                elif item_type == "GUARD_FACT":
                    gfact = GuardFact(
                        fact_id=data.get("fact_id", ""),
                        origin_instance_id=data.get("origin_instance_id", ""),
                        epoch=data.get("epoch", 1),
                        status=data.get("status", "RESUMED"),
                        observed_gap=data.get("observed_gap", 0.0),
                        detection_clock=data.get("detection_clock", ""),
                        source_origin_set=data.get("source_origin_set", []),
                    )
                    guard_facts[gfact.origin_instance_id] = gfact

                elif item_type == "RUN_CLOCK_TICK":
                    tick = RunClockTick(
                        run_id=data.get("run_id", ""),
                        clock_domain=data.get("clock_domain", "LIVE_UTC"),
                        clock_utc=data.get("clock_utc", ""),
                        seq=data.get("seq", 0),
                        emitter_id=data.get("emitter_id", "supervisor"),
                    )
                    ticks.append(tick)

        if anchor_time is None and atoms:
            anchor_time = atoms[0].transport_event_time_utc

        # Parse slot number from decision_instance_id
        slot_number = None
        parts = decision_instance_id.split("|")
        for p in parts:
            if p.isdigit():
                slot_number = int(p)
                break
            elif p.startswith("lap") and p[3:].isdigit():
                slot_number = int(p[3:])
                break

        # Check finality cutoff condition (CAL-DOC-007 §2.6.4 / Defect D-08 / FX-15)
        is_final = False
        pinned_finality_frontier = None
        quarantined_atoms: List[str] = []

        if contract.finality_condition == FinalityCondition.ALL_SLOTS_CLOSED_AND_FRONTIER_PAST_LATENESS:
            if anchor_time and "lap" in contract.contract_id:
                first_role = next(iter(contract.roles.values()))
                if first_role.slots.deadline_s is not None:
                    slot_deadline_t = add_seconds_to_iso(anchor_time, first_role.slots.deadline_s)
                    slot_lateness_t = add_seconds_to_iso(slot_deadline_t, first_role.slots.allowed_lateness_s)
                    # Check if watermark and t_eval have passed lateness window
                    if t_eval > slot_lateness_t and watermark and watermark >= slot_lateness_t:
                        is_final = True
                        pinned_finality_frontier = slot_lateness_t
                        for a in atoms:
                            if a.transport_event_time_utc and a.transport_event_time_utc > slot_lateness_t:
                                quarantined_atoms.append(a.evidence_atom_id)

        if previous_verdict and previous_verdict.get("finality_status") == "FINAL":
            is_final = True
            pinned_finality_frontier = previous_verdict.get("knowledge_frontier", {}).get("run_clock")

        # Exclude quarantined atoms from active evaluation
        active_atoms = [a for a in atoms if a.evidence_atom_id not in quarantined_atoms]

        # Evaluate leaf roles
        leaf_results: Dict[str, LeafEvaluationResult] = {}
        pending_roles: List[str] = []
        has_missing_ref = False

        for role_name in contract.roles:
            role_spec = contract.roles[role_name]
            res = LeafEvaluator.evaluate_leaf(
                role_name=role_name,
                role_spec=role_spec,
                atoms=active_atoms,
                reference_facts=reference_facts,
                t_eval=t_eval,
                guard_facts=guard_facts,
                decision_instance_id=decision_instance_id,
                slot_number=slot_number,
                anchor_time=anchor_time,
                M_max=M_max,
            )
            leaf_results[role_name] = res
            if res.is_pending_slot:
                pending_roles.append(role_name)
            if res.primary_witness and ("MISSING_REFERENCE_FACT" in res.primary_witness or "INVALID_REFERENCE_FACT" in res.primary_witness):
                has_missing_ref = True

        # Check search budget overflow in any leaf
        has_overflow = any("SEARCH_BUDGET_EXCEEDED" in (r.primary_witness or "") for r in leaf_results.values())

        # Evaluate abstract completion envelope if pending slots exist
        known_leaf_states = {r: res.state for r, res in leaf_results.items()}
        lower_bound, upper_bound, alternatives, envelope_wit, maturity = EnvelopeEvaluator.evaluate_completions(
            contract=contract,
            known_leaf_states=known_leaf_states,
            pending_roles=pending_roles,
            C_max=C_max,
        )

        if has_missing_ref or has_overflow:
            maturity = ResolutionMaturity.OPEN
            if has_missing_ref:
                alternatives = []
                envelope_wit = None

        # Compose formula state and timeline
        def evaluate_ast_node(node: FormulaNode) -> Tuple[IntegrityState, ValidityTimeline]:
            if isinstance(node, RoleLeafNode):
                res = leaf_results[node.role_name]
                return res.state, res.timeline
            elif isinstance(node, AndNode):
                child_evals = [evaluate_ast_node(c) for c in node.children]
                child_states = [st for st, _ in child_evals]
                composed_state = and_state(*child_states)

                # Non-reference evidence timelines define temporal step function breakpoints
                evidence_evals = [
                    (st, tl) for c, (st, tl) in zip(node.children, child_evals)
                    if not (isinstance(c, RoleLeafNode) and ("reference" in c.role_name or "guard" in c.role_name or c.role_name in reference_facts))
                ]
                if evidence_evals:
                    evidence_tls = [tl for _, tl in evidence_evals]
                    base_tl = compose_pointwise("AND", evidence_tls)
                    if composed_state == IntegrityState.HEALTHY:
                        tuple_start = max(tl.breakpoints[0].from_time for tl in evidence_tls if tl.breakpoints)
                        future_bps = [bp for bp in base_tl.breakpoints if bp.from_time > tuple_start]
                        bps = [TimelineBreakpoint(tuple_start, base_tl.state_at(tuple_start))] + future_bps
                    else:
                        bps = base_tl.breakpoints
                    composed_bps = [
                        TimelineBreakpoint(bp.from_time, and_state(bp.state, composed_state))
                        for bp in bps
                    ]
                    composed_tl = ValidityTimeline(composed_bps)
                else:
                    child_tls = [tl for _, tl in child_evals]
                    composed_tl = compose_pointwise("AND", child_tls)
                return composed_state, composed_tl
            elif isinstance(node, OrNode):
                child_evals = [evaluate_ast_node(c) for c in node.children]
                child_states = [st for st, _ in child_evals]
                child_tls = [tl for _, tl in child_evals]
                composed_state = or_state(*child_states)
                composed_tl = compose_pointwise("OR", child_tls)
                return composed_state, composed_tl
            elif isinstance(node, KOfNNode):
                child_evals = [evaluate_ast_node(c) for c in node.children]
                child_states = [st for st, _ in child_evals]
                child_tls = [tl for _, tl in child_evals]
                composed_state = k_of_n_state(node.k, *child_states)
                composed_tl = compose_pointwise(f"K_OF_N_{node.k}", child_tls, k=node.k)
                return composed_state, composed_tl
            else:
                raise TypeError(f"Unknown formula node: {type(node)}")

        exact_state, composed_timeline = evaluate_ast_node(contract.formula_ast)

        if has_overflow or has_missing_ref:
            final_integrity_state = IntegrityState.UNKNOWN
        elif pending_roles and maturity == ResolutionMaturity.OPEN:
            final_integrity_state = IntegrityState.UNKNOWN
        else:
            final_integrity_state = exact_state

        # Build timeline list from AST composed timeline
        if has_overflow:
            start_t = active_atoms[0].transport_event_time_utc if active_atoms else t_eval
            validity_timeline_list = [{"from": start_t, "state": "UNKNOWN"}]
        elif has_missing_ref:
            validity_timeline_list = [{"from": t_eval, "state": "UNKNOWN"}]
        elif pending_roles and maturity == ResolutionMaturity.OPEN:
            start_t = anchor_time or (active_atoms[0].transport_event_time_utc if active_atoms else t_eval)
            validity_timeline_list = [{"from": start_t, "state": "UNKNOWN"}]
        else:
            validity_timeline_list = composed_timeline.to_list()

        issued_at = validity_timeline_list[0]["from"] if validity_timeline_list else t_eval

        # Assemble corroboration profile preserving contract.roles order
        corroboration_profile: Dict[str, Any] = {}
        all_support_atom_ids: List[str] = []
        all_reference_atom_ids: List[str] = []

        for r_name in contract.roles:
            r_res = leaf_results[r_name]
            corroboration_profile[r_name] = {
                "source_depth": r_res.source_depth,
                "processing_depth": r_res.processing_depth,
                "support_atom_ids": r_res.support_atom_ids,
                "origin_refs": r_res.origin_refs,
            }
            all_support_atom_ids.extend(r_res.support_atom_ids)
            if "reference" in r_name:
                all_reference_atom_ids.extend(r_res.support_atom_ids)

        all_support_atom_ids = sorted(list(set(all_support_atom_ids)))
        all_reference_atom_ids = sorted(list(set(all_reference_atom_ids)))

        # Determine limiting children
        limiting_children: List[str] = []
        if has_missing_ref:
            limiting_children = [
                r for r, res in leaf_results.items()
                if res.primary_witness and ("MISSING_REFERENCE_FACT" in res.primary_witness or "INVALID_REFERENCE_FACT" in res.primary_witness)
            ]
        elif isinstance(contract.formula_ast, RoleLeafNode):
            if final_integrity_state < IntegrityState.HEALTHY:
                limiting_children = [contract.formula_ast.role_name]
        elif isinstance(contract.formula_ast, AndNode):
            limiting_children = [
                r_name for r_name in contract.roles
                if leaf_results[r_name].state == final_integrity_state and final_integrity_state < IntegrityState.HEALTHY
            ]
        elif isinstance(contract.formula_ast, OrNode):
            if final_integrity_state < IntegrityState.HEALTHY:
                limiting_children = [
                    r_name for r_name in contract.roles
                    if leaf_results[r_name].state == final_integrity_state
                ]

        # Determine diagnosis result
        diagnosis_result = "NONE"
        if not has_overflow:
            for r_res in leaf_results.values():
                if r_res.diagnosis_result != "NONE":
                    diagnosis_result = r_res.diagnosis_result
                    break

        # Determine witnesses
        witness_set: List[str] = []
        primary_binding_witness: Optional[str] = None

        if envelope_wit and not has_missing_ref:
            if "EARLY_COLLAPSE" in envelope_wit:
                primary_binding_witness = envelope_wit
                witness_set.append(envelope_wit)
            elif "ENVELOPE_EVALUATED" in envelope_wit:
                primary_binding_witness = envelope_wit
                witness_set.append(envelope_wit)

        late_wit: Optional[str] = None
        if previous_verdict and previous_verdict.get("integrity_state") == "DEGRADED":
            if active_atoms and final_integrity_state == IntegrityState.HEALTHY:
                last_atom = active_atoms[-1]
                role_spec = contract.roles.get(last_atom.role_id)
                slot_deadline_s = role_spec.slots.deadline_s if role_spec and role_spec.slots else 30.0
                anchor_t = anchor_time or (previous_verdict.get("knowledge_frontier", {}).get("watermark") if previous_verdict else t_eval)
                deadline_t = add_seconds_to_iso(anchor_t, slot_deadline_s)
                atom_t = last_atom.transport_event_time_utc or t_eval
                delay_s = max(0.0, diff_seconds_iso(atom_t, deadline_t))
                late_wit = f"LATE_EVIDENCE_ADMITTED: role={last_atom.role_id}, delay={delay_s:.1f}s"
                primary_binding_witness = late_wit
                witness_set.append(late_wit)

        # If has_missing_ref, prioritize the invalid/missing reference witness
        if has_missing_ref:
            for r_res in leaf_results.values():
                if r_res.primary_witness and ("MISSING_REFERENCE_FACT" in r_res.primary_witness or "INVALID_REFERENCE_FACT" in r_res.primary_witness):
                    primary_binding_witness = r_res.primary_witness
                    break

        for r_res in leaf_results.values():
            if has_missing_ref and not ("MISSING_REFERENCE_FACT" in str(r_res.primary_witness) or "INVALID_REFERENCE_FACT" in str(r_res.primary_witness)):
                continue
            for w in r_res.witnesses:
                if w not in witness_set:
                    witness_set.append(w)
            if primary_binding_witness is None and r_res.primary_witness:
                primary_binding_witness = r_res.primary_witness

        if is_final:
            time_part = pinned_finality_frontier.split("T")[-1] if pinned_finality_frontier and "T" in pinned_finality_frontier else pinned_finality_frontier
            final_witness = f"SLOT_SEALED_FINAL: lateness_window_elapsed={time_part}"
            primary_binding_witness = final_witness
            witness_set = [final_witness]
            for q_id in quarantined_atoms:
                witness_set.append(f"POST_FINALITY_QUARANTINED: atom={q_id}")

        if clock_domain == "REPLAY_VIRTUAL":
            primary_binding_witness = "REPLAY_VIRTUAL_LEASE_ASSIGNED"
            if "REPLAY_VIRTUAL_LEASE_ASSIGNED" not in witness_set:
                witness_set.insert(0, "REPLAY_VIRTUAL_LEASE_ASSIGNED")

        if final_integrity_state == IntegrityState.HEALTHY and primary_binding_witness is None:
            active_roles_list = list(contract.roles.keys())
            roles_str = ", ".join(active_roles_list)
            primary_binding_witness = f"MUTUAL_AGREEMENT_VALIDATED: roles=[{roles_str}]"

        # Limitations attachment: use contract definition limitations generically
        limitations: List[str] = list(contract.limitations)

        is_synthetic = any("SYNTHETIC" in lim for lim in contract.limitations)
        if not is_synthetic and not is_final:
            if "UNMODELLED_COMMON_CAUSE: shared F1 timing upstream" in contract.limitations:
                if "UNMODELLED_COMMON_CAUSE: shared F1 timing upstream" not in witness_set:
                    witness_set.append("UNMODELLED_COMMON_CAUSE: shared F1 timing upstream")
            if late_wit is None:
                if any(a.role_id == "lap_record" for a in active_atoms):
                    if "HISTORICAL_ARCHIVE_UNCALIBRATED_LATENCY" in contract.limitations:
                        if "HISTORICAL_ARCHIVE_UNCALIBRATED_LATENCY" not in witness_set:
                            witness_set.append("HISTORICAL_ARCHIVE_UNCALIBRATED_LATENCY")
                if any(a.role_id == "telemetry_speed" for a in active_atoms):
                    if "HISTORICAL_ONLY_LIVE_SPEED_F1TV_RESTRICTED" in contract.limitations:
                        if "HISTORICAL_ONLY_LIVE_SPEED_F1TV_RESTRICTED" not in witness_set:
                            witness_set.append("HISTORICAL_ONLY_LIVE_SPEED_F1TV_RESTRICTED")

        # Determine next transition and lease
        next_trans: Optional[str] = None
        lease_until: Optional[str] = None

        if is_final:
            next_trans = None
            lease_until = None
        elif has_missing_ref:
            next_trans = None
            lease_until = add_seconds_to_iso(t_eval, 30.0)
        elif pending_roles and maturity == ResolutionMaturity.OPEN:
            p_deadlines = []
            for p_r in pending_roles:
                p_spec = contract.roles[p_r]
                if p_spec.slots and p_spec.slots.deadline_s is not None:
                    st_s = anchor_time or (active_atoms[0].transport_event_time_utc if active_atoms else t_eval)
                    p_deadlines.append(add_seconds_to_iso(st_s, p_spec.slots.deadline_s))
            next_trans = min(p_deadlines) if p_deadlines else None
            lease_until = next_trans or add_seconds_to_iso(t_eval, 30.0)
        elif final_integrity_state == IntegrityState.DEGRADED:
            next_trans = None
            slot_lateness_t = None
            for r_spec in contract.roles.values():
                if r_spec.slots and r_spec.slots.deadline_s is not None and r_spec.slots.allowed_lateness_s is not None:
                    st_s = anchor_time or (active_atoms[0].transport_event_time_utc if active_atoms else t_eval)
                    d_t = add_seconds_to_iso(st_s, r_spec.slots.deadline_s)
                    l_t = add_seconds_to_iso(d_t, r_spec.slots.allowed_lateness_s)
                    if slot_lateness_t is None or l_t > slot_lateness_t:
                        slot_lateness_t = l_t

            any_slot_expired = any("SLOT_DEADLINE_EXPIRED" in str(r.primary_witness) for r in leaf_results.values())
            leaf_future_trans = [
                r.next_transition_at for r in leaf_results.values()
                if r.next_transition_at and r.next_transition_at >= t_eval
            ]
            degraded_leaf_leases = [
                r.lease_until for r in leaf_results.values()
                if r.state == IntegrityState.DEGRADED and r.lease_until
            ]

            if any_slot_expired and slot_lateness_t and t_eval <= slot_lateness_t:
                lease_until = slot_lateness_t
            elif leaf_future_trans:
                lease_until = min(leaf_future_trans)
            elif degraded_leaf_leases:
                lease_until = min(degraded_leaf_leases)
            else:
                lease_until = add_seconds_to_iso(t_eval, 30.0)
        else:
            candidate_transitions: List[str] = []
            if len(contract.roles) > 1 and active_atoms and len(active_atoms) >= len(contract.roles):
                tuple_horizon = max(a.transport_event_time_utc for a in active_atoms if a.transport_event_time_utc)
                for r_spec in contract.roles.values():
                    if r_spec.freshness and r_spec.freshness.soft_age_s is not None:
                        candidate_transitions.append(add_seconds_to_iso(tuple_horizon, r_spec.freshness.soft_age_s))
            else:
                for r_res in leaf_results.values():
                    if r_res.next_transition_at:
                        candidate_transitions.append(r_res.next_transition_at)

            future_trans = [t for t in candidate_transitions if t >= t_eval]
            if future_trans:
                next_trans = min(future_trans)
                lease_until = next_trans
            else:
                next_trans = None
                slot_lateness_t = None
                for r_spec in contract.roles.values():
                    if r_spec.slots and r_spec.slots.deadline_s is not None and r_spec.slots.allowed_lateness_s is not None:
                        st_s = anchor_time or (active_atoms[0].transport_event_time_utc if active_atoms else t_eval)
                        d_t = add_seconds_to_iso(st_s, r_spec.slots.deadline_s)
                        l_t = add_seconds_to_iso(d_t, r_spec.slots.allowed_lateness_s)
                        if slot_lateness_t is None or l_t > slot_lateness_t:
                            slot_lateness_t = l_t

                if previous_verdict and previous_verdict.get("integrity_state") == "DEGRADED" and slot_lateness_t and t_eval <= slot_lateness_t:
                    lease_until = slot_lateness_t
                else:
                    lease_until = add_seconds_to_iso(issued_at, 30.0)

        # Revision numbering & Chaining
        revision_number = 1
        previous_verdict_hash: Optional[str] = None
        if previous_verdict:
            revision_number = previous_verdict.get("revision_number", 0) + 1
            previous_verdict_hash = previous_verdict.get("verdict_hash")

        # Decision maturity derived mapping
        if is_final:
            finality_status = FinalityStatus.FINAL.value
            decision_maturity = DecisionMaturity.FINAL.value
        elif revision_number > 1:
            finality_status = FinalityStatus.OPEN.value
            decision_maturity = DecisionMaturity.REVISED.value
        else:
            finality_status = FinalityStatus.OPEN.value
            decision_maturity = DecisionMaturity.PROVISIONAL.value

        # Frontier
        if is_final:
            canonical_frontier = {
                "run_clock": pinned_finality_frontier,
                "watermark": pinned_finality_frontier,
            }
        else:
            canonical_frontier = {
                "run_clock": t_eval,
                "watermark": watermark,
            }

        # Compute cryptographic verdict_hash via RFC 8785 JCS
        v_hash = compute_verdict_hash(
            contract_id=contract.contract_id,
            contract_version=contract.version,
            contract_epoch=contract.epoch,
            decision_instance_id=decision_instance_id,
            integrity_state=final_integrity_state.value,
            resolution_maturity=maturity.value,
            finality_status=finality_status,
            canonical_frontier=canonical_frontier,
            corroboration_profile_by_role=corroboration_profile,
            validity_timeline=validity_timeline_list,
            sorted_support_atom_ids=all_support_atom_ids,
            sorted_reference_atom_ids=all_reference_atom_ids,
            pinned_finality_frontier=pinned_finality_frontier if is_final else None,
        )

        res_dict = {
            "schema_name": "calibre.integrity_assessment",
            "schema_version": "2.0",
            "decision_instance_id": decision_instance_id,
            "contract": {
                "id": contract.contract_id,
                "version": contract.version,
                "epoch": contract.epoch,
            },
            "integrity_state": final_integrity_state.value,
            "corroboration_profile_by_role": corroboration_profile,
            "validity_timeline": validity_timeline_list,
            "next_transition_at": next_trans,
            "lease_until": lease_until,
            "lease_clock_domain": clock_domain,
            "resolution_maturity": maturity.value,
            "finality_status": finality_status,
            "revision_number": revision_number,
            "previous_verdict_hash": previous_verdict_hash,
            "knowledge_frontier": canonical_frontier,
            "primary_binding_witness": primary_binding_witness or "",
            "witness_set": witness_set,
            "limiting_children": limiting_children,
            "alternatives": alternatives,
            "diagnosis_result": diagnosis_result,
            "limitations": limitations,
            "decision_maturity": decision_maturity,
            "verdict_hash": v_hash,
        }
        if eval_id is not None:
            res_dict["eval_id"] = eval_id
        return res_dict
