"""CALIBRE SIA v2.2 - Fault Isolation Engine (Theorem G4-I) per CAL-DOC-007 §2.4.3."""
from __future__ import annotations

import itertools
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Set, Tuple

from ..models.evidence import EvidenceAtom
from ..envelope.clique import atoms_agree


@dataclass(frozen=True, slots=True)
class DiagnosisOutcome:
    """Result of Theorem G4-I minimal admissible diagnosis evaluation."""
    diagnosis_result: str  # "NONE" | "EXCLUDED_FAULTS" | "DIAGNOSIS_AMBIGUOUS" | "FAULT_BUDGET_EXCEEDED"
    excluded_origins: Set[str]
    minimal_diagnoses: List[Set[str]]
    primary_witness: Optional[str]
    witnesses: List[str]
    conflicting_pairs: List[Tuple[EvidenceAtom, EvidenceAtom, float]]


class DiagnosisEngine:
    """Computes inclusion-minimal admissible diagnoses D_min and provable fault exclusions."""

    @staticmethod
    def diagnose_faults(
        atoms: Sequence[EvidenceAtom],
        fault_budget: int = 0,
        metric: str = "abs_diff",
        tolerance_eta: float = 0.05,
        unit: str = "s",
    ) -> DiagnosisOutcome:
        """Run Theorem G4-I diagnosis on a set of atoms."""
        n = len(atoms)
        if n < 2:
            return DiagnosisOutcome(
                diagnosis_result="NONE",
                excluded_origins=set(),
                minimal_diagnoses=[],
                primary_witness=None,
                witnesses=[],
                conflicting_pairs=[],
            )

        # Detect all conflicting pairs
        conflicts: List[Tuple[EvidenceAtom, EvidenceAtom, float]] = []
        conflict_edges: List[Set[str]] = []

        for i in range(n):
            for j in range(i + 1, n):
                a1 = atoms[i]
                a2 = atoms[j]
                if not atoms_agree(a1, a2, metric, tolerance_eta):
                    try:
                        delta = abs(float(a1.value) - float(a2.value))
                    except Exception:
                        delta = 0.0
                    conflicts.append((a1, a2, delta))
                    combined_origins = set(a1.source_origin_set) | set(a2.source_origin_set)
                    conflict_edges.append(combined_origins)

        if not conflicts:
            return DiagnosisOutcome(
                diagnosis_result="NONE",
                excluded_origins=set(),
                minimal_diagnoses=[],
                primary_witness=None,
                witnesses=[],
                conflicting_pairs=[],
            )

        # Build conflict witness strings
        conflict_witnesses = []
        for a1, a2, delta in conflicts:
            delta_str = f"{delta:.1f}" if delta == int(delta) else f"{delta:g}"
            eta_str = f"{tolerance_eta:.1f}" if tolerance_eta == int(tolerance_eta) else f"{tolerance_eta:g}"
            conflict_witnesses.append(
                f"UNRESOLVED_CONFLICT: pair=({a1.evidence_atom_id}, {a2.evidence_atom_id}), delta={delta_str}{unit}, eta={eta_str}{unit}"
            )

        if fault_budget <= 0:
            # Cannot tolerate any faults
            primary = conflict_witnesses[0]
            budget_wit = f"FAULT_BUDGET_EXCEEDED: f_budget={fault_budget}, conflicts={len(conflicts)}"
            return DiagnosisOutcome(
                diagnosis_result="FAULT_BUDGET_EXCEEDED",
                excluded_origins=set(),
                minimal_diagnoses=[],
                primary_witness=primary,
                witnesses=[primary, budget_wit],
                conflicting_pairs=conflicts,
            )

        # Universe of origins appearing in conflicts
        origin_universe = sorted(list(set(orig for edge in conflict_edges for orig in edge)))

        # Find all hitting sets of conflict_edges with size <= fault_budget
        admissible_diagnoses: List[Set[str]] = []
        for size in range(1, fault_budget + 1):
            for candidate in itertools.combinations(origin_universe, size):
                cand_set = set(candidate)
                # Check if cand_set hits all conflict edges
                if all(not cand_set.isdisjoint(edge) for edge in conflict_edges):
                    admissible_diagnoses.append(cand_set)

        if not admissible_diagnoses:
            # Fault budget exceeded
            primary = conflict_witnesses[0]
            budget_wit = f"FAULT_BUDGET_EXCEEDED: f_budget={fault_budget}, conflicts={len(conflicts)}"
            return DiagnosisOutcome(
                diagnosis_result="FAULT_BUDGET_EXCEEDED",
                excluded_origins=set(),
                minimal_diagnoses=[],
                primary_witness=primary,
                witnesses=[primary, budget_wit],
                conflicting_pairs=conflicts,
            )

        # Compute inclusion-minimal admissible diagnoses D_min
        d_min: List[Set[str]] = []
        for diag in admissible_diagnoses:
            if not any(other < diag for other in admissible_diagnoses):
                if diag not in d_min:
                    d_min.append(diag)

        # Sort d_min deterministically
        d_min.sort(key=lambda s: (len(s), sorted(list(s))))

        # Compute intersection
        if not d_min:
            intersection: Set[str] = set()
        else:
            intersection = set.intersection(*d_min)

        if not intersection:
            # Intersection is empty: ambiguous diagnosis (Counterexample D-01)
            formatted_diag = [
                "{" + ", ".join(sorted(list(d))) + "}" for d in d_min
            ]
            primary = (
                f"DIAGNOSIS_AMBIGUOUS: minimal_diagnoses=[{', '.join(formatted_diag)}], intersection=EMPTY"
            )
            # Include unresolved conflict witness
            pair_witness = f"UNRESOLVED_CONFLICT: pair=({sorted(list(conflicts[0][0].source_origin_set))[0]}, {sorted(list(conflicts[0][1].source_origin_set))[0]})"
            return DiagnosisOutcome(
                diagnosis_result="DIAGNOSIS_AMBIGUOUS",
                excluded_origins=set(),
                minimal_diagnoses=d_min,
                primary_witness=primary,
                witnesses=[primary, pair_witness],
                conflicting_pairs=conflicts,
            )

        # Intersection is non-empty: provably faulty origins
        sorted_excluded = sorted(list(intersection))
        excluded_str = ", ".join(sorted_excluded)
        diag_str = "{" + ", ".join(sorted(list(d_min[0]))) + "}"
        primary = f"EXCLUDED_FAULTY_ORIGIN: origin={excluded_str}, diagnosis={diag_str}"

        # Surviving atoms count
        survivors = [a for a in atoms if set(a.source_origin_set).isdisjoint(intersection)]
        g4v_wit = f"SUPPORT_SET_G4V_VALIDATED: size={len(survivors)}, f_budget={fault_budget}"

        return DiagnosisOutcome(
            diagnosis_result="EXCLUDED_FAULTS",
            excluded_origins=intersection,
            minimal_diagnoses=d_min,
            primary_witness=primary,
            witnesses=[primary, g4v_wit],
            conflicting_pairs=conflicts,
        )
