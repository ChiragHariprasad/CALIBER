"""CALIBRE SIA v2.2 - Maximum Agreeing Support Set Search per CAL-DOC-007 §2.4.1."""
from __future__ import annotations

import itertools
from typing import Any, Callable, List, Optional, Sequence, Set, Tuple

from ..models.evidence import EvidenceAtom
from ..origin.manager import OriginManager


def atoms_agree(
    a: EvidenceAtom,
    b: EvidenceAtom,
    metric: str = "abs_diff",
    tolerance_eta: float = 0.05,
) -> bool:
    """Check pairwise agreement between two atoms under the declared metric and tolerance."""
    try:
        if metric in ("abs_diff", "relative_diff"):
            val_a = float(a.value)
            val_b = float(b.value)
            if metric == "abs_diff":
                return abs(val_a - val_b) <= (tolerance_eta + 1e-9)
            else:
                denom = max(abs(val_a), abs(val_b), 1e-9)
                return (abs(val_a - val_b) / denom) <= (tolerance_eta + 1e-9)
        elif metric == "discrete_equality":
            return str(a.value) == str(b.value)
        else:
            val_a = float(a.value)
            val_b = float(b.value)
            return abs(val_a - val_b) <= (tolerance_eta + 1e-9)
    except (ValueError, TypeError):
        return str(a.value) == str(b.value)


class SupportSetSearch:
    """Finds maximum pairwise-agreeing, origin-disjoint support sets Q* bounded by M_max."""

    @staticmethod
    def search_support_set(
        atoms: Sequence[EvidenceAtom],
        metric: str = "abs_diff",
        tolerance_eta: float = 0.05,
        axis: str = "source",  # "source" | "processing"
        M_max: int = 10,
    ) -> Tuple[List[EvidenceAtom], bool, Optional[str]]:
        """Search for the largest agreeing, origin-disjoint support set Q*.
        
        Returns:
            Tuple of:
            - selected_support_set: List[EvidenceAtom]
            - budget_exceeded: bool (True if len(atoms) > M_max)
            - overflow_witness: Optional[str] (e.g. "SEARCH_BUDGET_EXCEEDED: count=12, M_max=10")
        """
        n = len(atoms)
        if n > M_max:
            witness = f"SEARCH_BUDGET_EXCEEDED: count={n}, M_max={M_max}"
            # Return all non-empty atoms as unverified support, but flag budget_exceeded
            # Per CAL-DOC-007 §2.4.1 / §2.9.2: reports conservative bound, emits witness, state <= UNKNOWN
            sorted_atoms = sorted(
                atoms,
                key=lambda a: (a.transport_event_time_utc, a.evidence_atom_id),
            )
            return list(sorted_atoms), True, witness

        if n == 0:
            return [], False, None

        if n == 1:
            return [atoms[0]], False, None

        # Build adjacency matrix of pairwise agreement and origin disjointness
        adj: List[List[bool]] = [[False] * n for _ in range(n)]
        for i in range(n):
            for j in range(i + 1, n):
                orig_disjoint = True
                if axis == "source":
                    orig_disjoint = OriginManager.are_disjoint(
                        atoms[i].source_origin_set, atoms[j].source_origin_set
                    )
                elif axis == "processing":
                    orig_disjoint = OriginManager.are_disjoint(
                        atoms[i].processing_origin_set, atoms[j].processing_origin_set
                    )

                agree = atoms_agree(atoms[i], atoms[j], metric, tolerance_eta)
                if orig_disjoint and agree:
                    adj[i][j] = True
                    adj[j][i] = True

        # Bron-Kerbosch maximal clique enumeration with pivoting
        cliques: List[List[int]] = []

        def bron_kerbosch(r: Set[int], p: Set[int], x: Set[int]) -> None:
            if not p and not x:
                cliques.append(list(r))
                return
            pivot = next(iter(p | x)) if (p | x) else -1
            candidates = [v for v in list(p) if not (pivot != -1 and adj[pivot][v])]
            for v in candidates:
                neighbors = {u for u in range(n) if adj[v][u]}
                bron_kerbosch(r | {v}, p & neighbors, x & neighbors)
                p.remove(v)
                x.add(v)

        bron_kerbosch(set(), set(range(n)), set())

        if not cliques:
            # Fallback to single atom
            return [atoms[0]], False, None

        # Sort cliques by:
        # 1. Size descending
        # 2. Canonical atom order of members ascending
        def clique_sort_key(c: List[int]) -> tuple:
            member_atoms = [atoms[idx] for idx in c]
            sorted_members = sorted(
                member_atoms,
                key=lambda a: (a.transport_event_time_utc, a.evidence_atom_id),
            )
            member_keys = [(a.transport_event_time_utc, a.evidence_atom_id) for a in sorted_members]
            return (-len(c), member_keys)

        cliques.sort(key=clique_sort_key)
        best_indices = cliques[0]

        best_atoms = [atoms[i] for i in best_indices]
        best_atoms.sort(key=lambda a: (a.transport_event_time_utc, a.evidence_atom_id))

        return best_atoms, False, None
