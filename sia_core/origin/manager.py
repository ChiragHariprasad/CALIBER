"""CALIBRE SIA v2.2 - Origin Manager and Corroboration Lineage per CAL-DOC-007 §2.3."""
from __future__ import annotations

from typing import Iterable, List, Optional, Sequence, Set
from ..models.evidence import EvidenceAtom


class OriginManager:
    """Manages dual-axis origin sets, independence analysis, and supersession."""

    @staticmethod
    def are_disjoint(origins_a: Iterable[str], origins_b: Iterable[str]) -> bool:
        """Two atoms are independent on an axis iff their origin sets on that axis are disjoint."""
        set_a = set(origins_a)
        set_b = set(origins_b)
        return set_a.isdisjoint(set_b)

    @staticmethod
    def propagate_origins(
        input_atoms: Sequence[EvidenceAtom],
        processor_family_id: str,
    ) -> tuple[Set[str], Set[str]]:
        """Propagate source and processing origins for derived evidence.
        Result is union of inputs' source origins, and union of processing origins + processor_family_id.
        """
        source_origins: Set[str] = set()
        processing_origins: Set[str] = {processor_family_id}

        for atom in input_atoms:
            source_origins.update(atom.source_origin_set)
            processing_origins.update(atom.processing_origin_set)

        return source_origins, processing_origins

    @staticmethod
    def apply_supersession(atoms: Sequence[EvidenceAtom]) -> List[EvidenceAtom]:
        """Apply anti-churn supersession per Defect D-09 / CAL-DOC-007 §2.3.3.
        
        If two atoms share (raw_event_ids, role_id, processor_family_id),
        the newer transform_version supersedes the older.
        """
        if not atoms:
            return []

        # Check explicit supersedes_atom_id links first
        superseded_ids = {a.supersedes_atom_id for a in atoms if a.supersedes_atom_id}

        # Also group by (tuple(sorted(raw_event_ids)), role_id, processor_family_id)
        grouped: dict[tuple, list[EvidenceAtom]] = {}
        for a in atoms:
            raw_key = tuple(sorted(a.raw_event_ids)) if a.raw_event_ids else (a.evidence_atom_id,)
            group_key = (raw_key, a.role_id, a.processor_family_id)
            grouped.setdefault(group_key, []).append(a)

        survivors: List[EvidenceAtom] = []
        for group in grouped.values():
            if len(group) == 1:
                if group[0].evidence_atom_id not in superseded_ids:
                    survivors.append(group[0])
            else:
                # If explicit supersedes_atom_id exists, prefer that
                explicit_active = [a for a in group if a.evidence_atom_id not in superseded_ids]
                if explicit_active:
                    # Pick highest transform_version
                    explicit_active.sort(key=lambda x: x.transform_version, reverse=True)
                    survivors.append(explicit_active[0])
                else:
                    group.sort(key=lambda x: x.transform_version, reverse=True)
                    survivors.append(group[0])

        return survivors
