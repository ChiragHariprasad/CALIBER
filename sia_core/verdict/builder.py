"""CALIBRE SIA v2.2 - Integrity Assessment Verdict Builder per CAL-DOC-007 §2.6."""
from __future__ import annotations

from typing import Any, Dict, List, Optional
from ..identity.hasher import compute_verdict_hash


def build_assessment_verdict(
    contract_id: str,
    contract_version: str,
    contract_epoch: int,
    decision_instance_id: str,
    integrity_state: str,
    resolution_maturity: str,
    finality_status: str,
    knowledge_frontier: Dict[str, Optional[str]],
    corroboration_profile_by_role: Dict[str, Any],
    validity_timeline: List[Dict[str, str]],
    primary_binding_witness: str,
    witness_set: List[str],
    limiting_children: List[str],
    alternatives: List[str],
    diagnosis_result: str,
    limitations: List[str],
    decision_maturity: str,
    revision_number: int = 1,
    previous_verdict_hash: Optional[str] = None,
    next_transition_at: Optional[str] = None,
    lease_until: Optional[str] = None,
    lease_clock_domain: str = "LIVE_UTC",
    pinned_finality_frontier: Optional[str] = None,
) -> Dict[str, Any]:
    """Assemble and hash a complete IntegrityAssessment verdict."""
    all_support: List[str] = []
    all_reference: List[str] = []
    for r_name, r_data in corroboration_profile_by_role.items():
        atoms = r_data.get("support_atom_ids", [])
        if "reference" in r_name:
            all_reference.extend(atoms)
        else:
            all_support.extend(atoms)

    sorted_support = sorted(list(set(all_support)))
    sorted_reference = sorted(list(set(all_reference)))

    canonical_frontier = {
        "run_clock": knowledge_frontier.get("run_clock", ""),
        "watermark": knowledge_frontier.get("watermark"),
    }

    vh = compute_verdict_hash(
        contract_id=contract_id,
        contract_version=contract_version,
        contract_epoch=contract_epoch,
        decision_instance_id=decision_instance_id,
        integrity_state=integrity_state,
        resolution_maturity=resolution_maturity,
        finality_status=finality_status,
        canonical_frontier=canonical_frontier,
        corroboration_profile_by_role=corroboration_profile_by_role,
        validity_timeline=validity_timeline,
        sorted_support_atom_ids=sorted_support,
        sorted_reference_atom_ids=sorted_reference,
        pinned_finality_frontier=pinned_finality_frontier,
    )

    return {
        "schema_name": "calibre.integrity_assessment",
        "schema_version": "2.0",
        "decision_instance_id": decision_instance_id,
        "contract": {
            "id": contract_id,
            "version": contract_version,
            "epoch": contract_epoch,
        },
        "integrity_state": integrity_state,
        "corroboration_profile_by_role": corroboration_profile_by_role,
        "validity_timeline": validity_timeline,
        "next_transition_at": next_transition_at,
        "lease_until": lease_until,
        "lease_clock_domain": lease_clock_domain,
        "resolution_maturity": resolution_maturity,
        "finality_status": finality_status,
        "revision_number": revision_number,
        "previous_verdict_hash": previous_verdict_hash,
        "knowledge_frontier": canonical_frontier,
        "primary_binding_witness": primary_binding_witness,
        "witness_set": witness_set,
        "limiting_children": limiting_children,
        "alternatives": alternatives,
        "diagnosis_result": diagnosis_result,
        "limitations": limitations,
        "decision_maturity": decision_maturity,
        "verdict_hash": vh,
    }
