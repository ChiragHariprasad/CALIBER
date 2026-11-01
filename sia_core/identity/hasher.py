"""High-Level Cryptographic Content Hasher per CAL-DOC-007 §2.1.1, §2.6.2."""
from __future__ import annotations

from typing import Any, Dict, List, Optional
from .canonical import jcs_sha256, jcs_serialize
from .quantization import quantize_value

__all__ = [
    "compute_raw_event_id",
    "compute_evidence_atom_id",
    "compute_verdict_hash",
    "quantize_value",
]


def compute_raw_event_id(
    provider: str,
    feed: str,
    source_record_key: Optional[str],
    session_id: Optional[str],
    scope: Dict[str, Any],
    source_time: Dict[str, Any],
    payload_sha256: str,
    received_at_utc: Optional[str] = None,
) -> str:
    """Compute deterministic raw_event_id content hash per CAL-DOC-007 §2.1.1."""
    canonical_obj: Dict[str, Any] = {
        "feed": feed,
        "payload_sha256": payload_sha256,
        "provider": provider,
        "scope": scope,
        "session_id": session_id,
        "source_record_key": source_record_key,
        "source_time": {
            "semantics": source_time.get("semantics", "UNKNOWN"),
            "unit": source_time.get("unit", "s"),
            "value": source_time.get("value"),
        },
    }
    # Acquisition-bound fallback rule (CAL-DOC-007 §2.1.1)
    semantics = canonical_obj["source_time"]["semantics"]
    if (semantics in ("UNKNOWN", "RECEIPT_TIME_ONLY")) and not source_record_key:
        if not received_at_utc:
            raise ValueError("received_at_utc required for ACQUISITION_BOUND raw_event_id fallback")
        canonical_obj["received_at_utc"] = received_at_utc

    return jcs_sha256(canonical_obj)


def compute_evidence_atom_id(
    raw_event_ids: List[str],
    processor_family_id: str,
    transform_version: str,
    role_id: str,
    scope_key: str,
    normalized_value: Any,
    reference_atom_ids: Optional[List[str]] = None,
    unit: str = "s",
) -> str:
    """Compute deterministic evidence_atom_id content hash per CAL-DOC-007 §2.1.1."""
    if isinstance(normalized_value, (float, int)) and not isinstance(normalized_value, bool):
        q_val = quantize_value(normalized_value, unit)
        val_hash = jcs_sha256(q_val)
    else:
        val_hash = jcs_sha256(normalized_value)

    canonical_obj: Dict[str, Any] = {
        "normalized_value_hash": val_hash,
        "processor_family_id": processor_family_id,
        "raw_event_ids": sorted(raw_event_ids),
        "reference_atom_ids": sorted(reference_atom_ids or []),
        "role_id": role_id,
        "scope_key": scope_key,
        "transform_version": transform_version,
    }
    return jcs_sha256(canonical_obj)


def compute_verdict_hash(
    contract_id: str,
    contract_version: str,
    contract_epoch: int,
    decision_instance_id: str,
    integrity_state: str,
    resolution_maturity: str,
    finality_status: str,
    canonical_frontier: Dict[str, Optional[str]],
    corroboration_profile_by_role: Dict[str, Any],
    validity_timeline: List[Dict[str, str]],
    sorted_support_atom_ids: List[str],
    sorted_reference_atom_ids: List[str],
    pinned_finality_frontier: Optional[str] = None,
) -> str:
    """Compute deterministic verdict_hash content hash per CAL-DOC-007 §2.6.2."""
    frontier = dict(canonical_frontier)
    if finality_status == "FINAL" and pinned_finality_frontier:
        frontier = {
            "run_clock": pinned_finality_frontier,
            "watermark": pinned_finality_frontier,
        }

    canonical_obj: Dict[str, Any] = {
        "canonical_frontier": frontier,
        "contract": {
            "epoch": contract_epoch,
            "id": contract_id,
            "version": contract_version,
        },
        "corroboration_profile_by_role": corroboration_profile_by_role,
        "decision_instance_id": decision_instance_id,
        "finality_status": finality_status,
        "integrity_state": integrity_state,
        "resolution_maturity": resolution_maturity,
        "sorted_reference_atom_ids": sorted(sorted_reference_atom_ids),
        "sorted_support_atom_ids": sorted(sorted_support_atom_ids),
        "validity_timeline": validity_timeline,
    }
    return jcs_sha256(canonical_obj)
