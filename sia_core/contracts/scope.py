"""Deterministic decision-instance keys derived from DEC scope contracts."""
from __future__ import annotations

from typing import Any, Mapping

from .models import DecisionEvidenceContract, ScopeClass


class DecisionScopeError(ValueError):
    """Raised when evidence cannot identify the scope required by a DEC."""


def decision_scope_key(contract: DecisionEvidenceContract, scope: Mapping[str, Any]) -> str:
    """Format the configured scope template after checking its required fields.

    This prevents session-scoped evidence from being split by incidental driver
    fields and prevents lap-scoped evidence from being merged across laps.
    """
    session_id = scope.get("session_id") or scope.get("session")
    driver_id = scope.get("driver_number") or scope.get("driver_id")
    lap_number = scope.get("lap_number") or scope.get("lap")
    sector_number = scope.get("sector_number") or scope.get("sector")

    required = {
        ScopeClass.SESSION: ("session_id", session_id),
        ScopeClass.DRIVER_SESSION: ("driver_id", driver_id),
        ScopeClass.DRIVER_LAP: ("driver_id", driver_id),
        ScopeClass.SECTOR: ("driver_id", driver_id),
    }
    field_name, value = required[contract.decision_scope.scope_class]
    if value is None or str(value).strip() == "":
        raise DecisionScopeError(f"MISSING_DECISION_SCOPE_FIELD:{field_name}")
    if contract.decision_scope.scope_class in (ScopeClass.DRIVER_LAP, ScopeClass.SECTOR):
        if lap_number is None or str(lap_number).strip() == "":
            raise DecisionScopeError("MISSING_DECISION_SCOPE_FIELD:lap_number")
    if contract.decision_scope.scope_class == ScopeClass.SECTOR:
        if sector_number is None or str(sector_number).strip() == "":
            raise DecisionScopeError("MISSING_DECISION_SCOPE_FIELD:sector_number")
    if session_id is None or str(session_id).strip() == "":
        raise DecisionScopeError("MISSING_DECISION_SCOPE_FIELD:session_id")

    template = contract.decision_scope.scope_key_pattern
    if not template:
        template = {
            ScopeClass.SESSION: "{session_id}",
            ScopeClass.DRIVER_SESSION: "{session_id}|{driver_id}",
            ScopeClass.DRIVER_LAP: "{session_id}|{driver_id}|lap{lap_number}",
            ScopeClass.SECTOR: "{session_id}|{driver_id}|lap{lap_number}|sector{sector_number}",
        }[contract.decision_scope.scope_class]
    values = {
        "session_id": str(session_id),
        "driver_id": str(driver_id) if driver_id is not None else "",
        "driver_number": str(driver_id) if driver_id is not None else "",
        "lap_number": str(lap_number) if lap_number is not None else "",
        "sector_number": str(sector_number) if sector_number is not None else "",
    }
    try:
        return template.format_map(values)
    except (KeyError, ValueError) as exc:
        raise DecisionScopeError(f"INVALID_DECISION_SCOPE_TEMPLATE:{exc}") from exc
