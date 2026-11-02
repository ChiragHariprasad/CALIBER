"""Run ownership checks for messages shared on Kafka topics."""
from __future__ import annotations

from typing import Any

REPLAY_SOURCE_MODES = {"HISTORICAL_REPLAY", "REPLAY", "BATCH"}


def message_run_scope(message: Any) -> tuple[str | None, str | None]:
    """Return (run_id, source_mode) from raw, evidence, output, or verdict data."""
    if not isinstance(message, dict):
        return None, None

    event = message.get("event") if isinstance(message.get("event"), dict) else message
    provenance = event.get("provenance", {}) if isinstance(event, dict) else {}
    scope = event.get("scope", {}) if isinstance(event, dict) else {}
    run_id = (
        message.get("run_id")
        or provenance.get("run_id")
        or scope.get("run_id")
    )
    source_mode = message.get("source_mode") or provenance.get("source_mode")

    if run_id is None:
        decision_id = message.get("decision_instance_id")
        marker = "#race:"
        if isinstance(decision_id, str) and marker in decision_id:
            run_id = decision_id.split(marker, 1)[1].split("|", 1)[0]

    return (str(run_id) if run_id is not None else None,
            str(source_mode).upper() if source_mode is not None else None)


def belongs_to_run(message: Any, target_run_id: str | None) -> bool:
    """Keep replay output isolated while allowing live feeds to fan into one pipeline."""
    if target_run_id is None:
        return True
    message_run_id, source_mode = message_run_scope(message)
    if source_mode in REPLAY_SOURCE_MODES:
        return message_run_id == target_run_id
    if message_run_id is None:
        return False
    # LIVE and MOCK are continuous shared feeds. Other unlabelled messages
    # must identify the exact run to enter a run-scoped pipeline.
    return source_mode in {"LIVE", "MOCK"} or message_run_id == target_run_id
