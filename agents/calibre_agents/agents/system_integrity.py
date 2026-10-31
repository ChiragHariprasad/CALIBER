from __future__ import annotations

from typing import Any
import hashlib
import json

from ..models import AgentOutput, SourceRecord
from .base import BaseAgent


TIME_SERIES_FEEDS = {
    "tel", "telemetry", "weather", "rcm", "race_control",
    "racecontrolmessages", "trackstatus", "track_status",
    "timingdata", "timing_data", "position", "cardata.z",
}
TELEMETRY_FEEDS = {"tel", "telemetry", "position", "cardata.z"}


class VehicleElectronicsSystemIntegrityAgent(BaseAgent):
    """Monitors observable feed and schema integrity, not hidden ECU state."""

    agent_id = "vehicle_electronics_and_system_integrity"
    agent_version = "0.1.0"
    handled_feeds = {"*"}

    def __init__(self, repeated_payload_limit: int = 20):
        self.repeated_payload_limit = repeated_payload_limit
        self._last_by_feed: dict[str, dict[str, Any]] = {}
        self._counts: dict[str, int] = {}
        self._ecu_visibility_reported: set[str] = set()

    def accepts(self, record: SourceRecord) -> bool:
        return True

    def process(self, record: SourceRecord) -> AgentOutput:
        feed = record.feed
        state_key = "|".join(str(value or "") for value in (
            record.provider, record.context.get("year"),
            record.context.get("meeting"), record.context.get("session"), feed,
        ))
        payload_text = json.dumps(record.payload, sort_keys=True, default=str)
        digest = hashlib.sha256(payload_text.encode("utf-8")).hexdigest()
        previous = self._last_by_feed.get(state_key)
        count = self._counts.get(state_key, 0) + 1
        self._counts[state_key] = count
        findings = []

        repeated_count = 0
        if previous and previous.get("digest") == digest:
            repeated_count = previous.get("repeated_count", 0) + 1
            if repeated_count == self.repeated_payload_limit:
                findings.append(self.finding(
                    record,
                    code="REPEATED_IDENTICAL_PAYLOADS",
                    severity="INFO",
                    message="The same payload has been observed repeatedly for this feed.",
                    details={"feed": feed, "repeat_count": repeated_count},
                    limitations=["Repeated values may be valid unchanged state or keyframes; this is a monitoring signal, not a failure verdict."],
                ))
        else:
            repeated_count = 0

        missing = []
        if not record.payload:
            missing.append("empty_payload")
        expects_time = feed in TIME_SERIES_FEEDS or feed.endswith("_tel")
        if expects_time and record.source_timestamp is None:
            missing.append("source_timestamp_missing")
        if missing:
            findings.append(self.finding(
                record,
                code="SOURCE_RECORD_INCOMPLETE",
                severity="WARN",
                message="A time-series source record is missing expected structural information.",
                details={"missing": missing, "feed": feed},
            ))

        ecu_keys = {"fault_codes", "ecu_status", "power_state", "can_bus_status", "pu_status"}
        exposed = sorted(ecu_keys.intersection(record.payload.keys()))
        session_key = "|".join(str(value or "") for value in (
            record.context.get("year"), record.context.get("meeting"),
            record.context.get("session"),
        )) or "unknown_session"
        is_telemetry_feed = feed in TELEMETRY_FEEDS or feed.endswith("_tel")
        if is_telemetry_feed and session_key not in self._ecu_visibility_reported and not exposed:
            self._ecu_visibility_reported.add(session_key)
            findings.append(self.finding(
                record,
                code="INTERNAL_ELECTRONICS_NOT_OBSERVABLE_IN_PUBLIC_FEED",
                severity="INFO",
                message="The selected public feed does not expose ECU, power-unit or internal bus diagnostic state.",
                details={"feed": feed, "checked_fields": sorted(ecu_keys)},
                limitations=["Absence of these public fields is not evidence of an electronic fault; internal ECU diagnostics require an authorized source."],
            ))

        self._last_by_feed[state_key] = {
            "digest": digest,
            "repeated_count": repeated_count,
            "source_timestamp": record.source_timestamp,
        }

        evidence = {
            "feed": feed,
            "record_count_seen": count,
            "payload_sha256": digest,
            "payload_size_bytes_estimate": len(payload_text.encode("utf-8")),
            "source_timestamp_present": record.source_timestamp is not None,
            "session_time_present": record.session_time_s is not None,
            "internal_electronics_fields_exposed": exposed,
            "monitoring_scope": "observable_feed_integrity_only",
        }
        output = AgentOutput(findings=findings)
        output.events.append(self.event(
            record,
            event_type="source_feed_integrity_assessed",
            evidence=evidence,
            findings=findings,
            provenance_extra={"does_not_assert_hidden_ecu_health": True},
        ))
        return output
