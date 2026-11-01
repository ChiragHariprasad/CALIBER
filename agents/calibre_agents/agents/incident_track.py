from __future__ import annotations

from dataclasses import replace
import re
from typing import Any

from ..models import AgentOutput, SourceRecord, stable_id
from .base import BaseAgent


class IncidentTrackAgent(BaseAgent):
    agent_id = "incident_and_track_state_assurance"
    agent_version = "0.1.0"
    handled_feeds = {"rcm", "race_control", "racecontrolmessages", "trackstatus", "track_status"}

    def process(self, record: SourceRecord) -> AgentOutput:
        if not self.accepts(record):
            return AgentOutput()
        payload = record.payload

        # Live SignalR RaceControlMessages commonly wraps individual message
        # objects under a Messages map. Historical rcm.json is row-oriented.
        nested = payload.get("Messages") or payload.get("messages")
        if isinstance(nested, dict):
            rows = [value for value in nested.values() if isinstance(value, dict)]
        elif isinstance(nested, list):
            rows = [value for value in nested if isinstance(value, dict)]
        else:
            rows = [payload]

        combined = AgentOutput()
        for index, row in enumerate(rows):
            child = replace(
                record,
                record_id=stable_id(record.record_id, index, row),
                payload=row,
                source_timestamp=(
                    row.get("Utc") or row.get("utc") or row.get("Timestamp")
                    or record.source_timestamp
                ),
            )
            combined.extend(self._process_one(child))
        return combined

    def _process_one(self, record: SourceRecord) -> AgentOutput:
        row = record.payload
        message = str(row.get("msg") or row.get("Message") or row.get("MessageString") or row.get("message") or "")
        category = str(row.get("cat") or row.get("Category") or row.get("category") or "").strip().lower()
        flag = str(row.get("flag") or row.get("Flag") or "").strip().upper()
        status = str(row.get("status") or row.get("Status") or "").strip().upper()
        scope = row.get("scope") or row.get("Scope")
        sector = row.get("sector") or row.get("Sector")
        driver_no = row.get("dNum") or row.get("RacingNumber") or row.get("driver_number")
        lap = row.get("lap") or row.get("Lap") or row.get("lap_number")
        text = " ".join([message, category, flag, status]).upper()

        event_kind = "track_state_update" if record.feed in {"trackstatus", "track_status"} else "race_control_message"
        severity = "INFO"
        if "YELLOW" in text or "RED FLAG" in text or "SAFETY CAR" in text or "VSC" in text:
            event_kind = "track_state_or_flag"
            severity = "WARN"
        if any(token in text for token in ("INCIDENT", "COLLISION", "CONTACT", "CRASH", "SPUN")):
            event_kind = "incident_notice"
            severity = "WARN"
        if any(token in text for token in ("PENALTY", "INVESTIGATION", "UNSAFE RELEASE", "TRACK LIMITS")):
            event_kind = "steward_or_rule_related_notice"
            severity = "WARN"
        if "DRS" in text:
            event_kind = "drs_state_notice"

        findings = []
        if event_kind in {"incident_notice", "track_state_or_flag", "steward_or_rule_related_notice"}:
            findings.append(self.finding(
                record,
                code=f"RACE_CONTROL_{event_kind.upper()}",
                severity=severity,
                message="A race-control message indicates a relevant track, incident or stewarding event.",
                details={
                    "message": message,
                    "category": category,
                    "flag": flag,
                    "status": status,
                    "scope": scope,
                    "sector": sector,
                    "driver_number": driver_no,
                    "lap": lap,
                },
                limitations=["This agent classifies the published message; it does not determine fault, infringement or penalty."],
            ))

        car_mentions = re.findall(r"\b(?:CAR|CARS)\s+(\d{1,2})\b", message.upper())
        evidence = {
            "event_kind": event_kind,
            "message": message,
            "category": category or None,
            "flag": flag or None,
            "status": status or None,
            "scope": scope,
            "sector": sector,
            "driver_number": driver_no,
            "lap": lap,
            "car_number_mentions": sorted(set(car_mentions)),
        }
        output = AgentOutput(findings=findings)
        output.events.append(self.event(
            record,
            event_type=event_kind,
            evidence=evidence,
            findings=findings,
            provenance_extra={"classification_is_textual": True},
        ))
        return output
