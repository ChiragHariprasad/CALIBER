from __future__ import annotations

from dataclasses import replace
from typing import Any

from ..models import AgentOutput, SourceRecord, stable_id
from .base import BaseAgent


def as_number(value: Any) -> float | None:
    if value is None or value == "None":
        return None
    if isinstance(value, dict):
        for key in ("Value", "value", "Time"):
            if key in value and value[key] is not None:
                value = value[key]
                break
    try:
        result = float(value)
        return result if result == result and abs(result) != float("inf") else None
    except (TypeError, ValueError):
        return None


class RaceTimingSessionAgent(BaseAgent):
    """Parses timing/session state into structured evidence for the core.

    This is a format-specific evidence adapter, not an independent sporting
    rules engine or a new decision authority.
    """

    agent_id = "race_timing_and_session_evidence"
    agent_version = "0.1.0"
    handled_feeds = {
        "session_laptimes", "laptimes", "timingdata", "timingstats",
        "timingappdata", "driverlist", "sessioninfo", "session_info",
        "lapcount", "sessionstatus", "session_status", "drivers",
    }

    def process(self, record: SourceRecord) -> AgentOutput:
        if not self.accepts(record):
            return AgentOutput()
        feed = record.feed
        row = record.payload

        if feed in {"session_laptimes", "laptimes"}:
            return self._process_lap_record(record)
        if feed in {"timingdata", "timingstats", "timingappdata"}:
            return self._process_timing_lines(record)
        if feed in {"driverlist", "drivers"}:
            return self._process_driver_metadata(record)
        if feed in {"sessioninfo", "session_info"}:
            return self._process_session_info(record)
        if feed in {"lapcount", "sessionstatus", "session_status"}:
            return self._process_session_state(record)
        return AgentOutput()

    def _process_lap_record(self, record: SourceRecord) -> AgentOutput:
        row = record.payload
        duration = as_number(row.get("time"))
        sector_times = [as_number(row.get(key)) for key in ("s1", "s2", "s3")]
        sector_sum = sum(x for x in sector_times if x is not None) if all(x is not None for x in sector_times) else None
        residual = (sector_sum - duration) if sector_sum is not None and duration is not None else None
        findings = []
        if duration is not None and duration <= 0:
            findings.append(self.finding(
                record,
                code="LAP_DURATION_NON_POSITIVE",
                severity="WARN",
                message="Lap duration is not positive.",
                details={"lap_duration_s": duration},
            ))
        if residual is not None and abs(residual) > 1.5:
            findings.append(self.finding(
                record,
                code="LAP_SECTOR_SUM_RESIDUAL",
                severity="INFO",
                message="The sum of three sector times differs from the reported lap duration.",
                details={"sector_sum_s": sector_sum, "lap_duration_s": duration, "residual_s": residual},
                limitations=["Sector and lap values may have different timing boundaries or source rounding."],
            ))

        evidence = {
            "driver_code": row.get("drv"),
            "driver_number": row.get("dNum"),
            "lap_number": row.get("lap"),
            "lap_duration_s": duration,
            "sector_1_s": sector_times[0],
            "sector_2_s": sector_times[1],
            "sector_3_s": sector_times[2],
            "sector_sum_s": sector_sum,
            "sector_sum_residual_s": residual,
            "session_elapsed_at_lap_end_s": as_number(row.get("sesT")),
            "session_elapsed_at_lap_start_s": as_number(row.get("lST")),
            "position": row.get("pos"),
            "tyre_compound": row.get("compound"),
            "stint": row.get("stint"),
            "pit_in_session_s": as_number(row.get("pin")),
            "pit_out_session_s": as_number(row.get("pout")),
            "lap_deleted": row.get("del"),
            "lap_deletion_reason": row.get("delR"),
            "personal_best": row.get("pb"),
        }
        output = AgentOutput(findings=findings)
        output.events.append(self.event(
            record,
            event_type="lap_timing_evidence",
            evidence=evidence,
            findings=findings,
            provenance_extra={"lap_time_field_semantics": "time_is_lap_duration; sesT_is_session_elapsed_at_lap_end"},
        ))
        return output

    def _process_timing_lines(self, record: SourceRecord) -> AgentOutput:
        payload = record.payload
        lines = payload.get("Lines") or payload.get("lines")
        if not isinstance(lines, dict):
            # Some handlers may already have a single driver-line object.
            lines = {str(payload.get("RacingNumber") or payload.get("driver_number") or "unknown"): payload}

        output = AgentOutput()
        for driver_number, line in lines.items():
            if not isinstance(line, dict):
                continue
            child = replace(
                record,
                record_id=stable_id(record.record_id, driver_number, line),
                payload=line,
                driver_number=str(driver_number),
            )
            evidence = {
                "driver_number": str(driver_number),
                "last_lap_time": line.get("LastLapTime"),
                "best_lap_time": line.get("BestLapTime"),
                "number_of_laps": line.get("NumberOfLaps"),
                "sectors": line.get("Sectors"),
                "speeds": line.get("Speeds"),
                "gap_to_leader": line.get("GapToLeader"),
                "interval_to_ahead": line.get("IntervalToNext") or line.get("IntervalToAhead"),
                "pit_status": {
                    key: line.get(key)
                    for key in ("InPit", "PitOut", "PitLaneTime", "NumberOfPitStops")
                    if key in line
                },
                "line_payload": line,
            }
            output.events.append(self.event(
                child,
                event_type="live_driver_timing_snapshot",
                evidence=evidence,
                provenance_extra={"payload_mode": "provider_topic_state"},
            ))
        if not output.events:
            output.events.append(self.event(
                record,
                event_type="live_timing_topic_snapshot",
                evidence={"payload": payload},
            ))
        return output

    def _process_driver_metadata(self, record: SourceRecord) -> AgentOutput:
        payload = record.payload
        nested = payload.get("Drivers") or payload.get("drivers")
        if isinstance(nested, dict):
            rows = [(str(number), value) for number, value in nested.items() if isinstance(value, dict)]
        elif isinstance(nested, list):
            rows = [(None, value) for value in nested if isinstance(value, dict)]
        else:
            # A live DriverList can be a direct map keyed by car number.
            rows = [
                (str(number), value) for number, value in payload.items()
                if isinstance(value, dict) and (str(number).isdigit() or "RacingNumber" in value)
            ]
            if not rows:
                rows = [(None, payload)]

        output = AgentOutput()
        for driver_number_key, row in rows:
            number = (row.get("dn") or row.get("RacingNumber") or row.get("DriverNumber") or driver_number_key)
            child = replace(record, record_id=stable_id(record.record_id, number, row), payload=row, driver_number=str(number) if number is not None else None)
            evidence = {
                "driver_code": row.get("driver") or row.get("Tla") or row.get("Abbreviation"),
                "driver_number": number,
                "first_name": row.get("fn") or row.get("FirstName"),
                "last_name": row.get("ln") or row.get("LastName"),
                "team": row.get("team") or row.get("TeamName"),
                "team_colour": row.get("tc") or row.get("TeamColour"),
                "raw_driver_metadata": row,
            }
            output.events.append(self.event(
                child,
                event_type="driver_metadata_observed",
                evidence=evidence,
            ))
        return output

    def _process_session_info(self, record: SourceRecord) -> AgentOutput:
        row = record.payload
        # Store status timelines separately; they are arrays over session time.
        evidence = {
            "session_t0_date": row.get("t0_date"),
            "session_start_time_s": row.get("session_start_time"),
            "scheduled_total_laps": row.get("total_laps"),
            "track_status_timeline": row.get("track_status"),
            "session_status_timeline": row.get("session_status"),
            "session_metadata": row.get("session_info"),
            "raw_session_info_record": row,
        }
        return AgentOutput(events=[self.event(
            record,
            event_type="session_metadata_observed",
            evidence=evidence,
        )])

    def _process_session_state(self, record: SourceRecord) -> AgentOutput:
        row = record.payload
        evidence = {
            "state_feed": record.feed,
            "status": row.get("Status") or row.get("status"),
            "current_lap": row.get("CurrentLap") or row.get("Count") or row.get("CurrentLapNumber"),
            "raw_state": row,
        }
        return AgentOutput(events=[self.event(
            record,
            event_type="session_state_observed",
            evidence=evidence,
        )])
