from __future__ import annotations

from typing import Any

from ..models import AgentOutput, SourceRecord
from .base import BaseAgent


def num(value: Any) -> float | None:
    if value is None or value == "None":
        return None
    if isinstance(value, dict):
        for key in ("Value", "value", "Current"):
            if key in value and value[key] is not None:
                value = value[key]
                break
    try:
        x = float(value)
        return x if x == x and abs(x) != float("inf") else None
    except (TypeError, ValueError):
        return None


def pick(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row and row[key] is not None:
            return row[key]
    return None


class EnvironmentalContextAgent(BaseAgent):
    agent_id = "environmental_context_assurance"
    agent_version = "0.1.0"
    handled_feeds = {"weather", "weatherdata", "weather_data"}

    def __init__(self, max_track_temp_jump_c: float = 8.0):
        self.max_track_temp_jump_c = max_track_temp_jump_c
        self._last: dict[tuple[Any, Any], dict[str, float]] = {}

    def process(self, record: SourceRecord) -> AgentOutput:
        if not self.accepts(record):
            return AgentOutput()
        row = record.payload
        environment = {
            "sample_time_s": num(pick(row, "wT", "Time", "time")),
            "air_temperature_c": num(pick(row, "wAT", "AirTemp", "air_temperature")),
            "track_temperature_c": num(pick(row, "wTT", "TrackTemp", "track_temperature")),
            "humidity_pct": num(pick(row, "wH", "Humidity", "humidity")),
            "pressure_mbar": num(pick(row, "wP", "Pressure", "pressure")),
            "rainfall": pick(row, "wR", "Rainfall", "rainfall"),
            "wind_direction_deg": num(pick(row, "wWD", "WindDirection", "wind_direction")),
            "wind_speed_mps": num(pick(row, "wWS", "WindSpeed", "wind_speed")),
        }
        findings = []
        bounds = (
            ("air_temperature_c", -50, 60),
            ("track_temperature_c", -20, 100),
            ("humidity_pct", 0, 100),
            ("pressure_mbar", 800, 1100),
            ("wind_direction_deg", 0, 360),
            ("wind_speed_mps", 0, 100),
        )
        for field, lower, upper in bounds:
            value = environment[field]
            if value is not None and not lower <= value <= upper:
                findings.append(self.finding(
                    record,
                    code="WEATHER_VALUE_OUT_OF_CONFIGURED_RANGE",
                    severity="WARN",
                    message=f"{field} is outside the configured plausibility range.",
                    details={"field": field, "value": value, "lower": lower, "upper": upper},
                    limitations=["These are broad input-screening bounds, not official circuit-specific weather limits."],
                ))

        key = (record.context.get("year"), record.context.get("meeting"), record.context.get("session"))
        previous = self._last.get(key)
        current_t = environment["sample_time_s"]
        current_track = environment["track_temperature_c"]
        if previous and current_t is not None and previous.get("time") is not None:
            delta_t = current_t - previous["time"]
            if delta_t <= 0:
                findings.append(self.finding(
                    record,
                    code="WEATHER_TIME_NOT_MONOTONIC",
                    severity="WARN",
                    message="Weather sample time is duplicate or moves backwards.",
                    details={"previous_time_s": previous["time"], "current_time_s": current_t},
                ))
            elif current_track is not None and previous.get("track") is not None:
                delta = current_track - previous["track"]
                if abs(delta) > self.max_track_temp_jump_c:
                    findings.append(self.finding(
                        record,
                        code="TRACK_TEMPERATURE_STEP_CHANGE",
                        severity="INFO",
                        message="Track temperature changed sharply between consecutive samples.",
                        details={"delta_c": delta, "delta_time_s": delta_t},
                        limitations=["A step change may reflect a source update or sampling discontinuity; it is not proof of a sensor fault."],
                    ))
        if current_t is not None:
            self._last[key] = {"time": current_t, "track": current_track}

        output = AgentOutput(findings=findings)
        output.events.append(self.event(
            record,
            event_type="environmental_sample_assessed",
            evidence=environment,
            findings=findings,
            provenance_extra={"weather_is_context_not_exculpatory_evidence": True},
        ))
        return output
