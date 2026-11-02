from __future__ import annotations

from typing import Any
from datetime import datetime

from ..models import AgentOutput, SourceRecord
from .base import BaseAgent


def number(value: Any) -> float | None:
    if value is None or value == "None":
        return None
    try:
        result = float(value)
        return result if result == result and abs(result) != float("inf") else None
    except (TypeError, ValueError):
        return None


class VehicleTwinSensorAgent(BaseAgent):
    """Telemetry quality and bounded physical-consistency evidence.

    Does not claim to have access to ECU/IMU raw channels. Exported acceleration
    fields and interpolated positions remain explicitly labelled as such.
    """

    agent_id = "vehicle_twin_sensor_assurance"
    agent_version = "0.1.0"
    handled_feeds = {"tel", "_tel", "telemetry", "car_data"}

    def accepts(self, record: SourceRecord) -> bool:
        return record.feed in self.handled_feeds or record.feed.endswith("_tel")

    def __init__(self, max_sample_gap_s: float = 1.0, max_speed_kmh: float = 450.0):
        self.max_sample_gap_s = max_sample_gap_s
        self.max_speed_kmh = max_speed_kmh
        self._last: dict[tuple[str | None, int | None], dict[str, float]] = {}

    def process(self, record: SourceRecord) -> AgentOutput:
        if not self.accepts(record):
            return AgentOutput()

        row = record.payload
        key = (
            record.context.get("year"),
            record.context.get("meeting"),
            record.context.get("session"),
            record.driver_number or record.driver_code,
            record.lap_number,
        )
        findings = []
        output = AgentOutput()

        fields = {
            "speed_kmh": number(row.get("speed")),
            "rpm": number(row.get("rpm")),
            "gear": number(row.get("n_gear", row.get("gear"))),
            "throttle": number(row.get("throttle")),
            "brake": number(row.get("brake")),
            "brake_signal_semantics": row.get("brake_signal_semantics", "SOURCE_REPORTED_BRAKE_VALUE"),
            "drs": number(row.get("drs")),
            "drs_signal_semantics": row.get("drs_signal_semantics", "SOURCE_REPORTED_DRS_VALUE"),
            "distance_m": number(row.get("distance")),
            # FastF1 reports both set-relative `Time` and session-wide `SessionTime`.
            # Prefer the session clock for cross-sample checks and preserve the
            # original set-relative coordinate separately.
            "lap_relative_time_s": number(row.get("lap_relative_time_s", row.get("time"))),
            "session_time_s": number(row.get("SessionTime", row.get("session_time_s"))),
            "source_timestamp": row.get("date"),
            "x_interpolated_m": number(row.get("x")),
            "y_interpolated_m": number(row.get("y")),
            "z_interpolated_m": number(row.get("z")),
            "acc_x_derived_mps2": number(row.get("acc_x")),
            "acc_y_derived_mps2": number(row.get("acc_y")),
            "acc_z_derived_mps2": number(row.get("acc_z")),
        }

        range_checks = [
            ("speed_kmh", 0, self.max_speed_kmh),
            ("rpm", 0, 20000),
            ("gear", 0, 8),
            ("throttle", 0, 100),
            ("brake", 0, 100),
            ("drs", 0, 14 if fields["drs_signal_semantics"] == "F1_DRS_STATUS_CODE" else 1),
        ]
        for field, lower, upper in range_checks:
            value = fields[field]
            if value is not None and not (lower <= value <= upper):
                findings.append(self.finding(
                    record,
                    code="TELEMETRY_VALUE_OUT_OF_CONFIGURED_RANGE",
                    severity="WARN",
                    message=f"{field} is outside the configured plausibility range.",
                    details={"field": field, "value": value, "lower": lower, "upper": upper},
                    limitations=["Configured bounds are screening thresholds, not FIA homologation limits."],
                ))

        sample_time = fields["session_time_s"] if fields["session_time_s"] is not None else fields["lap_relative_time_s"]
        if sample_time is None and fields["source_timestamp"]:
            try:
                sample_time = datetime.fromisoformat(str(fields["source_timestamp"]).replace("Z", "+00:00")).timestamp()
            except ValueError:
                sample_time = None
        previous = self._last.get(key)
        if (
            fields["throttle"] is not None
            and fields["brake"] is not None
            and 0 <= fields["throttle"] <= 100
            and 0 <= fields["brake"] <= 100
        ):
            brake_applied = (
                bool(fields["brake"])
                if fields["brake_signal_semantics"] == "BOOLEAN_BRAKE_APPLIED"
                else fields["brake"] >= 20
            )
            if fields["throttle"] >= 20 and brake_applied:
                findings.append(self.finding(
                    record,
                    code="THROTTLE_BRAKE_OVERLAP_SCREEN",
                    severity="WARN",
                    message="Throttle and brake are simultaneously reported above the configured screening thresholds.",
                    details={
                        "throttle_percent": fields["throttle"],
                        "brake_value": fields["brake"],
                        "brake_semantics": fields["brake_signal_semantics"],
                        "threshold_percent": 20,
                    },
                    limitations=["This is a cross-channel screening signal; it does not establish a sensor fault or driver input error."],
                ))
        if sample_time is not None and previous is not None:
            delta_t = sample_time - previous["time"]
            if delta_t <= 0:
                findings.append(self.finding(
                    record,
                    code="TELEMETRY_TIME_NOT_MONOTONIC",
                    severity="WARN",
                    message="Telemetry sample time is duplicate or moves backwards within this lap.",
                    details={"previous_time_s": previous["time"], "current_time_s": sample_time, "delta_s": delta_t},
                ))
            elif delta_t > self.max_sample_gap_s:
                findings.append(self.finding(
                    record,
                    code="TELEMETRY_SAMPLE_GAP",
                    severity="WARN",
                    message="A telemetry sampling gap exceeds the configured threshold.",
                    details={"gap_s": delta_t, "threshold_s": self.max_sample_gap_s},
                ))

            distance = fields["distance_m"]
            speed = fields["speed_kmh"]
            prev_distance = previous.get("distance")
            prev_speed = previous.get("speed")
            numeric_values = (distance, speed, prev_distance, prev_speed)
            if all(v is not None and v == v and abs(v) != float("inf") for v in numeric_values) and delta_t > 0:
                delta_distance = distance - prev_distance
                average_speed_mps = ((speed + prev_speed) / 2.0) / 3.6
                expected_distance = average_speed_mps * delta_t
                residual = delta_distance - expected_distance
                fields["kinematic_distance_residual_m"] = residual
                fields["kinematic_expected_distance_m"] = expected_distance
                if abs(residual) > max(8.0, expected_distance * 0.35):
                    findings.append(self.finding(
                        record,
                        code="KINEMATIC_CONSISTENCY_RESIDUAL",
                        severity="INFO",
                        message="Distance progression differs from the distance implied by adjacent speed samples.",
                        details={
                            "observed_delta_distance_m": delta_distance,
                            "expected_delta_distance_m": expected_distance,
                            "residual_m": residual,
                        },
                        limitations=["This residual is a screening signal only; telemetry may be resampled, interpolated or filtered."],
                    ))

        if sample_time is not None:
            self._last[key] = {
                "time": sample_time,
                "distance": fields["distance_m"],
                "speed": fields["speed_kmh"],
            }

        fields["measurement_semantics"] = {
            "x_y_z": "interpolated_position_export",
            "acc_x_acc_y_acc_z": "derived_acceleration_export",
            "other_channels": "public_archive_export; raw ECU provenance not established",
        }

        output.findings.extend(findings)
        output.events.append(self.event(
            record,
            event_type="vehicle_telemetry_sample_assessed",
            evidence=fields,
            findings=findings,
            provenance_extra={
                "position_semantics": "interpolated",
                "acceleration_semantics": "derived",
                "raw_ecu_sensor_access": False,
            },
        ))
        return output
