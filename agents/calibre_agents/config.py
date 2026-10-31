from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class AgentConfig:
    """Screening defaults. These are engineering heuristics, not FIA limits."""

    telemetry_max_sample_gap_s: float = 1.0
    telemetry_max_speed_kmh: float = 450.0
    track_temperature_step_change_c: float = 8.0
    repeated_payload_alert_count: int = 20
    config_version: str = "phase1-defaults-1"
