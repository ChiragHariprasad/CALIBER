from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable
import json

from .models import AgentOutput, SourceRecord
from .config import AgentConfig
from .normalization import normalize_payload
from .agents import (
    EnvironmentalContextAgent,
    IncidentTrackAgent,
    VehicleElectronicsSystemIntegrityAgent,
    VehicleTwinSensorAgent,
    RaceTimingSessionAgent,
)


@dataclass
class PipelineOutput:
    events: list[dict[str, Any]] = field(default_factory=list)
    findings: list[dict[str, Any]] = field(default_factory=list)
    records_processed: int = 0

    def absorb(self, output: AgentOutput) -> None:
        self.events.extend(event.to_dict() for event in output.events)
        self.findings.extend(finding.to_dict() for finding in output.findings)


class AgentManager:
    """Synchronous fan-out. Replace/precede with Kafka consumer later."""

    def __init__(self, config: AgentConfig | None = None) -> None:
        self.config = config or AgentConfig()
        self.agents = [
            VehicleTwinSensorAgent(
                max_sample_gap_s=self.config.telemetry_max_sample_gap_s,
                max_speed_kmh=self.config.telemetry_max_speed_kmh,
            ),
            IncidentTrackAgent(),
            EnvironmentalContextAgent(
                max_track_temp_jump_c=self.config.track_temperature_step_change_c,
            ),
            VehicleElectronicsSystemIntegrityAgent(
                repeated_payload_limit=self.config.repeated_payload_alert_count,
            ),
            RaceTimingSessionAgent(),
        ]

    def process(self, record: SourceRecord) -> AgentOutput:
        combined = AgentOutput()
        for agent in self.agents:
            if agent.accepts(record):
                combined.extend(agent.process(record))
        return combined

    def process_records(self, records: Iterable[SourceRecord]) -> PipelineOutput:
        result = PipelineOutput()
        for record in records:
            result.records_processed += 1
            result.absorb(self.process(record))
        return result


def build_lap_start_lookup(session_laptimes: Any) -> dict[tuple[str, int], float]:
    """Map (driver acronym, lap) to session-relative lap start from `lST`."""
    from .normalization import payload_to_rows
    lookup: dict[tuple[str, int], float] = {}
    for row in payload_to_rows(session_laptimes):
        driver = row.get("drv") or row.get("driver")
        lap = row.get("lap")
        lap_start = row.get("lST")
        try:
            lap_i = int(lap)
            start_f = float(lap_start)
        except (TypeError, ValueError):
            continue
        if driver and lap_i >= 0 and start_f >= 0:
            lookup[(str(driver), lap_i)] = start_f
    return lookup


def run_payload(
    *,
    provider: str,
    feed: str,
    payload: Any,
    context: dict[str, Any] | None = None,
    source_path: str | None = None,
    manager: AgentManager | None = None,
) -> PipelineOutput:
    from .normalization import normalize_payload
    manager = manager or AgentManager()
    records = normalize_payload(
        provider=provider,
        feed=feed,
        payload=payload,
        context=context,
        source_path=source_path,
    )
    return manager.process_records(records)


def load_raw_json(path: str | Path) -> Any:
    with Path(path).open("r", encoding="utf-8") as handle:
        return json.load(handle)


def write_pipeline_output(output: PipelineOutput, out_dir: str | Path, prefix: str) -> tuple[Path, Path]:
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    events_path = out_dir / f"{prefix}_evidence.jsonl"
    findings_path = out_dir / f"{prefix}_findings.json"
    with events_path.open("w", encoding="utf-8") as handle:
        for event in output.events:
            handle.write(json.dumps(event, ensure_ascii=False, default=str) + "\n")
    findings_path.write_text(
        json.dumps(output.findings, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    return events_path, findings_path
