from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any

from calibre_agents.normalization import (
    infer_context_from_filename,
    infer_feed_from_filename,
    normalize_payload,
    payload_to_rows,
)
from calibre_agents.runner import AgentManager, PipelineOutput, build_lap_start_lookup, load_raw_json, write_pipeline_output


def session_key(context: dict[str, Any]) -> tuple[Any, Any, Any]:
    return (
        context.get("year"),
        context.get("meeting"),
        context.get("session"),
    )


def discover_files(args: argparse.Namespace) -> list[Path]:
    result: list[Path] = []
    for raw in args.input or []:
        path = Path(raw)
        if path.is_dir():
            result.extend(sorted(path.glob("*.json")))
        elif path.exists():
            result.append(path)
        else:
            raise FileNotFoundError(path)
    if not result:
        default = Path(args.source_dir)
        if default.exists():
            result = sorted(default.glob("TRACINGINSIGHTS_*.json"))
    # Raw source JSON only; deduplicate while preserving order.
    seen = set()
    unique = []
    for path in result:
        resolved = path.resolve()
        if resolved not in seen and path.suffix.lower() == ".json":
            unique.append(path)
            seen.add(resolved)
    return unique


def extract_session_info(payload: Any) -> dict[str, Any]:
    if isinstance(payload, dict):
        return payload
    if isinstance(payload, list) and payload and isinstance(payload[0], dict):
        return payload[0]
    return {}


def run_files(files: list[Path], out_dir: Path) -> PipelineOutput:
    # First pass: build per-session timebase and per-driver/lap lookup.
    context_by_key: dict[tuple[Any, Any, Any], dict[str, Any]] = {}
    lap_lookup_by_key: dict[tuple[Any, Any, Any], dict[tuple[str, int], float]] = {}

    for path in files:
        feed = infer_feed_from_filename(path)
        context = infer_context_from_filename(path)
        if not context:
            continue
        key = session_key(context)
        payload = load_raw_json(path)

        if feed == "session_info":
            info = extract_session_info(payload)
            if isinstance(info.get("session_info"), dict):
                info = {**info, **info["session_info"]}
            if info.get("t0_date"):
                context_by_key.setdefault(key, {}).update({"t0_date": info["t0_date"]})

        elif feed == "session_laptimes":
            lap_lookup_by_key[key] = build_lap_start_lookup(payload)

    manager = AgentManager()
    output = PipelineOutput()

    for path in files:
        feed = infer_feed_from_filename(path)
        context = infer_context_from_filename(path)
        if not context:
            print(f"[SKIP] Could not infer session context from filename: {path.name}")
            continue

        key = session_key(context)
        context.update(context_by_key.get(key, {}))
        payload = load_raw_json(path)

        records = normalize_payload(
            provider="TRACINGINSIGHTS",
            feed=feed,
            payload=payload,
            context=context,
            source_path=str(path),
        )

        # Telemetry time is lap-relative. Map it to session elapsed time only
        # when session_laptimes provides a corresponding lST.
        lap_lookup = lap_lookup_by_key.get(key, {})
        for record in records:
            if record.feed.endswith("_tel") and record.driver_code and record.lap_number is not None:
                lap_start = lap_lookup.get((record.driver_code, record.lap_number))
                try:
                    relative = float(record.payload.get("time"))
                except (TypeError, ValueError):
                    relative = None
                if lap_start is not None and relative is not None:
                    record.context["lap_start_session_time"] = lap_start
                    record.session_time_s = lap_start + relative
                    record.time_semantics = "session_elapsed_from_lST_plus_lap_relative"
                    record.provenance["lap_start_source"] = "session_laptimes.lST"
                else:
                    record.time_semantics = "lap_relative_seconds_no_lST_mapping"
                    record.provenance["session_time_unavailable_reason"] = "No matching driver/lap lST in session_laptimes"

            output.records_processed += 1
            agent_output = manager.process(record)
            output.absorb(agent_output)

        print(
            f"[PROCESSED] {path.name}: {len(records)} records "
            f"(feed={feed})"
        )

    return output


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run CALIBRE Phase 1 evidence agents over downloaded TracingInsights files."
    )
    parser.add_argument(
        "--input", action="append", default=[],
        help="Raw JSON file or directory. Can be supplied multiple times."
    )
    parser.add_argument(
        "--source-dir", default="source_logs/raw_payloads",
        help="Default raw payload directory when --input is omitted."
    )
    parser.add_argument(
        "--output-dir", default="source_logs/agent_output",
        help="Output folder for evidence events and findings."
    )
    parser.add_argument(
        "--prefix", default="calibre_phase1",
        help="Output filename prefix."
    )
    args = parser.parse_args()

    files = discover_files(args)
    if not files:
        print("No source JSON files found. Run ingest.py historical mode first.")
        return 2

    print(f"[INPUT] {len(files)} raw JSON files")
    output = run_files(files, Path(args.output_dir))
    events_path, findings_path = write_pipeline_output(
        output, args.output_dir, args.prefix
    )

    print("\n========== AGENT RUN SUMMARY ==========")
    print(f"Source records processed : {output.records_processed}")
    print(f"Evidence events emitted  : {len(output.events)}")
    print(f"Findings emitted         : {len(output.findings)}")
    print(f"Evidence stream          : {events_path}")
    print(f"Findings report          : {findings_path}")

    by_agent: dict[str, int] = {}
    by_code: dict[str, int] = {}
    for event in output.events:
        by_agent[event["agent_id"]] = by_agent.get(event["agent_id"], 0) + 1
    for finding in output.findings:
        by_code[finding["code"]] = by_code.get(finding["code"], 0) + 1

    print("\nEvidence events by agent:")
    for agent, count in sorted(by_agent.items()):
        print(f"  {agent}: {count}")

    print("\nFindings by code:")
    for code, count in sorted(by_code.items()):
        print(f"  {code}: {count}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
