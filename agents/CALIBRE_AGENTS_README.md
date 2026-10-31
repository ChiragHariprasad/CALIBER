# CALIBRE Phase 1 — Evidence Processing Agents

Version: 0.1.0  
Scope: local/offline processing of TracingInsights historical exports and a callback adapter for Pitwall live SignalR events.  
Dependencies: Python 3.10+; standard library only.

## Architecture boundary

The agents are evidence processors. They parse provider payloads, preserve source and time provenance, attach scoped observations, and emit versioned evidence events. They do **not** decide sporting infringements, issue penalties, control race systems, or remediate source systems.

The future Kafka/Flink design remains:

```text
Pitwall live / TracingInsights historical
                 |
          Source adapters
                 |
        Common SourceRecord
                 |
      +----------+-----------+-----------+-----------+----------------+
      |                      |           |           |                |
 Vehicle twin &       Incident &    Environment  Timing/session  Feed/system
 sensor assurance     track state   context      parser           integrity
      |                      |           |           |                |
      +----------+-----------+-----------+-----------+----------------+
                 |
          EvidenceEvent JSON
                 |
        (later) Kafka topics
                 |
        (later) Flink integrity kernel
                 |
        scoped integrity verdicts
```

## Agents implemented

There are four specialist assurance processors plus a timing/session evidence parser. The timing parser exists to structure a direct source feed; it is not an additional decision authority.

### 1. Vehicle Twin & Sensor Assurance Agent

Input: per-lap telemetry exports (`*_tel.json`, e.g. `52_tel.json`).

Processing:
- Range screening for speed, RPM, gear, throttle, brake and DRS.
- Duplicate/backwards lap-relative timestamp detection.
- Telemetry sampling gap detection (default threshold 1.0 s; configurable in the class).
- A diagnostic distance-vs-speed residual between adjacent samples.
- Explicit labelling of interpolated position (`x`, `y`, `z`) and derived acceleration (`acc_x`, `acc_y`, `acc_z`).

Important: the public TracingInsights export is not established as direct raw ECU/IMU capture. Its documentation describes position as interpolated from GPS and acceleration as computed. The agent therefore preserves those semantics and does not claim raw sensor access or a validated high-fidelity digital twin.

### 2. Incident & Track-State Assurance Agent

Input: `rcm.json` and live `RaceControlMessages` / `TrackStatus`-type feeds.

Processing:
- Classifies published messages as incident notices, track/flag state, DRS notices or steward/rule-related notices.
- Preserves message text, category, flag, sector, racing number and lap.
- Emits a finding that a relevant published message was seen.

It does not infer guilt, adjudicate an infringement or assign a penalty.

### 3. Environmental Context Agent

Input: historical `weather.json` and live `WeatherData` feeds.

Processing:
- Normalises archive abbreviations (`wAT`, `wTT`, `wH`, `wP`, `wR`, `wWD`, `wWS`) and common live field names.
- Applies broad sanity bounds.
- Detects non-monotonic weather timestamps and unusually large consecutive track-temperature changes.

Weather is contextual evidence only. This agent must not use weather to dismiss unexplained telemetry or timing discrepancies.

### 4. Race Timing & Session Evidence Agent

Input: `session_laptimes.json`, `drivers.json`, `session_info.json`, and live `TimingData`, `TimingStats`, `TimingAppData`, `DriverList`, `LapCount` and `SessionStatus` topics.

Processing:
- Separates lap duration (`time`) from session elapsed lap-end time (`sesT`) and lap-start time (`lST`).
- Extracts sector times, position, tyre compound/stint, pit in/out, deletion and personal-best indicators.
- Unpacks live per-driver `Lines` snapshots where present and keeps the provider payload.
- Emits session metadata and driver metadata as evidence records.

This is a timing-format parser/evidence adapter. It is not a sporting rules engine and does not independently declare a breach.

### 5. Vehicle Electronics & System Integrity Agent

Input: all feeds.

Processing:
- Feed-level record count, timestamp presence, payload hashing and repeated-payload monitoring.
- Explicitly reports when selected public feeds do not expose ECU, power-unit or internal bus diagnostic fields.

It does **not** infer electronic faults from unavailable fields. It is an observable feed/system-integrity monitor until authorised vehicle-electronics signals become available.

## Time semantics — important

The normaliser does not pretend all source timestamps are the same kind of time.

| Feed | Source field | Meaning | Normalised field |
|---|---|---|---|
| Per-lap telemetry | `time` | Seconds from start of that lap | `session_time_s` is only set when matched `lST` is available; then `lST + time` |
| Session lap times | `time` | Lap duration | Not treated as event time |
| Session lap times | `sesT` | Session elapsed time at lap completion | `session_time_s` |
| Session lap times | `lST` | Session elapsed time at lap start | Used to anchor per-lap telemetry |
| Weather | `wT` | Session elapsed seconds | `session_time_s` |
| Race control | `time` | Absolute ISO timestamp in the export | Converted to session elapsed only when `session_info.t0_date` is loaded |
| Pitwall live callback | Provider timestamp | Retained as supplied | Not converted until its exact format is verified during live test |

`run_agents.py` first scans the selected raw JSON files for `session_info.json` and `session_laptimes.json`, then applies the `lST` mapping to telemetry files. Without a matching lap-start value, telemetry time stays lap-relative and is not mislabelled as session time.

## Run against the source files you already downloaded

From `E:\CALIBER\main`, copy this folder and `run_agents.py` into the project root, then run:

```powershell
python run_agents.py
```

By default it scans:

```text
source_logs\raw_payloads\TRACINGINSIGHTS_*.json
```

Or select one or more files/directories explicitly:

```powershell
python run_agents.py --input source_logs\raw_payloads
```

The output is saved to:

```text
source_logs\agent_output\calibre_phase1_evidence.jsonl
source_logs\agent_output\calibre_phase1_findings.json
```

To use another prefix:

```powershell
python run_agents.py --input source_logs\raw_payloads --prefix bahrain_validation
```

The `--input` option can be repeated to run only a selected set of files. For best time alignment on a telemetry lap, include the matching session's `session_info.json` and `session_laptimes.json` along with the `*_tel.json` file. The runner uses `lST` to anchor lap-relative telemetry and `t0_date` to convert absolute race-control timestamps.

## Live bridge for tomorrow's Pitwall test

`LiveAgentBridge` implements the callback signature used by Pitwall's `client.on_all(topic, callback)`:

```python
from calibre_agents.live_bridge import LiveAgentBridge

bridge = LiveAgentBridge(
    context={"mode": "live", "session": "Race"},
    output_sink=lambda event: print(event),
)
client.on_all(bridge.on_event)
```

The adapter marks Pitwall callback timestamps as `provider_timestamp_unconverted`. During the live session test, first capture actual timestamp examples and topic payloads. Only then should we implement source-specific time conversion. Pitwall's current auto-merge path is expected to provide merged topic state to this callback; the adapter records that it is not a raw-delta capture.

## Configuration

Screening defaults are held in `calibre_agents/config.py` and can be overridden by constructing `AgentManager(AgentConfig(...))`. Current defaults are: telemetry sample gap 1.0 s, speed upper bound 450 km/h, consecutive track-temperature step 8 C, and repeated identical-payload alert count 20. These are diagnostic starting points, not official limits; calibrate them against real source quality and session coverage before treating their findings as operational alerts.

## Tests

Run from this folder:

```powershell
python -m unittest discover -s calibre_agent_tests -v
```

Tests cover the observed TracingInsights row/columnar data structures, file naming, session/lap time semantics, telemetry evidence, race-control classification, weather normalisation, electronics observability caveats, and multi-agent fan-out.

## Current limitations (intentional)

- This is not yet Kafka or Flink code. The output contract is Kafka-friendly JSON, and the synchronous `AgentManager` is the local harness.
- No agent is an LLM. These are deterministic parsers and bounded detectors; adding generative models would increase uncertainty and latency without improving basic feed parsing.
- No full vehicle digital twin has been implemented. The current vehicle agent performs input quality and basic kinematic consistency screening only.
- No internal ECU fault diagnosis is possible from the public feeds tested so far.
- The agents do not produce CLEAR/BREACH verdicts. The central Flink kernel will own cross-source arbitration, dependency analysis, decision-scoped integrity status and final verdict maturity.
- Thresholds are configurable engineering screening defaults, not official FIA limits or validated fault thresholds.
