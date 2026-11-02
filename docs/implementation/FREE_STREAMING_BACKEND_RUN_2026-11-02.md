# Free streaming backend run — 2026-11-02

## Status

- **VERIFIED:** WSL Python compilation; Docker Kafka, Flink, and ZooKeeper availability; unauthenticated F1 SignalR handshake and topic subscription; raw events published to Kafka.
- **PARTIALLY VERIFIED:** Four specialist agents consumed applicable SignalR feeds, emitted evidence, and Flink evaluated contracts.
- **NOT VERIFIED:** Complete source-to-sink reconciliation, zero-loss drain, reliable identity handling under this live run, and vehicle-sensor analysis.
- **BLOCKED:** Public FastF1 session data for the requested historical checks was unavailable in this environment. No F1 TV credentials were used.

## Configuration and changes

The active CLI defaults to the no-auth F1 SignalR source for live use and adds `stream-fastf1` for historical sessions. The production CLI no longer exposes the mock producer or selects OpenF1. Existing Kafka topics and historical records were not deleted.

The live runner now waits briefly for its run-scoped Kafka consumers before starting the source, drains after the source stops, prints a `LIVE_RESULT` record, and returns a failing status when the bounded drain fails or Flink reports identity conflicts. Evidence IDs emitted by a specialist include the source event, output position, and a deterministic finding digest.

Relevant implementation: `run_pipeline.py`, `ingestion/fastf1_source.py`, `ingestion/service.py`, and `processors/agent_adapter.py`.

## WSL commands and service output

Compilation completed without output:

```text
python3 -m compileall -q run_pipeline.py ingestion processors
```

The final WSL service check reported:

```text
calibre-kafka               Up 16 hours (healthy)
calibre-flink-taskmanager   Up 16 hours
calibre-flink-jobmanager    Up 16 hours (healthy)
calibre-zookeeper           Up 16 hours (healthy)
Broker (127.0.0.1:9092): CONNECTED
Flink Version: 1.19.3
TaskManagers: 1
Total Slots: 2, Available: 2
Total Persisted Events: 7493
```

Live run command:

```text
python3 run_pipeline.py run --source signalr --duration-seconds 20 --run-id signalr-live-smoke-20261102d
```

The SignalR client log recorded `SignalR Core handshake complete` and `Subscribed to 18 topics`. The run published 77 events with zero Kafka publish failures:

| SignalR topic | Events |
|---|---:|
| TimingData | 61 |
| Heartbeat | 2 |
| AudioStreams, ContentStreams, DriverList, ExtrapolatedClock, LapCount, RaceControlMessages, SessionData, SessionInfo, SessionStatus, TimingAppData, TimingStats, TopThree, TrackStatus, WeatherData | 1 each |

These are actual received topic messages, not a claim that every topic contains a sensor stream. Auth-gated `CarData.z` and `Position.z` were excluded.

## Agent and Flink counters

Final `LIVE_RESULT` counters for run `signalr-live-smoke-20261102d`:

| Processor | Raw records processed | Evidence emitted | Errors |
|---|---:|---:|---:|
| Vehicle twin sensor | 0 | 0 | 0 |
| Incident track | 2 | 8 | 0 |
| Environmental context | 1 | 1 | 0 |
| Race timing session | 66 | 1,410 | 0 |
| System integrity | 77 | 77 | 0 |
| **Total expected evidence** |  | **1,496** | **0** |

| Flink metric | Count |
|---|---:|
| Evidence envelopes received | 2,992 |
| Evidence envelopes routed | 1,644 |
| Malformed envelopes | 0 |
| Identity conflicts | 1,348 |
| Quarantined events | 4,300 |
| DEC evaluation invocations | 1,488 |
| Verdict emissions | 1,488 |
| Contracts loaded | 11 |

Persistence reported 1,392 records. Using Flink's 1,644 routed envelopes plus 1,488 verdict emissions, the corresponding database output count is 3,132, leaving a 1,740-record gap at shutdown. The drain check timed out after 60 seconds. These figures **do not reconcile**: Flink received twice the agents’ expected evidence count, reported 1,348 identity conflicts, and routed fewer envelopes than received; the persistence count did not catch up with Flink output. The run is therefore not an end-to-end success.

An earlier bounded run after the evidence-ID change (`signalr-live-smoke-20261102c`) reported zero identity conflicts but received 638 envelopes against 1,612 agent emissions and also stopped before a full drain. The variation confirms that the latest evidence-ID change has not been verified under live stream conditions.

The vehicle sensor processor consumed zero records. The public topics received in these runs do not include `CarData.z`; vehicle telemetry/cross-sensor analysis must remain unavailable/UNKNOWN without those measurements. Timing and weather updates are not substitutes for vehicle throttle/brake samples.

## Historical FastF1 attempts

Attempted from WSL:

```text
python3 run_pipeline.py stream-fastf1 --year 2024 --event "Monaco Grand Prix" --session R --run-id fastf1-monaco-2024-smoke-20261102 --start-seconds 300 --max-session-seconds 3 --replay-speed 30
```

FastF1 did not provide usable loaded rows for the selected window; the runner reported `source_rows_selected: 0`, `published_events: 0`, and all configured feeds without records. It emitted no fabricated measurements. A 2026 Monaco archive request returned HTTP 401 in the WSL probe. FastF1 3.9.0 is installed in WSL and declared in `requirements.txt`.

FastF1's 3.9 release added Formula 1 account authentication support; public static archive availability is consequently not assumed. See the [FastF1 3.9 release](https://github.com/theOehrly/Fast-F1/releases) and [FastF1 issue #978](https://github.com/theOehrly/Fast-F1/issues/978). No F1 TV token/account was configured, as requested.

## Remaining backend verification

- Reconcile why the live run's Flink evidence count exceeds the agents' evidence emission count and eliminate the remaining identity conflicts.
- Make bounded drain report precise source, topic, evidence, verdict, and sink deltas and prove they match before calling the run verified.
- Obtain an allowed historical FastF1 session feed before claiming the historical path processes records.
- Keep vehicle sensor assessments UNKNOWN until actual throttle/brake telemetry is available from an allowed source.
- Legacy `calibre.raw.mock.*` and `calibre.raw.openf1.*` topics are still visible in the existing Kafka broker. They were not deleted because doing so would remove historical broker data; the active runner did not use those sources.
- A sink interruption/restart recovery test was not performed in this run.

Run logs: `data/signalr_live_smoke_20261102d.log`, `data/fastf1_monaco_2024_stream_20261102.log`, and `data/fastf1_monaco_stream_20261102.log`.
