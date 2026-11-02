# CALIBRE F1 Source and Streaming Contract

## Production source paths

- Live mode uses the repository's direct F1 SignalR client and subscribes only
  to topics declared unauthenticated in `pitwall_source/topics.py`. It uses no
  F1 TV token. Each merged feed update is published as a separate raw Kafka
  event under `calibre.raw.f1_signalr.<feed>`.
- Historical mode uses FastF1 to load a selected season/event/session. The
  adapter merges the session's telemetry, laps, weather, track-status, and race
  control tables by session time and publishes individual rows under
  `calibre.raw.fastf1.<feed>`. The default pace is one source-session second
  per wall-clock second; `--replay-speed` can speed up a bounded verification
  run. The events still pass through Kafka, the specialist processors, Flink,
  and configured sinks.
- Historical source results/classification are deliberately not emitted. Lap
  position is removed from historical lap payloads to avoid injecting a
  retrospective classification into the evidence stream.

## Known source limitations

- F1 SignalR's free topic set supplies timing, weather, race control, track
  status, session/driver state, and related public feeds. `CarData.z` and
  `Position.z` are excluded because the bundled topic contract marks them
  authentication-gated. No token is read or attempted. Consequently, live
  vehicle telemetry and live GPS-based decisions have no admitted signal and
  must not be described as evaluated.
- FastF1 is historical retrieval, not a substitute for the SignalR live
  stream. FastF1 documentation says detailed timing, telemetry, and position
  are available from 2018 onward and commonly appear 30–120 minutes after a
  session. A missing FastF1 table yields no raw events for that feed; CALIBRE
  does not backfill synthetic measurements.
- FastF1 brake data can be boolean. The normalizer preserves and labels that
  meaning; it does not convert a brake-applied flag to a percentage.
- FastF1's DRS field is a status code where present. It is not constrained to
  the binary range used by some other providers.
- Historical replay facts use session elapsed seconds. Those values remain
  replay coordinates and are not relabeled as UTC.

## Commands

```bash
# Start the pipeline with unauthenticated F1 SignalR subscriptions.
python run_pipeline.py run --source signalr

# Stream a bounded historical session through the same Kafka/Flink route.
python run_pipeline.py stream-fastf1 --year 2026 --event "Monaco Grand Prix" \
  --session R --replay-speed 30 --max-session-seconds 30
```

The streaming command prints a JSON summary with raw publish counts, per-feed
counts, specialist processor counts, Flink receive/route counters, persistence
counts, and whether the bounded drain completed. A successful command is not
a claim that excluded live topics or missing historical tables were observed.
