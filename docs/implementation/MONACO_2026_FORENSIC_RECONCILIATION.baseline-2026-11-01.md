# Monaco 2026: forensic execution and recovery report

**Run date:** 2026-11-01  
**Runtime:** WSL2; Docker Compose, Kafka, Zookeeper, and Apache Flink REST were accessed from WSL.  
**Full replay:** `monaco2026-wsl-full-02`  
**Recovery replay:** `monaco2026-wsl-recovery-12` (200 source records)  
**Purpose:** Validate universal accounting and evidence semantics. Monaco is a baseline capture; race results and later findings were not evaluator inputs.

## Status

| Status | Result |
|---|---|
| **VERIFIED** | The full run recorded all 2,015 source records as `PERSISTED`, emitted and routed 4,030 specialist evidence events, and persisted all 4,030. The 12,309 database, dashboard, and analytics output-topic messages drained. A 200-record sink interruption/replay test preserved all 799 verdict hashes and did not add verdicts on the duplicate pass. All 8,279 verdict hashes in the full-run DB recompute from the persisted JCS basis; all previous-hash links are contiguous. API route smoke checks and an explicit replay-domain bitemporal lookup passed. |
| **PARTIALLY VERIFIED** | Four relevant specialists processed inputs. The vehicle-twin sensor specialist processed zero because the raw capture has no `car_data`. The full replay produced 1,525 overflow events. Worker-level Python Kafka consumers and all three sinks ran; the Apache Flink cluster had no submitted jobs. |
| **NOT VERIFIED** | No assessment reached `TUPLE_RESOLVED` or `FINAL`. Therefore final verdict closure is not established. A finite replay clock tick was processed, but it did not make unresolved references/overflow into a resolved tuple. The 200-record recovery test proves bounded replay idempotence for this fixture and code path, not every failure mode or production durability guarantee. |
| **BLOCKED** | A Monaco throttle/brake anomaly assessment is blocked by absent raw `car_data`. The replay cannot infer sensor measurements from post-race knowledge, lap timing, or weather. The sensor verdict must remain UNKNOWN / NOT EVALUATED until admissible telemetry is supplied. |

**Overall:** The backend is useful for controlled universal replay and produces reproducible, traceable assessments, but it is **not fully verified or production-ready**. In particular, do not claim Monaco vehicle-sensor evaluation or final assessment closure.

## WSL commands and infrastructure evidence

The full replay command was run from `/mnt/e/CALIBER`:

```bash
python3 run_monaco_full_race.py \
  --run-id monaco2026-wsl-full-02 \
  --db-path data/monaco_wsl_full_run_02.db \
  --delay 0.001
```

The recovery command was:

```bash
python3 run_monaco_full_race.py \
  --run-id monaco2026-wsl-recovery-12 \
  --db-path data/monaco_wsl_recovery_run_12.db \
  --recovery-test --limit 200 --delay 0.001
```

`docker compose ps` output from WSL:

```text
NAME                        SERVICE             STATUS
calibre-flink-jobmanager    flink-jobmanager    Up 13 hours (healthy)
calibre-flink-taskmanager   flink-taskmanager   Up 13 hours
calibre-kafka               kafka               Up 13 hours (healthy)
calibre-zookeeper           zookeeper           Up 13 hours (healthy)
```

Flink REST output:

```json
{"taskmanagers":1,"slots-total":2,"slots-available":2,"jobs-running":0,
 "jobs-finished":0,"jobs-cancelled":0,"jobs-failed":0,
 "flink-version":"1.19.3","flink-commit":"4105f8d"}
{"jobs":[]}
```

The runtime named `FlinkIntegrityCore` in the report is the repository's Python Kafka consumer, not an Apache Flink job. Apache Flink was reachable and healthy, but no native Flink job was submitted. This distinction is reflected in all statuses below.

## 1. Source, evidence, route, and persistence reconciliation

Raw capture inventory and terminal outcomes:

| Feed | Captured and published | Terminal input outcome | Evidence envelopes emitted | Persisted evidence rows |
|---|---:|---|---:|---:|
| Laps | 1,452 | 1,452 `PERSISTED` | 2,904 (timing + system-integrity agents) | 2,904 |
| Pit | 86 | 86 `PERSISTED` | 172 (timing + system-integrity agents) | 172 |
| Race control | 269 | 269 `PERSISTED` | 538 (incident + system-integrity agents) | 538 |
| Weather | 208 | 208 `PERSISTED` | 416 (environment + system-integrity agents) | 416 |
| **Total** | **2,015** | **2,015 `PERSISTED`; 0 pending/failed/filtered/unaccounted** | **4,030** | **4,030** |

The original “2,015 evidence events” phrasing conflated source events and specialist evidence envelopes. This run had 2,015 distinct source records, 4,030 emitted evidence envelopes because multiple specialists consume the same source record, 4,030 envelopes routed by the Python integrity worker, and 4,030 evidence rows persisted. No evidence-envelope drop is present in this run. The 2015 source ledger accounts for source inputs; it is not the evidence-envelope count.

Run-ledger SQL and output:

```sql
SELECT feed, terminal_status, count(*)
FROM replay_inputs
WHERE run_id = 'monaco2026-wsl-full-02'
GROUP BY feed, terminal_status ORDER BY feed, terminal_status;

SELECT count(*) AS rows, count(DISTINCT correlation_id) AS source_correlations
FROM persisted_events WHERE correlation_id LIKE 'monaco2026-wsl-full-02:%';
```

```text
laps PERSISTED 1452
pit PERSISTED 86
rcm PERSISTED 269
weather PERSISTED 208
4030 2015
```

The second query has 4,030 rows and 2,015 source correlations by design: source correlation is shared by specialist evidence envelopes. The source ledger additionally records `publish_status`, `payload_sha256`, evidence count, and publish attempts. All terminal input rows are `PERSISTED`; there were no missing records requiring an inferred dedupe/filter/failure category.

## 2. Specialist input and emission counts

The output below is the run summary's durable receipt-backed count, not an estimate derived from contract verdicts:

| Specialist | Inputs consumed | Evidence emitted | Errors | Result |
|---|---:|---:|---:|---|
| `vehicle_twin_sensor_agent` | 0 | 0 | 0 | No `car_data` feed existed in the replay input set. |
| `incident_track_agent` | 269 | 269 | 0 | Processed all race-control records. |
| `environmental_context_agent` | 208 | 208 | 0 | Processed all weather records. |
| `race_timing_session_agent` | 1,538 | 1,538 | 0 | Processed 1,452 lap + 86 pit records. |
| `vehicle_electronics_system_integrity_agent` | 2,015 | 2,015 | 0 | Catch-all integrity/provenance processing across the four captured feeds. |

SQLite event provenance confirms the per-feed specialist emissions:

```sql
SELECT json_extract(event_json, '$.producer.processor_id') AS agent,
       feed, count(*) AS emitted, count(DISTINCT correlation_id) AS sources
FROM persisted_events
WHERE correlation_id LIKE 'monaco2026-wsl-full-02:%'
GROUP BY agent, feed ORDER BY agent, feed;
```

```text
environmental_context_agent weather 208 208
incident_track_agent rcm 269 269
race_timing_session_agent laps 1452 1452
race_timing_session_agent pit 86 86
vehicle_electronics_system_integrity_agent laps 1452 1452
vehicle_electronics_system_integrity_agent pit 86 86
vehicle_electronics_system_integrity_agent rcm 269 269
vehicle_electronics_system_integrity_agent weather 208 208
```

This demonstrates relevant processing by four specialists. It does not demonstrate sensor-specialist execution. An idle sensor agent is not counted as a successful signal evaluation.

## 3. DEC evaluations, verdict emissions, unique decisions, and revisions

The full run recorded 112 distinct decision instance IDs, 8,279 DEC evaluation invocations, and 8,279 verdict emissions. Every emitted full-run verdict is present with a distinct hash in the final run database. The 8,279 records are assessments/revisions; they are not 8,279 independent decisions.

| Contract | Decision instances | Persisted verdict rows / hashes |
|---|---:|---:|
| `lap_time_consistency` | 22 | 1,560 |
| `pit_entry_speed_evidence` | 22 | 1,560 |
| `pit_lane_timing_evidence` | 22 | 1,560 |
| `race_control_incident_timing` | 23 (22 driver-scoped + 1 session-scoped) | 1,830 |
| `telemetry_session_alignment` | 22 | 1,560 |
| `weather_strategy_advisory` | 1 session-scoped | 209 |
| **Total** | **112** | **8,279** |

SQL used:

```sql
SELECT contract_id, count(DISTINCT decision_instance_id) AS decisions,
       count(*) AS persisted_assessments
FROM persisted_verdicts
WHERE decision_instance_id LIKE 'monaco_2026#race:monaco2026-wsl-full-02|%'
GROUP BY contract_id ORDER BY contract_id;

SELECT count(*) AS assessments,
       count(DISTINCT verdict_hash) AS distinct_hashes,
       count(DISTINCT decision_instance_id) AS decisions,
       sum(revision_number > 1) AS revisions_after_initial
FROM persisted_verdicts
WHERE decision_instance_id LIKE 'monaco_2026#race:monaco2026-wsl-full-02|%';
```

```text
8279 assessments | 8279 hashes | 112 decisions | 8167 revisions after initial
```

The attached execution's DEC counts summing to 5,808 are from a different, mixed-scope database snapshot; they are not the counts for this run. The full-run summary initially undercounted session-scoped verdicts by 112 because it filtered all verdict rows by a source `correlation_id`; session-level verdicts have no single source correlation. The runner's run-scope SQL now uses the namespaced `decision_instance_id`. The counts above come from direct post-shutdown SQL against the complete full-run database, not by editing expected results.

The historical full-run summary also had a stale `persisted_verdict_rows` snapshot (8,167) for the same scope-selection reason. The post-shutdown SQLite query returns 8,279. Recovery run 12 used the corrected query and its persisted summary matches its database.

## 4. Kafka topic counts and Python stream-worker metrics

The runner captured end-offset deltas during `monaco2026-wsl-full-02`:

| Kafka topic | Run delta |
|---|---:|
| `calibre.raw.openf1.laps` | 1,452 |
| `calibre.raw.openf1.pit` | 86 |
| `calibre.raw.openf1.rcm` | 269 |
| `calibre.raw.openf1.weather` | 208 |
| `calibre.evidence.timing` | 1,538 |
| `calibre.evidence.incident` | 269 |
| `calibre.evidence.environment` | 208 |
| `calibre.evidence.system` | 2,015 |
| `calibre.evidence.vehicle` | 0 |
| `calibre.guard.clock` | 1 |
| `calibre.core.verdict_history` | 8,279 |
| `calibre.out.database` | 12,309 |
| `calibre.out.dashboard` | 12,309 |
| `calibre.out.analytics` | 12,309 |
| `calibre.analytics.metrics` | 2,461 |
| `calibre.side.overflow` | 1,525 |
| `calibre.side.quarantine` | 0 |
| `calibre.alerts` | 0 |

The three output topics contain three-way fan-out (evidence, verdict, and clock/control records), not 12,309 verdicts. The single-partition topic end-offset deltas are the Kafka message counts for this isolated run.

Run-scoped Python Flink-core metrics:

```text
contracts_loaded=11
events_received=4030  events_routed=4030  events_malformed=0
verdicts_emitted=8279  dec_evaluation_invocations=8279
overflow_events=1525  quarantined_events=0  identity_conflicts=0
duplicate_clock_ticks=0  clock_ticks=1
health=DEGRADED
```

`health=DEGRADED` is consistent with the measured overflow. The overflow side topic must be examined and mitigated through generic contract/search-budget or capacity work before claiming clean verification; the report does not discard those events.

## 5. Car data, weather, drivers, and laps

The runner read four raw files: laps, race control, weather, and pit. `EDA/data/raw/car_data_session_11299.json` does not exist. No throttle, brake, RPM, gear, or high-frequency speed telemetry was supplied to this run. There are zero vehicle-agent inputs and zero `calibre.evidence.vehicle` messages. The vehicle anomaly contract's throttle/brake cross-correlation was **not evaluated** and must be shown as UNKNOWN / NOT EVALUATED. Later proof that sensors were faulty is ground truth for offline evaluation only; it was not used to produce this replay's result.

Weather capture output:

```text
weather rows=208; rainfall=0 in 208/208 observations
```

OpenF1 documents `rainfall` as whether rainfall is present, an observed condition; it is not a probability forecast. See [OpenF1 API documentation](https://openf1.org/docs/). The UI and API must label this as observed rainfall, not “rain probability.”

Raw inventory output:

```text
drivers rows=22
session_result rows=22
laps=1452; duration_not_null=1445; duration_null=7; distinct lap drivers=22
classified number_of_laps sum=1445; distinct classified drivers=22
car_data_exists=False
```

Thus, 1,452 is the number of lap-feed rows; seven have no lap duration. The 1,445 duration-bearing lap samples equal the sum of classified completed laps. There are 22 drivers in the driver and classification captures. “20 drivers” describes neither of those tables; it may reflect the pit feed's driver coverage and must not be presented as the classification size.

## 6. Verdict witness and replay clock example

Representative persisted assessment:

```text
decision: monaco_2026#race:monaco2026-wsl-full-02|12|lap_time_consistency
revision: 1; hash: 4a8225e9f4f6526c22da075bba7216cad004a8f915deae36cd08b7f4e17f6462
integrity=UNKNOWN; resolution=OPEN; finality=OPEN
primary evidence witness: MISSING_REFERENCE_FACT: role=lap_start_reference, key=...|12
witness_set also includes: UNMODELLED_COMMON_CAUSE; HISTORICAL_ARCHIVE_UNCALIBRATED_LATENCY
lease metadata: clock_domain=REPLAY_VIRTUAL; timestamp_semantics=REPLAY_VIRTUAL
```

`REPLAY_VIRTUAL_LEASE_ASSIGNED` is not used as an evidence witness in the updated evaluator. Lease clock domain, lease deadline, and timestamp semantics are carried in `lease_metadata`; the primary witness and witness set describe the limiting evidence or result.

The table retains legacy column names `valid_time_utc`, `knowledge_time_utc`, and `lease_until_utc`. Those names are not sufficient to establish UTC semantics. Each verdict also carries `lease_clock_domain`, `run_clock_domain`, and `lease_metadata.timestamp_semantics`; this run's values are `REPLAY_VIRTUAL`. The API returns the parameters as `valid_time`, `knowledge_time`, and an explicit `clock_domain`; it does not relabel this virtual coordinate as UTC. The bitemporal route returned 200 for the exact `REPLAY_VIRTUAL` domain and 404 for `LIVE_UTC` on the same record. The legacy SQL column names remain a schema-cleanup risk and are called out for the UI to display the domain explicitly.

## 7. Finality and slot closure

Post-shutdown verdict counts from SQLite:

```text
OPEN / OPEN / PROVISIONAL = 112
OPEN / OPEN / REVISED    = 7968
STATE_RESOLVED / OPEN / REVISED = 199
TUPLE_RESOLVED = 0
FINAL = 0
```

The replay runner emitted one explicit final replay clock/watermark tick after source processing, and the Python worker counted `clock_ticks=1`; the 200-record recovery test counted its two first/duplicate clock ticks. The tick is not itself proof that each assessment is final. Contract evaluation preserves unresolved reference facts, pending roles, and search overflow as unresolved; FINAL also requires a tuple-resolved result. The representative lap decision remains OPEN because `lap_start_reference` is missing. The full run has 1,525 overflow events and no tuple-resolved assessment. Therefore the run clock message was processed, but completion of all required slot closures and final assessments is **NOT VERIFIED**. Do not promote OPEN results merely because the replay reached its end.

## 8. Bitemporal query, hash reproducibility, and revision history

The API bitemporal query checks decision ID, valid-time coordinate, knowledge-time coordinate, and clock domain. The WSL API smoke test returned the expected revision at the exact stored valid/knowledge coordinate for `REPLAY_VIRTUAL`; querying the same times under `LIVE_UTC` returned 404.

Independent full-run hash check recomputed each row with `compute_verdict_hash` from the stored contract, frontier, corroboration profile, validity timeline, and `hash_basis`:

```text
rows=8279 hash_recomputed=8279 hash_mismatches=0 revision_chain_errors=0
```

All 112 decision histories have revision 1 and subsequent revisions linked by `previous_verdict_hash`. There are 8,167 revisions after each decision's initial assessment. Revision count is not decision count and does not imply finality.

## 9. Consumer groups and sink recovery

Full and recovery groups use separate run IDs. Broker group listing showed distinct groups for all specialists, the Python integrity worker, persistence, dashboard, and analytics, for example:

```text
calibre-flink-core-group-monaco2026-wsl-full-02
calibre-persistence-sink-group-monaco2026-wsl-full-02
calibre-flink-core-group-monaco2026-wsl-recovery-12
calibre-persistence-sink-group-monaco2026-wsl-recovery-12
```

After a finite replay exits, its Kafka group has no active member; its committed offset remains. A later run's messages appear as lag relative to the stopped group's old end offset, which is expected for an isolated completed-run group and is not evidence that the earlier run failed to drain. The active `backend-live-20261101` workers use their own groups.

Recovery run 12 deliberately stopped the persistence sink during the first pass, restarted it to drain, replayed the same 200 source records, and drained after restart. Actual summary values:

```text
source_records_expected=200 source_records_published=200 source_outcomes=PERSISTED:200
evidence_events_per_first_pass=400
recovery_first_sink_drained=True recovery_second_sink_drained=True final_sink_drain=True
first snapshot: 400 evidence rows / 799 verdict hashes
second and post-shutdown snapshot: 400 evidence rows / 799 verdict hashes
duplicate event deliveries=400 duplicate verdict deliveries=0
second-pass verdict emissions=0
recovery_hashes_reproducible=True
```

This establishes deterministic duplicate handling and sink recovery for that replay slice. It does not prove native Apache Flink restart recovery because the native Flink job list was empty; the interrupted component was the Python persistence sink.

## 10. API and pipeline startup

The new API entry point is `api.py`, started in WSL with:

```bash
uvicorn api:app --host 127.0.0.1 --port 8000
```

Routes exercised successfully (HTTP 200): `/v1/health/live`, `/v1/health/ready`, `/v1/system/status`, `/v1/sources`, `/v1/assessments?limit=2`, `/v1/agents?run_id=monaco2026-wsl-full-02`, and `/v1/runs/monaco2026-wsl-full-02`. The bitemporal route was separately checked as described above. `/v1/agents` requires a `run_id`; a request without it correctly returns 422.

The persistent WSL workers were then started with `python3 run_pipeline.py run --run-id backend-live-20261101`. Kafka showed active, unique consumer groups for all five processors, the Python integrity worker, persistence, dashboard, and analytics. Uvicorn is listening at `http://127.0.0.1:8000`; the live HTTP health/status/source/assessment checks returned 200. The API and all worker processes use WSL. They start at the current topic boundary and do not consume old replay history.

The live `/v1/system/status` response reports Kafka `CONNECTED` (30 topics), Flink REST `CONNECTED`, and the configured SQLite sink `CONNECTED`. It reports zero running Apache Flink jobs. The API's dashboard subcomponent is `is_live=true`, `is_ready=true`, with no events yet and status `UNKNOWN`, which is an idle state rather than evidence of a failed connection. The default API database is the shared legacy `data/calibre_persistence.db` (currently 1,644 evidence rows and 5,808 verdict rows from mixed history); those counts are not scoped to either Monaco run. Set `CALIBRE_PERSISTENCE_DB` to a run database when inspecting a run-specific replay. A central multi-run registry remains future work.

The active persistence group was checked after startup:

```text
GROUP                                                TOPIC                CURRENT-OFFSET  LOG-END-OFFSET  LAG
calibre-persistence-sink-group-backend-live-20261101 calibre.out.database 111324          111324          0
```

## 11. Code and regression checks

Generic fixes in this work include run-scoped consumer groups and source ledger/receipts; deterministic agent replay output; Python stream-worker deduplication and conflict quarantine; replay clock progression; finality invariants; witness/lease separation; persisted hash basis; bitemporal queries with clock-domain filtering; and read-only API health, source, assessment, agent, run, and history hooks. These changes are provider/session-neutral; no contract special case was added for Monaco.

WSL checks:

```text
python3 -m py_compile run_monaco_full_race.py flink/job.py processors/service.py persistence/service.py dashboard/service.py analytics/service.py api.py
pytest -q tests/test_flink_streaming.py tests/test_persistence_durability_and_replay.py tests/test_evaluator.py tests/test_e2e_pipeline.py
44 passed in 21.99s
```

## Remaining verification work

1. Supply complete source telemetry, including `car_data`, before evaluating throttle/brake sensor anomaly behavior. Keep the result UNKNOWN when the feed is absent.
2. Investigate the 1,525 overflow records and the contract-level missing references. Preserve the measured overflow count and unresolved verdicts until generic causes are fixed.
3. Decide whether and when the product will submit a native Apache Flink job. Current execution uses a Python Kafka worker; the Flink cluster being healthy is not evidence of an executing Flink job.
4. Exercise recovery against each production sink and a native Flink task restart when a native job exists; record offsets, row counts, and hashes after shutdown.
5. Keep time APIs and UI labels domain-aware. Consider a future schema migration from legacy `_utc` column names to domain-neutral valid/knowledge-time fields; current API behavior already requires an explicit clock domain.

