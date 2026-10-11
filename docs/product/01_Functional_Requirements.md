# CALIBRE — Functional Requirements Specification

**Document status:** Baseline for repository audit and implementation  
**Product:** CALIBRE — Agentic F1 Performance Twin and Streaming Intelligence Platform  
**Version:** 1.0

## 1. Purpose

Define the functional behaviour CALIBRE must deliver. This document is a target specification, not proof that existing code already satisfies it. Each requirement must be mapped to repository code and verified through an observable test.

## 2. Product objectives

CALIBRE must:
1. Ingest available Formula 1 timing, telemetry, weather, tyre, position and race-control data.
2. Carry events through Kafka as a real event-streaming backbone.
3. detect telemetry integrity failures and communicate data quality to downstream agents.
4. Compare driver performance with an evidence-supported reference lap/performance envelope.
5. Diagnose measurable performance gaps and produce contextual, confidence-aware recommendations.
6. Re-evaluate race strategy when meaningful race-state events occur.
7. Replay historical races as paced event streams when no live race is running.
8. Expose operation, health, agent output and event provenance through APIs and a useful dashboard.
9. Support deterministic simulation and fault injection without confusing synthetic events with real observations.

## 3. Operating modes

### FR-MODE-001 — Live mode
The system shall consume only sources explicitly configured for live use. It shall display source status, last-event time, field availability and freshness. Loss of a source shall not silently switch the system to replay or simulation.

### FR-MODE-002 — Historical replay
An operator shall be able to select a locally cached or retrievable completed session and replay its recorded events through the same normalisation, Kafka, agent, persistence, API and UI path used by live events.

Replay controls shall include play, pause, resume, restart, seek, playback speed and step-to-next-lap, subject to implementation feasibility. The UI shall clearly label replay and show its replay clock.

### FR-MODE-003 — Simulation
The system shall support deterministic synthetic scenarios, with configurable random seeds and explicit provenance. Simulated data shall be visibly labelled and shall not overwrite real observations without an explicit scenario-overlay policy.

### FR-MODE-004 — Mode integrity
Every event and output shall identify its operating mode and source provenance. Mode transitions shall be explicit and auditable.

## 4. Data ingestion and event contracts

### FR-DATA-001 — Source adapters
Each adapter shall expose a consistent lifecycle, supported event capabilities, health status and asynchronous event interface. Sources include F1 live timing gateway/direct connector where available, OpenF1 historical access, Jolpica historical results, replay and mock sources.

### FR-DATA-002 — Capability-aware routing
The router shall select providers by event category and actual capability, not by one global priority ranking. It shall prevent duplicate upstream paths from producing duplicate agent actions.

### FR-DATA-003 — Canonical event envelope
Each normalised event shall include schema version, event ID, event type, session ID, driver ID when applicable, source event time, ingestion time, source ID, operating mode, quality state and payload. Raw payloads or a durable reference to them shall be retained where configured.

### FR-DATA-004 — Data provenance
Values shall be classified as observed, derived, estimated, simulated or unavailable. Missing physical measurements shall never be fabricated to fill gaps.

### FR-DATA-005 — Validation
The pipeline shall validate schema, units, timestamps, plausible ranges and ordering. Invalid records shall be quarantined or routed to a dead-letter topic with a reason.

### FR-DATA-006 — Deduplication
Stable source identities or source-specific sequence/timestamp keys shall prevent repeated processing. Timestamp alone shall not be assumed unique.

### FR-DATA-007 — Freshness
Freshness shall be evaluated by source and event category. A connected source or session heartbeat shall not imply that every telemetry channel is fresh.

## 5. Kafka and streaming

### FR-KAFKA-001 — Kafka is the actual transport
Normalised events and agent outputs shall flow through configured Kafka topics. JSONL logging or in-process calls shall not be presented as proof of Kafka integration.

### FR-KAFKA-002 — Topics
Initial topic set:
- `calibre.raw.telemetry`
- `calibre.normalized.telemetry`
- `calibre.race.events`
- `calibre.weather`
- `calibre.sensor.integrity`
- `calibre.reference.performance`
- `calibre.performance.diagnostics`
- `calibre.strategy.predictions`
- `calibre.agent.decisions`
- `calibre.alerts`
- `calibre.deadletter`

The repository audit may consolidate topics if it preserves clear event ownership and independent consumer responsibilities.

### FR-KAFKA-003 — Reliability
The system shall expose producer failures, consumer lag, throughput, processing latency, retries, dead-letter counts and consumer health. Recovery shall not silently duplicate agent actions.

### FR-KAFKA-004 — Ordering
Where order matters, events shall be keyed appropriately, for example by session and driver. The system shall document where ordering is guaranteed and where it is not.

## 6. Sensor Integrity Intelligence

### FR-SENSOR-001 — Detection categories
Detect, where applicable:
- isolated spikes and outliers;
- sustained drift and step changes;
- stuck/repeated readings;
- missing, duplicated, delayed or out-of-order samples;
- implausible values and rates of change;
- cross-signal disagreement when independent evidence exists;
- schema, unit and timestamp inconsistencies.

### FR-SENSOR-002 — Complementary methods
Use physically justified bounds, robust outlier detection, CUSUM/change detection, temporal consistency checks and residual analysis as appropriate. CUSUM shall not be the only detector.

### FR-SENSOR-003 — Evidence
Each finding shall include signal, driver/session, observed and reference values where available, detection method, severity, confidence, timestamps, evidence window, data quality and recommended handling.

### FR-SENSOR-004 — Downstream quality
Downstream agents shall receive quality flags. When critical inputs are unreliable or insufficient, rule evaluations and recommendations shall defer rather than assert unsupported conclusions.

## 7. Agentic reference-performance twin

### FR-TWIN-001 — Reference model
The system shall calculate an evidence-supported reference-performance envelope using comparable historical laps and available telemetry. It shall distinguish best observed lap, feasible reference estimate and predicted achievable time.

### FR-TWIN-002 — Track alignment
Where source data supports it, align telemetry with track segments, sectors and turns. If the available spatial resolution cannot support corner-level diagnosis, the system shall state that limitation.

### FR-TWIN-003 — Environmental context
Incorporate available tyre compound/age, track and air temperature, weather, wind, track status, traffic and race-control context. Each feature shall retain its sampling rate and provenance.

### FR-TWIN-004 — Physics-derived features
Derived features such as acceleration, segment deltas and approximate aerodynamic drag shall identify assumptions and uncertainty. The UI must not imply unmeasured drag, tyre temperature, brake temperature, setup or steering angle was observed.

### FR-TWIN-005 — Diagnosis
For each supported deviation, return track section, actual/reference delta, estimated time impact, supporting signals, probable cause(s), alternatives, confidence, data quality and a next-lap review suggestion.

### FR-TWIN-006 — Explainability
Recommendations shall be traceable to events, samples, model version and calculation. An LLM may phrase a finding but shall not invent evidence or be the sole authority for physics calculations.

## 8. Race strategy intelligence

### FR-STRAT-001 — State updates
Update race state from relevant timing, tyre, weather, position and race-control events.

### FR-STRAT-002 — Strategy alternatives
Evaluate relevant tyre/stint and pit-window alternatives using available inputs, assumptions and uncertainty. Do not imply access to competitors' private strategy or car state.

### FR-STRAT-003 — Event distinction
Distinguish observed events (e.g., an official race-control message) from predicted risks (e.g., estimated safety-car likelihood). Predictions must include horizon, confidence and evidence.

### FR-STRAT-004 — Event-driven execution
Recompute agents when relevant state changes, using appropriate cadences rather than needlessly recomputing on every low-level sample.

### FR-STRAT-005 — Recommendation arbitration
Prioritise, reconcile and expire recommendations. Prevent contradictory high-priority messages from being emitted simultaneously without an explicit resolution.

## 9. Verdict and pit-lane monitor

### FR-VERDICT-001 — Verdict states
- `BREACH`: sufficient valid evidence establishes violation of the configured condition.
- `CLEAR`: sufficient valid evidence supports compliance for the evaluated window.
- `DEFER`: evidence is missing, stale, inconsistent or otherwise insufficient.

### FR-VERDICT-002 — Context
Record rule ID/version, applicable configured limit, session context, monitored zone, evidence window, source and data quality. Do not hard-code a universal pit-lane limit.

### FR-VERDICT-003 — Scope
CALIBRE produces analytical assessments, not official FIA rulings. Unreliable sensor inputs shall prevent unsupported definitive conclusions.

## 10. API, UI and persistence

### FR-OUT-001 — REST
Expose health, source capabilities, sessions, replay controls, latest race state, sensor findings, reference comparisons, diagnoses, strategy recommendations, verdicts and logs.

### FR-OUT-002 — WebSocket
Push timestamped, typed events for session state, timing, telemetry, source health, integrity, performance, strategy, verdicts and recommendations.

### FR-OUT-003 — Resynchronisation
A reconnecting client shall be able to retrieve a current snapshot and resume without reconstructing all state from events it may have missed.

### FR-OUT-004 — Audit log
Record raw/normalised events as configured, agent findings, model/version identifiers, recommendations, verdicts, source state and operating mode.

### FR-OUT-005 — UI provenance
All screens shall distinguish live observations, historical replay, synthetic events, derived features and estimates.

## 11. Non-functional requirements

- Reliability: source and consumer failure must be visible and recoverable.
- Observability: logs, metrics and health endpoints must identify where a pipeline is stalled.
- Reproducibility: identical replay inputs, scenario configuration and model versions should produce materially equivalent results.
- Performance: measure end-to-end event latency and publish actual measurements; do not promise a target until a baseline is collected.
- Security: secrets stay out of source control; mutating replay controls are protected against conflicting controllers.
- Maintainability: typed schemas, modular adapters, explicit configuration and useful error messages.
- Offline demonstration: cached historical data and local Kafka shall support a repeatable demonstration without an active race or external network access.

## 12. Acceptance criteria

A requirement is complete only when:
1. Its implementation is mapped to actual code.
2. A test exercises the real component and asserts expected behaviour.
3. Evidence is recorded (test output, API response, Kafka record, UI observation or trace).
4. Failure cases are tested where applicable.
5. Documentation accurately states any limitations.

Passing a unit test alone does not establish end-to-end acceptance.
