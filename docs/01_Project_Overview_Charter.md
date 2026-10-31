# CALIBRE — Project Overview and Charter

> **Calibration-Aware, Lineage-Indexed, Bitemporal Rule Evaluation** for motorsport evidence integrity.

| Document control | Value |
|---|---|
| Document ID | CAL-DOC-001 |
| Version | 1.0-draft |
| Date | 1 November 2026 |
| Status | Platform design baseline |
| Owner | CALIBRE project team |
| Related documents | CAL-DOC-002 through CAL-DOC-006 |

## 1. Executive summary

CALIBRE is an independent, lightweight evidence-integrity and assurance platform for motorsport data and rule-relevant monitoring. It acquires heterogeneous race evidence, preserves the original source payload and its lineage, processes the evidence through pluggable specialist processors, and evaluates the trustworthiness of decision-relevant evidence in a central Apache Flink integrity core.

The system addresses a specific gap: a downstream calculation can be mathematically correct while the evidence supplied to it is incomplete, stale, temporally misaligned, duplicated, derived from an uncertain source, or affected by a fault in an upstream dependency. CALIBRE makes those limitations explicit and produces explainable integrity assessments alongside operational analytics.

The target pipeline is:

**Public evidence sources → ingestion → Kafka raw evidence → specialist evidence processing → Kafka evidence events → Flink integrity core → (1) persistence, (2) dashboard stream, and (3) analytics/rule processing → human-review alerts.**

CALIBRE is an assurance and decision-support layer. It does not replace FIA/FOM systems, issue official sporting penalties, make race-control decisions, or autonomously modify external systems.

## 2. Problem statement

Motorsport monitoring depends on data from different feeds and systems. These feeds can differ in sampling cadence, update semantics, timestamps, availability and precision. A single operational conclusion may rely on multiple signals and derived values.

A dashboard can display a number without indicating whether the supporting evidence is suitable for the decision being made. A conventional threshold rule can trigger on a value without knowing whether the input is fresh, complete, correctly aligned or independent of other corroborating evidence.

CALIBRE introduces a separate evidence-assurance layer that answers:

- What was received, from which provider and feed?
- What does the timestamp mean, and can it be aligned to session time?
- Which transformations or processors produced this evidence?
- Which source records and assumptions does a derived value depend on?
- Is the evidence complete and sufficiently fresh for this particular scope?
- Are observations contradictory, duplicated, delayed or correlated through a common upstream source?
- Which downstream assessments are affected by a source or processing fault?

## 3. Vision, mission and outcomes

**Vision:** provide a transparent and auditable evidence-integrity layer for motorsport monitoring and rule-support workflows.

**Mission:** preserve evidence lineage and temporal meaning, assess source and processing quality, correlate relevant evidence in real time, and expose explainable integrity and analytics outputs to human operators.

**Expected outcomes:**

1. A reproducible record of acquired source evidence and transformations.
2. A consistent event contract across live and historical sources.
3. Independently consumable evidence streams for specialist processors.
4. Decision-scoped integrity assessments from Flink.
5. Continuous dashboard data, persistent audit records and derived analytics.
6. Explicit human-review alerts when evidence or system behaviour requires investigation.

## 4. Objectives

- Support a free/public Phase 1 data configuration without paid API subscriptions or F1 TV credentials.
- Retain the original provider payload or a retrievable immutable raw representation.
- Preserve provider, feed, source timestamp, receipt timestamp, session context and transformation provenance.
- Distinguish lap-relative, lap-duration, session-relative and absolute timestamps.
- Support pluggable evidence processors without hard-coding the final agent count or deployment topology.
- Use Kafka for buffering, fan-out, replay and decoupling.
- Use Flink for event-time processing, temporal alignment, cross-stream correlation and scoped integrity assessment.
- Separate evidence integrity from operational metrics and rule evaluation.
- Provide explainable, auditable outputs without autonomous actuation.

## 5. Scope

### 5.1 Phase 1 — platform foundation

**In scope:**

- Pitwall/F1 SignalR acquisition for the public unauthenticated live topics that are available.
- TracingInsights public historical archive retrieval and replay, limited to files and fields actually available.
- Raw evidence envelopes and source-preserving storage.
- Kafka raw and processed-evidence topic architecture.
- Pluggable processing/agent layer for parsing, normalisation, domain checks and evidence production.
- Versioned evidence and output contracts.
- Flink integrity-core foundation for event-time alignment, evidence correlation, dependency-aware assessment and output routing.
- Independent database, dashboard and analytics paths.
- Analytics metrics, configured rule evaluation and human-review alerts.
- Replay, fault-injection, schema, timestamp, recovery and end-to-end validation.

### 5.2 Explicitly out of scope for Phase 1

- Paid F1 TV access or paid API subscriptions.
- OpenF1 as an active source dependency; the tested endpoint returned HTTP 401 without authentication.
- F1 static archive as an active source dependency; the tested endpoint returned HTTP 401 without authentication.
- F1-TV-gated live `CarData.z` and `Position.z` feeds.
- Autonomous sporting decisions, penalties, flags, Safety Car/VSC calls or control-system commands.
- Automatic modification or repair of external F1 systems.
- Claims that public/interpolated historical fields are raw ECU, IMU or official sensor measurements.
- Final selection of agent count, agent process boundaries or agent framework. These will be redesigned around the final platform contracts.

### 5.3 Later-phase candidates

- Authorised remediation workflows after a human approves a proposed action.
- Additional independently licensed or public source adapters.
- More advanced vehicle-dynamics models and fault-isolation methods.
- Expanded operator workflow, incident case management and post-session investigation tools.
- Additional deployments and high-availability topologies.

## 6. Target operating model

### 6.1 Ingestion

Source adapters acquire data and publish raw, source-preserving events to Kafka. Ingestion owns connectivity, capture, source metadata, receipt timestamps, replay identification and delivery health. It does not make integrity verdicts or operational rule decisions.

### 6.2 Evidence processing

The evidence-processing layer subscribes to relevant raw topics. It parses provider-specific formats, validates and normalises records, performs bounded domain checks, and emits versioned evidence events with lineage, time semantics, scope, findings and limitations. Its internal structure is deliberately pluggable.

### 6.3 Flink integrity core

Flink consumes evidence events, validates contracts, assigns event time where justified, aligns evidence, handles late/duplicate records, evaluates dependencies and assembles decision-scoped evidence bundles. It emits integrity assessments and routes the resulting data to separate downstream paths.

### 6.4 Analytics and human review

The analytics layer consumes integrity-aware inputs from Flink. It calculates operational metrics, evaluates configured rules, identifies conditions requiring investigation, and publishes alerts with evidence references and explanations. Human operators remain responsible for decisions and actions.

## 7. Architectural principles

1. **Preserve before interpreting:** capture the source representation before parsing or normalising it.
2. **Time is typed:** a number called `time` is not sufficient without units and semantics.
3. **Lineage is first-class:** derived evidence references the source events and transformations that produced it.
4. **Scope faults narrowly:** a failure should affect only decisions that depend on the impaired evidence.
5. **Correlated evidence is not independent corroboration:** shared upstream dependencies must be visible.
6. **Unknown is not healthy:** absent or unconvertible information must remain explicitly unknown or degraded.
7. **Processors are replaceable:** the platform contract must not depend on the current agent layout.
8. **Integrity and analytics are separate:** evidence fitness is not the same as whether a sporting/operational rule is breached.
9. **Replay must be reproducible:** historical and captured data should traverse the same downstream contracts.
10. **Human authority is retained:** outputs are advisory and reviewable.

## 8. Stakeholders and system actors

| Actor | Interest / responsibility |
|---|---|
| System operator | Starts/stops ingestion and replay, observes source health and platform operations. |
| Evidence processor developer | Implements parsers and domain processors against stable event contracts. |
| Integrity-core developer | Implements Flink event-time, dependency, correlation and assessment behaviour. |
| Analytics/rule developer | Defines derived metrics, rule configuration and alert policies. |
| Human reviewer | Investigates alerts and decides whether further action is required. |
| Data/ML researcher | Uses retained evidence and lineage for analysis and model validation. |
| Platform maintainer | Operates Kafka, Flink, persistence, monitoring and deployment. |

## 9. Success criteria

CALIBRE Phase 1 is successful when:

- Live public SignalR topics can be captured and published without paid credentials.
- Historical TracingInsights records can be replayed without losing original source-time semantics.
- Every accepted raw event is traceable to provider, feed, session and raw payload/reference.
- Evidence processors publish versioned events independently of their internal deployment topology.
- Flink correlates relevant evidence by time and scope rather than arrival order alone.
- Missing, stale, invalid or contradictory evidence does not silently produce a healthy assessment.
- Late evidence can produce a revision while retaining prior assessment history.
- The persistence, dashboard and analytics consumers can operate independently.
- Analytics alerts explain why review is requested and identify the evidence involved.
- No Phase 1 component performs autonomous sporting or external-system actuation.

## 10. Constraints and assumptions

### Constraints

- The approved Phase 1 source stack must remain free to use.
- Public live feeds do not expose every F1 internal signal.
- TracingInsights is a curated historical archive; its available sessions, feeds and derived fields vary.
- Public-source availability, rate limits and repository completeness are external dependencies.
- Live payload semantics must be verified using actual live captures before finalising timestamp and field mappings.
- Runtime agent topology is intentionally open for redesign.

### Assumptions

- Kafka and Flink are the chosen event transport and central stream-processing technologies.
- JSON is the initial interchange representation; a schema registry and binary format may be introduced later.
- The platform will retain both source time and system receipt/processing times.
- The first deployment is a development/test environment, not a certified motorsport safety system.

## 11. Risks and mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| Public source changes or becomes unavailable | Loss of acquisition continuity | Adapter isolation, clear source-health state, local capture/replay and alternate-source evaluation. |
| Incorrect timestamp interpretation | Mis-correlated evidence and invalid conclusions | Explicit time-semantics enum, source-specific normalisers and regression fixtures. |
| Derived fields mistaken for raw sensor values | False confidence in evidence | Field-level provenance and limitations in evidence contracts. |
| Agents are tightly coupled | Difficult replacement and scaling | Stable raw/evidence contracts; do not encode current agent topology into Flink. |
| Kafka duplicate or late delivery | Duplicate or revised assessments | Idempotency keys, event-time policy and append-only revisions. |
| Analytics rules overreach | False alarms or incorrect conclusions | Human review, explainability, versioned rule configuration and test fixtures. |
| Output consumers become coupled | Dashboard or database outage affects processing | Independent topics/consumer groups and backpressure isolation. |

## 12. Delivery gates

1. **Source validation:** verify live topic payloads, timestamps and feed coverage; verify historical feed semantics.
2. **Raw event pipeline:** publish and consume schema-valid raw events through Kafka.
3. **Evidence layer:** implement pluggable processors and verify evidence events and lineage.
4. **Flink foundation:** implement event-time handling, correlation, scope and assessment lifecycle.
5. **Output paths:** validate persistence, dashboard and analytics independently.
6. **End-to-end validation:** replay representative sessions and fault scenarios; then test live source capture.

## 13. Glossary

| Term | Meaning |
|---|---|
| Raw event | Source-preserving event emitted by ingestion. |
| Evidence event | Parsed/normalised observation or finding emitted by an evidence processor. |
| Evidence bundle | Flink-assembled set of evidence relevant to a defined decision scope. |
| Integrity assessment | Flink's assessment of evidence fitness for a particular scope. |
| Event time | Time at which the source event/observation occurred, where it can be established. |
| Processing time | Time at which a component processes the event. |
| Session time | Elapsed time relative to a session reference point. |
| Lineage | Traceable relationship between an output and its source records/transformations. |
| Human-in-the-loop | A workflow in which an authorised person reviews alerts and makes operational decisions. |

## 14. Charter approval

This document establishes the platform-level project baseline. Detailed component algorithms, processor count, exact deployment topology, resource sizing and operational thresholds are controlled by their respective design and implementation documents and may be revised without changing the project purpose.
