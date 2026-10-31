# CALIBRE — Functional Requirements

| Document control | Value |
|---|---|
| Document ID | CAL-DOC-002 |
| Version | 1.0-draft |
| Date | 1 November 2026 |
| Status | Target functional baseline |
| Parent | CAL-DOC-001 — Project Overview and Charter |

## 1. Purpose and conventions

This document defines externally observable functions required of the CALIBRE platform. Requirements are technology-aware where architecture decisions have been made, but do not prescribe the final internal structure or count of evidence processors.

**Priority:** `MUST` is required for Phase 1 acceptance; `SHOULD` is expected unless explicitly deferred; `COULD` is optional or later-phase.

Each requirement has an acceptance condition that can be verified by inspection, test, demonstration or replay.

## 2. Source acquisition and ingestion

| ID | Priority | Requirement | Acceptance condition |
|---|---|---|---|
| FR-001 | MUST | The platform shall acquire unauthenticated live topics available through the approved Pitwall/F1 SignalR integration. | A live capture records provider, topic, source timestamp if supplied and receipt time. |
| FR-002 | MUST | The platform shall retrieve and replay available TracingInsights historical session data. | An operator can select an available season, event, session and feed/driver file. |
| FR-003 | MUST | The approved Phase 1 configuration shall not depend on paid APIs, F1 TV access or paid data subscriptions. | The configured source path operates without those credentials. |
| FR-004 | MUST | OpenF1 and the tested F1 static archive endpoint shall be excluded from the active acquisition path. | No active adapter requires either endpoint. |
| FR-005 | MUST | Ingestion shall preserve the original source payload or an immutable raw payload reference before interpretation. | Every accepted raw event can be traced to the retained original representation. |
| FR-006 | MUST | Ingestion shall add provider, feed, receipt timestamp, source mode and session context where known. | Raw envelope contains the required metadata or explicitly marks it unavailable. |
| FR-007 | MUST | Ingestion shall preserve source time separately from receipt time and processing time. | No timestamp is silently overwritten by a later pipeline stage. |
| FR-008 | MUST | Historical replay shall retain original source time and identify replay/run context. | Repeated replay retains source timestamps and uses a distinct run identifier. |
| FR-009 | SHOULD | Historical replay shall support configurable throughput or pacing. | The operator can select fast replay or a configured playback rate. |
| FR-010 | MUST | Ingestion shall publish raw events to the appropriate Kafka topic. | A consumer receives a schema-valid raw envelope from the expected topic. |
| FR-011 | MUST | Ingestion shall expose connection, retrieval, parsing and publishing failures. | A test failure appears in logs/metrics and is not silently dropped. |

## 3. Parsing, normalisation and evidence processing

| ID | Priority | Requirement | Acceptance condition |
|---|---|---|---|
| FR-012 | MUST | The evidence-processing layer shall consume applicable raw Kafka events independently of other processors. | A raw event can be delivered to each applicable processor without a shared consumer group causing competing consumption. |
| FR-013 | MUST | Processors shall validate required fields, data types, units and source-specific structures. | Invalid records produce an explicit processing result/error and are not emitted as valid evidence. |
| FR-014 | MUST | Processors shall preserve the original provider fields or a raw-event reference. | Each evidence event identifies the raw source event(s) from which it was derived. |
| FR-015 | MUST | Processors shall attach processor identity, version, transformation version and processing time. | Evidence records are reproducibly attributable to a processor build. |
| FR-016 | MUST | Processors shall express time semantics explicitly. | A timestamp field identifies its meaning and unit, not just its raw value. |
| FR-017 | MUST | Processors shall distinguish observations from derived values and assessments. | Derived/interpolated/estimated fields are marked with their derivation or source limitation. |
| FR-018 | MUST | Processors shall emit versioned evidence events and structured findings. | Evidence events validate against the published contract. |
| FR-019 | SHOULD | Processor implementations shall be replaceable without requiring changes to raw-source adapters or Flink output consumers. | A processor implementation can be replaced while preserving the event contract. |

## 4. Time and data semantics

| ID | Priority | Requirement | Acceptance condition |
|---|---|---|---|
| FR-020 | MUST | The platform shall distinguish absolute UTC time, session elapsed time, lap-relative time, lap duration, receipt time and unknown time. | Each accepted time value has an explicit semantic classification. |
| FR-021 | MUST | For TracingInsights per-lap telemetry, session elapsed time shall be derived as `lST + telemetry.time` only when a matching lap-start reference is available. | Without `lST`, the event retains lap-relative time and session time remains null/unavailable. |
| FR-022 | MUST | For TracingInsights lap-time records, `time` shall not be interpreted as a session timestamp. | Lap duration remains a duration; `sesT` and `lST` are retained as separate session-time values when present. |
| FR-023 | MUST | TracingInsights weather `wT`, when present, shall be treated as session elapsed seconds. | The normalised event maps `wT` to session time with seconds as its unit. |
| FR-024 | MUST | Absolute race-control timestamps shall be retained as absolute time. Session-time conversion may occur only when a matching session reference such as `t0_date` is available. | Conversion is traceable and is not fabricated when the reference is missing. |
| FR-025 | MUST | The platform shall retain timezone and precision information for absolute timestamps. | Original timestamp string and normalised UTC value are both traceable. |
| FR-026 | MUST | The platform shall mark TracingInsights interpolated/derived position and computed acceleration fields as such. | Such fields are not labelled as raw GPS/ECU/IMU observations. |

## 5. Evidence correlation and integrity core

| ID | Priority | Requirement | Acceptance condition |
|---|---|---|---|
| FR-027 | MUST | Flink shall validate incoming evidence-event schemas and required identifiers. | Invalid events are rejected or routed to an error path with reason metadata. |
| FR-028 | MUST | Flink shall use event time when source semantics support it and retain uncertainty when they do not. | Unknown or unconvertible times are not silently replaced with event time. |
| FR-029 | MUST | Flink shall handle out-of-order and late-arriving evidence using configured event-time and allowed-lateness policies. | Test fixtures demonstrate initial, late and revised assessments. |
| FR-030 | MUST | Flink shall deduplicate repeated delivery using an explicit idempotency/deduplication key. | Replayed duplicate messages do not create duplicate logical evidence or duplicate side effects. |
| FR-031 | MUST | Flink shall correlate evidence by session, time and applicable scope. | A driver-level event is not indiscriminately joined to unrelated driver/session scopes. |
| FR-032 | MUST | Flink shall represent evidence dependencies and shared upstream lineage. | Correlated evidence is not treated as independent corroboration solely because it comes from different processors. |
| FR-033 | MUST | Flink shall produce decision-scoped evidence bundles and integrity assessments. | Output identifies scope, evidence references, coverage, limitations and assessment state. |
| FR-034 | MUST | Flink shall scope integrity impact to affected dependencies. | A source failure affects dependent assessments but does not invalidate unrelated scopes by default. |
| FR-035 | MUST | Flink shall distinguish integrity status from operational rule outcome. | A healthy evidence assessment does not itself mean a rule is breached or satisfied. |
| FR-036 | MUST | Flink shall retain assessment revisions when late evidence changes an earlier result. | Previous assessment history remains retrievable after a revision. |

## 6. Outputs, analytics and human review

| ID | Priority | Requirement | Acceptance condition |
|---|---|---|---|
| FR-037 | MUST | Flink shall publish persistence-oriented outputs independently of dashboard and analytics consumers. | Database sink can consume its output without requiring the dashboard or analytics service. |
| FR-038 | MUST | Flink shall publish a continuous operational data stream for dashboard delivery. | Dashboard gateway can consume current state and updates from its own topic/consumer group. |
| FR-039 | MUST | Flink shall publish integrity-aware inputs to the analytics service. | Analytics receives evidence bundle/assessment references and associated scope. |
| FR-040 | MUST | Analytics shall calculate derived operational metrics from the accepted inputs. | Metric output identifies metric name, value, unit, source scope, calculation/rule version and time. |
| FR-041 | MUST | Analytics shall evaluate configured operational rules separately from evidence-integrity assessment. | Rule result and integrity status are independently represented. |
| FR-042 | MUST | Analytics shall emit explainable alerts for conditions requiring human investigation. | Alert includes severity, scope, trigger, explanation and evidence references. |
| FR-043 | MUST | Dashboard delivery shall combine live operational state, metrics and alerts without making the dashboard the authoritative rule engine. | Dashboard displays upstream outputs and does not independently determine integrity or alert status. |
| FR-044 | MUST | Human review shall be represented as an explicit alert lifecycle. | An alert can be acknowledged and dispositioned without changing the original evidence or assessment. |
| FR-045 | MUST | Phase 1 shall not issue official sporting decisions or commands to external systems. | No autonomous actuation path exists in the Phase 1 deployment. |

## 7. Operations, resilience and reproducibility

| ID | Priority | Requirement | Acceptance condition |
|---|---|---|---|
| FR-046 | MUST | The platform shall expose health and lag for sources, Kafka topics, processors, Flink and output consumers. | Operator can identify the failing or delayed stage. |
| FR-047 | MUST | Processing errors shall include component, event ID, schema version, reason and retry/dead-letter disposition where applicable. | Error can be traced to a specific input and processing stage. |
| FR-048 | MUST | The platform shall support replay of retained historical evidence through the same contracts used by live data. | The same parser/core path can process replay and live envelopes. |
| FR-049 | MUST | Reprocessing shall be idempotent or clearly identified as a new replay/run. | Re-run does not silently overwrite or duplicate prior audit records. |
| FR-050 | SHOULD | Configuration and rule changes shall be versioned and attributable. | A metric or alert can be traced to the configuration version used. |
| FR-051 | SHOULD | The platform shall support synthetic fault-injection fixtures for signals not exposed by public sources. | Fault scenarios are labelled as tests and cannot be confused with acquired source evidence. |

## 8. Business invariants

1. A missing timestamp is not a zero timestamp.
2. Receipt time is not a substitute for source event time.
3. Lap duration is not session elapsed time.
4. A lap-relative telemetry timestamp cannot be promoted to session time without a valid lap-start reference.
5. Derived/interpolated data is never labelled as raw sensor data.
6. Unknown evidence coverage cannot yield an unqualified healthy assessment.
7. A processor finding is evidence for the core, not a final system-wide verdict.
8. Integrity assessment and rule outcome are separate dimensions.
9. One failing source does not invalidate unrelated decisions by default.
10. Historical replay preserves source-time values and identifies replay context.
11. Every alert must reference the evidence and rule/configuration version that produced it.
12. Phase 1 remains read-only with respect to external motorsport systems.

## 9. Deferred functions

- Final evidence-processor decomposition, count, packaging and deployment strategy.
- Final Flink state model, dependency graph algorithms, watermark strategy and integrity scoring/decision logic.
- Remediation or self-correction of external sources/systems.
- Public external REST API for third-party clients.
- Full user/role management and notification-channel integrations.

## 10. Acceptance test suites

- Source adapter contract tests for historical and live payloads.
- Timestamp and unit semantics tests.
- Raw payload retention and lineage tests.
- Processor schema, parser and fault-screening tests.
- Kafka fan-out, consumer-group and retry tests.
- Flink event-time, duplicate, late-arrival, dependency and revision tests.
- Persistence, dashboard and analytics isolation tests.
- Rule/alert explainability and human-review lifecycle tests.
- End-to-end historical replay and live-capture tests.
