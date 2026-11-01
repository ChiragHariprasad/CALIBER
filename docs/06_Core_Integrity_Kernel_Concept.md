# CALIBRE — Core Integrity Kernel: Preliminary Concept

| Document control | Value |
|---|---|
| Document ID | CAL-DOC-006 |
| Version | 1.0-draft |
| Date | 1 November 2026 |
| Status | Concept only — detailed design intentionally deferred |
| Parent | CAL-DOC-005 — System Architecture |

## 1. Purpose

This document records the basic idea behind the CALIBRE core. It is not an implementation specification. Exact algorithms, state models, dependency graph representation, watermark policies, thresholds, assessment transitions and Flink job decomposition will be defined after the platform scaffold and live-source contract are validated.

## 2. Central concept: decision-scoped evidence integrity

The core should not merely label a whole feed or system as healthy/unhealthy. It should assess whether the evidence required for a particular monitoring or rule-support decision is sufficiently complete, current, temporally aligned, internally consistent and traceable.

The same source problem can affect some decisions and leave others unaffected. For example, a weather-feed outage should not automatically invalidate a pit-entry timing assessment if that assessment does not depend on weather. Conversely, an incorrect lap-start reference can invalidate telemetry-to-session-time alignment for the affected driver/lap even if the telemetry payload itself is present.

The intended output is therefore an integrity assessment attached to an explicit decision scope and evidence bundle, not a global trust score detached from context.

## 3. Conceptual responsibilities

At a high level, the core is expected to:

1. Validate incoming versioned evidence events and their provenance.
2. Interpret timestamps only according to declared semantics and available reference data.
3. Track source freshness, missing records, sequence gaps, duplicate delivery and late arrival.
4. Resolve which evidence is relevant to a defined session/track/sector/driver/lap/decision scope.
5. Represent source and transformation dependencies.
6. Correlate observations over bounded temporal and scope relationships.
7. Recognise that two processor outputs may share the same upstream source and therefore are not independent corroboration.
8. Assemble a decision-scoped evidence bundle with explicit coverage and limitations.
9. Produce an integrity status and explanation with provisional/final/revised maturity.
10. Publish output versions while retaining previous assessments for audit.

## 4. Preliminary vocabulary

### 4.1 Integrity state

| State | Normative definition (CAL-DOC-007 §2.6) |
|---|---|
| `HEALTHY` | Evidence required for this scope is present, sufficiently fresh, mutually agreeing within tolerance $\eta$, and passing corroboration requirements. |
| `SUSPECT` | Evidence exists but exhibits pairwise divergence ($|v_1 - v_2| > \eta$), soft-age expiration, unmodelled common cause, or unresolvable fault diagnosis. |
| `UNKNOWN` | The core cannot establish required evidence or dependencies: evidence slot open within arrival deadline, missing/corrupted reference facts, or state overflow. |
| `DEGRADED` | Relevant evidence is no longer admissible: slot arrival deadline closed without admissible evidence, evidence has hard-expired, origin instance is `STALLED`, or state overflow occurred. |

States adhere strictly to the total order $\text{DEGRADED} < \text{UNKNOWN} < \text{SUSPECT} < \text{HEALTHY}$.

### 4.2 Orthogonal verdict dimensions

Per ERR-02, the verdict model defines four orthogonal dimensions:
1. `integrity_state`: $\text{DEGRADED} < \text{UNKNOWN} < \text{SUSPECT} < \text{HEALTHY}$.
2. `resolution_maturity`: `OPEN`, `STATE_RESOLVED`, `TUPLE_RESOLVED`, `REOPENED`.
3. `finality_status`: `OPEN`, `FINAL`, `FINALITY_VIOLATION`.
4. `revision_number`: Monotonic integer ($H_1 \to H_2 \to \dots$).

`decision_maturity` (`PROVISIONAL`, `FINAL`, `REVISED`) is retained strictly as a derived display field.

### 4.3 Separation from analytics outcome

Integrity status and operational rule outcome are different dimensions.

- **Integrity status:** can the evidence be trusted sufficiently for this use case?
- **Rule outcome:** does the accepted evidence satisfy, breach or fail to establish a configured operational condition?

A rule may be `DEFERRED` because the required evidence is degraded. A `HEALTHY` integrity assessment does not imply that a rule is clear; it means only that the evidence passed the applicable integrity policy.

## 5. Illustrative example

A vehicle telemetry stream reports speed and position. A lap timing feed provides a lap-start reference. A weather feed reports track conditions. A system-health observation reports that one feed has stopped updating.

The core should:

- retain the source and event identifiers behind each observation;
- distinguish lap-relative telemetry time from session elapsed time;
- use the lap-start reference only if it is valid and correctly matched;
- determine whether weather is required for the specific assessment;
- identify whether missing system-health evidence affects this driver's timing/telemetry scope;
- produce a bundle with available and missing evidence listed;
- issue an integrity assessment with a reasoned status and maturity;
- update the assessment if an accepted late record changes the evidence picture.

This example illustrates the intended reasoning boundary only. It does not prescribe the eventual formulas, thresholds or Flink state implementation.

## 6. Non-goals

The Phase 1 core is not intended to:

- replace official race-control or stewarding authority;
- independently adjudicate sporting penalties;
- issue flags, Safety Car or VSC commands;
- repair or modify external telemetry systems;
- treat all feeds as independent sources;
- convert every timestamp into a global clock when the required reference is unavailable;
- hide uncertainty behind a single confidence number;
- make analytics/rule decisions on behalf of the downstream analytics service.

## 7. Inputs and outputs — conceptual

**Inputs:** versioned EvidenceEvents, processor findings, source/time semantics, session context, scope, lineage/dependency references and policy/configuration versions.

**Outputs:** EvidenceBundles, IntegrityAssessments, revisions, processing-quality metrics and explicit unresolved/unknown conditions. These are routed by Flink to persistence, dashboard and analytics consumers.

## 8. Detailed design deferred until scaffold readiness

The following must be specified after live and historical event contracts are proven end-to-end:

1. Event-time assignment and per-feed watermark strategy.
2. Allowed lateness, provisional emission and finalisation rules.
3. Scope keys and correlation-window policy.
4. Dependency graph model and common-source/correlation treatment.
5. Deduplication keys and replay identity semantics.
6. Source-health and evidence-quality state transitions.
7. Integrity assessment rules, thresholds and uncertainty representation.
8. Bundle revision and supersession model.
9. Flink state layout, checkpointing and recovery strategy.
10. Performance, throughput and resource constraints.
11. Test scenarios and measurable acceptance thresholds.

## 9. Readiness gate

Detailed core design begins only after:

- live Pitwall message formats and timestamp semantics are captured and documented;
- TracingInsights historical semantics are covered by regression fixtures;
- the raw envelope and EvidenceEvent contracts are implemented and validated;
- duplicate, missing, out-of-order and malformed event behaviour is defined;
- Kafka-to-processor-to-Flink data flow is operational in the development scaffold.

Until then, this document is the agreed conceptual direction—not a claim that the integrity kernel is implemented.
