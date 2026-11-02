# CALIBRE UI/UX Plan

Status: implementation plan for the UI phase. The backend API routes listed below are the integration contract; the UI itself is intentionally deferred.

## Product intent

The interface should help an operator see what evidence arrived, how each specialist assessed it, which contract produced an assessment, what is known and unknown, and whether the transport and sinks are keeping up. It must not need race results or a human ground-truth label to produce an integrity assessment. Historical outcomes may be used for offline validation, never as live decision inputs.

The interface is provider- and event-type-neutral. Monaco is one replayable session. A future user must be able to select any event, session, source, contract, or run without Monaco-specific navigation or logic.

## Users and primary tasks

| User | Primary tasks |
|---|---|
| Race control / integrity operator | Monitor current session, spot a possible integrity issue, inspect evidence and uncertainty, acknowledge or escalate it. |
| Data analyst | Compare source feeds, inspect timing and valid/knowledge time, export a trace, compare revisions. |
| Platform operator | Check Kafka, processor, stream worker, and sink health; find lag, failures, and recovery gaps. |
| Reviewer | Reconstruct what the system knew at a past time and distinguish contemporaneous evidence from later revisions. |

## Navigation and information architecture

1. **Overview** — session selector, system health, event rate, active assessments, unresolved alerts, and source freshness.
2. **Sessions** — session/run list with provider, time range, status, data coverage, and replay/live mode.
3. **Assessment queue** — assessments grouped by contract and severity, with state, maturity, finality, confidence, and last change.
4. **Assessment detail** — evidence tuple, source provenance, witnesses, limitations, clock domain, valid time, knowledge time, lease metadata, hash, and revision chain.
5. **Evidence explorer** — searchable raw and normalized events, feed coverage, source gaps, and correlation trace.
6. **Pipeline health** — Kafka topics and consumer groups, specialist consumption/emission/filter/failure counters, stream-worker metrics, sink row counts, lag, and last successful activity.
7. **Replay and review** — run selection, source inventory, record accounting, and side-by-side revision history. Replay controls should be role-gated and audited.
8. **Contracts and sources** — contract versions, required signal fields, source adapters, unit/time semantics, and known data limitations.

## Overview screen

- Header: selected event/session, LIVE or REPLAY mode, clock domain, and a visible “data as of” timestamp.
- Health strip: Kafka, evidence processors, integrity worker, database sink, dashboard stream, analytics sink, and Apache Flink cluster reported separately. The UI must not imply that the Apache Flink cluster is executing the repository’s Python consumer worker.
- Coverage panel: expected signals by contract, observed signals by provider/feed, freshness, source gaps, and unsupported signals. Missing measurements appear as UNKNOWN/NOT EVALUATED.
- Assessment list: integrity state, resolution maturity, finality, revision number, contract, affected scope, and evidence age. OPEN, REVISED, and unresolved items remain visible until explicitly filtered.
- Timeline: source events and assessment changes on the event clock, with a separate knowledge-time marker for when the system received or revised each item.

## Assessment detail and evidence semantics

The detail view must keep evidence and operational metadata visually separate:

- **Evidence witnesses:** source observations, agreement checks, slot closure, missing/contradictory evidence, and evidence-limiting reasons.
- **Lease metadata:** consumer identity, clock domain, lease expiry, and timestamp semantics. A lease assignment must never be rendered as an evidence witness.
- **Time:** source/valid time, system knowledge time, and replay clock domain shown in separate fields. REPLAY_VIRTUAL timestamps must not be formatted or labeled as UTC.
- **Revision history:** append-only sequence, prior hash, current hash, reason for revision, and “what changed” comparison.
- **Limitations:** explicit missing signals and unsupported inference, such as a sensor contract without car-data throttle/brake samples.
- **No hindsight:** do not show race classification or later result labels beside a live assessment by default. Offline audit screens may show ground truth in a clearly separated validation mode.

## Evidence explorer and correlation trace

- Filters: event/session, run ID, provider, feed, event type, domain, driver/session scope, source time range, knowledge time range, and processing status.
- Record view: raw payload (read-only), normalized evidence, source references, payload hash, source timestamp semantics, ingestion timestamp, and correlation ID.
- Trace view: raw source record → specialist receipt → evidence event → routed envelope → DEC evaluation/output → persistence/dashboard/analytics sink.
- Per-record terminal outcome: INSERTED/PERSISTED, DEDUPLICATED, FILTERED, PENDING, FAILED, or UNACCOUNTED. UNACCOUNTED must be prominent and must block a “complete run” badge.

## Pipeline health and sink status

- Show per-topic partition offsets, consumer group committed offsets, lag, and last-seen timestamps.
- Show per-agent consumed, emitted, filtered, failed, retry, and duration counters, scoped by run and provider.
- Show evidence and verdict sink accepted/duplicate/error counts, database row totals, and latest persisted timestamp.
- Distinguish the local Python Kafka stream worker from the separately deployed Apache Flink cluster. A healthy cluster with no submitted job must read “cluster healthy; no Flink job submitted.”
- Health labels: VERIFIED, PARTIALLY VERIFIED, NOT VERIFIED, BLOCKED, and UNKNOWN, with an evidence link and time of last probe.

## API integration contract

Base URL is configured at deployment. OpenAPI is available at `/docs`.

| Method and path | UI use |
|---|---|
| `GET /v1/health/live` | Liveness probe. |
| `GET /v1/health/ready` | Dashboard/API readiness. |
| `GET /v1/system/status` | Kafka, Apache Flink REST, database, and dashboard connectivity. |
| `GET /v1/sources` | Persisted provider/feed coverage and latest source/persist times. |
| `GET /v1/assessments?limit=50` | Latest persisted assessment per decision instance. |
| `GET /v1/agents?run_id=…` | Durable specialist receipt counts when the configured database contains a replay run ledger. |
| `GET /v1/runs/{run_id}` | Durable run summary when the configured database contains that run. |
| `GET /metrics` | Dashboard stream counters. |
| `GET /events/recent?limit=20` | Recent dashboard events; limit 0–200. |
| `GET /trace/{correlation_id}` | Existing end-to-end correlation trace buffer. |
| `GET /v1/stream/events` | Server-sent event stream for live dashboard updates; supports `Last-Event-ID`. |
| `GET /v1/decisions/{decision_instance_id}` | Persisted revision history. |
| `GET /v1/decisions/{decision_instance_id}/at?valid_time=…&knowledge_time=…&clock_domain=LIVE_UTC` | Bitemporal assessment query. Set `clock_domain=REPLAY_VIRTUAL` for a replay; these coordinates must not be labeled UTC. |

Future API additions before UI build: paginated raw evidence search; a central multi-run registry across separate SQLite files; authenticated replay/acknowledgement commands; contract catalogue; signed export. These should be added to the same OpenAPI schema, with stable pagination and explicit schemas before the UI consumes them.

## Interaction and visual rules

- Use a neutral dark/light accessible palette with semantic state colors plus text labels and icons; color alone never communicates state.
- UNKNOWN and missing data use neutral styling, never green.
- FINAL is shown only when the backend says finality is FINAL and tuple resolution is complete. A user acknowledgement is not finality.
- Every count has a scope and denominator, for example “1,452 lap source records; 1,448 persisted; 4 pending.”
- Tables support keyboard navigation, sort, filter, and stable row links. Detail panels preserve the selected assessment when filters change.
- Every timestamp includes a clock-domain label and a readable timezone/semantics indicator.
- Exports include run ID, query filters, time coordinates, contract version, and limitations.

## Accessibility, security, and audit

- Target WCAG 2.2 AA: keyboard-only navigation, visible focus, screen-reader labels, reduced-motion support, and tested contrast.
- Read-only monitoring is the default. Replay, acknowledgement, and configuration actions require authenticated roles and server-side authorization.
- Never expose Kafka credentials, raw infrastructure secrets, or unrestricted SQL through the API.
- Audit mutations with actor, timestamp, prior value, new value, and reason. Provide a confirmation step for replay or configuration changes after the backend has shown the exact scope and source count.
- Redact or mask personal data and restrict raw payload access according to deployment policy.

## UI acceptance criteria

1. An operator can identify the current health of every pipeline component and see whether a real Apache Flink job is running.
2. A selected assessment can be traced to raw records, specialist receipts, contract requirements, witnesses, and sink status.
3. The operator can query a verdict at separate valid-time and knowledge-time coordinates and inspect all revisions.
4. Missing or unavailable signals remain UNKNOWN/NOT EVALUATED and are never converted into a healthy state or fabricated measurement.
5. Source counts reconcile to terminal outcomes; an unaccounted record prevents a complete-run indicator.
6. Live stream reconnects resume from the last event ID without duplicating already displayed items.
7. Historical replay time is visibly distinct from UTC wall-clock time.
8. No screen requires Monaco-specific fields or uses post-race classification to decide an integrity state.
