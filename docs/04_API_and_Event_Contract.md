# CALIBRE — API and Event Contract

| Document control | Value |
|---|---|
| Document ID | CAL-DOC-004 |
| Version | 1.0-draft |
| Date | 1 November 2026 |
| Status | Proposed platform event contract |
| Parents | CAL-DOC-002 and CAL-DOC-003 |

## 1. Purpose and contract boundaries

This document defines the proposed internal event contracts exchanged across CALIBRE's Kafka-based pipeline. It is an event/API contract, not a public REST API specification. Public HTTP endpoints for the dashboard or third-party clients are deferred until the UI and access model are selected.

The contract is intentionally independent of the final agent/processor decomposition. Evidence producers may be reorganised, consolidated or independently deployed as long as they comply with the published contracts.

## 2. Contract conventions

- Initial wire format: UTF-8 JSON.
- Every contract includes `schema_name` and `schema_version`.
- Times are typed. Original source-time values and semantics are preserved; normalised times are added only when derivable.
- UTC timestamps use ISO 8601 with an explicit timezone, preferably `Z`.
- Numeric time values include a unit and semantic label.
- Unknown or unavailable values are `null` or an explicit `UNKNOWN` semantic, not fabricated defaults.
- Original source payloads are retained in the raw event or referenced through an immutable payload location/hash.
- Consumers must tolerate unknown optional fields and reject incompatible major schema versions.
- Kafka delivery is treated as at-least-once; consumers/sinks must be idempotent.
- Event identity and deduplication identity are distinct concepts.

## 3. Kafka topic catalogue

Topic names are logical baseline names. Feed slugs, partition counts, retention, replication and compaction are deployment configuration.

| Topic | Producer | Consumer(s) | Purpose |
|---|---|---|---|
| `calibre.raw.pitwall.<feed>` | Ingestion | Applicable evidence processors | Raw live provider messages/snapshots. |
| `calibre.raw.tracinginsights.<feed>` | Ingestion/replay | Applicable evidence processors | Raw historical records or replay payloads. |
| `calibre.evidence.<domain>` | Evidence processing layer | Flink core | Normalised observations and structured findings. Domain values are implementation-defined and contract-controlled. |
| `calibre.core.bundle` | Flink core | Persistence, dashboard, analytics | Decision-scoped evidence bundles. |
| `calibre.core.integrity` | Flink core | Persistence, dashboard, analytics | Integrity assessments and revisions. |
| `calibre.out.database` | Flink and analytics output router | Database sink | Unified persistence event stream or references to persisted event families. |
| `calibre.out.dashboard` | Flink output router | Dashboard gateway | Continuous operational state and evidence summaries. |
| `calibre.out.analytics` | Flink core | Analytics service | Integrity-aware evidence for metrics and rule evaluation. |
| `calibre.analytics.metrics` | Analytics service | Dashboard, database sink | Derived metrics and time series. |
| `calibre.alerts` | Analytics service | Dashboard, notification adapter | Human-review alerts and lifecycle changes. |
| `calibre.review.dispositions` | Dashboard/review service | Analytics, persistence | Human acknowledgement and disposition events. |
| `calibre.config.contracts` | Contract administrator | Flink core | Compacted topic for versioned DEC contracts. |
| `calibre.guard.liveness` | Supervisor | Flink core | Durable GuardFact (STALLED / RESUMED) stream. |
| `calibre.guard.clock` | Supervisor | Flink core | Durable RunClockTick facts for deterministic expiry. |
| `calibre.core.admission_log` | Ingress admission grader | Replay/audit | Append-only admitted and quarantined atom ledger. |
| `calibre.core.verdict_history` | Flink core | Persistence | Append-only bitemporal verdict revision log. |
| `calibre.dlq.<component>` | Any component | Operations/replay tooling | Invalid or unprocessable events with failure metadata. |

**Topic design note:** `calibre.evidence.<domain>` is a topic family. The actual domain names are governed by the selected processor decomposition. The platform does not require five particular agent services or a fixed list of domain topics at this stage.

## 4. Partitioning and consumer groups

### 4.1 Partition key

A recommended logical key is derived from the narrowest stable scope that requires ordered processing:

- session-level event: `session_id`;
- driver/lap event: `session_id:driver_id`;
- track/sector event: `session_id:track:sector_id`;
- source-health event: `provider:feed`.

The key must not be constructed from a field that is absent or unstable. A fallback key may use the source/feed and event identifier. Cross-scope correlation is handled in Flink state; Kafka partitions do not provide global ordering across keys.

### 4.2 Consumer groups

Each independently running processor that must see every applicable raw event must use its own consumer group. Multiple processors sharing one consumer group will compete for records and will not each receive a full copy. Flink, database, dashboard and analytics consumers must also be independent groups when each requires the full stream.

## 5. Common event envelope

All event types should carry these common fields where applicable:

```json
{
  "schema_name": "calibre.raw_source_envelope",
  "schema_version": "1.0",
  "event_id": "uuid-or-stable-source-event-id",
  "event_type": "raw_source_event",
  "provider": "pitwall",
  "feed": "TimingData",
  "source_mode": "LIVE",
  "run_id": "live-session-2026-11-01-r01",
  "received_at_utc": "2026-11-01T12:00:00.123Z",
  "session": {
    "season": 2026,
    "meeting_id": "meeting-key-or-null",
    "meeting_name": "Example Grand Prix",
    "session_id": "session-key-or-local-id",
    "session_name": "Race"
  },
  "source_time": {
    "value": "2026-11-01T12:00:00.000Z",
    "semantics": "ABSOLUTE_UTC",
    "unit": "iso8601"
  },
  "event_time_utc": "2026-11-01T12:00:00.000Z",
  "session_time_s": null,
  "payload_format": "JSON",
  "payload": {"provider_specific": "original provider payload"},
  "payload_ref": null,
  "payload_sha256": "optional-hash-of-exact-retained-payload"
}
```

The example is illustrative. `event_time_utc` and `session_time_s` are nullable and must not be populated unless conversion is supported by source semantics and a valid reference.

## 6. RawSourceEnvelope v1

### 6.1 Purpose

`RawSourceEnvelope` represents one acquired source message or one explicitly identified historical source record. It preserves original provider data and acquisition context. It is published by ingestion to `calibre.raw.*`.

### 6.2 Required fields

| Field | Type | Required | Meaning |
|---|---|---:|---|
| `schema_name` | string | Yes | `calibre.raw_source_envelope`. |
| `schema_version` | string | Yes | Contract version. |
| `event_id` | string | Yes | Unique envelope ID or stable source ID when available. |
| `event_type` | string | Yes | `raw_source_event`. |
| `provider` | string | Yes | Provider/adapter identifier. |
| `feed` | string | Yes | Topic/feed/file identifier. |
| `source_mode` | enum | Yes | `LIVE`, `HISTORICAL_REPLAY`, or `BATCH`. |
| `run_id` | string | Yes | Acquisition or replay run identifier. |
| `received_at_utc` | datetime | Yes | Ingestion receipt time. |
| `source_time` | object | Yes | Original value, semantics and unit; semantics may be `UNKNOWN`. |
| `session` | object | No | Season, meeting and session context when known. |
| `scope` | object | No | Driver/lap/sector/etc. when known without interpretation. |
| `payload_format` | string | Yes | `JSON`, `TEXT`, `BINARY_REFERENCE`, etc. |
| `payload` | any | Conditional | Original JSON/text payload where reasonable to inline. |
| `payload_ref` | string | Conditional | Immutable URI/path when payload is stored externally. |
| `payload_sha256` | string | No | Hash to verify retained payload identity. |
| `source_locator` | object | No | Source path, URL, repository/ref or topic metadata. |

Exactly one of `payload` or `payload_ref` should be supplied for a retained source payload. If both are supplied, `payload_ref` identifies the canonical retained object and `payload` may be a bounded copy.

### 6.3 Time semantics vocabulary

Initial values:

- `ABSOLUTE_UTC`
- `ABSOLUTE_WITH_OFFSET`
- `SESSION_ELAPSED_SECONDS`
- `LAP_RELATIVE_SECONDS`
- `LAP_DURATION_SECONDS`
- `RECEIPT_TIME_ONLY`
- `UNKNOWN`

New values require a contract change. The raw source value must remain intact even after a normalised event time is added.

## 7. EvidenceEvent v1

### 7.1 Purpose

`EvidenceEvent` is the output of an evidence processor. It represents a parsed observation, derived value, source-health observation or processor finding. It is published to `calibre.evidence.<domain>` and consumed by Flink.

### 7.2 Contract

| Field | Type | Required | Meaning |
|---|---|---:|---|
| `schema_name` | string | Yes | `calibre.evidence_event`. |
| `schema_version` | string | Yes | Contract version. |
| `event_id` | string | Yes | Unique evidence-event ID. |
| `event_type` | string | Yes | Stable type such as `telemetry_sample`, `race_control_message`, `weather_sample`, `lap_timing`, `source_health`, `processing_finding`. |
| `domain` | string | Yes | Evidence domain governed by implementation. |
| `producer` | object | Yes | Processor ID, version and build. |
| `produced_at_utc` | datetime | Yes | Evidence processing time. |
| `source_refs` | array | Yes | One or more raw `event_id`/payload references. |
| `session` | object | No | Meeting/session context. |
| `scope` | object | Yes | Explicit scope; may be session-, track-, sector-, driver-, lap- or source-scoped. |
| `time` | object | Yes | Original time semantics plus supported normalised values. |
| `value_origin` | enum/string | Yes | `SOURCE_REPORTED`, `DERIVED`, `INTERPOLATED`, `ESTIMATED`, `PROCESSOR_ASSESSMENT`, `UNKNOWN`. |
| `evidence` | object | Yes | Domain-specific structured evidence. |
| `findings` | array | Yes | Zero or more structured findings. |
| `limitations` | array | Yes | Known limitations, unavailable fields or interpretation caveats. |
| `dependency_refs` | array | No | Upstream evidence/source dependencies. |

### 7.3 Example

```json
{
  "schema_name": "calibre.evidence_event",
  "schema_version": "1.0",
  "event_id": "ev-9f3c",
  "event_type": "telemetry_sample",
  "domain": "vehicle_telemetry",
  "producer": {
    "processor_id": "telemetry_processor",
    "version": "0.1.0",
    "build": "dev"
  },
  "produced_at_utc": "2026-11-01T12:00:00.450Z",
  "source_refs": ["raw-21a8"],
  "session": {"season": 2026, "session_id": "session-abc", "session_name": "Race"},
  "scope": {"driver_id": "VER", "lap_number": 52},
  "time": {
    "source_value": 0.27,
    "source_semantics": "LAP_RELATIVE_SECONDS",
    "source_unit": "s",
    "session_time_s": 3521.484,
    "event_time_utc": null,
    "conversion_method": "lap_start_session_time_plus_lap_relative_time"
  },
  "value_origin": "SOURCE_REPORTED",
  "evidence": {
    "speed_kph": 104.9,
    "rpm": 9285,
    "gear": 2,
    "throttle_pct": 23.1,
    "brake": false
  },
  "findings": [],
  "limitations": ["Historical telemetry is a curated archive; field origin must follow source documentation."],
  "dependency_refs": ["raw-21a8", "lap-start-ref-52"]
}
```

The `session_time_s` in this illustrative example is valid only if the correct lap-start reference is available and the sample mapping is unambiguous.

## 8. Finding v1

A finding is a structured observation, not automatically a final verdict.

| Field | Type | Description |
|---|---|---|
| `finding_id` | string | Unique finding identifier. |
| `code` | string | Stable machine-readable finding code. |
| `category` | string | Examples: `MISSING_DATA`, `STALE_DATA`, `OUT_OF_RANGE`, `TIME_ALIGNMENT`, `DUPLICATE`, `INCONSISTENCY`, `PROVENANCE_LIMITATION`, `PROCESSING_ERROR`. |
| `severity` | enum | `INFO`, `LOW`, `MEDIUM`, `HIGH`, `CRITICAL`. |
| `message` | string | Human-readable explanation. |
| `observed_value` | any | Optional observed value. |
| `expected_range` | object | Optional range or reference. |
| `source_refs` | array | Evidence/source records supporting the finding. |
| `confidence` | number/null | Optional processor confidence in [0,1]; not a universal truth probability. |
| `requires_review` | boolean | Whether downstream review is suggested. |

## 9. EvidenceBundle v1

### 9.1 Purpose

`EvidenceBundle` is assembled by Flink for a defined session/time/scope/decision context. It groups references to relevant EvidenceEvents and describes evidence coverage, dependencies and unresolved limitations. It does not itself mean that a rule is breached.

### 9.2 Required fields

| Field | Type | Meaning |
|---|---|---|
| `schema_name` / `schema_version` | string | `calibre.evidence_bundle` and version. |
| `bundle_id` | string | Unique bundle ID. |
| `revision` | integer | Monotonic revision for this logical bundle. |
| `created_at_utc` | datetime | Core processing time. |
| `session` | object | Session context. |
| `scope` | object | Exact context the bundle covers. |
| `window` | object | Event-time/session-time interval and time basis. |
| `evidence_refs` | array | Evidence event IDs grouped by domain/type. |
| `coverage` | object | Required/available/missing evidence summary. |
| `dependency_graph_ref` | string/null | Reference to the dependency representation, if externalised. |
| `unresolved_items` | array | Missing, late, conflicting or unknown evidence. |
| `decision_context` | object | Decision/use-case identifier and required evidence policy version. |

### 9.3 Example

```json
{
  "schema_name": "calibre.evidence_bundle",
  "schema_version": "1.0",
  "bundle_id": "bundle-session-abc-VER-lap52-001",
  "revision": 1,
  "created_at_utc": "2026-11-01T12:00:01.000Z",
  "session": {"session_id": "session-abc", "session_name": "Race"},
  "scope": {"driver_id": "VER", "lap_number": 52},
  "window": {"basis": "SESSION_ELAPSED_SECONDS", "start_s": 3521.0, "end_s": 3522.0},
  "evidence_refs": {
    "vehicle": ["ev-9f3c"],
    "timing": ["ev-112a"],
    "environment": ["ev-887a"],
    "incident_track": [],
    "system": ["ev-91bc"]
  },
  "coverage": {"required": 4, "available": 3, "missing": ["track_state_for_sector"]},
  "dependency_graph_ref": null,
  "unresolved_items": ["No track-state evidence was available for the requested sector window."],
  "decision_context": {"use_case": "vehicle_behaviour_review", "policy_version": "0.1"}
}
```

Domain names above are illustrative and do not prescribe the final processor topology.

## 10. IntegrityAssessment v1

### 10.1 Purpose

`IntegrityAssessment` is the Flink core's scoped assessment of evidence fitness. It is independent of any downstream operational rule result.

### 10.2 Contract

| Field | Type | Required | Meaning |
|---|---|---:|---|
| `schema_name` | string | Yes | `calibre.integrity_assessment`. |
| `schema_version` | string | Yes | Version. |
| `assessment_id` | string | Yes | Unique assessment version ID. |
| `logical_assessment_id` | string | Yes | Stable ID across revisions. |
| `revision` | integer | Yes | Monotonic revision. |
| `bundle_id` | string | Yes | Related EvidenceBundle. |
| `integrity_state` | enum | Yes | `HEALTHY`, `SUSPECT`, `UNKNOWN`, `DEGRADED` (total order). |
| `resolution_maturity` | enum | Yes | `OPEN`, `STATE_RESOLVED`, `TUPLE_RESOLVED`, `REOPENED`. |
| `finality_status` | enum | Yes | `OPEN`, `FINAL`, `FINALITY_VIOLATION`. |
| `revision_number` | integer | Yes | Monotonic revision number $H_1 \to H_2$. |
| `decision_maturity` | enum | Yes | Derived display field (`PROVISIONAL`, `FINAL`, `REVISED`). |
| `verdict_hash` | string | Yes | RFC 8785 JCS SHA-256 canonical hex digest. |
| `previous_verdict_hash` | string/null | No | Hash pointer to preceding revision. |
| `lease_until` | datetime | Yes | Consumer validity lease expiration boundary. |
| `lease_clock_domain` | enum | Yes | `LIVE_UTC` or `REPLAY_VIRTUAL`. |
| `validity_timeline` | array | Yes | Pointwise validity timeline breakpoints. |
| `corroboration_profile_by_role` | object | Yes | Disjoint source and processing depths by role. |
| `primary_binding_witness` | string | Yes | Explanatory witness for binding decision. |
| `witness_set` | array | Yes | Complete audit witness references. |
| `assessed_at_utc` | datetime | Yes | Processing time. |
| `scope` | object | Yes | Assessment scope (`session_instance_id`, `driver_id`). |
| `explanation` | string | Yes | Human-readable assessment explanation. |
| `supporting_findings` | array | Yes | Finding/evidence references. |
| `affected_dependencies` | array | Yes | Dependencies considered impaired or uncertain. |
| `policy_version` | string | Yes | Integrity policy/config version. |
| `supersedes_assessment_id` | string/null | No | Prior assessment superseded by a revision. |

`HEALTHY` does not mean a sporting rule was satisfied. It means the evidence required by the named decision context passed the configured integrity policy. Per ERR-02, IntegrityAssessment v2.0 defines four orthogonal fields: `integrity_state`, `resolution_maturity`, `finality_status`, and monotonic integer `revision_number`. `decision_maturity` is retained strictly as a derived display field.

## 11. AnalyticsMetric v1

```json
{
  "schema_name": "calibre.analytics_metric",
  "schema_version": "1.0",
  "metric_id": "metric-123",
  "metric_name": "sector_time_delta",
  "value": 0.125,
  "unit": "s",
  "observed_at_utc": "2026-11-01T12:00:02.000Z",
  "session": {"session_id": "session-abc"},
  "scope": {"driver_id": "VER", "lap_number": 52, "sector": 2},
  "input_bundle_ids": ["bundle-session-abc-VER-lap52-001"],
  "integrity_status": "HEALTHY",
  "calculation_id": "sector_delta_v1",
  "calculation_version": "1.0"
}
```

Required fields: schema identity/version, metric ID/name, value, unit, observation time, scope, input references and calculation identity/version. The `integrity_status` is included as context and does not replace the metric value.

## 12. AnalyticsAlert v1

```json
{
  "schema_name": "calibre.analytics_alert",
  "schema_version": "1.0",
  "alert_id": "alert-123",
  "alert_revision": 1,
  "created_at_utc": "2026-11-01T12:00:03.000Z",
  "status": "OPEN",
  "severity": "HIGH",
  "category": "POTENTIAL_SENSOR_ANOMALY",
  "title": "Telemetry consistency requires review",
  "message": "The observed signal deviated from the configured reference envelope.",
  "session": {"session_id": "session-abc"},
  "scope": {"driver_id": "VER", "lap_number": 52},
  "related_bundle_ids": ["bundle-session-abc-VER-lap52-001"],
  "related_assessment_ids": ["assessment-789"],
  "evidence_refs": ["ev-9f3c"],
  "rule_id": "vehicle_signal_consistency_v1",
  "rule_version": "1.0",
  "recommended_review": "Inspect the source signal and its upstream dependencies.",
  "human_disposition": null
}
```

Alert status is a review lifecycle, not an integrity state. Acknowledgement or resolution is emitted as a separate review/disposition event and must not mutate the original evidence or assessment.

## 13. Error and dead-letter contract

Every processing error should include:

- `schema_name`, `schema_version`;
- `error_id`, `occurred_at_utc`;
- `component`, `component_version`, `stage`;
- `input_event_id` and source/payload reference where available;
- `error_code`, `error_message`, retryability;
- `attempt_count` and final disposition;
- safe diagnostic context (no credentials or secrets).

Invalid source messages should be preserved or referenced so that parser defects can be diagnosed and fixed records can be replayed deliberately.

## 14. Idempotency and deduplication

- `event_id` identifies an emitted event; `deduplication_key` identifies a logical source observation where a stable identity can be determined.
- A recommended deduplication-key composition may include provider, feed, session, source sequence/message key and source timestamp. The exact key must be feed-specific; timestamps alone are not guaranteed unique.
- Retries may publish the same event more than once. Consumers and sinks must be idempotent.
- Historical replay must carry a `run_id`; reprocessing can either reuse a stable logical source identity or create a new processing-run identity while retaining the original source locator.
- Assessment revisions are append-only and use `logical_assessment_id` plus `revision`.

## 15. Schema evolution

- Backward-compatible optional fields may be added in a minor version.
- Breaking changes to required fields, meanings or enums require a major version.
- Consumers must validate `schema_name` and supported major version.
- Unknown optional fields should be ignored but retained if raw preservation applies.
- A schema registry is recommended when the event set stabilises; it is not a Phase 1 prerequisite.

## 16. Deferred external APIs

Dashboard REST endpoints, authentication/authorisation, query filtering, pagination, websocket/SSE transport details and third-party API access are deferred. The first integration boundary is Kafka event contracts; a separate UI/API specification will be created when the dashboard and operator workflows are selected.
