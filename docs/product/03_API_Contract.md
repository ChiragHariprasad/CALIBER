# CALIBRE — API Contract

**Document status:** Target contract; compare against the current FastAPI/OpenAPI implementation before changing routes.  
**Base path:** `/api/v1`

## 1. Contract rules

- Existing endpoints should be retained where possible; introduce aliases or versioned changes rather than breaking working clients unnecessarily.
- The live API, replay API and simulation API must expose operating mode and provenance.
- Use UTC timestamps in ISO 8601 format.
- Validate requests and return structured errors.
- Do not return fabricated healthy states or synthetic values without an explicit mode/source label.
- REST controls manage sessions; WebSocket distributes events and state updates.
- Use the generated OpenAPI document as the contract source after implementation.

## 2. Standard response and error model

Suggested error shape:

```json
{
  "error": {
    "code": "REPLAY_SESSION_NOT_FOUND",
    "message": "The requested replay session is not available.",
    "details": {"session_id": "example"},
    "request_id": "correlation-id"
  }
}
```

Use suitable HTTP status codes: 400 invalid request, 404 missing resource, 409 conflicting state, 422 schema/semantic validation failure, 429 rate limit, 503 dependency unavailable.

## 3. Health and operations

### `GET /health`
Purpose: process-level health.

### `GET /api/v1/health`
Purpose: application readiness and dependency summary.

Suggested response:

```json
{
  "status": "degraded",
  "mode": "HISTORICAL_REPLAY",
  "dependencies": {
    "api": {"status": "healthy"},
    "kafka": {"status": "healthy"},
    "storage": {"status": "healthy"},
    "websocket": {"status": "healthy"}
  },
  "checked_at": "2026-10-11T00:00:00Z"
}
```

The example is illustrative; status must be computed from real checks.

### `GET /api/v1/live/status`
Return current mode, session state, selected session/driver, replay clock if applicable, source freshness and last processed event.

### `GET /api/v1/live/sources`
Return configured sources, enabled capabilities, connection state, last successful event, last error, retry count and whether the source is suitable for the current mode.

### `GET /api/v1/streaming/status`
Return Kafka broker health, topic/consumer status, event rates, consumer lag, processing latency and dead-letter counts where instrumentation is available. If a metric is unavailable, return `null`/`unknown`, not a fabricated zero.

## 4. Sessions and replay

### `GET /api/v1/sessions`
List sessions available from configured providers and local cache.

Query parameters:
- `year` optional integer
- `source` optional string
- `cached_only` optional boolean

Response fields should include session ID/key, meeting name, circuit, year/date, session type, available drivers, data categories available, source and cache status.

### `GET /api/v1/sessions/{session_id}`
Return session metadata, data coverage, drivers, event count if known, timestamp range and missing categories.

### `POST /api/v1/replay`
Start a replay.

Request:

```json
{
  "session_id": "11731",
  "driver": "VER",
  "speed": 2.0,
  "scenario_profile": null,
  "use_cached_data": true
}
```

Response: replay ID, state, mode `HISTORICAL_REPLAY`, speed, session metadata and current replay clock. Validate supported speeds and session availability.

### `GET /api/v1/replay/{replay_id}`
Return replay state, current clock, current lap, speed, progress, last published event time and error state.

### `POST /api/v1/replay/{replay_id}/pause`
Pause event-time advancement and replay publication.

### `POST /api/v1/replay/{replay_id}/resume`
Resume from the paused clock.

### `POST /api/v1/replay/{replay_id}/restart`
Restart from the beginning and reset replay state safely.

### `POST /api/v1/replay/{replay_id}/seek`
Request:

```json
{"target_lap": 24}
```

Optionally support `target_time` if event indexing permits. Seeking must define how downstream state is rebuilt and how duplicate actions are prevented.

### `PATCH /api/v1/replay/{replay_id}/speed`
Request:

```json
{"speed": 5.0}
```

### `POST /api/v1/replay/{replay_id}/stop`
Stop the replay and publish a terminal state.

### `GET /api/v1/replay/{replay_id}/events`
Paginated event inspection for audit/debug use. Do not use this polling endpoint as the primary live streaming mechanism.

## 5. Race state and performance

### `GET /api/v1/live/race`
Return the latest race/session snapshot: lap, session clock, positions, tyre/stint state, track status, data mode, source freshness and coverage. Omit unavailable values or mark them unavailable.

### `GET /api/v1/live/drivers/{driver_id}/performance`
Return latest reference versus actual performance, sector deltas, segment alignment, estimated time impact, uncertainty and comparability warnings.

### `GET /api/v1/live/drivers/{driver_id}/diagnostics`
Return performance diagnoses with location/sector, observed evidence, reference evidence, likely causes, confidence, quality flags and recommended review action.

### `GET /api/v1/live/reference-performance`
Query parameters: `session_id`, `driver`, optional `lap`, optional `sector`.

Return model version, baseline selection, comparability rules, reference envelope, uncertainty and input coverage.

## 6. Sensor integrity and verdicts

### `GET /api/v1/live/integrity`
Query parameters: `session_id`, `driver`, `severity`, `since`, `limit`, `cursor`.

Return anomaly ID, signal, category, severity, confidence, observed/reference values, event time, detection method, evidence reference, quality impact and lifecycle state.

### `GET /api/v1/live/integrity/{finding_id}`
Return full finding and relevant evidence window.

### `POST /api/v1/live/integrity/scenarios`
Development/test-only endpoint to inject an explicitly synthetic fault. Must be disabled or protected in production.

### `GET /api/v1/live/verdicts`
Return verdict history, including `BREACH`, `CLEAR`, `DEFER`, rule/version, threshold, evidence window, data quality, mode and explanation.

### `GET /api/v1/live/pit_monitor`
Return monitor configuration, applicable speed limit, monitored-zone state, latest evidence and health. Never imply a universal pit-lane limit.

## 7. Strategy, alerts and radio suggestions

### `GET /api/v1/live/strategy`
Return current strategy state, tyre/stint state, candidate alternatives, assumptions, estimated outcomes, confidence and model version.

### `GET /api/v1/live/alerts`
Filters: `severity`, `status`, `driver`, `session_id`, `since`.

Return alert ID, category, priority, title, message, event time, source evidence, confidence, expiry and status.

### `GET /api/v1/live/radio`
Return driver-radio-style suggestions, not actual team communications. Include message, driver/session, timestamp, category, priority, evidence IDs, confidence, expiry and whether the message is synthetic.

### `POST /api/v1/live/alerts/{alert_id}/acknowledge`
Acknowledge an alert where user identity/access control is configured.

## 8. Event WebSocket

### `WS /api/v1/live/ws`

On connection, send a snapshot or snapshot reference, followed by typed events.

Suggested event envelope:

```json
{
  "schema_version": "1.0",
  "event_id": "event-unique-id",
  "type": "performance.diagnosis",
  "session_id": "session-id",
  "driver_id": "VER",
  "mode": "HISTORICAL_REPLAY",
  "source": "calibre.performance_agent",
  "event_time": "2026-10-11T00:00:00Z",
  "ingested_at": "2026-10-11T00:00:00Z",
  "quality": {"is_valid": true, "is_stale": false},
  "payload": {}
}
```

Event types may include:
- `session.state`
- `timing.update`
- `telemetry.update`
- `race_control.event`
- `weather.update`
- `source.health`
- `streaming.metrics`
- `sensor.integrity`
- `reference.performance`
- `performance.diagnosis`
- `strategy.update`
- `verdict.update`
- `alert.created`
- `alert.updated`
- `replay.state`
- `system.error`

Support client filters if practical. Define ping/pong, reconnect and snapshot-resynchronisation behaviour. Avoid sending high-frequency telemetry indiscriminately to every client if a subscription/filter model is available.

## 9. Kafka event contract

Kafka topic payloads shall use the same versioned envelope or a documented compatible schema. Include:
- stable event identity;
- session/driver key where relevant;
- source event time and ingestion time;
- operating mode;
- provenance and quality;
- typed payload.

Topic names proposed in the functional requirements are a starting point. Document actual topic names, partitioning, keys, retention and consumer groups in the deployed configuration.

## 10. Existing endpoint compatibility

The current implementation reportedly exposes some of:
- `/api/v1/health`
- `/api/v1/live/status`
- `/api/v1/live/sources`
- `/api/v1/live/pit_monitor`
- `/api/v1/live/verdicts`
- `/api/v1/live/radio`

Verify the actual OpenAPI schema and handlers before treating this list as authoritative. Preserve existing working routes or document deliberate migrations.

## 11. API verification requirements

For every endpoint:
- Test expected success response against a real service instance.
- Test invalid parameters and missing resources.
- Test unavailable Kafka/source/storage dependencies.
- Validate response schema.
- Confirm fields match the active operating mode and provenance.
- Verify mutating endpoints change observable system state.
- Verify replay controls alter event publication and replay clock.
- Verify WebSocket messages correspond to actual backend events.
- Record status code, response body, test command and timestamp.

A `200 OK` response alone does not prove that the underlying subsystem is working.
