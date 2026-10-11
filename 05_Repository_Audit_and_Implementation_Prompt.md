# CALIBRE Repository Audit, Productisation and Verification Prompt

You are working in the existing CALIBRE repository. The system has reported passing tests, but the UI is sparse and the owner cannot tell whether the end-to-end system is genuinely working. Do not assume that reported tests prove integration.

## Mandatory first step: inspect the repository
Inspect source, tests, frontend routes/components, API routes/OpenAPI, Kafka configuration, replay implementation, agent implementations, fixtures, hard-coded demo values and current git status. Read the four product documents:
- `01_Functional_Requirements.md`
- `02_UI_Requirements_and_Plan.md`
- `03_API_Contract.md`
- `04_Planned_Architecture.md`

If they are not in the repository, copy them into a suitable `docs/product/` directory from the provided files or recreate them without losing requirements.

## Do not do this
- Do not claim a feature works merely because its unit test passes.
- Do not change tests just to make them pass.
- Do not use mocks for an integration test that claims to verify Kafka.
- Do not display fabricated telemetry, fake health, static charts or hard-coded success states as real.
- Do not silently switch live mode to replay or simulation.
- Do not replace working architecture wholesale without evidence.
- Do not claim the race replay is real-time live data.
- Do not assert exact corner-level diagnosis when available data only supports sector-level analysis.
- Do not claim a reference lap is physically perfect or exact when important car state is unavailable.

## Workstream A — requirements traceability
Create `docs/product/requirements_traceability.md` mapping every functional/UI/API/architecture requirement to:
- implementation file/module;
- API endpoint or event topic if relevant;
- test file and test type;
- observed evidence;
- status: VERIFIED, PARTIALLY VERIFIED, NOT IMPLEMENTED, BLOCKED;
- remaining gap.

Only mark VERIFIED when an appropriate test or runtime observation proves the requirement.

## Workstream B — establish a truthful baseline
Run existing test commands, but classify them as unit, integration, contract, Kafka E2E, UI E2E and live-feed smoke tests. Record exact commands, exit codes, pass/fail/skip counts and log locations.

Start the application and inspect actual endpoints and frontend behaviour. Check whether the current dashboard uses real backend data or hard-coded fixtures. Inspect browser console/network failures if browser tooling is available; otherwise use API/WebSocket integration tests and state the limitation.

Test API responses with both normal and unavailable dependencies. A `200 OK` is insufficient unless response content and underlying state are asserted.

## Workstream C — build one verified vertical slice first
Demonstrate:
1. Select a real completed race from a cached/downloaded historical session.
2. Start historical replay.
3. Verify replay events are published to a real Kafka broker.
4. Verify a real consumer receives and processes those events.
5. Verify sensor-integrity, performance comparison or race-event processing uses those inputs.
6. Verify the API and WebSocket expose the result.
7. Verify the UI visibly updates from those API/WebSocket events.
8. Verify provenance labels say HISTORICAL_REPLAY and observed/derived/estimated correctly.

Capture concrete evidence: event IDs, topic names, produced/consumed counts, timestamps, API response, WebSocket message, UI screenshot or automated browser assertion, and test log. If Kafka is not available in the environment, mark the Kafka E2E test BLOCKED; do not substitute an in-memory queue and call it verified.

## Workstream D — verify agent intelligence
For sensor integrity, inject a known spike and a separate sustained drift. Assert distinct expected detection types and evidence.

For reference performance, use actual session data, document baseline selection and exclusions, calculate actual/reference deltas, and verify uncertainty/coverage. Inject a controlled deviation only in simulation and confirm the diagnosis uses that deviation rather than generic canned text.

For strategy, inject a specific event such as a safety-car status change and verify that race state changes, strategy evaluation runs, and an evidence-linked recommendation is emitted or explicitly deferred.

Test that unavailable or corrupted inputs cause a clear defer/degraded result, not a confident generic answer.

## Workstream E — UI productisation
Implement the UI requirements document using existing framework and components where possible. Priority order:
1. Global operating mode, session and dependency health.
2. Race Control screen with functioning historical replay controls.
3. Streaming Operations screen proving Kafka topics, throughput, consumer lag and processing latency.
4. Sensor Integrity screen with evidence.
5. Performance Twin screen with actual/reference comparison and diagnosis.
6. Strategy & Alerts screen.
7. Session audit and system verification status.

Every panel must have loading, empty, stale, disconnected and error states. Remove or clearly label placeholders. No static green health badges. All buttons must invoke real operations and update state.

## Workstream F — API and contract integrity
Compare the API document with actual FastAPI routes and generated OpenAPI. Preserve working routes where possible. Add missing routes only when implemented, not as empty placeholders. Test request validation, status codes, schemas, operating-mode correctness and source provenance.

## Workstream G — fault injection and end-to-end tests
Add tests for:
- source failure and reconnect;
- Kafka unavailable;
- consumer crash and recovery;
- duplicate and out-of-order events;
- stale or missing telemetry;
- invalid schema and dead-letter routing;
- replay pause/resume/restart/seek;
- mode isolation;
- API/WebSocket consistency;
- UI controls and error states;
- no duplicate recommendation after replay restart;
- source and consumer metrics.

Keep unit tests and integration tests separately reported. Add a test that would fail if the pipeline bypasses Kafka.

## Workstream H — deliverables
Update the four documents to match the actual implementation and add:
- `docs/product/requirements_traceability.md`
- `docs/product/verification_report.md`
- `docs/product/demo_runbook.md`

The demo runbook must explain a clean start, selection of a historical race, starting playback, observing Kafka, viewing a sensor anomaly, seeing performance diagnosis and triggering a strategy event. Include commands and expected observable outputs.

## Final response
Report:
1. What changed.
2. Files changed.
3. Exact commands executed and their results.
4. Which requirements are VERIFIED, PARTIALLY VERIFIED, NOT IMPLEMENTED or BLOCKED.
5. Evidence for the complete replay → Kafka → consumer/agent → API/WebSocket → UI path.
6. Remaining limitations and next steps.

Do not state “all root causes resolved” or “system fully working” unless the evidence justifies it. Be blunt and specific about anything that could not be tested.
