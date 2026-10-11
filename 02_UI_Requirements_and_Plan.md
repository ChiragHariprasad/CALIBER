# CALIBRE — UI Requirements and Implementation Plan

**Document status:** Target UX specification; validate against the existing frontend before implementation.

## 1. UX objective

Replace a sparse or opaque dashboard with an operational race-engineering workspace. A user must be able to answer immediately:

- Is CALIBRE running?
- Is the system using LIVE, HISTORICAL REPLAY or SIMULATION?
- Is data actually flowing through Kafka?
- Which sources and telemetry fields are healthy or stale?
- What does the reference model think the driver could achieve?
- Where was time lost, what evidence supports the diagnosis, and what should be reviewed next?
- What tactical events or strategy changes need attention?
- Can I reproduce this demonstration without an active Grand Prix?

Do not create decorative charts filled with hard-coded values. Every displayed value must come from a real API response, a clearly identified derived calculation, or visibly labelled demo fixture.

## 2. Information architecture

### Primary navigation
1. **Race Control** — session selection, playback/live state, timing, event timeline.
2. **Performance Twin** — actual versus reference performance, segment deltas and diagnoses.
3. **Sensor Integrity** — anomalies, signal quality, evidence and detector status.
4. **Strategy & Alerts** — tyre/stint state, scenario comparisons, race events and recommendations.
5. **Streaming Operations** — Kafka topics, throughput, consumer lag, source health and processing latency.
6. **Session Audit** — event provenance, verdict history, agent decisions and replay/export metadata.
7. **System Health** — dependencies, configuration readiness and test/verification evidence.

Use the existing frontend framework and design system. Audit current routes and components before replacing anything.

## 3. Global application shell

Persistent header:
- CALIBRE product identity.
- Operating-mode badge: LIVE / HISTORICAL REPLAY / SIMULATION.
- Selected event/session and driver.
- Feed health indicator.
- Kafka status indicator based on a real backend health check.
- Last processed event age.
- Current session/replay clock.
- Connection status for WebSocket/API.

A green badge must mean a verified healthy condition, not simply that the frontend loaded. Use explicit states: Healthy, Degraded, Stale, Disconnected, Unknown.

Provide useful empty states that explain what is missing and how to start a session. Never render blank panels without an explanation.

## 4. Screen requirements

### 4.1 Race Control — primary demo screen

Layout:
- Top control bar: mode, session, driver, playback speed and play/pause/restart controls.
- Race status strip: lap, session time, position, tyre compound/age and track status, when available.
- Main centre panel: track map or segment progression only if valid position/track data is available.
- Timing panel: lap time, sectors, gap to reference, last/best lap as available.
- Right-side event stream: race-control events, sensor findings, strategy changes and recommendations.
- Bottom timeline: lap-by-lap progression, pit stops, safety-car periods and detected anomalies.

Historical replay controls:
- Session/Grand Prix selector.
- Driver selector.
- Play, pause, restart, seek and next-lap.
- Speeds: 0.25x, 0.5x, 1x, 2x, 5x, 10x.
- Current replay time and original event time distinction.
- Data-source and provenance label.
- Offline-cached session indicator.

Acceptance:
- Pressing Play must cause replay events to enter Kafka and update downstream panels.
- Pause must stop replay-time advancement and new replay publication.
- Restart/seek must not create duplicate agent actions.
- The mode remains visibly HISTORICAL REPLAY throughout.
- If data is unavailable, show an actionable error instead of fabricated metrics.

### 4.2 Performance Twin

Show:
- Actual lap/sector trace versus reference envelope.
- Sector and segment delta bars.
- Lap/sector selection and comparison baseline.
- Track segment/turn identifier where data supports it.
- Evidence panel: speed, throttle, brake, gear, position and weather values actually available.
- Estimated time loss/gain and uncertainty.
- Likely explanations ranked by confidence.
- Next-lap review recommendation.
- Data coverage and comparability warnings.

Never label the reference as “perfect lap” unless the product explicitly explains it is an estimate. Preferred language: “Reference performance” or “Estimated achievable envelope”.

If the available source cannot locate a deviation to a corner, show sector-level analysis and explain why finer localisation is unavailable.

### 4.3 Sensor Integrity

Show:
- Active anomaly count by severity.
- Signal health/freshness.
- Timeline of spikes, drift, stuck readings and missing/out-of-order data.
- Selected anomaly details with raw/observed value, baseline, detection method, evidence window and confidence.
- Downstream impact: which agent or verdict was deferred/affected.
- A replay/simulation fault-injection control for demonstrative scenarios.

Each anomaly must link to evidence and a timestamp. A test-only injected fault must be labelled SIMULATED.

### 4.4 Strategy & Alerts

Show:
- Current tyre/stint state, pit-stop history and relevant timing gaps.
- Race-control state and known penalties.
- Alternative strategy cards with assumptions, projected outcomes and uncertainty.
- Prioritised alert queue with severity, driver/session, source evidence, confidence, expiry and status.
- Decision history explaining why a recommendation was issued, changed or withdrawn.

Distinguish confirmed race-control facts from model forecasts. Never present a forecast safety car or collision as a confirmed event.

### 4.5 Streaming Operations

This screen is essential to prove Kafka is genuinely used.

Show:
- Kafka broker connectivity.
- Topic list and partition count.
- Producer rate and consumer rate by topic.
- Consumer lag and last consumed timestamp.
- End-to-end processing latency.
- Invalid/dead-letter counts.
- Source-to-topic event flow.
- Consumer group health and reconnect/error counts.

The UI must retrieve these metrics from real instrumentation or a clearly identified development diagnostic endpoint. Do not display static “Kafka connected” values.

Provide a recent-event inspector with event ID, topic, key, source, mode, event time, ingestion time and schema version. Do not expose secrets or unrestricted raw payloads.

### 4.6 Session Audit

Show a searchable chronological table of:
- Source events.
- Normalised events.
- Sensor findings.
- Reference updates.
- Strategy decisions.
- Verdicts.
- Driver recommendations.
- Errors and retries.

Filters: session, driver, event type, source, severity, operating mode and time range.

Selecting a record shows correlation IDs, model/rule version, input evidence and related downstream outputs.

### 4.7 System Health and verification

Display:
- API status.
- Kafka status.
- Database/storage status if configured.
- Source connector health and last successful event.
- WebSocket status.
- Active operating mode.
- Test-run history with timestamp, command, exit code, passed/failed/skipped counts and log link.
- End-to-end verification evidence.

Never equate “unit tests passed” with “full system verified”. Separate Unit, Integration, Contract, Kafka E2E, UI E2E and Live Feed Smoke Test statuses.

## 5. Visual design principles

Use a professional motorsport engineering aesthetic:
- Dark, high-contrast workspace with restrained accent colours.
- Strong information hierarchy and dense but readable telemetry tables.
- Consistent typography, spacing, panel alignment and chart axes.
- Avoid gratuitous animations, oversized cards and empty decorative widgets.
- Use colour plus text/icon/state labels; do not rely on colour alone.
- Make source freshness, confidence and provenance visible near the value they qualify.
- Design for a desktop evaluator presentation, with responsive behaviour for smaller screens.

Do not redesign the whole frontend blindly. First inventory existing pages, API contracts and component reuse, then implement the highest-value flows.

## 6. Loading, empty and error states

Every data-driven panel must support:
- Loading.
- No session selected.
- No data for this session/driver.
- Stale feed.
- Source disconnected.
- Partial field availability.
- Invalid response.
- Backend unavailable.
- Kafka unavailable.
- Replay completed.
- Replay paused.
- Replay failed.

Error states should include the failed dependency, time of failure and a concrete next action where possible.

## 7. UI data contract

All charts and components must consume typed API/WebSocket contracts. Every data item should carry or inherit:
- session ID;
- operating mode;
- source/provenance;
- event time;
- freshness or quality;
- observed/derived/estimated/simulated classification where relevant.

Do not fabricate values for chart continuity. Gaps should remain gaps or be explicitly interpolated and labelled.

## 8. Implementation plan

### Phase 1 — Audit
- Inventory existing routes, components and styles.
- Capture current screenshots and identify empty/placeholder panels.
- Map every widget to its actual endpoint/event.
- Identify dead controls and hard-coded fixture data.

### Phase 2 — Functional shell
- Implement global mode/session/source/Kafka status.
- Add useful loading, empty and error states.
- Connect all controls to real backend actions.

### Phase 3 — Race Control and Replay
- Make session selection and replay controls functional.
- Verify Kafka event counts increase and consumers process events during playback.
- Add current lap, timing and event timeline.

### Phase 4 — Performance Twin and Integrity
- Build evidence-first comparison charts and anomaly inspector.
- Link recommendations to the supporting evidence and event timeline.

### Phase 5 — Strategy and Streaming Operations
- Add strategy decision history and real Kafka/consumer observability.
- Ensure predictions and observed race-control events are visually distinct.

### Phase 6 — UI verification
- Add browser-based end-to-end tests for primary flows.
- Test backend-offline, Kafka-offline, no-data, stale-feed and replay-failure states.
- Confirm no core panel depends on hard-coded success data.

## 9. UI acceptance criteria

The UI is acceptable only when an evaluator can:
1. Start the application and understand its current health.
2. Start a historical replay without an active race.
3. See replay events flow through Kafka.
4. Observe lap/sector progression update.
5. Open an integrity finding and inspect its evidence.
6. Open a performance diagnosis and trace it to actual inputs.
7. See strategy/alert changes when a test scenario triggers them.
8. Identify whether every displayed item is live, replayed, simulated, observed or estimated.
9. See useful error messages when a dependency is stopped.
10. Complete these flows without reading server logs or editing source code.
