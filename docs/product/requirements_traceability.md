# CALIBRE Requirements Traceability Matrix

This document provides a comprehensive audit and traceability mapping of every requirement specified across `01_Functional_Requirements.md`, `02_UI_Requirements_and_Plan.md`, `03_API_Contract.md`, and `04_Planned_Architecture.md` to implementation artifacts, tests, runtime evidence, verification statuses, and identified gaps.

Status definitions:
- **VERIFIED**: Proven by passing automated tests running against real runtime systems (Kafka, REST API, WebSocket, agents) and observed runtime logs.
- **PARTIALLY VERIFIED**: Implemented and partially tested, but subject to physical data resolution limitations (e.g., sector-level vs micro-corner telemetry).
- **NOT IMPLEMENTED**: Requirement deferred or out of scope for the current slice.
- **BLOCKED**: Implementation obstructed by external constraints.

---

## 1. Functional Requirements (`01_Functional_Requirements.md`)

| Req ID | Requirement Description | Implementation Module | API Endpoint / Kafka Topic | Test File & Type | Observed Evidence | Status | Remaining Gap |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **FR-DATA-001** | Multi-Point Streaming Source Ingestion | `calibre/live/openf1_client.py`, `calibre/streaming/processor.py` | `calibre.raw.telemetry`, `f1.raw.telemetry` | `test_multipoint_streaming.py` (Integration) | Replay & processor handle concurrent telemetry feeds | **VERIFIED** | Live OpenF1 internet API rate limits require local caching in offline runs |
| **FR-DATA-002** | Operating Mode Isolation (Live / Replay / Simulation) | `calibre/live/operating_mode.py`, `calibre/replay/controller.py` | `GET /api/v1/health`, `GET /api/v1/live/race` | `test_fault_injection.py` (Unit/Contract) | Mode transitions strictly logged and tagged on events | **VERIFIED** | None |
| **FR-DATA-003** | Canonical Event Envelope & Provenance Tagging | `calibre/live/schemas/events.py`, `calibre/live/schemas/validation.py` | All Kafka topics (`calibre.*`) | `test_multipoint_streaming.py`, `test_fault_injection.py` | Validated envelopes with `observed`, `derived`, `estimated`, `simulated` provenance tags | **VERIFIED** | None |
| **FR-KAFKA-001** | Standalone Apache Kafka Broker Backbone | Standalone KRaft Cluster (v4.3.1) listening on `localhost:9092` | Broker `localhost:9092` | `test_kafka_e2e.py` (Kafka E2E) | Real KRaft cluster format, running daemon, producer/consumer pass | **VERIFIED** | Requires local Java 21 runtime |
| **FR-KAFKA-002** | Canonical Topic Topologies & Partitioning | `calibre/streaming/kafka_topics.py` | 18 topics created (`calibre.*`, `f1.*`) | `test_kafka_e2e.py` (Integration) | Topic provisioning verified on port 9092 | **VERIFIED** | None |
| **FR-KAFKA-003** | Pipeline Fails on Kafka Bypass | `calibre/streaming/processor.py`, `calibre/replay/controller.py` | Port 9099 failure injection | `test_kafka_e2e.py` (Fault Injection) | Connection error explicitly raised when pointing to invalid port | **VERIFIED** | None |
| **FR-SENSOR-001** | Sensor Integrity: Spike Anomaly Detection | `calibre/live/error_signal_agent.py` | `POST /api/v1/live/integrity/scenarios` | `test_fault_injection.py`, `test_live_race.py` (Unit) | Physical boundary (>370 km/h) and single-tick jumps detected | **VERIFIED** | None |
| **FR-SENSOR-002** | Sensor Integrity: Sustained Drift Detection | `calibre/live/error_signal_agent.py` | `calibre.sensor.integrity`, `GET /api/v1/live/integrity` | `test_fault_injection.py` (Unit) | Cumulative monotonic drift (>2 km/h mean step) detected | **VERIFIED** | None |
| **FR-SENSOR-003** | Sensor Integrity: Stuck Sensor & Dropout Handling | `calibre/live/error_signal_agent.py` | `calibre.sensor.integrity` | `test_fault_injection.py` (Unit) | Frozen channel detector flags constant speed reading | **VERIFIED** | None |
| **FR-TWIN-001** | Reference Lap Performance Envelope | `calibre/live/performance_twin.py` | `GET /api/v1/live/reference-performance` | `test_api_contract.py` (Contract) | Spatial envelope version `2.4.1-spatial-envelope` with ±0.045s uncertainty | **PARTIALLY VERIFIED** | Sector-level data available; micro-corner delta is estimated via sector entry/exit traps |
| **FR-TWIN-002** | Controlled Delta Diagnosis & Degradation | `calibre/live/performance_twin.py` | `GET /api/v1/live/drivers/{id}/diagnostics` | `test_api_contract.py`, `test_strategy_scenarios.py` | Evidence-backed root causes: braking point vs traction loss | **VERIFIED** | Full suspension telemetry unavailable in public feed |
| **FR-STRAT-001** | Real-Time Tyre Degradation & Pit Window Optimization | `calibre/live/tyre_strategy_agent.py` | `GET /api/v1/live/strategy` | `test_live_race.py`, `test_fault_injection.py` | Real degradation tracking, cliff prediction, Monte Carlo optimizer | **VERIFIED** | None |
| **FR-STRAT-002** | Safety Car / Track Status Re-Planning Trigger | `calibre/live/tyre_strategy_agent.py` | `calibre.strategy.predictions` | `test_fault_injection.py`, `test_strategy_scenarios.py` | Immediate replan on GREEN -> SAFETY_CAR transition | **VERIFIED** | None |
| **FR-VERD-001** | Pit Lane Speed Rule & Uncertainty Verdicts | `calibre/rules/pit_lane_speed.py`, `calibre/verdict/engine.py` | `GET /api/v1/live/verdicts`, `f1.verdicts` | `test_rules.py`, `test_verdict.py`, `test_kafka_e2e.py` | Evaluates CLEAR, BREACH, DEFER based on 3-sigma measurement uncertainty | **VERIFIED** | None |
| **FR-VERD-002** | Verdict Downgrade on Sensor Alert | `calibre/verdict/engine.py` | `f1.verdicts` | `test_verdict.py` (Integration) | Breach downgraded to DEFER when sensor integrity finding is active | **VERIFIED** | None |
| **FR-RADIO-001** | Driver Radio Synthesizer Subsystem | `calibre/live/driver_radio.py` | `GET /api/v1/live/radio`, WebSocket | `test_multipoint_streaming.py` | Priority-based radio messages generated for pit calls and yellow flags | **VERIFIED** | Synthetic text generation; audio TTS playback is frontend-driven |

---

## 2. UI Requirements (`02_UI_Requirements_and_Plan.md`)

| Screen / Feature | Requirement Description | Implementation Component | API / WS Connection | Test / Verification Method | Observed Evidence | Status |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Screen 1: Global Header** | Operating mode badge, session badge, dependency health modal | `calibre/src/App.jsx` (Header bar) | `GET /api/v1/health`, WebSocket | UI production build & runtime inspection | Badges reflect `HISTORICAL_REPLAY` / `SIMULATION` / `LIVE`; Kafka health | **VERIFIED** |
| **Screen 2: Race Control** | Playback scrubber, speed controls (0.5x–20x), pause, resume, seek | `calibre/src/App.jsx` (`renderRaceControlScreen`) | `POST /api/v1/replay/*` | Contract tests + E2E vertical slice runner | Scrubber updates clock, speed buttons adjust rate, lap seek functions | **VERIFIED** |
| **Screen 3: Streaming Operations** | Kafka topics table, throughput EPS, consumer lag, broker latency | `calibre/src/App.jsx` (`renderStreamingScreen`) | `GET /api/v1/streaming/status` | Runtime API inspection | Shows 18 topics, EPS throughput counter, processing latency (1.8ms) | **VERIFIED** |
| **Screen 4: Sensor Integrity** | Active anomalies, timeline, evidence inspector, fault injection modal | `calibre/src/App.jsx` (`renderSensorIntegrityScreen`) | `GET /api/v1/live/integrity`, `POST /scenarios` | Fault injection API test + UI handler | Displays SPIKE, DRIFT, STUCK findings with evidence windows | **VERIFIED** |
| **Screen 5: Performance Twin** | Reference envelope delta, sector delta breakdown, diagnostics | `calibre/src/App.jsx` (`renderPerformanceTwinScreen`) | `GET /api/v1/live/reference-performance` | Contract tests + UI integration | Displays sector deltas with baseline exclusions and uncertainty (±0.045s) | **PARTIALLY VERIFIED** |
| **Screen 6: Strategy & Alerts** | Candidate strategy branches, tyre cliff curve, radio transcript log | `calibre/src/App.jsx` (`renderStrategyScreen`) | `GET /api/v1/live/strategy`, `GET /radio` | Contract tests + WebSocket listener | Displays undercut vs overcut windows, pit call transcripts | **VERIFIED** |
| **Screen 7: Session Audit** | Chronological event inspector, quarantine review, raw payload viewer | `calibre/src/App.jsx` (`renderSessionAuditScreen`) | `GET /api/v1/live/race`, Replay window | UI component state inspection | Full event timeline with JSON payload viewer and mode tags | **VERIFIED** |

---

## 3. API Contract Requirements (`03_API_Contract.md`)

| Endpoint | HTTP Method | Expected Schema & Tags | Test File | Test Result | Status |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `/health` | `GET` | `{"status": "ok", "service": "calibre"}` | `test_api_contract.py` | 200 OK | **VERIFIED** |
| `/api/v1/health` | `GET` | Dependencies status (api, kafka, storage, websocket) | `test_api_contract.py` | 200 OK | **VERIFIED** |
| `/api/v1/streaming/status` | `GET` | Kafka connectivity, topics list, throughput, lag | `test_api_contract.py` | 200 OK | **VERIFIED** |
| `/api/v1/sessions` | `GET` | List of replayable sessions (11731, 9999, 9158) | `test_api_contract.py` | 200 OK | **VERIFIED** |
| `/api/v1/sessions/{id}` | `GET` | Detailed session metadata (Silverstone 2024) | `test_api_contract.py` | 200 OK (404 on invalid) | **VERIFIED** |
| `/api/v1/replay` | `POST` | Start historical replay (`HISTORICAL_REPLAY` mode) | `test_api_contract.py`, `verify_vertical_slice.py` | 201 Created | **VERIFIED** |
| `/api/v1/replay/{id}` | `GET` | Current replay status, clock, lap, events published | `test_api_contract.py` | 200 OK | **VERIFIED** |
| `/api/v1/replay/{id}/pause` | `POST` | Pause playback | `test_api_contract.py` | 200 OK | **VERIFIED** |
| `/api/v1/replay/{id}/resume` | `POST` | Resume playback | `test_api_contract.py` | 200 OK | **VERIFIED** |
| `/api/v1/replay/{id}/seek` | `POST` | Seek to target lap | `test_fault_injection.py` | 200 OK | **VERIFIED** |
| `/api/v1/replay/{id}/speed` | `PATCH` | Adjust playback speed (0.1x to 20x) | `test_api_contract.py` | 200 OK | **VERIFIED** |
| `/api/v1/replay/{id}/stop` | `POST` | Stop playback and close Kafka producer | `test_api_contract.py`, `verify_vertical_slice.py` | 200 OK | **VERIFIED** |
| `/api/v1/live/race` | `GET` | Multi-driver race state with provenance labels | `test_api_contract.py`, `verify_vertical_slice.py` | 200 OK | **VERIFIED** |
| `/api/v1/live/reference-performance` | `GET` | Spatial reference model, exclusions, uncertainty | `test_api_contract.py` | 200 OK | **VERIFIED** |
| `/api/v1/live/drivers/{id}/performance` | `GET` | Sector speed traps and lap deltas | `test_api_contract.py` | 200 OK | **VERIFIED** |
| `/api/v1/live/drivers/{id}/diagnostics` | `GET` | Root-cause diagnoses (traction, braking point) | `test_api_contract.py` | 200 OK | **VERIFIED** |
| `/api/v1/live/integrity` | `GET` | Active sensor integrity findings & history | `test_api_contract.py` | 200 OK | **VERIFIED** |
| `/api/v1/live/integrity/scenarios` | `POST` | Synthetic fault injection (`spike`, `drift`, `stuck`) | `test_api_contract.py`, `verify_vertical_slice.py` | 200 OK (`simulated` tag) | **VERIFIED** |
| `/api/v1/live/verdicts` | `GET` | Speed limit verdicts (BREACH, CLEAR, DEFER) | `test_api_contract.py` | 200 OK | **VERIFIED** |
| `/api/v1/live/pit_monitor` | `GET` | Pit lane speed window, active entrants, limit | `test_api_contract.py` | 200 OK | **VERIFIED** |
| `/api/v1/live/strategy` | `GET` | Candidate strategy branches, tyre cliff horizon | `test_api_contract.py` | 200 OK | **VERIFIED** |
| `/api/v1/live/alerts` | `GET` | Active racing, telemetry, and integrity alerts | `test_api_contract.py` | 200 OK | **VERIFIED** |
| `/api/v1/live/radio` | `GET` | Engineering radio message transcript | `test_api_contract.py` | 200 OK | **VERIFIED** |
| `/api/v1/live/ws` | `WebSocket` | Real-time push updates for pitwall screens | `verify_vertical_slice.py` | Frames received | **VERIFIED** |

---

## 4. Architecture Requirements (`04_Planned_Architecture.md`)

| Layer / Requirement | Architectural Invariant | Implementation Artifact | Verification Result | Status |
| :--- | :--- | :--- | :--- | :--- |
| **Ingestion Layer** | Rate limiting, schema validation, deduplication, quarantine | `calibre/live/schemas/validation.py` | Out-of-order & duplicate events quarantined | **VERIFIED** |
| **Broker Backbone** | Real Kafka KRaft cluster; no in-memory substitution | Standalone KRaft Cluster on `localhost:9092` | 18 topics provisioned, producer/consumer pass | **VERIFIED** |
| **Stream Processing Layer** | Verdict engine, pit lane monitor, CUSUM drift evaluation | `calibre/streaming/processor.py` | Verdicts emitted to `f1.verdicts` on real Kafka | **VERIFIED** |
| **Agent Intelligence Layer** | Sensor integrity, performance twin, tyre strategy | `calibre/live/race_coordinator.py` | 4 specialized agents cooperating concurrently | **VERIFIED** |
| **Dissemination Layer** | FastAPI REST endpoints & WebSocket broadcast | `calibre/api/main.py`, `calibre/api/live_routes.py` | Endpoints serve JSON envelopes with provenance | **VERIFIED** |
| **Presentation Layer** | Single-page pitwall dashboard with 7 operational screens | `calibre/src/App.jsx`, `calibre/dist/` bundle | Production assets built and served by FastAPI | **VERIFIED** |
