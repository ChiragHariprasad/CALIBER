# CALIBRE — Streaming Data Platform Backbone

> **Calibration-Aware, Lineage-Indexed, Bitemporal Rule Evaluation**  
> An independent evidence-integrity and assurance platform for motorsport telemetry and race events.

---

## 1. System Architecture

```text
                    DATA SOURCES
                          |
             +------------+------------+
             |            |            |
          Pitwall     TracingInsights  Mock/Replay
          Live Feed    Historical Data  Test Source
             |            |            |
             +------------+------------+
                          |
                   SOURCE INGESTION
                          |
                          v
                  KAFKA RAW TOPICS
               (calibre.raw.<source>.<feed>)
                          |
                          v
              AGENT / PROCESSING LAYER
               5 Independent Processors
             (vehicle, incident, environment,
               timing, system integrity)
                          |
                          v
                 KAFKA EVIDENCE TOPICS
               (calibre.evidence.<domain>)
                          |
                          v
                 APACHE FLINK CORE
          (Validation, Lineage Tracking,
          Routing, DLQ, Status Enrichment)
                          |
             +------------+------------+------------------+
             |                         |                  |
             v                         v                  v
     calibre.out.database     calibre.out.dashboard   calibre.out.analytics
             |                         |                  |
             v                         v                  v
     PERSISTENCE SERVICE       DASHBOARD GATEWAY      ANALYTICS SERVICE
     (SQLite / Relational      (FastAPI REST/SSE      (Throughput & Event
      Audit History Store)      Live Event Stream)     Metrics / Alerts)
             |                         |                  |
             +------------+------------+------------------+
                          |
                    SYSTEM HEALTH
                     AND OBSERVABILITY
```

---

## 2. Kafka Topic Catalogue

| Topic Name | Producer | Consumer(s) | Purpose |
|---|---|---|---|
| `calibre.raw.pitwall.<feed>` | Ingestion Service | Specialist Processors | Raw messages from public Pitwall SignalR topics |
| `calibre.raw.tracinginsights.<feed>` | Ingestion Service | Specialist Processors | Historical records from TracingInsights |
| `calibre.raw.mock.<feed>` | Mock Producer | Specialist Processors | Deterministic synthetic events for testing |
| `calibre.evidence.<domain>` | Agent Processors | Apache Flink Core | Normalised observations, scope, and findings |
| `calibre.out.database` | Apache Flink | Persistence Sink | Audit-grade stream for relational persistence |
| `calibre.out.dashboard` | Apache Flink | Dashboard Gateway | Real-time event stream for operations UI |
| `calibre.out.analytics` | Apache Flink | Analytics Service | Operational evidence stream for metrics |
| `calibre.analytics.metrics` | Analytics Service | Operations / UI | Derived pipeline throughput and event rates |
| `calibre.alerts` | Analytics Service | Operations / Review | Operational alerts for high-severity findings |
| `calibre.dlq` | Any Component / Flink | Operations Tooling | Malformed, unparseable, or failed events |

---

## 3. Quick Start & Operational Commands

All platform operations can be executed from the repository root via `run_pipeline.py`.

### Prerequisites
- Python 3.10+
- Docker and Docker Compose
- Virtualenv with requirements installed (`pip install -r requirements.txt`)

### 1. Start Infrastructure
Starts Zookeeper, Kafka broker, Flink JobManager, Flink TaskManager, and initializes all Kafka topics:
```bash
python run_pipeline.py start
```

### 2. Check System Health
Inspects running containers, Kafka connectivity, topic list, Flink cluster status, and database:
```bash
python run_pipeline.py status
```

### 3. Run End-to-End Integration Tests
Executes the full automated pipeline verification (verifying all 11 assertions and failure-path DLQ routing):
```bash
python run_pipeline.py test
```

### 4. Run Streaming Services Interactively
Starts all 5 streaming services (Processors, Flink Core, Persistence, Dashboard, Analytics) listening to Kafka:
```bash
python run_pipeline.py run
```

### 5. Inject a Deterministic Mock Event
Injects a single synthetic event to trace through the pipeline:
```bash
python run_pipeline.py produce-mock --type weather --corr my-trace-001
python run_pipeline.py produce-mock --type telemetry --corr my-trace-002
python run_pipeline.py produce-mock --type rcm --corr my-trace-003
python run_pipeline.py produce-mock --type timing --corr my-trace-004
```

### 6. Replay a Historical Log File
Replays recorded JSONL fixtures into the live Kafka pipeline:
```bash
python run_pipeline.py replay-log --limit 5
```

### 7. Inspect Persisted Events
Queries events stored by the persistence sink:
```bash
python run_pipeline.py query-db --limit 10
python run_pipeline.py query-db --corr my-trace-001
```

### 8. List Kafka Topics
```bash
python run_pipeline.py topics
```

### 9. Stop Infrastructure
```bash
python run_pipeline.py stop
```

---

## 4. Service Inventory

### 1. `ingestion` (Source Ingestion Service)
- **Responsibility:** Ingests live SignalR (Pitwall) or historical (TracingInsights/fixtures) records, wraps them into `RawSourceEnvelope`, and publishes to `calibre.raw.*`.
- **Entry Points:** `ingestion/service.py`, `ingestion/mock_source.py`
- **Health:** Reports dependency on Kafka, events in/out, error counts.

### 2. `processors` (Evidence Processing Layer)
- **Responsibility:** Multi-processor worker service subscribing independently (with independent consumer groups) to raw topics, passing payloads to the 5 specialist assurance agents, and emitting `EvidenceEvent`s to `calibre.evidence.<domain>`.
- **Agents Integrated:**
  - `VehicleTwinSensorAgent` (`domain: vehicle`)
  - `IncidentTrackAgent` (`domain: incident`)
  - `EnvironmentalContextAgent` (`domain: environment`)
  - `RaceTimingSessionAgent` (`domain: timing`)
  - `VehicleElectronicsSystemIntegrityAgent` (`domain: system`)
- **Entry Point:** `processors/service.py`

### 3. `flink` (Apache Flink Integrity Core)
- **Responsibility:** Consumes `calibre.evidence.*`, validates envelope structure, preserves correlation ID and lineage, enriches with metadata, routes to 3 downstream outputs (`out.database`, `out.dashboard`, `out.analytics`), and sends malformed events to `calibre.dlq`.
- **Integrity Status:** Explicitly set to `"NOT_EVALUATED"` during scaffold phase (maintaining semantic separation between pipeline operational health and sporting integrity).
- **Entry Point:** `flink/job.py`
- **Flink Cluster:** Apache Flink 1.19 (JobManager at `http://localhost:8081`).

### 4. `persistence` (Relational Audit Sink)
- **Responsibility:** Consumes `calibre.out.database`, stores records idempotently into SQLite (`data/calibre_persistence.db`), indexes by `correlation_id` and `event_id`.
- **Entry Point:** `persistence/service.py`

### 5. `dashboard` (Operational Gateway API)
- **Responsibility:** Consumes `calibre.out.dashboard`, provides in-memory trace indexing, serves REST endpoints (`/health`, `/metrics`, `/events/recent`, `/trace/{correlation_id}`).
- **Entry Point:** `dashboard/service.py` (FastAPI app)

### 6. `analytics` (Stream Metrics Service)
- **Responsibility:** Consumes `calibre.out.analytics`, aggregates totals per provider/processor/domain, computes throughput (events/sec), emits operational metrics to `calibre.analytics.metrics`, and emits alert stubs to `calibre.alerts`.
- **Entry Point:** `analytics/service.py`

---

## 5. Event Contract & Envelope Specification

### RawSourceEnvelope (`calibre.raw_source_envelope` v1.0)
```json
{
  "schema_name": "calibre.raw_source_envelope",
  "schema_version": "1.0",
  "event_id": "fe92064856930911b4b502b1",
  "correlation_id": "trace-e2e-assert-001",
  "event_type": "raw_source_event",
  "provider": "MOCK",
  "feed": "weather",
  "source_mode": "LIVE",
  "run_id": "mock-run-01",
  "received_at_utc": "2026-11-01T01:07:52.552Z",
  "source_time": {
    "value": 124.5,
    "semantics": "SESSION_ELAPSED_SECONDS",
    "unit": "seconds"
  },
  "payload_format": "JSON",
  "payload": { "wT": 124.5, "wAT": 24.5, "wTT": 32.8, "wH": 55.0, "wP": 1013.2, "wR": false },
  "payload_sha256": "4b68427f7f02...",
  "session": { "season": 2026, "meeting": "Bahrain Grand Prix", "session_name": "Race" }
}
```

### EvidenceEvent (`calibre.evidence_event` v1.0)
```json
{
  "schema_name": "calibre.evidence_event",
  "schema_version": "1.0",
  "event_id": "8c374fa0e2194811a011234b",
  "correlation_id": "trace-e2e-assert-001",
  "event_type": "weather_sample",
  "domain": "environment",
  "producer": { "processor_id": "environmental_context_agent", "version": "0.1.0", "build": "scaffold-v1" },
  "produced_at_utc": "2026-11-01T01:07:52.680Z",
  "source_refs": ["fe92064856930911b4b502b1"],
  "time": { "source_value": 124.5, "source_semantics": "SESSION_ELAPSED_SECONDS" },
  "value_origin": "SOURCE_REPORTED",
  "evidence": { "air_temp_c": 24.5, "track_temp_c": 32.8, "humidity_pct": 55.0, "rainfall": false },
  "findings": [],
  "limitations": []
}
```

### FlinkOutputEnvelope (`calibre.flink_output_envelope` v1.0)
```json
{
  "schema_name": "calibre.flink_output_envelope",
  "schema_version": "1.0",
  "envelope_id": "d8204b1234900aef12345678",
  "correlation_id": "trace-e2e-assert-001",
  "processing_stage": "FLINK_INTEGRITY_VALIDATED",
  "integrity_status": "NOT_EVALUATED",
  "processed_at_utc": "2026-11-01T01:07:52.710Z",
  "event": { ... },
  "routed_to": ["PERSISTENCE", "DASHBOARD", "ANALYTICS"],
  "flink_job_id": "calibre-flink-stream-job-v1",
  "validation_status": "VALID"
}
```

---

## 6. Verification & Test Evidence

The pipeline scaffold is covered by 23 passing tests:
- **`tests/test_e2e_pipeline.py`** (3 integration tests):
  - `test_01_end_to_end_traceability`: Verifies all 11 pipeline assertions with end-to-end correlation ID tracking.
  - `test_02_failure_path_dlq_routing`: Verifies that malformed envelopes are routed to `calibre.dlq` without pipeline termination.
  - `test_03_historical_fixture_replay`: Verifies that real recorded JSONL logs replay into Kafka and reach persistence.
- **`agents/calibre_agent_tests/`** (20 unit tests):
  - 19 processor/agent tests verifying normalization, range bounds, and timestamp semantics.
  - 1 pipeline integration test verifying multi-file batch execution.

