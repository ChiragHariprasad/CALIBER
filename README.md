# CALIBRE — Streaming Data Platform Backbone

> **Calibration-Aware, Lineage-Indexed, Bitemporal Rule Evaluation**  
> An independent evidence-integrity and assurance platform for motorsport telemetry and race events powered by the **Scoped Integrity Algebra (SIA) v2.2**.

---

## 1. System Architecture

CALIBRE ingests heterogeneous motorsport telemetry from live and historical sources, preserves source lineage, normalizes observations through specialist processor agents, and evaluates decision fitness in a pure, deterministic integrity core embedded in Apache Flink:

```text
LIVE / HISTORICAL / SYNTHETIC SOURCES
                 │
                 ▼
          SOURCE INGESTION (F1 SignalR Free Topics / FastF1 Historical Stream)
                 │
                 ▼
            KAFKA RAW TOPICS (calibre.raw.<source>.<feed>)
                 │
                 ▼
        AGENT / PROCESSOR LAYER (5 Specialist Domain Processors)
        (vehicle, incident, environment, timing, system integrity)
                 │
                 ▼
         KAFKA EVIDENCE TOPICS (calibre.evidence.<domain>)
         + GUARD & CONFIG (calibre.guard.*, calibre.config.contracts)
                 │
                 ▼
     ┌────────────────────────────────────────────────────────┐
     │           SIA PURE EVALUATOR ENGINE (sia_core)         │
     │                                                        │
     │  1. Evidence Admission & RFC 8785 Canonical Identity   │
     │  2. Fact & Typed-Time Resolution (Legal Conversions)   │
     │  3. Origin Management (3-Tier & Shared Upstreams)      │
     │  4. Pairwise Agreement Clique Search (M_max = 10)       │
     │  5. Fault Diagnosis (Theorem G4-I: ⋂ D_min)             │
     │  6. Decision Evidence Contract Evaluation (DEC AST)    │
     │  7. Abstract Verdict Envelope (C_max = 32 & P4/P5)     │
     │  8. Pointwise Validity Timeline Composition σ(t)       │
     │  9. Bitemporal Verdict Builder & Hash Linkage          │
     │ 10. Dual-Domain Leases (LIVE_UTC vs REPLAY_VIRTUAL)    │
     └───────────────────────────┬────────────────────────────┘
                                 │
                                 ▼
         APACHE FLINK INTEGRITY OPERATOR (flink/job.py)
                 │ (Keyed by session_instance_id:driver_id)
          +──────┴───────────────+
          │                      │
          ▼                      ▼
  DOWNSTREAM SINKS        SIDE-OUTPUT QUARANTINE
  ├─ calibre.out.database  ├─ calibre.dlq (malformed)
  ├─ calibre.out.dashboard ├─ calibre.side.quarantine (late/finality)
  ├─ calibre.out.analytics └─ calibre.side.overflow (search budget capped)
  └─ calibre.core.verdict_history
          │                      │                      │
          ▼                      ▼                      ▼
  PERSISTENCE SERVICE    DASHBOARD GATEWAY      ANALYTICS SERVICE
  (SQLite Bitemporal     (FastAPI REST/SSE      (Throughput, Metrics
   Audit Ledger)          Operations UI)         & Operational Alerts)
```

---

## 2. Kafka Topic Catalogue

| Topic Name | Key Semantic | Producer | Consumer(s) | Purpose |
|:---|:---|:---|:---|:---|
| `calibre.raw.f1_signalr.<feed>` | `event_id` | Ingestion Service | Specialist Processors | Unauthenticated public live timing topics |
| `calibre.raw.fastf1.<feed>` | `event_id` | Ingestion Service | Specialist Processors | Historical FastF1 records streamed by session time |
| `calibre.evidence.<domain>` | `event_id` | Agent Processors | Apache Flink / SIA Core | Normalized observations, findings, and scope |
| `calibre.guard.liveness` | `origin_id` | Supervisor Agent | Apache Flink / SIA Core | Durable liveness state facts (`STALLED` / `RESUMED`) |
| `calibre.guard.clock` | `clock_domain` | Run Clock Generator | Apache Flink / SIA Core | Recorded `RunClockTick` facts ("Time is Data") |
| `calibre.config.contracts` | `contract_id` | Admin / Config CLI | Apache Flink / SIA Core | Versioned Decision Evidence Contracts (DECs) |
| `calibre.out.database` | `correlation_id` | Apache Flink Core | Persistence Sink | Audit-grade stream for SQLite bitemporal store |
| `calibre.out.dashboard` | `correlation_id` | Apache Flink Core | Dashboard Gateway | Real-time event stream with consumer leases for UI |
| `calibre.out.analytics` | `correlation_id` | Apache Flink Core | Analytics Service | Integrity-gated evidence stream for metrics/rules |
| `calibre.core.verdict_history` | `decision_id` | Apache Flink Core | Persistence Sink | Immutable revision chain ledger ($H_1 \to H_2 \to \dots$) |
| `calibre.dlq` | `correlation_id` | Any Component / Flink | Operations Monitor | Malformed, unparseable, or schema-invalid events |
| `calibre.side.quarantine` | `decision_id` | Apache Flink Core | Integrity Audit Monitor | Late evidence arriving after canonical finality cutoffs |
| `calibre.side.overflow` | `decision_id` | Apache Flink Core | Performance Monitor | Search budget overflows ($M_{\max}=10, C_{\max}=32$) |

---

## 3. Quick Start & Operational Commands

All platform operations are orchestrated from the repository root via `run_pipeline.py`:

### Prerequisites
- Python 3.10+ (tested on Python 3.13)
- Docker & Docker Compose
- Virtualenv with requirements installed:
  ```bash
  pip install -r requirements.txt
  ```

### 1. Infrastructure Management
```bash
# Start Docker infrastructure (Zookeeper, Kafka, Flink) and initialize topics:
python run_pipeline.py start

# Check running containers, Kafka connectivity, Flink cluster, and database health:
python run_pipeline.py status

# Cleanly stop all background streaming services and Docker containers:
python run_pipeline.py stop-all
```

### 2. Comprehensive Test Suites
```bash
# Run the master regression suite (741 tests in ~21s):
pytest -q

# Run bit-for-bit Golden Fixture parity check (20/20 bit-for-bit SHA-256 match):
python tests/test_eval_parity.py

# Run the 30 mandatory property and adversarial tests:
pytest tests/test_30_adversarial_scenarios.py -v

# Run end-to-end integration pipeline verification:
python run_pipeline.py test

# Run dedicated Apache Flink streaming verification:
python run_pipeline.py test-flink
```

### 3. Streaming Operations & Interactive Ingestion
```bash
# Start all streaming services and subscribe to unauthenticated F1 SignalR topics:
python run_pipeline.py run --source signalr

# Stream Monaco historical timing/telemetry from FastF1 through the same Kafka/Flink path.
# One session second per wall-clock second by default; set --max-session-seconds for a bounded run.
python run_pipeline.py stream-fastf1 --year 2026 --event "Monaco Grand Prix" --session R --max-session-seconds 30

# Accelerate historical streaming for a short pipeline smoke run:
python run_pipeline.py stream-fastf1 --year 2026 --event "Monaco Grand Prix" --session R --replay-speed 30 --max-events 500

# Query persisted bitemporal verdicts in SQLite:
python run_pipeline.py query-db --limit 10
python run_pipeline.py query-db --corr trace-001
```

---

## 4. Platform Service Inventory

### 1. `sia_core` (Scoped Integrity Algebra Core)
- **Responsibility:** Pure zero-dependency decision integrity engine implementing:
  - **4-State Total Order Lattice:** $\text{DEGRADED} < \text{UNKNOWN} < \text{SUSPECT} < \text{HEALTHY}$.
  - **RFC 8785 Canonical Identity:** Deterministic JSON Canonicalization Scheme (JCS) and SHA-256 hashing.
  - **Decision Evidence Contracts (DECs):** Versioned YAML contracts with compile-time validator rules V-01..V-10.
  - **Pointwise Validity Timelines:** Non-increasing step functions $\sigma(t) = \sigma_1(t) \odot \dots \odot \sigma_n(t)$.
  - **Theorem G4-I Fault Isolation:** Minimal hitting-set diagnosis $\bigcap \mathcal{D}_{\min}$ avoiding improper blaming on star graphs.
  - **Complexity Bounding:** $M_{\max} = 10$ Bron-Kerbosch clique search and $C_{\max} = 32$ abstract completion cap.
  - **Dual-Domain Consumer Leases:** `LIVE_UTC` (wall clock) vs `REPLAY_VIRTUAL` (run clock facts).
- **Location:** `sia_core/`

### 2. `ingestion` (Source Ingestion Service)
- **Responsibility:** Ingests public unauthenticated F1 SignalR topics or time-ordered FastF1 historical records, wraps them into `RawSourceEnvelope`, and publishes to `calibre.raw.*`.
- **Entry Points:** `ingestion/service.py`, `ingestion/fastf1_source.py`

### 3. `processors` (Specialist Processor Layer)
- **Responsibility:** 5 specialist processors subscribing independently to raw topics, normalizing observations into `EvidenceEvent`s, and publishing to `calibre.evidence.<domain>`:
  - `VehicleTwinSensorAgent` (`domain: vehicle`)
  - `IncidentTrackAgent` (`domain: incident`)
  - `EnvironmentalContextAgent` (`domain: environment`)
  - `RaceTimingSessionAgent` (`domain: timing`)
  - `VehicleElectronicsSystemIntegrityAgent` (`domain: system`)
- **Entry Point:** `processors/service.py`

### 4. `flink` (Apache Flink Integrity Core)
- **Responsibility:** Keyed streaming operator (`session_instance_id:driver_id`), evaluates evidence against versioned DECs, emits 9-field `IntegrityAssessment` envelopes, routes to triple sinks, and quarantines post-finality or malformed events.
- **Entry Point:** `flink/job.py`
- **Flink Cluster:** Apache Flink 1.19 (JobManager at `http://localhost:8081`).

### 5. `persistence` (Bitemporal Relational Audit Sink)
- **Responsibility:** Stores verdicts and raw envelopes idempotently into SQLite (`data/calibre_persistence.db`) with bitemporal coordinates (valid time $\times$ knowledge time), supporting As-Of-K historical queries and revision chain traversals ($H_1 \to H_2$).
- **Entry Point:** `persistence/service.py`

### 6. `dashboard` (Operational Gateway API)
- **Responsibility:** Serves REST and SSE endpoints (`/health`, `/metrics`, `/events/recent`, `/trace/{correlation_id}`) with consumer lease validation.
- **Entry Point:** `dashboard/service.py` (FastAPI app)

### 7. `analytics` (Stream Metrics & Alerts)
- **Responsibility:** Partitions analytics into 3 non-overlapping classes: Pipeline Telemetry (ungated), Race Metrics (labelled), and Gated Rules Evaluation.
- **Entry Point:** `analytics/service.py`

---

## 5. Event Contract & Envelope Specifications

### 1. RawSourceEnvelope (`calibre.raw_source_envelope` v1.0)
Preserves authentic source payload, origin provider, received timestamp, and cryptographic hash:
```json
{
  "schema_name": "calibre.raw_source_envelope",
  "schema_version": "1.0",
  "event_id": "fe92064856930911b4b502b1",
  "correlation_id": "trace-e2e-assert-001",
  "event_type": "raw_source_event",
  "provider": "FASTF1",
  "feed": "weatherdata",
  "source_mode": "HISTORICAL_REPLAY",
  "run_id": "fastf1-2026-monaco-race",
  "received_at_utc": "2026-11-01T01:07:52.552Z",
  "source_time": {
    "value": 124.5,
    "semantics": "SESSION_ELAPSED_SECONDS",
    "unit": "seconds"
  },
  "payload_format": "JSON",
  "payload": { "Time": 124.5, "AirTemp": 24.5, "TrackTemp": 32.8, "Humidity": 55.0, "Pressure": 1013.2, "Rainfall": false },
  "payload_sha256": "4b68427f7f02...",
  "session": { "season": 2026, "meeting": "Bahrain Grand Prix", "session_name": "Race" }
}
```

### 2. EvidenceEvent (`calibre.evidence_event` v1.0)
Emitted by specialist processors containing normalized observations and domain findings:
```json
{
  "schema_name": "calibre.evidence_event",
  "schema_version": "1.0",
  "event_id": "8c374fa0e2194811a011234b",
  "correlation_id": "trace-e2e-assert-001",
  "event_type": "weather_sample",
  "domain": "environment",
  "producer": { "processor_id": "environmental_context_agent", "version": "0.1.0", "build": "v1.0" },
  "produced_at_utc": "2026-11-01T01:07:52.680Z",
  "source_refs": ["fe92064856930911b4b502b1"],
  "time": { "source_value": 124.5, "source_semantics": "SESSION_ELAPSED_SECONDS" },
  "value_origin": "SOURCE_REPORTED",
  "evidence": { "air_temp_c": 24.5, "track_temp_c": 32.8, "humidity_pct": 55.0, "rainfall": false },
  "findings": [],
  "limitations": []
}
```

### 3. IntegrityAssessment (`calibre.integrity_assessment` v1.0)
The central 9-field verdict envelope produced by the Scoped Integrity Algebra core with 4 decoupled orthogonal dimensions:
```json
{
  "schema_name": "calibre.integrity_assessment",
  "schema_version": "1.0",
  "verdict_hash": "00dbb61092b5e28a...",
  "decision_id": "session-bahrain-2026|VER|lap_44",
  "contract_id": "lap_time_consistency",
  "contract_version": "1.0",
  "evaluation_instant": "2026-11-01T01:08:15.000Z",
  "integrity_state": "HEALTHY",
  "resolution_maturity": "TUPLE_RESOLVED",
  "finality_status": "PROVISIONAL",
  "revision_number": 1,
  "previous_verdict_hash": null,
  "validity_timeline": [
    {"from": "2026-11-01T01:08:15.000Z", "until": "2026-11-01T01:08:45.000Z", "state": "HEALTHY"},
    {"from": "2026-11-01T01:08:45.000Z", "until": null, "state": "DEGRADED"}
  ],
  "lease_until": "2026-11-01T01:08:30.000Z",
  "clock_domain": "LIVE_UTC",
  "corroboration_profile_by_role": {
    "lap_duration": { "kappa_src": 0, "kappa_proc": 1, "status": "SATISFIED" }
  },
  "primary_binding_witness": "atom_lap_duration_44",
  "witness_set": ["atom_lap_duration_44", "ref_session_t0"],
  "limitations": ["UNMODELLED_COMMON_CAUSE"]
}
```

---

## 6. Verification, Testing & Empirical Benchmarks

CALIBRE enforces zero-tolerance formal verification across the entire stack:

### Test Suite Summary
- **Master Regression Suite:** **741 passed** across 11 test modules in **21.07s (100% pass rate)**.
- **Golden Fixtures Bit-for-Bit Parity:** **20 / 20 passed** (`FX-01` through `FX-20`) with identical RFC 8785 SHA-256 hashes across all 9 fields.
- **30 Mandatory Adversarial Counterexamples:** **30 / 30 passed** covering `TC-P01`..`TC-P10` (properties), `TC-A01`..`TC-A12` (adversarial), and `TC-S01`..`TC-S08` (streaming).
- **Cross-Language RFC 8785 Vectors:** **123 / 123 passed** matching the official JSON Canonicalization Scheme standard.

### Measured Performance Profile (Spike S-1 & Benchmarks)
- **Single-Core Evaluator Throughput:** **5,859.2 decisions evaluated per second** ($+192.9\%$ over the 2,000 ops/s requirement).
- **Latency Profile (Measured over 2,000 iterations):**
  - **P50:** **155.75 $\mu s$** ($6.4\times$ faster than the 1,000 $\mu s$ budget).
  - **P95:** **253.50 $\mu s$** ($9.8\times$ faster than the 2,500 $\mu s$ budget).
  - **P99:** **377.60 $\mu s$** ($13.2\times$ faster than the 5,000 $\mu s$ budget).
- **State Footprint:** **3,195.0 bytes (~3.19 KB) per active decision instance** (~319.5 MB for 100,000 concurrent keys).
- **Hypothesis H1 Verification:** **Confirmed**. SIA completely eliminated false-HEALTHY verdicts (**0.0%**) compared to a naive per-feed freshness baseline (**33.4% false-HEALTHY**).

---

## 7. Documentation Roadmap

Complete architectural, normative, and operational specifications are available in the [`docs/`](file:///e:/CALIBER/docs/README.md) directory:

- [**Documentation Master Index**](file:///e:/CALIBER/docs/README.md): Comprehensive navigation guide for all platform documents.
- [**Final Engineering Handover Report**](file:///e:/CALIBER/docs/implementation/SIA_FINAL_HANDOVER_REPORT.md): Definitive delivery report covering Phase 0 through Phase 7.
- [**SIA Operations Runbook**](file:///e:/CALIBER/docs/core/SIA_OPERATIONS_RUNBOOK.md): Operations manual, topic retentions, dynamic contract updates, triage & recovery procedures.
- [**Mathematical Specification v1.0**](file:///e:/CALIBER/docs/core/SIA_MATHEMATICAL_SPECIFICATION_v1.0.md): The 4-state lattice, Theorem G4-I fault isolation, and clique complexity bounds.
- [**DEC Schema Specification v1.0**](file:///e:/CALIBER/docs/core/SIA_DEC_SCHEMA_v1.md): Declarative contract schema, AST grammar, and static validation rules V-01..V-10.
- [**SIA v2.2.1 Errata Catalog**](file:///e:/CALIBER/docs/implementation/SIA_v2.2.1_ERRATA.md): Detailed audit and resolution of 20 defects (D-01..D-20) and 9 conflicts.
- [**Performance Benchmark Report**](file:///e:/CALIBER/docs/implementation/SIA_BENCHMARK_REPORT.md): Empirical benchmarks and Hypothesis H1 validation.
- [**Master Test Catalogue**](file:///e:/CALIBER/docs/implementation/SIA_TEST_CATALOGUE.md): Complete index of all 741 tests, 30 adversarial scenarios, and 20 golden fixtures.

---

## 8. License & Governance

CALIBRE is developed as an independent assurance and decision-support layer for motorsport telemetry.  
**Governing Standard:** CAL-DOC-007 v2.2 (Amending CAL-DOC-001 through CAL-DOC-006).  
*CALIBRE does not issue official sporting penalties or replace FIA/FOM race-control systems; all outputs are explainable decision support for human review.*
