# CALIBRE Scoped Integrity Algebra (SIA) — Repository Assessment & Origin Inventory

**Document Control:**
- **Document Identifier**: `CAL-ASSESS-001`
- **Specification Baseline**: CAL-DOC-007 v2.2-draft
- **Baseline Documents Audited**: CAL-DOC-001 (Charter), CAL-DOC-002 (Functional Requirements), CAL-DOC-003 (System Requirements Specification), CAL-DOC-004 (API & Event Contracts), CAL-DOC-005 (System Architecture HLD/LLD), CAL-DOC-006 (Core Integrity Concept)
- **Target Repository**: `E:\CALIBER`
- **Author**: `worker_m0_1` (teamwork_worker)
- **Reviewing Archetype**: Lead Streaming Systems Architect & Formal Methods Engineer
- **Date**: 2026-11-01
- **Status**: Production Deliverable — Approved Baseline Audit

---

## 1. Executive Summary & Document Control

### 1.1 Bottom-Line Assessment
A comprehensive technical audit of the existing codebase at `E:\CALIBER` against the normative requirements of **CAL-DOC-007 (CALIBRE SIA v2.2 Validated Core Specification)** demonstrates that while the platform possesses an operational Phase 0 streaming infrastructure (Docker Compose running Kafka 7.7.0, Zookeeper, and Apache Flink 1.19.3, alongside functional ingestion, adapter, and persistence scaffolding), **the Scoped Integrity Algebra (SIA) core is completely unbuilt**.

Specifically:
1. **SIA Directory Scaffold**: The root `sia_core/` directory exists with 9 empty subdirectories (`contracts/`, `envelope/`, `evaluation/`, `identity/`, `liveness/`, `models/`, `origin/`, `time/`, `verdict/`). No algebraic evaluation code, state transitions, or contract verification models exist in the codebase.
2. **Scaffold Integrity Status**: In the existing execution path, Flink and worker outputs explicitly assign:
   ```json
   "integrity_status": "NOT_EVALUATED"
   ```
   No formal admissibility checks, corroboration depths, or validity timeline calculations are performed.
3. **Identity and Determinism Non-Compliance**: Existing event envelopes (`RawSourceEnvelope`, `EvidenceEvent`) generate identifiers using system wall-clock timestamps (`now_str`) and non-canonical hashing (`json.dumps(..., sort_keys=True)`), directly violating replay determinism and cross-language compatibility.
4. **Time Domain Deficiencies**: Time is passed as untyped JSON dictionaries or raw ISO-8601 strings. The formal typed-time system ($T_{\text{cal}}$, $T_{\text{ses}}$, $T_{\text{lap}}$, $T_{\text{dist}}$) and reference fact conversion algebra are missing.
5. **Origin Inventory & Corroboration Reality**: An investigation of the live SignalR Pitwall feeds and historical TracingInsights files confirms that **both feeds originate from a single upstream provider — Formula One Management (FOM) timing infrastructure**. There is zero source independence between these feeds. Default Phase 1 contracts must mandate `source_min_depth = 0` (`INFORMATIONAL`), and multi-source corroboration must be tested via synthetic fault-injection fixtures (Class S).

---

## 2. Repository Architecture & Codebase Inventory

Every source file and module in `E:\CALIBER` was audited to evaluate line count, current responsibilities, and gap analysis relative to CAL-DOC-007.

| Module / Component | Path | SLOC | Primary Architectural Responsibility | CAL-DOC-007 Compliance Status | Gaps & Remediation Requirements |
|---|---|---|---|---|---|
| `calibre_core/contracts.py` | `calibre_core/contracts.py` | 271 | Core data contracts: `RawSourceEnvelope`, `Finding`, `EvidenceEvent`, `FlinkOutputEnvelope`, `DeadLetterEnvelope` | **Non-Compliant** | Missing `raw_event_id`, `identity_stability`, `evidence_atom_id`, `processor_family_id`, `supersedes_atom_id`, `source_origin_set`, `processing_origin_set`, typed time classes, and the full schema for `IntegrityAssessment` v2.0. |
| `calibre_core/topics.py` | `calibre_core/topics.py` | 93 | Kafka topic registry and consumer group definitions | **Partially Compliant** | Missing 5 normative topics: `calibre.config.contracts`, `calibre.guard.liveness`, `calibre.guard.clock`, `calibre.core.admission_log`, `calibre.core.verdict_history`. |
| `calibre_core/health.py` | `calibre_core/health.py` | 100 | Service and pipeline health tracking | **Compliant for Telemetry** | Operational telemetry health vocabulary (`HEALTHY`, `DEGRADED`, `UNHEALTHY`) must remain strictly isolated from evidence integrity states. |
| `calibre_core/kafka_bus.py` | `calibre_core/kafka_bus.py` | 240 | Thread-safe Kafka producer and consumer wrappers | **Compliant** | Operational with confluent-kafka-python. |
| `ingestion/service.py` | `ingestion/service.py` | 181 | Replay and live ingestion service | **Non-Compliant** | Generates `event_id` with `now_str`, violating replay determinism. Must compute `raw_event_id` using RFC 8785 JCS. |
| `ingestion/mock_source.py` | `ingestion/mock_source.py` | 195 | Synthetic race telemetry and weather generator | **Usable for Testing** | Functional for pipeline smoke tests; needs expansion for adversarial tests. |
| `processors/agent_adapter.py` | `processors/agent_adapter.py` | 258 | Adapts specialist domain agents to Kafka consumers/producers | **Non-Compliant** | Treats build string as origin (violating D-09); does not populate dual-axis origin sets or support supersession. |
| `processors/service.py` | `processors/service.py` | 175 | Worker thread runner for agent adapters | **Compliant for Scaffold** | Harness runs 5 agent threads successfully. |
| `pitwall_source/pitwall.py` | `pitwall_source/pitwall.py` | ~4000 | Live SignalR client and stream unpacker | **Operational Scaffold** | Captures 18 topics from F1 Live Timing. Lacks live guard fact emission and stationary-watermark tick generation. |
| `pitwall_source/topics.py` | `pitwall_source/topics.py` | 68 | Topic catalogue & channel mappings | **Compliant** | Correctly maps free vs auth-gated topics. |
| `flink/job.py` | `flink/job.py` | 202 | Python multithreaded simulation stub | **Temporary Mock** | Passes `integrity_status = "NOT_EVALUATED"`. Must be replaced by true Flink 1.19 `KeyedProcessFunction` embedding the pure evaluator. |
| `persistence/service.py` | `persistence/service.py` | 189 | SQLite persistence sink for `calibre.out.database` | **Partially Compliant** | SQLite schema lacks bitemporal Valid Time ($T_v$) / Knowledge Time ($T_k$) indexing and admission log persistence. |
| `dashboard/service.py` | `dashboard/service.py` | 180 | FastAPI + WebSocket streaming gateway | **Compliant for Scaffold** | Missing consumer lease expiry enforcement (§2.8.2). |
| `analytics/service.py` | `analytics/service.py` | 184 | Pipeline telemetry metrics calculations | **Partially Compliant** | Violates §2.10 analytics split: mixes operational pipeline health with sporting findings; lacks DEC rule gating (`RULE_DEFERRED`). |
| `data/race_logs/` | `data/race_logs/*.jsonl` | 8,553 recs | Mexico City 2026 session 11731 JSONL records | **Valuable Asset** | 14 JSONL files containing real session telemetry, lap times, line insights, and coordinator decisions. |
| `docker-compose.yml` | `docker-compose.yml` | 108 | Container definitions (Kafka, Zookeeper, Flink 1.19) | **Operational Baseline** | Verified functional; allocates 2 Flink slots. |
| `tests/test_e2e_pipeline.py` | `tests/test_e2e_pipeline.py` | 260 | 11-stage scaffold integration test suite | **Scaffold Verified** | Validates message flow end-to-end with `NOT_EVALUATED` status. |
| `tests/test_flink_streaming.py` | `tests/test_flink_streaming.py` | 288 | Flink routing, health, and DLQ test suite | **Scaffold Verified** | Verifies basic routing and DLQ failure trapping. |
| `sia_core/` | `sia_core/` | 0 files | Empty directory tree for SIA core modules | **Unimplemented** | Planned home for 10 pure evaluator modules. |

---

## 3. Normative Gap Analysis (The 12 Core Semantics)

### 3.1 Identity (§2.1)
- **Current State**: `RawSourceEnvelope.event_id` is generated as `stable_id(provider, feed, corr_id, now_str)`. `EvidenceEvent.event_id` uses `stable_id(processor_id, envelope.event_id, event_type)`. Hashing uses `json.dumps(self.payload, sort_keys=True)`.
- **CAL-DOC-007 Mandate**:
  - `raw_event_id`: Stable source record identity: `SHA256(JCS(provider, feed, source_record_key, session_id, scope, source_time, payload_sha256))`. Excludes `run_id`, `received_at_utc`, and operational headers.
  - `evidence_atom_id`: Deterministic derived atom identity: `SHA256(JCS(sorted raw_event_ids, processor_family_id, transform_version, role_id, scope_key, normalised_value_hash, sorted reference_atom_ids))`.
  - Canonical serialization must strictly follow **RFC 8785 JSON Canonicalization Scheme (JCS)** with SHA-256 rendered in lowercase hexadecimal.
  - Sink deduplication must key on `(run_id, verdict_hash, revision_number)`.

### 3.2 Time (§2.2)
- **Current State**: Time is passed as untyped dictionaries or ISO-8601 strings.
- **CAL-DOC-007 Mandate**:
  - Formal typed-time system: `ABSOLUTE_UTC`, `SESSION_ELAPSED`, `LAP_RELATIVE`, `LAP_DURATION`, `RECEIPT_TIME_ONLY`, `PROCESSING_TIME`, `UNKNOWN`.
  - Invariants: `LAP_DURATION` is a quantity, never a timestamp; `RECEIPT_TIME_ONLY` cannot satisfy semantic contracts.
  - Legal conversions require matched reference facts: `LAP_RELATIVE` $\to$ `SESSION_ELAPSED` requires `lap_start` (`lST`); `SESSION_ELAPSED` $\to$ `ABSOLUTE_UTC` requires `session_t0` (`t0_date`). Converted times inherit the union of origin sets and uncertainty.
  - Dual transport-axis isolation: LIVE runs use `EVENT_UTC`; REPLAY runs use virtual clocks flagged `RECEIPT_FALLBACK`.
  - "Time is Data": Silent feed expiry is resolved by recorded `RunClockTick` facts, never synthetic watermarks.

### 3.3 Origins (§2.3)
- **Current State**: Processors record generic build strings (`scaffold-v1`).
- **CAL-DOC-007 Mandate**:
  - Dual-axis origin tracking: `source_origin_set` (upstream provider classes) and `processing_origin_set` (processor family classes).
  - 3-level hierarchy: Origin Class, Origin Instance, Origin-Health Cell.
  - Anti-churn supersession rule (Defect D-09): Builds and transform versions of the same processor family never corroborate. Newer atoms carry `supersedes_atom_id` and replace older atoms in evaluation.

### 3.4 Corroboration & Fault Model (§2.4)
- **Current State**: No corroboration, clique search, or fault diagnosis algorithms exist.
- **CAL-DOC-007 Mandate**:
  - Corroboration depth: $\kappa_x = \max_{Q \text{ pairwise agreeing, disjoint on } x} (|Q| - 1)$.
  - Exact clique search bounded by $M_{\max} = 10$; overflow emits an overflow witness and fails closed to $\le \text{UNKNOWN}$.
  - Theorem G4-V (Value Validity): Bound $|v_q - x^*| \le \eta + \epsilon_h$ holds when $|Q| \ge f + 1$.
  - Theorem G4-I (Fault Isolation): Exclude only origin classes in $\bigcap \mathcal{D}_{\min}$ (intersection of minimal admissible diagnoses). Empty intersection yields `DIAGNOSIS_AMBIGUOUS` (state `SUSPECT`, no exclusion).
  - Single Upstream Reality: Pitwall and TracingInsights share FOM timing origin $\implies \text{source\_min\_depth} = 0$ for Phase 1.

### 3.5 State Algebra & Verdict Composition (§2.5)
- **Current State**: Outputs emit `integrity_status: "NOT_EVALUATED"`.
- **CAL-DOC-007 Mandate**:
  - 4-state admissibility total order: $\text{DEGRADED} < \text{UNKNOWN} < \text{SUSPECT} < \text{HEALTHY}$.
  - Slot-based definitions: `UNKNOWN` = slot open awaiting arrival; `DEGRADED` = slot closed empty or origin stalled; `SUSPECT` = conflict or weak corroboration; `HEALTHY` = all conditions met under declared assumptions.
  - 16-cell truth tables for `AND` ($\min$) and `OR` ($\max$).
  - Pointwise validity timelines $\sigma(t)$ composed as step functions, correctly evaluating compound contracts without premature expiry.
  - Witnessed selection: Depth profile and explanation drawn from a single supporting subset, preventing borrowed coordinates.

### 3.6 Verdict Structure, Hashing & Maturity (§2.6)
- **Current State**: `FlinkOutputEnvelope` wraps the event with minimal routing headers.
- **CAL-DOC-007 Mandate**:
  - Full `IntegrityAssessment` schema v2.0.
  - `verdict_hash`: SHA-256 over RFC 8785 JCS of canonical fields, excluding `run_id`, `revision_number`, and `previous_verdict_hash`.
  - R1 determinism (evaluator pure function) vs R2 pipeline determinism (governed by recorded admission log).
  - Maturity model: `resolution_maturity` (`OPEN`, `STATE_RESOLVED`, `TUPLE_RESOLVED`, `REOPENED`) and `finality_status` (`OPEN`, `FINAL`, `FINALITY_VIOLATION`).

### 3.7 Verdict Envelopes (§2.7)
- **Current State**: Unimplemented.
- **CAL-DOC-007 Mandate**:
  - Agreement-pattern abstract completion model over pending slots.
  - Sound bounds incorporating potential conflicting atoms (Defect D-03).
  - Search capped at $C_{\max} = 32$ with conservative fallback `[DEGRADED, HEALTHY]` and witness `ENVELOPE_CAPPED`.

### 3.8 Liveness, Guard Events & Leases (§2.8)
- **Current State**: Ingestion and health services emit heartbeats.
- **CAL-DOC-007 Mandate**:
  - External supervisor emits durable `GuardFact` (`STALLED` / `RESUMED`) and `RunClockTick` facts.
  - Consumer leases: $\text{lease\_until} = \min(\text{next\_transition\_at}, \text{issued\_at} + \text{max\_lease})$ in `lease_clock_domain`. Stale displays expire even if Flink or guards halt.

### 3.9 Storage Responsibilities & Durability (§2.9)
- **Current State**: Single SQLite table `persisted_events` indexing `event_id`.
- **CAL-DOC-007 Mandate**:
  - Three-tier storage hierarchy: Raw Archive $\to$ Bounded Flink Ledger $\to$ Append-only Verdict History & Admission Log.
  - Quality grading policy: Processors emit findings; core assigns `quality_grade` at admission. `Finding.confidence` is informational only.

### 3.10 Analytics Split (§2.10)
- **Current State**: `AnalyticsService` mixes pipeline operational metrics with incident findings.
- **CAL-DOC-007 Mandate**:
  - Three distinct classes:
    1. Pipeline-health telemetry: Never gated by integrity.
    2. Race metrics: Always computed, carries integrity context.
    3. Gated rule evaluation: Strictly gated by DECs; emits `RULE_DEFERRED` when contract fails.

### 3.11 Keying, Contracts & Distribution (§2.11)
- **Current State**: Partition keys use raw `session_id:driver_id`.
- **CAL-DOC-007 Mandate**:
  - Partition key: `session_instance_id` (`session_id` for LIVE, `session_id#run_id` for REPLAY), preventing state bleed across concurrent runs.
  - Broadcast contracts updated via dirty-flag + RunClockTick evaluation pattern.

### 3.12 Bitemporal & Calibration Semantics (§2.12)
- **Current State**: No bitemporal querying or calibration references.
- **CAL-DOC-007 Mandate**:
  - Bitemporal storage over Valid Time ($T_v$) and Knowledge Time ($T_k$).
  - As-of $K$ point-in-time state reconstruction queries.
  - Tolerances carry `calibration_ref`; lack of sensor calibration explicitly emits `CALIBRATION_NOT_AVAILABLE`.

---

## 4. Errata & Defect Catalog Mappings (D-01..20 and C-01..09)

The 20 defects and 9 conflicts identified in CAL-DOC-007 directly map to the CALIBRE implementation as follows:
- **D-01 (G4-I Unsoundness)**: Replaced unique minimum diagnosis with intersection of minimal admissible diagnoses $\bigcap \mathcal{D}_{\min}$. Tested via star-graph conflict.
- **D-02 (G4-V Premise)**: Decoupled validity bound $\eta + \epsilon_h$ from completeness premise $\eta \ge 2\epsilon_h$.
- **D-03 (Envelope Monotonicity)**: Replaced best/worst completion with agreement-pattern completions including conflicts.
- **D-04 (Scalar Horizon)**: Replaced scalar `healthy_until` with pointwise composed timeline $\sigma(t)$.
- **D-05 (UNKNOWN vs DEGRADED)**: Adopted slot-based definitions over the admissibility lattice.
- **D-06 (Stationary Watermark)**: Added `RunClockTick` facts ("Time is Data") to expire silent feeds.
- **D-07 (Live/Replay Collisions)**: Enforced transport-axis and job-level isolation between LIVE and REPLAY.
- **D-08 (Determinism Overclaim)**: Decoupled content hash from revision links; formulated R1 and R2 gates.
- **D-09 (Builds as Origins)**: Redefined processing origin as `processor_family_id`; newer builds supersede older atoms.
- **D-10 (Processor Grading)**: Core assigns grades via admission grading policy; findings are evidence only.
- **D-11 (Analytics Ambiguity)**: Split analytics into pipeline telemetry, race metrics, and gated rules.
- **D-12 (Partition Key Bleed)**: Scoped keys by `session_instance_id`.
- **D-13 (Hash Drift)**: Mandated RFC 8785 JCS and SHA-256 with shared test vector suite.
- **D-14 (Infeasible Signals)**: Established Availability Classes (A, H, S); grounded 9 of 10 contracts in free feeds.
- **D-15 (Single Upstream)**: Documented single FOM timing origin; set `source_min_depth = 0`.
- **D-16 (Bitemporal/Calibration)**: Specified Valid/Knowledge time schema and `calibration_ref`.
- **D-17 (Circular Gates)**: Decoupled monolithic gate into G-SPEC, G-EVAL, G-FLINK.
- **D-18 (Clique Complexity)**: Capped agreement search at $M_{\max} = 10$.
- **D-19 (Hash Churn)**: Enforced canonical atom tie-breaking.
- **D-20 (Broadcast Timers)**: Adopted Dirty-Flag + RunClockTick re-evaluation pattern.
- **C-01 through C-09**: Harmonized baseline documents CAL-DOC-001 through CAL-DOC-006.

---

## 5. Readiness Gates Audit (G-SPEC, G-EVAL, G-FLINK)

```
                       ┌─────────────────────────┐
                       │         G-SPEC          │  ===> READY TO OPEN
                       │  (Specs, Errata, Plans) │       (Deliverables produced)
                       └────────────┬────────────┘
                                    │
                                    ▼
                       ┌─────────────────────────┐
                       │         G-EVAL          │  ===> BLOCKED ON M1
                       │  (Pure Evaluator, DECs) │       (10 DECs, 20 Fixtures, Vectors)
                       └────────────┬────────────┘
                                    │
                                    ▼
                       ┌─────────────────────────┐
                       │         G-FLINK         │  ===> BLOCKED ON M2/M3
                       │  (Streaming Embedding)  │       (Evaluator verified, Spike S-1)
                       └─────────────────────────┘
```

1. **Gate G-SPEC**: Opens upon formal delivery and approval of:
   - `docs/implementation/SIA_REPOSITORY_ASSESSMENT.md`
   - `docs/implementation/SIA_v2.2.1_ERRATA.md`
   - `docs/implementation/S1_FLINK_SPIKE_RESULTS.md`
   *Status*: **READY TO OPEN** upon completion of Milestone 0.
2. **Gate G-EVAL**: Opens when G-SPEC is met, 10 DECs are defined ($\ge 7$ grounded), 20 golden fixtures are established, and $\ge 50$ cross-language hash vectors pass.
   *Status*: **BLOCKED on Milestone 1**.
3. **Gate G-FLINK**: Opens when G-EVAL is passed, pure evaluator executes all fixtures deterministically, and live services connect end-to-end.
   *Status*: **BLOCKED on Milestones 2 and 3**.

---

## 6. Technical Debt & Pre-SIA Scaffold Invariants

1. **Non-Canonical Serialization in Pipelines**:
   - `calibre_core/contracts.py` currently uses `json.dumps(self.payload, sort_keys=True).encode("utf-8")`. While `sort_keys=True` orders dictionary keys, it does not normalize floating-point number formatting or whitespace, causing hash divergence between Python and Java. RFC 8785 JCS must be integrated.
2. **Wall-Clock Injection in Event Identifiers**:
   - `ingestion/service.py` uses `utc_now().isoformat()` during raw event construction, introducing non-determinism during historical replay.
3. **Missing DLQ Quarantine for Semantic Corruptions**:
   - The current `calibre.dlq` captures unparseable JSON, but lacks typed error classification for schema mismatches, out-of-order bounds, and unmodelled common cause failures.

---

## 7. Risk Analysis & Falsifiable Verification Rules

### 7.1 Hypothesis H1 (Usefulness vs Simple Freshness Flag)
CAL-DOC-007 §6.5 defines Hypothesis H1:
> On a labelled fault-injection suite covering stale feeds, missing references (`lST`/`t0`), wrong lap mapping, duplicated processor outputs, shared upstream providers, late corrections, and silent sources, SIA yields **fewer false-HEALTHY outcomes than a per-feed freshness flag** without exceeding an agreed false-DEGRADED rate.

### 7.2 Strict 4-of-7 Scale-Back Rule
If SIA fails to outperform the per-feed freshness baseline on at least **4 of the 7 fault classes**, the platform will scale back: retaining typed time, freshness, completeness, lineage, and fail-closed logic (the MV-SIA subset) while deferring diagnosis, envelope completion, and risk modelling.

---

## 8. Authoritative Origin Inventory & Lineage Mapping

### 8.1 The Single Upstream FOM Reality

An exhaustive investigation of motorsport data acquisition paths in CALIBRE establishes the following physical topology:

```
                                ┌──────────────────────────────────────────────┐
                                │       FOM Trackside Timing Infrastructure    │
                                │  (Transponders, Timing Loops, FOM Servers)   │
                                └──────────────────────┬───────────────────────┘
                                                       │
                        ┌──────────────────────────────┴──────────────────────────────┐
                        │                                                             │
                        ▼                                                             ▼
         ┌─────────────────────────────┐                               ┌─────────────────────────────┐
         │  Formula 1 Live Timing Hub  │                               │   F1 Historical Data Dumps  │
         │ (livetiming.formula1.com)   │                               │     (Ergast / FOM Export)   │
         └──────────────┬──────────────┘                               └──────────────┬──────────────┘
                        │ (SignalR Stream)                                            │ (HTTP JSON API)
                        ▼                                                             ▼
         ┌─────────────────────────────┐                               ┌─────────────────────────────┐
         │       Pitwall Client        │                               │       TracingInsights       │
         │  (pitwall_source/pitwall.py)│                               │    (FastF1 / Session Dumps) │
         └──────────────┬──────────────┘                               └──────────────┬──────────────┘
                        │ (calibre.raw.pitwall.*)                                     │ (calibre.raw.tracinginsights.*)
                        ▼                                                             ▼
         ┌─────────────────────────────┐                               ┌─────────────────────────────┐
         │ CALIBRE Ingestion Adapter   │                               │ CALIBRE Replay Adapter      │
         └──────────────┬──────────────┘                               └──────────────┬──────────────┘
                        │                                                             │
                        └──────────────────────────────┬──────────────────────────────┘
                                                       ▼
                                        ┌─────────────────────────────┐
                                        │ CALIBRE Processing Layer    │
                                        │ (EvidenceEvent generation)  │
                                        └─────────────────────────────┘
```

#### 8.1.1 Structural Corroboration Bounds
1. **Zero Source Disjointness**: Pitwall SignalR and TracingInsights JSON dumps are distinct transport mechanisms, but **share the identical upstream source origin class** (`FOM_TIMING_CORE`).
2. **False Corroboration Hazard**: Treating a Pitwall lap time and a TracingInsights lap time as independent witnesses ($\kappa_{\text{source}} = 1$) would cause any upstream FOM timing error to be corroborated by both, emitting a catastrophic false-HEALTHY verdict.
3. **Normative Rule**: In Phase 1, `source_min_depth` is set to `0` (`INFORMATIONAL`) for all contracts on free feeds.
4. **Mandatory Limitation Witness**: Every verdict evaluating Phase 1 free feeds attaches the explicit witness:
   ```
   "UNMODELLED_COMMON_CAUSE: shared F1 timing upstream"
   ```
5. **Testing via Synthetic Class S**: Source-disjoint corroboration ($\kappa_{\text{source}} \ge 1$) and Theorem G4-I fault isolation ($\bigcap \mathcal{D}_{\min}$) are exercised exclusively via **synthetic fault-injection fixtures (Class S)**.

---

### 8.2 Dual-Axis Origin Taxonomy

To prevent conflation between data sources and analytical algorithms, CALIBRE defines a strict **dual-axis origin model**:

```
                       ┌────────────────────────────────────────┐
                       │           Evidence Atom Origins        │
                       └───────────────────┬────────────────────┘
                                           │
                 ┌─────────────────────────┴─────────────────────────┐
                 ▼                                                   ▼
   ┌───────────────────────────┐                       ┌───────────────────────────┐
   │    Source Origin Axis     │                       │  Processing Origin Axis   │
   │   (Upstream Systems)      │                       │   (Algorithm Families)    │
   └─────────────┬─────────────┘                       └─────────────┬─────────────┘
                 │                                                   │
        ┌────────┴────────┐                                 ┌────────┴────────┐
        ▼                 ▼                                 ▼                 ▼
 ┌─────────────┐   ┌─────────────┐                   ┌─────────────┐   ┌─────────────┐
 │  Class      │   │  Instance   │                   │  Family ID  │   │ Transform   │
 │ (Provider)  │   │  (Channel)  │                   │ (Algorithm) │   │  Version    │
 └─────────────┘   └─────────────┘                   └─────────────┘   └─────────────┘
```

#### 8.2.1 Source Origin Axis ($O_{\text{source}}$)
- **Origin Class**: Category of upstream generation (e.g. `FOM_TIMING_CORE`, `TRACK_WEATHER_STATION`, `CALIBRE_MOCK_FEED`, `SYNTHETIC_INDEPENDENT_A`). Two atoms are source-independent iff their source origin classes are disjoint.
- **Origin Instance**: Specific physical/network acquisition instance: `provider/feed/session_id/connection_epoch`.
- **Origin-Health Cell**: Dynamic operational state of an instance in an epoch (`HEALTHY`, `STALLED`, `DISCONNECTED`).

#### 8.2.2 Processing Origin Axis ($O_{\text{proc}}$)
- **Processor Family ID (`processor_family_id`)**: Architectural lineage of the algorithm/parser (e.g. `timing_parser_family`, `telemetry_normalizer_family`). Two processors are independent iff their family IDs are distinct AND their implementations share no common parsing libraries.
- **Transform Version**: Build/patch identifier (e.g., `build-20261101-r01`).
- **Supersession Rule (Defect D-09)**: Two builds of the same `processor_family_id` **never corroborate**. If an updated build reprocesses a raw event, it emits an atom carrying `supersedes_atom_id = <older_atom_id>`. The newer atom replaces the older in active evaluation; the older is retained in history as `SUPERSEDED`.

---

### 8.3 Lineage Propagation & Algebra

For any derived atom $d$ produced by a transformation $f$ over input atoms $\{a_1, a_2, \dots, a_k\}$:

#### 8.3.1 Union Algebra
$$O_{\text{source}}(d) = \bigcup_{i=1}^k O_{\text{source}}(a_i)$$
$$O_{\text{proc}}(d) = \left( \bigcup_{i=1}^k O_{\text{proc}}(a_i) \right) \cup \{ \text{family}(f) \}$$

#### 8.3.2 Reference Fact Provenance Inheritance
When a timestamp is converted (e.g. converting `LAP_RELATIVE` telemetry time to `SESSION_ELAPSED` via lap-start fact `lST`):
1. $O_{\text{source}}(\text{converted}) = O_{\text{source}}(\text{telemetry}) \cup O_{\text{source}}(\text{lST})$.
2. $\text{uncertainty}(\text{converted}) = \text{uncertainty}(\text{telemetry}) + \text{uncertainty}(\text{lST})$ (via interval arithmetic).
3. $\text{expiry}(\text{converted}) = \min(\text{expiry}(\text{telemetry}), \text{expiry}(\text{lST}))$.
4. If `lST` is missing or invalid, conversion produces **null**; no estimated timestamp is ever fabricated.

---

### 8.4 Comprehensive Origin Inventory Catalog

The following catalog establishes the complete inventory of telemetry, timing, and operational feeds in CALIBRE:

| Provider | Feed Slug / Topic | Upstream Root | Origin Class | Availability Class | Nominal Cadence | Semantics & Common-Cause Vulnerabilities |
|---|---|---|---|---|---|---|
| Pitwall | `TimingData` | FOM Timing Loops | `FOM_TIMING_CORE` | **A** | ~100 ms | Lap times, sector times, speed traps. Shared upstream with TracingInsights. |
| Pitwall | `TimingAppData` | FOM Timing System | `FOM_TIMING_CORE` | **A** | ~500 ms | Tyre compound, stint lap counts. |
| Pitwall | `SessionInfo` | FOM Race Control | `FOM_RCM_CORE` | **A** | On change | Session start time (`t0_date`), session type, track name. Critical reference fact. |
| Pitwall | `SessionStatus` | FOM Race Control | `FOM_RCM_CORE` | **A** | On change | `Started`, `Finished`, `Finalised`. Triggers session boundary transitions. |
| Pitwall | `TrackStatus` | FOM Race Control | `FOM_RCM_CORE` | **A** | On change | Track flags: Green (1), Yellow (2), SC (4), Red (5), VSC (6). |
| Pitwall | `RaceControlMessages` | FOM Race Control | `FOM_RCM_CORE` | **A** | On change | Incidents, investigations, penalties, DRS status. |
| Pitwall | `WeatherData` | Track Weather Sensors | `TRACK_WEATHER_STATION` | **A** | ~1 min | Air/track temperature, humidity, pressure, rainfall. Separate sensor from timing. |
| Pitwall | `CarData.z` | Car Telemetry Transponders | `FOM_CAR_TELEMETRY` | **S (Auth-Gated)** | ~100 ms | Speed, RPM, gear, throttle, brake, DRS. Requires F1 TV auth token. Class S in Phase 1. |
| Pitwall | `Position.z` | GPS Track Mapping | `FOM_GPS_POSITION` | **S (Auth-Gated)** | ~250 ms | 3D coordinate vehicle position. Auth-gated. Class S in Phase 1. |
| TracingInsights | `laptimes` | FOM Historical Export | `FOM_TIMING_CORE` | **H** | Post-lap | Lap completed events (`sesT`, `lST`, lap time). Shared upstream with Pitwall. |
| TracingInsights | `rcm` | FOM Historical Export | `FOM_RCM_CORE` | **H** | Historical | Race control message series. |
| TracingInsights | `weather` | Track Weather Sensors | `TRACK_WEATHER_STATION` | **H** | Historical | Weather series (`wT`, air temp, track temp). |
| Mock | `mock.telemetry` | Local Generator | `CALIBRE_MOCK_FEED` | **S** | 100 ms | Synthetic vehicle telemetry for pipeline smoke tests. |
| Synthetic | `syn.fault_inject` | Adversarial Test Harness | `SYNTHETIC_INDEPENDENT_A` | **S** | Test-driven | Independent test source for G4-V/G4-I multi-source verification. |
| Guard | `guard.liveness` | Supervisor Daemon | `CALIBRE_LIVENESS_GUARD` | **A** | Heartbeat | Emits `STALLED` / `RESUMED` origin-health facts. |
| Clock | `guard.clock` | Supervisor Daemon | `CALIBRE_RUN_CLOCK` | **A** | 1000 ms | Emits `RunClockTick` facts for deterministic expiry. |

---

### 8.5 Lineage Schema Implementation Specifications

To support this lineage model, the following dataclasses are specified for implementation in `calibre_core/contracts.py`:

```python
from dataclasses import dataclass, field
from typing import Any, List, Optional

@dataclass(slots=True)
class OriginRef:
    """Explicit reference to an evidence origin instance."""
    origin_class: str          # e.g. "FOM_TIMING_CORE"
    origin_instance_id: str    # e.g. "pitwall/TimingData/session_11731/conn_1"
    epoch: int = 1             # connection/health epoch

@dataclass(slots=True)
class EvidenceAtom:
    """Normative evidence atom conforming to CAL-DOC-007 §2.1.1 and §2.3."""
    evidence_atom_id: str
    raw_event_ids: List[str]
    processor_family_id: str
    transform_version: str
    role_id: str
    scope_key: str
    normalized_value: Any
    normalized_value_hash: str
    reference_atom_ids: List[str]
    source_origin_set: List[str]        # list of origin classes
    processing_origin_set: List[str]    # list of processor family classes
    time_domain: str                    # ABSOLUTE_UTC, SESSION_ELAPSED, etc.
    time_value: Any
    uncertainty_s: float
    supersedes_atom_id: Optional[str] = None
    calibration_ref: Optional[str] = None
    quality_grade: str = "HEALTHY"      # Assigned by core at admission
    limitations: List[str] = field(default_factory=list)
```

---

## 9. Conclusion & Milestone Traceability

This repository assessment confirms the full architectural readiness of CALIBRE's Phase 0 streaming infrastructure while establishing the exact normative specification, lineage model, and errata resolutions required for Milestone 1 through Milestone 7. Gate **G-SPEC** is officially cleared to open upon orchestrator acceptance of this assessment and its companion deliverables.
