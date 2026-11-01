# CALIBRE — Documentation Architecture and Navigation Guide

> **Calibration-Aware, Lineage-Indexed, Bitemporal Rule Evaluation**  
> An independent evidence-integrity and assurance platform for motorsport telemetry and race events.

This document serves as the master index and navigation roadmap for the entire CALIBRE documentation corpus.

---

## 1. Documentation Hierarchy & Structure

The documentation is organized into three logical tiers:
1. **Platform Baselines (`docs/01_*` to `docs/06_*`):** Foundational platform charters, functional requirements, architectural designs, API contracts, and kernel concepts, formally amended to integrate the Scoped Integrity Algebra (SIA) v2.2 per CAL-DOC-007 §9.
2. **Normative Core Specifications (`docs/core/`):** Exact mathematical specifications, DEC schema definitions, bitemporal/temporal models, origin taxonomies, and production runbooks for the SIA engine.
3. **Implementation, Verification & Handover Reports (`docs/implementation/`):** Architectural assessments, errata catalogs, empirical spike measurements, benchmark reports, test catalogues, and formal handover reports.

```text
e:\CALIBER\docs
├── 01_Project_Overview_Charter.md              (CAL-DOC-001, Amended)
├── 02_Functional_Requirements.md               (CAL-DOC-002, Amended)
├── 03_System_Requirements_Specification.md     (CAL-DOC-003, Amended)
├── 04_API_and_Event_Contract.md                (CAL-DOC-004, Amended)
├── 05_System_Architecture_HLD_LLD.md           (CAL-DOC-005, Amended)
├── 06_Core_Integrity_Kernel_Concept.md          (CAL-DOC-006, Amended)
│
├── core/                                       [Normative SIA Core Specs]
│   ├── SIA_MATHEMATICAL_SPECIFICATION_v1.0.md   (Lattice, Clique, Theorem G4-I)
│   ├── SIA_DEC_SCHEMA_v1.md                    (DEC AST & Compile-Time Rules V-01..V-10)
│   ├── SIA_EVENT_AND_STATE_MODELS.md           (EvidenceAtom, ReferenceFact, GuardFact)
│   ├── SIA_TEMPORAL_MODEL.md                   (Typed Time, Conversions, Pointwise Timelines)
│   ├── SIA_ORIGIN_AND_PROVENANCE_MODEL.md      (3-Tier Origins, Disjoint Corroboration)
│   ├── SIA_VERDICT_AND_REVISION_MODEL.md       (9-Field Envelope, Hash Linking, Dual Leases)
│   └── SIA_OPERATIONS_RUNBOOK.md               (Topology, Topics, Recovery R1/R2, Triage)
│
└── implementation/                             [Audit, Tests, Handover & Errata]
    ├── SIA_FINAL_HANDOVER_REPORT.md            (Master Engineering Handover Report)
    ├── SIA_v2.2.1_ERRATA.md                     (20 Defects D-01..D-20 & 9 Conflicts C-01..C-09)
    ├── SIA_TEST_CATALOGUE.md                    (741 Tests, 30 Adversarial Scenarios, 20 Fixtures)
    ├── SIA_BENCHMARK_REPORT.md                  (Throughput, Latency & Hypothesis H1 Validation)
    ├── SIA_IMPLEMENTATION_STATUS.md             (Gate Sign-offs G-SPEC through G-FINAL)
    ├── SIA_REPOSITORY_ASSESSMENT.md             (Pre-Implementation Repo & Module Audit)
    ├── S1_FLINK_SPIKE_RESULTS.md                (Empirical Flink OS Loopback Measurements)
    └── SIA_FINAL_COMPLETION_REPORT.md           (Milestone Handover Summary)
```

---

## 2. Complete Document Catalogue

| Document ID | File Path | Title | Category | Scope / Responsibilities | Status |
|:---|:---|:---|:---:|:---|:---:|
| **CAL-DOC-001** | [`01_Project_Overview_Charter.md`](file:///e:/CALIBER/docs/01_Project_Overview_Charter.md) | Project Overview & Charter | Baseline | Vision, scope boundaries, human-in-the-loop charter, SIA v2.2 adoption | **Amended v1.1** |
| **CAL-DOC-002** | [`02_Functional_Requirements.md`](file:///e:/CALIBER/docs/02_Functional_Requirements.md) | Functional Requirements | Baseline | Ingestion, processors, Flink integrity core, 3-class analytics, FR-052..FR-062 | **Amended v1.1** |
| **CAL-DOC-003** | [`03_System_Requirements_Specification.md`](file:///e:/CALIBER/docs/03_System_Requirements_Specification.md) | System Requirements Specification | Baseline | Performance, R1/R2 determinism, $M_{\max}=10$ & $C_{\max}=32$ caps, dual leases | **Amended v1.1** |
| **CAL-DOC-004** | [`04_API_and_Event_Contract.md`](file:///e:/CALIBER/docs/04_API_and_Event_Contract.md) | API and Event Contract | Baseline | Kafka topic schemas, `IntegrityAssessment` 4-orthogonal dimensions | **Amended v1.1** |
| **CAL-DOC-005** | [`05_System_Architecture_HLD_LLD.md`](file:///e:/CALIBER/docs/05_System_Architecture_HLD_LLD.md) | System Architecture HLD/LLD | Baseline | Streaming topology, component design, ADR-008 through ADR-011 | **Amended v1.1** |
| **CAL-DOC-006** | [`06_Core_Integrity_Kernel_Concept.md`](file:///e:/CALIBER/docs/06_Core_Integrity_Kernel_Concept.md) | Core Integrity Kernel Concept | Baseline | Kernel foundations, total order lattice, orthogonal verdict dimensions | **Amended v1.1** |
| **CAL-SPEC-001** | [`core/SIA_MATHEMATICAL_SPECIFICATION_v1.0.md`](file:///e:/CALIBER/docs/core/SIA_MATHEMATICAL_SPECIFICATION_v1.0.md) | Mathematical Specification v1.0 | Normative Core | 4-state lattice algebra, Bron-Kerbosch, Theorem G4-I minimal hitting sets | **Normative** |
| **CAL-SPEC-002** | [`core/SIA_DEC_SCHEMA_v1.md`](file:///e:/CALIBER/docs/core/SIA_DEC_SCHEMA_v1.md) | DEC Schema Specification v1.0 | Normative Core | Contract YAML syntax, grammar AST, static validation rules V-01..V-10 | **Normative** |
| **CAL-SPEC-003** | [`core/SIA_EVENT_AND_STATE_MODELS.md`](file:///e:/CALIBER/docs/core/SIA_EVENT_AND_STATE_MODELS.md) | Event and State Models | Normative Core | `EvidenceAtom`, `ReferenceFact`, `GuardFact`, `DecisionInstanceKey` schemas | **Normative** |
| **CAL-SPEC-004** | [`core/SIA_TEMPORAL_MODEL.md`](file:///e:/CALIBER/docs/core/SIA_TEMPORAL_MODEL.md) | Temporal Model Specification | Normative Core | Typed time classes, legal coordinate conversions, pointwise timelines $\sigma(t)$ | **Normative** |
| **CAL-SPEC-005** | [`core/SIA_ORIGIN_AND_PROVENANCE_MODEL.md`](file:///e:/CALIBER/docs/core/SIA_ORIGIN_AND_PROVENANCE_MODEL.md) | Origin & Provenance Model | Normative Core | 3-tier origin taxonomy, disjoint corroboration, build supersession | **Normative** |
| **CAL-SPEC-006** | [`core/SIA_VERDICT_AND_REVISION_MODEL.md`](file:///e:/CALIBER/docs/core/SIA_VERDICT_AND_REVISION_MODEL.md) | Verdict & Revision Model | Normative Core | 9-field verdict envelope, RFC 8785 hash chaining, dual-domain consumer leases | **Normative** |
| **CAL-OPS-001** | [`core/SIA_OPERATIONS_RUNBOOK.md`](file:///e:/CALIBER/docs/core/SIA_OPERATIONS_RUNBOOK.md) | SIA Operations Runbook | Operations | Cluster management, topic retention, dynamic contract updates, triage & recovery | **Production** |
| **CAL-REP-FINAL-001** | [`implementation/SIA_FINAL_HANDOVER_REPORT.md`](file:///e:/CALIBER/docs/implementation/SIA_FINAL_HANDOVER_REPORT.md) | Final Engineering Handover Report | Verification | Complete Phase 0–7 handover, 741 tests, 20 golden fixtures, benchmarks | **Frozen** |
| **CAL-ERR-001** | [`implementation/SIA_v2.2.1_ERRATA.md`](file:///e:/CALIBER/docs/implementation/SIA_v2.2.1_ERRATA.md) | SIA v2.2.1 Errata & Defects | Specification | Detailed audit and formal resolution of 20 defects (D-01..D-20) and 9 conflicts | **Validated** |
| **CAL-TEST-001** | [`implementation/SIA_TEST_CATALOGUE.md`](file:///e:/CALIBER/docs/implementation/SIA_TEST_CATALOGUE.md) | Test Catalogue & Fixtures | Verification | 741 tests, 30 mandatory adversarial scenarios (TC-P/A/S), 20 golden fixtures | **Validated** |
| **CAL-BENCH-001** | [`implementation/SIA_BENCHMARK_REPORT.md`](file:///e:/CALIBER/docs/implementation/SIA_BENCHMARK_REPORT.md) | Performance Benchmark Report | Verification | 5,859.2 ops/s throughput, 155.75 $\mu s$ P50 latency, Hypothesis H1 confirmed | **Validated** |
| **CAL-REP-001** | [`implementation/SIA_IMPLEMENTATION_STATUS.md`](file:///e:/CALIBER/docs/implementation/SIA_IMPLEMENTATION_STATUS.md) | Implementation Status Report | Management | Phase-by-phase delivery status, quality gates G-SPEC through G-FINAL | **Validated** |
| **CAL-AUD-001** | [`implementation/SIA_REPOSITORY_ASSESSMENT.md`](file:///e:/CALIBER/docs/implementation/SIA_REPOSITORY_ASSESSMENT.md) | Repository & Baseline Assessment | Architecture | Initial 19-module audit, gap analysis, and implementation roadmap | **Completed** |
| **CAL-SPK-001** | [`implementation/S1_FLINK_SPIKE_RESULTS.md`](file:///e:/CALIBER/docs/implementation/S1_FLINK_SPIKE_RESULTS.md) | Flink Spike S-1 Measurements | Architecture | Authentic OS duplex socket benchmarks, heap profiling, Table 7 constants | **Validated** |

---

## 3. Core Architectural Concepts

### 3.1 The 4-State Total Order Lattice ($\mathcal{L}$)
SIA integrity states form a total order:
$$\text{DEGRADED} (0) < \text{UNKNOWN} (1) < \text{SUSPECT} (2) < \text{HEALTHY} (3)$$
- **Meet ($\wedge$, AND):** $\min(a, b)$ — failure propagates conservatively.
- **Join ($\vee$, OR):** $\max(a, b)$ — valid alternative branch satisfies requirement.
- **$k$-of-$n$ Composition:** $s_{(k)}$ — $k$-th highest state among candidate branches.

### 3.2 Decision Evidence Contracts (DECs)
Every decision evaluation is strictly bounded by a versioned DEC declaring:
- **Decision Scope:** Driver/lap, driver/session, or global session key patterns.
- **Required Roles:** Time classes, agreement metrics, tolerances ($\eta$), and honest sensor noise ($\varepsilon_h$).
- **Origin Requirements:** Admissible providers, processors, minimum corroboration depths ($\kappa_{\text{src}}, \kappa_{\text{proc}}$), and fault budgets ($f$).
- **Evidence Slot Deadlines:** Arrival window, maximum admissions, and deadline expiry to DEGRADED.

### 3.3 Theorem G4-I Fault Isolation
When sensor observations diverge ($|v_1 - v_2| > \eta$), conflict origin edges are evaluated via minimal hitting sets ($\mathcal{D}_{\min}$). Under Theorem G4-I:
$$\mathcal{O}_{\text{excluded}} = \bigcap_{D \in \mathcal{D}_{\min}} D$$
On ambiguous star graphs, $\bigcap \mathcal{D}_{\min} = \emptyset$, guaranteeing that innocent agreeing nodes are never blamed.

### 3.4 Bitemporal Model & Dual-Domain Consumer Leases
- **Bitemporality:** Every evaluation records both **valid time** (physical event interval $[t_{\text{start}}, t_{\text{end}}]$) and **knowledge time** (ingestion instant $K$).
- **Dual-Domain Leases:**
  - `LIVE_UTC`: Consumer lease expires on wall-clock progression ($t_{\text{issued}} + \text{lease\_duration}$).
  - `REPLAY_VIRTUAL`: Consumer lease evaluates strictly against recorded `RunClockTick` facts, enabling repeatable regression replays without premature lease timeout.

---

## 4. Verification & Testing Quick Navigation

CALIBRE maintains a comprehensive zero-regression test suite:

```bash
# Run full master test suite (741 tests in ~21s)
pytest -q

# Run bit-for-bit Golden Fixture parity test (20/20 bit-for-bit SHA-256 match)
python tests/test_eval_parity.py

# Run the 30 mandatory property and adversarial tests
pytest tests/test_30_adversarial_scenarios.py -v

# Run RFC 8785 cross-language canonical serialization tests (123 tests)
pytest tests/test_rfc8785_vectors.py -v

# Run Flink streaming topology and DLQ tests
pytest tests/test_flink_streaming.py -v

# Run end-to-end pipeline trace verification
pytest tests/test_e2e_pipeline.py -v
```

For operational procedures and pipeline commands, refer to [`docs/core/SIA_OPERATIONS_RUNBOOK.md`](file:///e:/CALIBER/docs/core/SIA_OPERATIONS_RUNBOOK.md) and [`README.md`](file:///e:/CALIBER/README.md).
