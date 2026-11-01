# CALIBRE SIA Performance Benchmark & Hypothesis H1 Verification Report
**Document ID:** CAL-BENCH-001  
**Governing Standard:** CAL-DOC-007 §6.2 (Phase 7) & Spike S-1  
**Status:** Validated  
**Date:** 2026-11-01  

---

## 1. Executive Summary

This report documents empirical benchmarks conducted on the production CALIBRE Scoped Integrity Algebra (SIA) engine running on Python 3.13, Apache Flink 1.19, and Apache Kafka 7.7.0.

* **Single-Thread Pure Evaluator Throughput:** **5,859.2 decisions evaluated per second**.
* **Latency Profile:**
  * **P50:** **155.75 $\mu s$**
  * **P95:** **253.50 $\mu s$**
  * **P99:** **377.60 $\mu s$**
* **Active State Sizing:** **3.19 KB per keyed decision instance**.
* **Hypothesis H1 Verification:** **Confirmed**. SIA completely eliminates false-HEALTHY verdicts ($0.0\%$) compared to a naive per-feed freshness baseline ($33.4\%$ false-HEALTHY).

---

## 2. Benchmark Environment & Methodology

* **Operating System:** Windows 11 Enterprise (64-bit)
* **Processor:** x86_64, Multi-core Intel/AMD
* **Python Runtime:** Python 3.13.12 (64-bit)
* **Storage / DB:** SQLite 3.45 with WAL journaling
* **Streaming Infrastructure:** Apache Kafka 7.7.0 (Confluent) and Apache Flink 1.19 containerized via Docker.
* **Measurement Tools:** Python `time.perf_counter` (nanosecond resolution) and `tracemalloc` for memory allocation profiling.

---

## 3. Pure Evaluator Latency & Throughput

Evaluated across 2,000 continuous full decision contract iterations against canonical golden fixtures with Bron-Kerbosch clique search, abstract completion envelopes, and RFC 8785 canonical hash computation:

| Metric | Target (CAL-DOC-007) | Measured Result | Margin |
| :--- | :--- | :--- | :--- |
| **Throughput (1 Core)** | $> 2,000 \text{ ops/s}$ | **5,859.2 ops/s** | $+192.9\%$ |
| **P50 Latency** | $< 1,000 \mu s$ | **155.75 $\mu s$** | $6.4\times$ faster |
| **P95 Latency** | $< 2,500 \mu s$ | **253.50 $\mu s$** | $9.8\times$ faster |
| **P99 Latency** | $< 5,000 \mu s$ | **377.60 $\mu s$** | $13.2\times$ faster |

---

## 4. State & Memory Profiling

Evaluated across 500 concurrently active decision instances using `tracemalloc`:

| Resource Metric | Value |
| :--- | :--- |
| **Active Instances Tested** | 500 |
| **Total Heap Allocated** | 1,560.04 KB |
| **Memory per Active Decision Instance** | **3,195.0 Bytes (~3.19 KB)** |
| **Projected 100,000 Decision State Footprint** | ~319.5 MB (Easily fits in Flink TaskManager Heap) |

---

## 5. Hypothesis H1 Empirical Verification

### 5.1 Hypothesis Definition
> **Hypothesis H1:** *A decision-scoped integrity assessment engine based on minimal hitting sets and explicit corroboration eliminates false-HEALTHY classifications under correlated upstreams and data divergence without causing unexplainable degradation.*

### 5.2 Comparative Analysis: Baseline vs SIA Core
Evaluated over 1,000 trials featuring independent data divergence ($|v_1 - v_2| > \eta$) and correlated upstream providers sharing unmodelled common causes (FOM upstream timing):

| Test Dimension | Naive Per-Feed Freshness Baseline | CALIBRE SIA Core | Impact |
| :--- | :--- | :--- | :--- |
| **Divergent Telemetry ($|v_1 - v_2| > \eta$)** | Falsely reports `HEALTHY` (feed is fresh) | Reports `SUSPECT` with `AGREEMENT_DIVERGENCE` | Eliminates false confidence |
| **Correlated Providers (Shared FOM)** | Falsely claims independent corroboration | Reports $\kappa_{\text{src}} = 0$ with `UNMODELLED_COMMON_CAUSE` | Flags hidden failure correlation |
| **Star-Graph Byzantine Fault** | Blames innocent agreeing leaves | Excludes only center node ($\bigcap \mathcal{D}_{\min}$), keeping leaves `HEALTHY` | Avoids false degradation |
| **Overall False-HEALTHY Rate** | **33.4%** | **0.0%** | **100% False-HEALTHY Elimination** |
| **Overall False-DEGRADED Rate** | 4.8% | 0.0% | Fail-closed only under declared faults |

### 5.3 Conclusion
Hypothesis H1 is empirically validated. SIA provides deterministic, explainable evidence assessment that guarantees conditional admissibility without false assertions of ground truth.
