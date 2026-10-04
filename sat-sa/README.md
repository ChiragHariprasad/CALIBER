# SAT-SA — Supervisory Analytics Tool for SOC Assessment

> **Smart India Hackathon 2026** • **Problem SIH26157** • **NCIIPC / NTRO**  
> **Theme:** Blockchain & Cybersecurity  
> **Repository:** Offline-first, evidence-driven supervisory batch analytics engine.

---

## 🎯 Executive Overview

**SAT-SA** is a batch analytics and supervisory triage platform designed for national cyber security regulators (NCIIPC / NTRO) to assess Critical Sector Entities (CSEs). 

Rather than relying on self-reported compliance questionnaires or raw security metrics, SAT-SA answers one core question:  
👉 **Where does an entity's submitted SOC evidence disagree with the resilience it claims, and which specific records should a supervisory examiner open first?**

### Key Features
1. **12 Core Signal Detectors (§5.2):** Evidence-backed detection of execution gaps (e.g. unescalated critical cases, shallow investigation, closure bunching, template notes) and negative space (e.g. missing threat detection categories, funnel breaks).
2. **Supervisory Action Index (SAI 0–100 §5.4):** Non-linear Noisy-OR failure probability combined with coverage penalties to classify entities into Priority Tiers (T1 Critical Triage to T4 Clean Claim).
3. **Deterministic Examiner Sampling (§6.2):** Seed-locked prioritised record selection identifying the top 15 log/case records for examiner inspection.
4. **TRUST Audit Ledger (§7):** Cryptographically signed (Ed25519) hash chain storing submission manifests and assessment run blocks with SHA-256 Merkle tree inclusion proofs to guarantee zero evidence tampering.
5. **Interactive Triage UI (§8):** 3-screen Streamlit dashboard featuring the V3 Claimed Resilience vs Disagree Rate scatter plot, proof inspector, and ledger verifier.

---

## 🚀 Quick Start

### 1. Installation

Ensure Python 3.10+ is installed. All dependencies are CPU-only and fully offline-capable:

```bash
cd sat-sa
pip install -r requirements.txt
```

### 2. Generate Synthetic Benchmark Dataset (40 CSEs)

Generate a complete 40-CSE dataset featuring planted failure scenarios (F1–F7) and benign look-alikes:

```bash
python -m satsa generate --output-dir ./data/sample --num-cses 40 --seed 42
```

### 3. Run Offline Batch Assessment

Run SAT-SA on a CSE submission directory:

```bash
python -m satsa run --input-dir ./data/sample/CSE-101 --entity-id CSE-101 --entity-name "State Bank Ops" --sector Banking
```

### 4. Cryptographic Ledger Verification (`satsa verify`)

Verify zero-tampering on the TRUST ledger and Merkle root chain:

```bash
python -m satsa verify --ledger-path ./trust_ledger.db
```

### 5. Launch Interactive Streamlit Dashboard

```bash
streamlit run ui/app.py
```

---

## 🧪 Validation & Performance Benchmark

Run the full automated benchmark harness:

```bash
python validation/harness.py
```

### Benchmark Targets & Expected Results
- **Recall (Sensitivity):** ≥ 95% on planted failure scenarios (F1–F7)
- **False Positive Rate:** ≤ 5% on benign look-alikes
- **Batch Runtime:** < 30 seconds for 40 full CSE assessments (CPU-only)
- **Determinism:** 100% reproducible hold-out sampling across seeds

---

## 📁 Repository Architecture

```
sat-sa/
├── satsa/                   # Core SAT-SA Analytics Engine
│   ├── api/                 # Orchestrator & runner
│   ├── baselines/           # Peer z-scores & zero-inflated count models
│   ├── evidence/            # Proof-carrying Evidence object schemas
│   ├── ingest/              # Validation & DuckDB ingest pipeline
│   ├── ledger/              # Merkle tree & TRUST cryptographic ledger
│   ├── sampling/            # Examiner priority record drawer (seed locked)
│   ├── scoring/             # Noisy-OR & Supervisory Action Index (SAI)
│   ├── signals/             # 12 Core Signal Detectors
│   ├── store/               # DuckDB & Polars query store
│   ├── cli.py               # CLI entry point
│   ├── config.py            # Central configuration & settings
│   └── schemas.py           # Data contract (Tier 1 & Tier 2 schemas)
├── generator/               # Synthetic Data Generator (40 CSEs)
├── ui/                      # Streamlit 3-Screen Dashboard
├── validation/              # Accuracy & performance benchmark harness
├── tests/                   # Pytest test suite (100% pass)
└── requirements.txt         # Offline dependencies
```

---

## 📜 Regulatory Framing & Compliance

Designed in alignment with NCIIPC / NTRO supervisory frameworks. All evidence findings emit verifiable Merkle inclusion proofs, ensuring legally defensible auditability.
