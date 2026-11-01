# CALIBRE Scoped Integrity Algebra (SIA) — Verdict and Revision Model
**Document ID:** CAL-SPEC-006  
**Governing Standard:** CAL-DOC-007 §2.6, §2.7, §2.12  
**Implementation Modules:** `sia_core/verdict/builder.py`, `sia_core/envelope/completion.py`, `sia_core/identity/hasher.py`  
**Status:** Validated & Frozen  
**Date:** 2026-11-01  

---

## 1. 9-Field Canonical Verdict Structure

Every SIA assessment produces an immutable, explainable verdict envelope:

```json
{
  "schema_name": "calibre.integrity_assessment",
  "schema_version": "2.0",
  "assessment_id": "sha256:...",
  "logical_assessment_id": "session-abc|VER|lap52|pit_timing",
  "decision_instance_id": "session-abc|VER|lap52|pit_timing",
  "contract": {"id": "pit_timing", "version": "1.0", "epoch": 1},
  "integrity_state": "HEALTHY",
  "corroboration_profile_by_role": {
    "pit_entry": {
      "source_depth": 0,
      "processing_depth": 0,
      "support_atom_ids": ["atom-pit-01"],
      "origin_refs": ["PITWALL_TIMING"]
    }
  },
  "validity_timeline": [
    {"from": "2026-11-01T14:00:00.000Z", "state": "HEALTHY"},
    {"from": "2026-11-01T14:00:30.000Z", "state": "DEGRADED"}
  ],
  "next_transition_at": "2026-11-01T14:00:30.000Z",
  "lease_until": "2026-11-01T14:00:30.000Z",
  "lease_clock_domain": "LIVE_UTC",
  "resolution_maturity": "TUPLE_RESOLVED",
  "finality_status": "OPEN",
  "revision_number": 0,
  "previous_verdict_hash": null,
  "knowledge_frontier": {
    "watermark": "2026-11-01T14:00:05.000Z",
    "run_clock": "2026-11-01T14:00:05.000Z"
  },
  "primary_binding_witness": "MUTUAL_AGREEMENT_VALIDATED: roles=[pit_entry]",
  "witness_set": ["atom-pit-01"],
  "alternatives": [],
  "diagnosis_result": "NONE",
  "limitations": ["UNMODELLED_COMMON_CAUSE: shared F1 timing upstream"],
  "verdict_hash": "3bb456a41c78...",
  "decision_maturity": "PROVISIONAL"
}
```

---

## 2. Content Identity vs Revision-Chain Linking (Defect D-08)

To preserve reproducibility under out-of-order provisional revisions:
1. **`verdict_hash` is pure content identity:**
   $$\text{verdict\_hash} = \mathcal{H}(\text{contract}, \text{decision\_id}, \text{sorted atoms}, \text{frontier}, \text{state}, \text{timeline}, \text{profile}, \text{maturity}, \text{finality})$$
   *Explicitly excluded:* `run_id`, `revision_number`, `previous_verdict_hash`, wall-clock emission timestamps.
2. **Revisions form an append-only chain:**
   The revision path is captured through `revision_number` and `previous_verdict_hash` as explicit links ($H_1 \to H_2$), ensuring provisional arrival order does not churn the content hash of identical states.
3. **Canonical Finality Instant:**
   For `FINAL` verdicts, the knowledge frontier used in the hash is fixed to the canonical instant:
   $$t_{\text{canonical}} = \text{slot\_window\_end} + \text{allowed\_lateness\_s}$$
   This guarantees that historical replays produce identical `FINAL` verdict hashes regardless of when the evaluation job executed.

---

## 3. Abstract Envelope Completions & Early Collapse (Obligations P4 & P5)

### 3.1 Non-Monotonic Evidence Additions (Defect D-03)
In integrity evaluation, evidence additions are **not monotone**: a newly arrived atom can disagree with existing evidence, causing an assessment to downgrade from `HEALTHY` to `SUSPECT`.

Therefore, the verdict envelope $[\text{lower}, \text{upper}]$ must be computed over **all admissible abstract completions**:
* **Absent:** Slot closes without observation.
* **Agreeing Arrival:** Observation arrives within tolerance.
* **Conflicting Arrival:** Observation arrives and causes pairwise conflict.
* **Degraded Arrival:** Observation arrives with degraded quality or expired time.

### 3.2 Combinatorial Bounds ($C_{\max} = 32$)
For $k$ pending slots, the number of abstract completion patterns scales as $\mathcal{O}(3^k)$.
* If completions $\le C_{\max} = 32$, bounds are evaluated exhaustively.
* If completions $> C_{\max}$, the evaluator reports the conservative envelope $[\text{DEGRADED}, \text{HEALTHY}]$ with witness `ENVELOPE_CAPPED`.

### 3.3 Resolution Maturity Invariants
* **`STATE_RESOLVED`**: Lower state equals upper state ($\min(\text{state}) = \max(\text{state})$) across all completions.
* **`TUPLE_RESOLVED`**: State, depth profile, and validity timeline agree across all completions.
* **`FINAL`**: Requires all pending slots closed and knowledge frontier past the allowed lateness cutoff, OR sound early collapse.
* **Invariant G11 (Finality Stability):**
  $$\text{finality\_status} = \text{FINAL} \implies \text{resolution\_maturity} = \text{TUPLE\_RESOLVED}$$
  Any evidence arriving after the finality cutoff is **quarantined** and counted as an audit violation; it never mutates the `FINAL` verdict hash.

---

## 4. Dual-Domain Leases (`LIVE_UTC` vs `REPLAY_VIRTUAL`)

Consumer leases prevent downstream dashboards from displaying stale verdicts during network or engine partitions:
$$\text{lease\_until} = \min(\text{next\_transition\_at}, \text{issued\_at} + \text{max\_lease})$$

* **`LIVE_UTC` Domain:** Issued in wall-clock UTC.
* **`REPLAY_VIRTUAL` Domain:** Issued in historical virtual simulation time, driven by recorded `RunClockTick` facts. Replayed verdicts do not immediately expire upon consumption.

---

## 5. Bitemporal History & Reproducibility Levels

History is append-only, indexed by:
$$(\text{decision\_instance\_id}, \text{valid\_time\_window}, \text{knowledge\_time}, \text{revision\_number})$$

1. **"As-of Knowledge Time $K$":** What did CALIBRE conclude about decision $D$ at run-clock time $K$? Returns the latest revision where $\text{knowledge\_time} \le K$.
2. **"Valid Window Evolution":** How did the assessment of lap 52 change as late evidence arrived? Returns the forward revision chain.
3. **Reproducibility Guarantees:**
   * **R1 (Pure Evaluator):** Identical canonical inputs and frontier produce identical `verdict_hash` unconditionally.
   * **R2 (Streaming Pipeline):** Replay reproduces identical `FINAL` verdict hashes when using the durable admission log.
