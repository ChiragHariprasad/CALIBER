# CALIBRE Scoped Integrity Algebra (SIA) v2.2.1 — Formal Errata & Defect Catalog

**Document Control:**
- **Document Identifier**: `CAL-DOC-007-ERRATA`
- **Specification Baseline**: CAL-DOC-007 v2.2-draft
- **Baseline Documents Audited**: CAL-DOC-001 through CAL-DOC-006
- **Target Repository**: `E:\CALIBER` (All Modules)
- **Author**: `worker_m0_1` (teamwork_worker)
- **Reviewing Archetype**: Lead Streaming Systems Architect & Formal Methods Engineer
- **Date**: 2026-11-01
- **Status**: Production Deliverable — Binding Normative Errata

---

## 1. Executive Summary & Document Control

This document constitutes the authoritative, binding catalog of mathematical, algorithmic, and architectural corrections for the Scoped Integrity Algebra (SIA) v2.2.1 within CALIBRE. 

During the formal audit of SIA v2.1 against the foundational baseline documents (CAL-DOC-001 through CAL-DOC-006) and streaming execution realities, 20 architectural and algorithmic defects (D-01 through D-20), 9 inter-document specification conflicts (C-01 through C-09), and 11 open empirical items (O-01 through O-11) were identified.

Every defect cataloged herein is backed by problem descriptions, formal proofs or counter-examples, normative resolutions conforming to CAL-DOC-007, affected module bindings, and test case verifications. Furthermore, seven concrete baseline amendment directives (ERR-01 through ERR-07) are provided to ensure complete coherence across the CALIBRE documentation suite.

---

## 2. Classification & Severity Model

### 2.1 Classification Scheme
- **`D-xx` (Defect)**: Algorithmic, mathematical, or structural flaws in earlier SIA working designs.
- **`C-xx` (Conflict)**: Direct contradictions between SIA specifications and baseline platform documents (CAL-DOC-001..006).
- **`O-xx` (Open Item)**: Empirical measurements, research items, or parameter freezes tracked in CAL-DOC-007 §10.
- **`ERR-xx` (Baseline Errata)**: Concrete textual amendments applied to baseline platform documents CAL-DOC-001 through CAL-DOC-006.

### 2.2 Severity Levels
- **`CRITICAL` (Soundness / Correctness / Data Integrity)**: Algorithmic unsoundness, silent exclusion of honest origins, state corruption, false corroboration, or streaming deadlocks. Must be resolved prior to any production evaluation.
- **`HIGH` (Architecture / Latency / Determinism)**: Major semantic divergence, exponential NP-hard latency, replay non-determinism, or stationary watermark timer failures.
- **`MEDIUM` (Contract Feasibility / Scope Realism)**: Infeasible contracts on free feeds, contradictory milestone gating, or storage explosion from hash churn.
- **`LOW` (Terminology / Documentation / Metadata)**: Vocabulary collisions, naming clarity, or informational metadata handling.

---

## 3. Comprehensive Catalog of 20 Defects (D-01 to D-20)

### D-01: G4-I Fault Isolation Unsoundness in Unique Minimum Diagnosis Rule
- **Title**: Unsound Fault Isolation via Unique Minimum Diagnosis
- **Severity**: `CRITICAL`
- **Governing Spec**: CAL-DOC-007 §1.3, §2.4.3; CAL-DOC-005 ADR-010
- **Affected Modules**: `sia_core/evaluation/diagnosis.py`, `tests/property/test_tc_a02_star_graph.py`, `tests/property/test_tc_a03_unique_diagnosis.py`
- **Problem Description**: In v2.1, the G4-I fault isolation rule stated: *"Exclude an origin class if and only if the minimum-cardinality diagnosis is unique."* This rule is mathematically unsound. In symmetric or multi-fault conflict graphs, a unique minimum-cardinality diagnosis can exist while an alternative, equally plausible diagnosis with higher or equal cardinality within the fault budget $f$ is also admissible. Excluding the unique minimum origin silently risks discarding an honest origin class.
- **Mathematical Proof & Counter-Example**:
  - *Setting*: Consider a conflict graph forming a star graph with center node $A$ and leaf nodes $B$ and $C$. Fault budget $f = 2$.
  - *Conflict Edges*: $E = \{(A, B), (A, C)\}$. Node $B$ and Node $C$ agree with each other ($|v_B - v_C| \le \eta$).
  - *Admissible Diagnoses*: An admissible diagnosis $D$ is any set of origins with $|D| \le f = 2$ that hits every conflict edge in $E$.
    - Diagnosis $D_1 = \{A\}$ has cardinality 1. Hits $(A, B)$ and $(A, C)$. Admissible.
    - Diagnosis $D_2 = \{B, C\}$ has cardinality 2. Hits $(A, B)$ and $(A, C)$. Admissible since $|D_2| \le f = 2$.
  - *The Flaw in v2.1*: The minimum-cardinality diagnosis is $D_1 = \{A\}$, and it is unique (no other diagnosis has cardinality 1). Under v2.1, origin $A$ would be excluded. However, if origin $A$ was honest ($v_A = x^*$) while origins $B$ and $C$ suffered correlated faults ($v_B, v_C \ne x^*$), $D_2$ is the true fault set! Excluding $A$ would falsely discard honest evidence.
- **Normative Resolution (Theorem G4-I)**:
  - Let $\mathcal{D}_{\min}$ be the set of **inclusion-minimal admissible diagnoses** (hitting sets of conflict pairs of size $\le f$).
  - By Theorem G4-I, the true faulty origin set $F$ must contain an inclusion-minimal hitting set $D' \subseteq F$ with $|D'| \le |F| \le f$. Thus, $D' \in \mathcal{D}_{\min}$, which implies:
    $$\bigcap \mathcal{D}_{\min} \subseteq D' \subseteq F$$
  - Therefore, every origin in the **intersection** $\bigcap \mathcal{D}_{\min}$ is provably faulty.
  - **Evaluator Normative Rule**:
    1. Compute $\mathcal{D}_{\min}$ using bounded search tree $O(b^f)$.
    2. Exclude **only** origin classes in $\bigcap \mathcal{D}_{\min}$.
    3. If $\mathcal{D}_{\min} = \emptyset$: Fault budget exceeded $\to$ emit state `SUSPECT` with witness `FAULT_BUDGET_EXCEEDED`; exclude nothing.
    4. If $\bigcap \mathcal{D}_{\min} = \emptyset$: Diagnosis ambiguous $\to$ emit state `SUSPECT` with witness `DIAGNOSIS_AMBIGUOUS`; exclude nothing.
    5. In the star graph counter-example: $\mathcal{D}_{\min} = \{\{A\}, \{B, C\}\} \implies \bigcap \mathcal{D}_{\min} = \emptyset$. Origin $A$ is **not** excluded.
- **Verification Obligation**: Test Case `TC-A02` (Star graph counter-example), `TC-A03` (Unique intersection isolation), `TC-A04` (Fault budget exceeded).

---

### D-02: Conflation of Value Validity Premise ($\eta \ge 2\varepsilon_h$) with Completeness
- **Title**: Conflation of Triangle Inequality Validity with Honest Atom Completeness
- **Severity**: `HIGH`
- **Governing Spec**: CAL-DOC-007 §1.3, §2.4.2, §2.4.3
- **Affected Modules**: `sia_core/evaluation/validity.py`, `tests/property/test_tc_a05_liar_bound.py`
- **Problem Description**: In v2.1, the premise $\eta \ge 2\varepsilon_h$ was stated as a requirement for value validity (Theorem G4-V). This was mathematically incorrect. Confusing validity with completeness obscured which assumption protects against which failure mode and weakened the formal rigor of the specification.
- **Mathematical Proof & Evidence**:
  - *Theorem G4-V (Value Validity)*: Let $Q$ be a pairwise-agreeing ($|v_a - v_b| \le \eta$) and pairwise origin-disjoint set with $|Q| \ge f + 1$.
  - Since at most $f$ origin classes are faulty, $Q$ must contain at least one honest atom $h$ with $|v_h - x^*| \le \varepsilon_h$.
  - For every $q \in Q$:
    $$|v_q - x^*| \le |v_q - v_h| + |v_h - x^*| \le \eta + \varepsilon_h$$
  - This bound holds purely from the triangle inequality and $|Q| \ge f + 1$, **without requiring** $\eta \ge 2\varepsilon_h$.
  - *Lemma C (Completeness)*: The condition $\eta \ge 2\varepsilon_h$ is required **only** to guarantee that two honest atoms never conflict:
    $$|v_{h1} - v_{h2}| \le |v_{h1} - x^*| + |x^* - v_{h2}| \le \varepsilon_h + \varepsilon_h = 2\varepsilon_h \le \eta$$
  - If $\eta < 2\varepsilon_h$, honest atoms could conflict due to measurement noise, violating the hitting set premise of G4-I.
- **Normative Resolution**: Formally decouple Theorem G4-V (value validity bound $\eta + \varepsilon_h$) from Lemma C (completeness of honest agreement). Contracts record $\eta$ and $\varepsilon_h$ explicitly via `calibration_ref`.
- **Verification Obligation**: Test Case `TC-A05` (Undetectable consistent liar respecting G4-V bound).

---

### D-03: Non-Monotonicity of Leaf Evaluation in Envelopes ("Worst/Best Completion" Assumption)
- **Title**: Unsound Assumption of Monotonic Evidence Improvement in Envelopes
- **Severity**: `CRITICAL`
- **Governing Spec**: CAL-DOC-007 §1.3, §2.7; CAL-DOC-006 §2
- **Affected Modules**: `sia_core/envelope/completion.py`, `sia_core/verdict/envelope.py`, `tests/property/test_tc_p06_envelope_soundness.py`
- **Problem Description**: v2.1 assumed that late-arriving evidence could only improve a verdict, defining an envelope via "worst completion" (empty slot) and "best completion" (all pending evidence present and agreeing). However, adding an evidence atom can **downgrade** an existing verdict (e.g., an atom arriving with a conflicting value downgrades `HEALTHY` to `SUSPECT`). Monotonicity does not hold when adding atoms.
- **Mathematical Proof & Evidence**:
  - Let existing support set be $Q = \{a\}$ with grade `HEALTHY`.
  - Let a pending observation arrive as atom $b$ from an independent origin.
  - If $b$ conflicts with $a$ ($|v_a - v_b| > \eta$) and fault budget $f = 0$, the state transitions from `HEALTHY` to `SUSPECT`.
  - Under v2.1's "best completion", the upper bound would be computed assuming $b$ agrees, falsely claiming the lower bound is already `HEALTHY`. The envelope would fail to bound the actual outcome.
- **Normative Resolution**: Replace the scalar worst/best completion with an **Abstract Completion Model** (§2.7.2).
  - An abstract completion maps each pending observation to either `ABSENT` or an agreement pattern tuple `(agreement_pattern ⊆ existing, grade, time_class)`.
  - The enumeration **must include conflict patterns**.
  - Lower bound = $\min_{\text{completions}} \sigma$; Upper bound = $\max_{\text{completions}} \sigma$.
  - If completions exceed $C_{\max}$ (default $C_{\max} = 32$), emit conservative fail-closed envelope `[DEGRADED, HEALTHY]` with witness `ENVELOPE_CAPPED`.
- **Verification Obligation**: Test Case `TC-P06` (Envelope soundness via exhaustive enumeration on small domains), `TC-P07` (Early-collapse soundness), `TC-S01` (Search cap overflow).

---

### D-04: Premature Expiry from Scalar Horizon on Compound Contracts (`OR` / `k-of-n`)
- **Title**: Premature Expiry of Compound Contracts via Scalar Horizon
- **Severity**: `HIGH`
- **Governing Spec**: CAL-DOC-007 §1.3, §2.5.3
- **Affected Modules**: `sia_core/time/timeline.py`, `sia_core/verdict/timeline.py`, `tests/property/test_tc_p02_timeline_monotonicity.py`
- **Problem Description**: In v2.1, contract validity horizon was represented by a single scalar timestamp `healthy_until = min(child_healthy_until)`. In compound contracts containing `OR` or `k-of-n` logic, this prematurely expired valid verdicts. If Child 1 expires at $t_1$, but Child 2 remains valid until $t_2 > t_1$, the compound `OR` contract remains valid until $t_2$. A scalar horizon collapsed the lifespan of the parent to $t_1$.
- **Mathematical Proof & Evidence**:
  - Let formula be $\Phi = \text{OR}(C_1, C_2)$.
  - At evaluation instant $t_0$, $C_1(t)$ is `HEALTHY` for $t \in [t_0, t_1)$, then `DEGRADED`.
  - $C_2(t)$ is `HEALTHY` for $t \in [t_0, t_2)$ where $t_2 > t_1$.
  - At any $t \in [t_1, t_2)$, $\Phi(t) = \max(C_1(t), C_2(t)) = \max(\text{DEGRADED}, \text{HEALTHY}) = \text{HEALTHY}$.
  - A scalar horizon $\min(t_1, t_2) = t_1$ falsely invalidates $\Phi$ at $t_1$.
- **Normative Resolution**: Replace scalar horizon with a **Pointwise Validity Timeline** $\sigma(t)$.
  - Each leaf emits a non-increasing step function $\sigma(t)$ over time breakpoints.
  - Compound contracts compose child timelines **pointwise**:
    $$\sigma_{\text{AND}}(t) = \min_i \sigma_i(t), \quad \sigma_{\text{OR}}(t) = \max_i \sigma_i(t), \quad \sigma_{k\text{-of-}n}(t) = k\text{-th largest}_i \sigma_i(t)$$
  - Pointwise lattice operations preserve the non-increasing property.
  - The verdict publishes the full `validity_timeline` breakpoints and `next_transition_at`. `healthy_until` is derived as the earliest breakpoint where state drops below `HEALTHY`.
- **Verification Obligation**: Test Case `TC-P02` (Pointwise composed timeline monotonicity).

---

### D-05: Inconsistent Definitions of `UNKNOWN` vs `DEGRADED`
- **Title**: Semantic Inconsistency of Inadmissible Evidence States
- **Severity**: `HIGH`
- **Governing Spec**: CAL-DOC-007 §1.3, §2.5.1; CAL-DOC-006 §4.1
- **Affected Modules**: `sia_core/models/states.py`, `docs/06_Core_Integrity_Kernel_Concept.md`, `tests/property/test_tc_s04_state_algebra.py`
- **Problem Description**: CAL-DOC-006 §4.1 defined `DEGRADED` as "required dependency unavailable", while v2.1 treated `UNKNOWN` as missing/pending evidence and `DEGRADED` as poor quality. Across documents, the boundaries between pending evidence and failed evidence were blurred, preventing deterministic state algebra.
- **Normative Resolution**: Establish strict, orthogonal, **slot-based definitions** over the admissibility total order $\text{DEGRADED} < \text{UNKNOWN} < \text{SUSPECT} < \text{HEALTHY}$:
  - `UNKNOWN`: The slot is still open (arrival deadline has not passed), or a required reference fact is pending resolution. (Evidence basis is pending/provisional).
  - `DEGRADED`: Relevant evidence is no longer admissible: slot arrival deadline closed without evidence, evidence has hard-expired, origin is `STALLED`, or state overflow occurred. (Evidence basis has definitively failed).
  - `SUSPECT`: Evidence exists and is admitted, but exhibits conflict, weak corroboration, soft expiry, or provenance limitations.
  - `HEALTHY`: All mandatory conditions, corroboration depths, and freshness constraints hold under declared assumptions.
  - Complete 16-cell truth tables: $\text{AND}(U, D) = D$; $\text{OR}(U, D) = U$.
- **Verification Obligation**: Test Case `TC-S04` (Complete 16 AND / 16 OR state algebra matrix).

---

### D-06: Event-Time Timers Cannot Fire Under Stationary Watermarks
- **Title**: Watermark Stall Deadlock on Silent Feed Expiry
- **Severity**: `CRITICAL`
- **Governing Spec**: CAL-DOC-007 §1.3, §2.2.5; CAL-DOC-005 ADR-008
- **Affected Modules**: `flink/supervisor.py`, `sia_core/liveness/run_clock.py`, `flink/job.py`, `tests/property/test_tc_a11_silent_source.py`
- **Problem Description**: In Apache Flink, event-time timers fire only when watermarks advance. When an upstream source goes silent (the precise failure mode a health guard is designed to detect), no incoming events arrive and the event-time watermark halts. v2.1 forbade synthetic watermarks but provided no mechanism to fire expiry timers, leaving verdicts stuck indefinitely in `HEALTHY` or `UNKNOWN` inside streaming state.
- **Normative Resolution**: Formulate **Run-Clock Facts ("Time is Data")**:
  - Passage of time enters the streaming engine as durable, recorded stream facts:
    $$\text{RunClockTick}\{\text{run\_id}, \text{clock\_domain}, \text{clock\_utc}, \text{seq}, \text{emitter\_id}\}$$
  - Ticks are emitted periodically by an independent supervisor process every `guard_period` (default 1000ms).
  - Ticks are durable inputs with deterministic identity $(\text{run\_id}, \text{seq})$. They **never advance Flink watermarks**.
  - Evaluation instant $t_{\text{eval}}$ is passed explicitly:
    $$t_{\text{eval}} = \max(\text{frontier from admitted evidence}, \text{latest admitted RunClockTick})$$
  - Expiry and slot closure evaluate against $t_{\text{eval}}$, guaranteeing that silent feeds degrade on schedule even when watermarks are completely stationary.
- **Verification Obligation**: Test Case `TC-A11` (Silent source with stationary watermark).

---

### D-07: Shared Transport Axis, Watermark, and Wall-Clock Leases Between Live and Replay
- **Title**: Transport Axis and Clock Domain Collisions Between Live and Replay
- **Severity**: `CRITICAL`
- **Governing Spec**: CAL-DOC-007 §1.3, §2.2.4, §2.8.2; CAL-DOC-005 ADR-011
- **Affected Modules**: `flink/job.py`, `sia_core/verdict/lease.py`, `tests/property/test_tc_a12_lease_expiry.py`
- **Problem Description**: In v2.1, live and replay traffic shared the same transport axis and Flink watermark, and leases were calculated using system wall-clock time. If a historical session was replayed, historical timestamps stalled live watermarks. Furthermore, replayed verdicts emitted with wall-clock leases appeared instantly expired to downstream consumers (`lease_until < now`).
- **Normative Resolution**:
  - **Transport Axis Isolation**: LIVE and REPLAY runs execute in separate Flink job instances.
  - **Dual-Clock Leases**: Leases explicitly declare `lease_clock_domain`: `LIVE_UTC` vs `REPLAY_VIRTUAL`.
  - In `REPLAY_VIRTUAL`, leases advance according to the replay controller's virtual clock ticks, ensuring replayed verdicts remain valid during simulation and replay analysis.
- **Verification Obligation**: Test Case `TC-A12` (Consumer lease expiry behavior across clock domains).

---

### D-08: Over-Claimed Replay Determinism Due to Revision Paths and Watermark Skew
- **Title**: Replay Non-Determinism from Hash Churn and Partition Interleaving
- **Severity**: `HIGH`
- **Governing Spec**: CAL-DOC-007 §1.3, §2.6.2, §2.6.3; CAL-DOC-005 ADR-009
- **Affected Modules**: `sia_core/identity/hasher.py`, `persistence/admission_log.py`, `tests/property/test_tc_s07_replay_determinism.py`
- **Problem Description**: v2.1 over-claimed that restarting and replaying raw Kafka records would always produce identical verdict hashes. This failed because: (a) intermediate verdict hashes included `previous_verdict_hash` and `revision_number`, which depend on network arrival order of provisional revisions; and (b) whether an atom arriving near the lateness boundary was classified as late depended on non-deterministic partition watermark interleaving.
- **Normative Resolution**:
  - Decouple **Content Hash** (`verdict_hash`) from **Revision Link** (`previous_verdict_hash`, `revision_number`). The content hash depends solely on canonical contract, decision instance, canonical support atom IDs, canonical frontier, state, timeline, corroboration profile, maturity, and finality.
  - For `FINAL` verdicts, the knowledge frontier is fixed to the **canonical finality instant** ($\text{slot\_window\_end} + \text{allowed\_lateness}$).
  - Define two formal reproducibility tiers:
    - **R1 (Evaluator Determinism)**: Pure function reproducibility. Given identical inputs, identical hash is guaranteed.
    - **R2 (Pipeline Determinism)**: Replay from raw data reproduces FINAL hashes for all decisions with no atoms in the boundary band. Full pipeline reproducibility is guaranteed when replaying from the **recorded admission log** (`calibre.core.admission_log`).
- **Verification Obligation**: Test Case `TC-P10` (R1 determinism), `TC-S07` (Pipeline replay determinism R1/R2).

---

### D-09: Processor Build/Version Treated as Independent Processing Origin
- **Title**: False Corroboration via Parser Build and Transform Version Bumps
- **Severity**: `CRITICAL`
- **Governing Spec**: CAL-DOC-007 §1.3, §2.3.3
- **Affected Modules**: `sia_core/origin/manager.py`, `sia_core/admission/builder.py`, `tests/property/test_tc_a09_processor_builds.py`
- **Problem Description**: v2.1 treated processor build IDs or transform versions as distinct processing origins. Reprocessing a raw feed with a bumped patch version (e.g., `v1.0.1` vs `v1.0.0`) falsely yielded processing corroboration depth $\kappa_{\text{proc}} = 1$, even though both outputs derived from the identical parser family and algorithm.
- **Normative Resolution**:
  - Processing origin is strictly defined as `processor_family_id` + independent implementation lineage.
  - Different builds or transform versions of the same family **never corroborate**.
  - If a newer version processes the same raw event, it emits an atom carrying `supersedes_atom_id`. The newer atom **supersedes** (replaces) the older atom in active evaluation.
- **Verification Obligation**: Test Case `TC-A09` (Two builds of one parser family).

---

### D-10: Processors Assigning `quality_grade` Directly to Evidence Atoms
- **Title**: Violation of Business Invariant 7 via Distributed Quality Grading
- **Severity**: `MEDIUM`
- **Governing Spec**: CAL-DOC-007 §1.3, §2.9.3; CAL-DOC-002 Invariant 7
- **Affected Modules**: `processors/`, `sia_core/admission/grader.py`, `docs/02_Functional_Requirements.md`
- **Problem Description**: In v2.1, processors assigned `quality_grade` directly to evidence atoms. This violated CAL-DOC-002 Business Invariant 7 ("a processor finding is evidence, not a verdict"). Allowing upstream processors to set quality grades distributed integrity logic across uncontrolled parsers.
- **Normative Resolution**: Processors emit raw findings only. The SIA Core assigns `quality_grade` at admission using a centrally configured, versioned **Grading Policy** (`calibre.config.grading_policy`). Proposed default mapping: CRITICAL/HIGH $\to$ `DEGRADED`; MEDIUM $\to$ `SUSPECT`; LOW/INFO $\to$ `HEALTHY`. `Finding.confidence` is informational metadata only.
- **Verification Obligation**: Unit tests validating admission grading policy enforcement and processor decoupling.

---

### D-11: Pipeline Health Telemetry Termed "Operational Analytics"
- **Title**: Vocabulary Clash Over "Operational" Analytics
- **Severity**: `LOW`
- **Governing Spec**: CAL-DOC-007 §1.3, §2.10; CAL-DOC-002 FR-040/041
- **Affected Modules**: `analytics/`, `docs/02_Functional_Requirements.md`, `docs/05_System_Architecture_HLD_LLD.md`
- **Problem Description**: v2.1 used the term "operational analytics" for pipeline system health telemetry. This directly clashed with CAL-DOC-001..005, where "operational" refers to sporting rules, race director decisions, and on-track metrics.
- **Normative Resolution**: Partition analytics into three distinct, non-overlapping classes (§2.10):
  1. **Pipeline-Health Telemetry**: Ingestion lag, stalls, late counts, backpressure. Never gated by integrity verdicts (must operate during degraded data).
  2. **Race Metrics**: Derived sporting stats (sector deltas, lap times). Always computed; carries `integrity_status` as context.
  3. **Gated Rule Evaluation & Alerts**: Incident classification, track limits, sporting penalties. Gated: defers with `RULE_DEFERRED` when contract fails.
- **Verification Obligation**: Documentation review and architecture separation in analytics pipeline.

---

### D-12: Partition Key `(session_id, driver_id)` Colliding Across Concurrent Replays
- **Title**: Flink Key Space Collision Under Concurrent Historical Replays
- **Severity**: `HIGH`
- **Governing Spec**: CAL-DOC-007 §1.3, §2.11; CAL-DOC-004 §4.1
- **Affected Modules**: `flink/job.py`, `sia_core/identity/keys.py`
- **Problem Description**: Keying streaming state solely by `(session_id, driver_id)` caused state bleeding and collision when multiple replays of the same session ran concurrently or alongside a live session.
- **Normative Resolution**: Scope all streaming keys by `session_instance_id`:
  - LIVE: `session_instance_id = session_id`
  - REPLAY/TEST: `session_instance_id = session_id#run_id`
  - Partition keys: `(session_instance_id, driver_id)`.
  - Content hashes (`verdict_hash`) explicitly **exclude** `session_instance_id`, ensuring replay verdicts reproduce identical hashes to live runs.
- **Verification Obligation**: Unit and integration tests for Flink key selector isolation.

---

### D-13: Hashing Specified Without Cross-Language Canonical Specification
- **Title**: Cross-Language Hash Drift Between Python and Java
- **Severity**: `CRITICAL`
- **Governing Spec**: CAL-DOC-007 §1.3, §2.1.3; RFC 8785
- **Affected Modules**: `sia_core/identity/jcs.py`, `tests/vectors/`, `tests/property/test_tc_s06_cross_language_jcs.py`
- **Problem Description**: Ingestion and processors are implemented in Python while the evaluator is implemented in Java. Standard JSON serialization exhibits differences in key ordering, whitespace, float representations (e.g. `1.0` vs `1`), and Unicode escaping, causing identical data to produce different SHA-256 hashes.
- **Normative Resolution**:
  - Mandate **RFC 8785 JSON Canonicalization Scheme (JCS)** over UTF-8.
  - Floating-point numbers entering identity must be quantised to declared unit precision prior to serialization.
  - SHA-256 hash rendered in lowercase hexadecimal.
  - Deliver a shared test vector suite of $\ge 50$ cross-language vectors (Unicode, numbers, arrays, empty objects) as a mandatory CI gate for both Python and Java.
- **Verification Obligation**: Test Case `TC-S06` (Cross-language RFC 8785 conformance).

---

### D-14: Candidate Contracts Demanding Signals Unavailable in Free Phase 1 Sources
- **Title**: Infeasible Contract Requirements on Free Ingestion Feeds
- **Severity**: `MEDIUM`
- **Governing Spec**: CAL-DOC-007 §1.3, §4.3
- **Affected Modules**: `sia_core/contracts/`, `docs/04_API_and_Event_Contract.md`
- **Problem Description**: v2.1 candidate contracts included signals (such as live track-limit crossings and live high-frequency telemetry) that do not exist in free feeds (Pitwall SignalR, TracingInsights), causing the Phase 2 DEC gate to fail or rely on ungrounded synthetic data.
- **Normative Resolution**: Define formal **Availability Classes**:
  - Class A: Live free feeds and historical.
  - Class H: Historical TracingInsights only.
  - Class S: Synthetic only (reserved for adversarial fixtures; excluded from grounded count).
  - Require that $\ge 7$ of the 10 initial contracts are Class A or H. Ground 9 of 10 contracts in actual free feed parameters.
- **Verification Obligation**: Phase 0 capture verification and Phase 2 contract groundings.

---

### D-15: Absence of Origin Inventory Analysis Regarding Single Upstream FOM Origin
- **Title**: False Assumption of Multi-Source Independence on Free Feeds
- **Severity**: `HIGH`
- **Governing Spec**: CAL-DOC-007 §1.3, §2.4.5, §4.2; `docs/implementation/SIA_REPOSITORY_ASSESSMENT.md`
- **Affected Modules**: `sia_core/origin/inventory.py`, `sia_core/contracts/`, `tests/property/test_tc_a10_shared_upstream.py`
- **Problem Description**: v2.1 designed multi-source corroboration without recognizing that both Pitwall live topics and TracingInsights historical files originate from the same Formula One Management (FOM) timing infrastructure. Mandating source-disjoint corroboration on live feeds would cause all decisions to be permanently flagged `SUSPECT`.
- **Normative Resolution**:
  - Explicitly document that all free feeds share upstream origin class `FOM_TIMING_CORE`.
  - Default Phase 1 contracts set `source_min_depth = 0` (`INFORMATIONAL`).
  - Verdicts attach explicit limitation: `UNMODELLED_COMMON_CAUSE: shared F1 timing upstream`.
  - Multi-source corroboration and G4-I isolation are exercised via synthetic fault-injection fixtures in Phase 1.
- **Verification Obligation**: Test Case `TC-A10` (Two providers sharing one upstream infrastructure).

---

### D-16: Unfulfilled Name Promises: Bitemporal and Calibration-Aware Semantics
- **Title**: Missing Formal Specifications for Bitemporal State and Calibration
- **Severity**: `HIGH`
- **Governing Spec**: CAL-DOC-007 §1.3, §2.12; CAL-DOC-005 ADR-008
- **Affected Modules**: `sia_core/time/bitemporal.py`, `persistence/verdict_log.py`, `tests/property/test_tc_s08_bitemporal_queries.py`
- **Problem Description**: The project charter promised "bitemporal" and "calibration-aware" evaluation, but v2.1 lacked explicit formal definitions, database schemas, or query APIs.
- **Normative Resolution**:
  - **Bitemporal Semantics**: Distinguish **Valid Time** $T_v$ (decision event window) from **Knowledge Time** $T_k$ (frontier / run-clock at publication). Store verdicts in append-only history keyed by `(decision_instance_id, valid_window, knowledge_time, revision)`. Implement "As-of $K$" point-in-time state reconstruction queries.
  - **Calibration-Aware Semantics**: Contract tolerances ($\eta, \varepsilon_h$) carry `calibration_ref`. Free sources lack sensor calibration, so verdicts attach explicit limitation `CALIBRATION_NOT_AVAILABLE`.
- **Verification Obligation**: Test Case `TC-S08` (Bitemporal query execution).

---

### D-17: Contradictory Readiness Gates Between v2.1 and CAL-DOC-006 §9
- **Title**: Deadlock in Specification vs Scaffold Readiness Gating
- **Severity**: `MEDIUM`
- **Governing Spec**: CAL-DOC-007 §1.3, §6.1; CAL-DOC-006 §9
- **Affected Modules**: `PROJECT.md`, `docs/06_Core_Integrity_Kernel_Concept.md`
- **Problem Description**: v2.1 scheduled detailed core specification work immediately, but CAL-DOC-006 §9 decreed that detailed core design could only start after the complete Flink streaming scaffold readiness gate was passed. This created an unexecutable circular dependency.
- **Normative Resolution**: Split the monolithic readiness gate into three sequential, decoupled gates (§6.1):
  1. **`G-SPEC`**: Mathematical specification accepted $\to$ allows writing contracts, fixtures, and hash vectors.
  2. **`G-EVAL`**: G-SPEC + Phase 2 passed $\to$ allows building the zero-dependency pure evaluator library.
  3. **`G-FLINK`**: G-EVAL passed + scaffold operational + Phase 0 measurements complete $\to$ allows embedding evaluator into Flink.
- **Verification Obligation**: Milestone progression alignment in `PROJECT.md`.

---

### D-18: Unbounded NP-Hard Complexity in Maximum Agreeing Support Set Search
- **Title**: Algorithmic Latency Explosion in Support Set Clique Search
- **Severity**: `HIGH`
- **Governing Spec**: CAL-DOC-007 §1.3, §2.4.1
- **Affected Modules**: `sia_core/envelope/clique.py`, `sia_core/evaluation/leaf.py`, `tests/property/test_tc_s01_search_cap_overflow.py`
- **Problem Description**: Determining the maximum pairwise-agreeing, origin-disjoint support set is equivalent to the Maximum Clique problem, which is NP-hard. In adversarial or dense conflict graphs, exhaustive search risks exponential latency ($O(2^N)$), causing streaming pipeline backpressure or thread starvation.
- **Normative Resolution**: Bound the search space:
  - Exact clique search is executed only up to $M_{\max}$ atoms per leaf (empirically frozen at $M_{\max} = 10$).
  - If atoms exceed $M_{\max}$, the evaluator reports a conservative lower bound and emits witness `SEARCH_BUDGET_EXCEEDED`; state fails closed to $\le \text{UNKNOWN}$.
- **Verification Obligation**: Test Case `TC-S01` (Search cap overflow).

---

### D-19: Verdict Hash Churn from Non-Semantic Witness Order Changes
- **Title**: Spurious Revision Explosion from Witness Selection Oscillation
- **Severity**: `MEDIUM`
- **Governing Spec**: CAL-DOC-007 §1.3, §2.5.4
- **Affected Modules**: `sia_core/verdict/witness.py`, `tests/property/test_tc_a07_k_of_n_ties.py`
- **Problem Description**: When multiple maximal support sets exist with identical cardinality and state, non-deterministic or unstable tie-breaking causes witness selection to flip between evaluations, producing spurious revisions and mutating verdict hashes without semantic change.
- **Normative Resolution**: Enforce deterministic tie-breaking:
  - Support set selection breaks ties using canonical atom order: `(transport_event_time_utc, evidence_atom_id)`.
  - Phase 7 benchmarks monitor revisions per decision to prove zero spurious churn.
- **Verification Obligation**: Test Case `TC-A07` ($k$-of-$n$ canonical tie-breaking).

---

### D-20: Lack of Re-Evaluation Mechanism for Dynamic Contract Updates via Flink Broadcast State
- **Title**: Inability to Trigger Keyed Re-Evaluation on Broadcast Contract Updates
- **Severity**: `HIGH`
- **Governing Spec**: CAL-DOC-007 §1.3, §2.11; Flink Architecture
- **Affected Modules**: `flink/job.py`, `flink/spike_s1/`
- **Problem Description**: In Apache Flink, broadcast process functions cannot register keyed timers. When a new DEC contract version is broadcast, open decision slots in keyed state could not be re-evaluated without an incoming event for that key.
- **Normative Resolution**: Implement the **Dirty-Flag + Run-Clock Tick Pattern**:
  - Broadcast updates mark affected keyed decision states as `dirty`.
  - When periodic `RunClockTick` facts or subsequent keyed events arrive, the evaluator detects the dirty flag and re-evaluates active decisions against the new contract version.
  - Empirically verified in Phase 0 Spike S-1.
- **Verification Obligation**: Flink Spike S-1 execution and contract update tests.

---

## 4. Comprehensive Catalog of 9 Specification Conflicts (C-01 to C-09)

| ID | Baseline Document | Specific Conflict | Normative Resolution in SIA v2.2.1 | Baseline Amendment Required |
|---|---|---|---|---|
| **C-01** | CAL-DOC-004 §10 | CAL-DOC-004 defines `decision_maturity` as a 3-value enum `{PROVISIONAL, FINAL, REVISED}`. v2.1 split this into four orthogonal fields. | Adopt the four orthogonal fields: `integrity_state`, `resolution_maturity`, `finality_status`, `revision_number`. Retain `decision_maturity` as a **derived display field** for legacy consumers (§2.6.4). | Amend CAL-DOC-004 §10 to specify `IntegrityAssessment` schema v2.0 with the four orthogonal fields. |
| **C-02** | CAL-DOC-004 §5, §6, §14 | CAL-DOC-004 uses `event_id` ("uuid or stable source ID") and specifies `deduplication_key`. v2.1 introduced `raw_event_id` and `evidence_atom_id`. | `event_id` remains an operational envelope ID (UUID permitted). `raw_event_id` is the deterministic content hash of source payload and serves as the normative `deduplication_key` (§2.1.1). | Amend CAL-DOC-004 §5/§6/§14 to define `raw_event_id` and assign it as `deduplication_key`. |
| **C-03** | CAL-DOC-004 §8 | CAL-DOC-004 defines `Finding.confidence` as a float $[0, 1]$. v2.1 strictly forbade single confidence numbers. | `confidence` is retained as processor-supplied **informational metadata only**. It is NEVER an input to the Scoped Integrity Algebra (§2.9.3). | Amend CAL-DOC-004 §8 to designate `Finding.confidence` as non-normative informational metadata. |
| **C-04** | CAL-DOC-006 §4.1 | CAL-DOC-006 defines `DEGRADED` as "required dependency unavailable". v2.1 defined `UNKNOWN` and `DEGRADED` inconsistently. | Adopt slot-based definitions: `UNKNOWN` = slot open / reference pending; `DEGRADED` = slot closed empty / hard expired / stalled origin (§2.5.1). | Amend CAL-DOC-006 §4.1 to replace dependency status with the 4-state admissibility total order. |
| **C-05** | CAL-DOC-006 §9 | CAL-DOC-006 requires full Flink scaffold readiness before detailed core design begins. v2.1 scheduled spec work immediately. | Monolithic gate replaced by three decoupled gates: `G-SPEC` (spec, contracts), `G-EVAL` (pure evaluator), `G-FLINK` (streaming embedding) (§6.1). | Amend CAL-DOC-006 §9 to replace monolithic gate with the G-SPEC / G-EVAL / G-FLINK progression. |
| **C-06** | CAL-DOC-002 FR-040/041 | CAL-DOC-002 uses "operational metrics" for race rules. v2.1 used "operational analytics" for pipeline health telemetry. | Three distinct analytics classes defined: Pipeline-Health Telemetry, Race Metrics, and Gated Rule Evaluation (§2.10). | Amend CAL-DOC-002 FR-040/041 and CAL-DOC-005 to adopt the three distinct analytics classes. |
| **C-07** | CAL-DOC-003 §8.1 | CAL-DOC-003 defines source health states (`HEALTHY`, `STALE`, `DISCONNECTED`, `ERROR`). v2.1 introduced guard facts (`STALLED`, `RESUMED`). | Map the two vocabularies: `STALLED` maps to `STALE`; `DISCONNECTED` and `ERROR` originate from ingestion adapters. Guard facts provide durable stream records (§2.8.1). | Amend CAL-DOC-003 §8.1 with formal vocabulary mapping to `GuardFact`. |
| **C-08** | CAL-DOC-004 §4.1 | CAL-DOC-004 specified partition key `session_id:driver_id`, causing collisions during concurrent replays. | Amend partition key to `session_instance_id[:driver_id]`, where `session_instance_id = session_id` (LIVE) and `session_id#run_id` (REPLAY) (§2.11). | Amend CAL-DOC-004 §4.1 and CAL-DOC-005 §8 to require `session_instance_id`. |
| **C-09** | CAL-DOC-004 §3 | CAL-DOC-004 omitted Kafka topics for contracts, liveness guard facts, run-clock ticks, admission logs, and verdict history. | Add 5 normative topics: `calibre.config.contracts`, `calibre.guard.liveness`, `calibre.guard.clock`, `calibre.core.admission_log`, `calibre.core.verdict_history` (§9.3). | Amend CAL-DOC-004 §3 topic catalogue to add all 5 missing topics. |

---

## 5. Open Items Register (O-01 to O-11)

| ID | Description | Fallback / Proposed Bound | Gating Milestone | Recommended Engineering Resolution |
|---|---|---|---|---|
| **O-01** | Feed-specific watermark strategy, allowed lateness, and `guard_period` | Allowed lateness = 45s, `guard_period` = 1000ms | Phase 0 measurements | Measured in Spike S-1: $W_{\text{out\_of\_order}} = 2.0\text{s}$, $T_{\text{idleness}} = 15.0\text{s}$ (recalibrated to avoid false idle flapping on ~9.5s nominal telemetry gaps), $T_{\text{guard\_period}} = 1000\text{ms}$. |
| **O-02** | Search and state bounds: $M_{\max}$, $C_{\max}$, and per-key state caps | $M_{\max} = 10$ atoms, $C_{\max} = 32$ completions | Phase 0 / Phase 7 | Empirically verified in Spike S-1: $M_{\max} = 10$ keeps clique search $< 0.1\text{ms}$. |
| **O-03** | Admission grading policy threshold mapping | CRITICAL/HIGH $\to$ DEGRADED; MEDIUM $\to$ SUSPECT; LOW/INFO $\to$ HEALTHY | Phase 2 fixtures | Formalize mapping table in `calibre.config.grading_policy` schema. |
| **O-04** | Proposed residual risk expression $p^{\kappa+1} + \beta \cdot p$ | Bypassed; marked `[OPEN]` | Formal justification | Do not expose scalar probability in verdicts until rigorous stochastic dependency calibration is proven. |
| **O-05** | Running live and replay concurrently in a single Flink job on virtual axes | Forbidden; separate jobs mandated | Spike S-1 | Spike S-1 Protocol D proved catastrophic 1-year watermark skew. Mandate separate Flink job instances. |
| **O-06** | Flink contract update re-evaluation mechanism | Dirty-flag marking + tick trigger | Spike S-1 | Prototype in Spike S-1 confirmed: O(1) dirty-flag check takes 1.3 microseconds per tick. |
| **O-07** | Verdict hash churn from witness-only selection changes | Canonical sorting key tie-break | Phase 7 benchmarks | Monitor revision rates; verify that non-semantic witness order changes do not emit spurious revisions. |
| **O-08** | Finality rule for feeds that legitimately emit post-race timing corrections | Post-finality quarantine in admission log | Phase 2 contracts | Define explicit contract parameter `reopen_on_official_correction = true/false`. |
| **O-09** | Calibration procedure for tolerances $\eta, \varepsilon_h$ from empirical captures | Explicit `CALIBRATION_NOT_AVAILABLE` | Phase 0 / 2 | Compute standard deviations on historical timing data to establish statistically sound $\eta$ values. |
| **O-10** | Live Pitwall SignalR topic coverage across all DEC contract classes | Expected coverage documented | Phase 0 captures | Pitwall live capture confirmed coverage of TimingData, TimingAppData, SessionInfo, SessionStatus, TrackStatus, WeatherData, RCM. |
| **O-11** | Patent strategy and intellectual property claims | Technical-effect framing (CRI 2025) | Legal counsel review | Base patentability claims on concrete technical effects (deterministic replay, bounded state, stationary watermark expiry). |

---

## 6. Baseline Documentation Amendments (ERR-01 to ERR-07)

### ERR-01: Amendment to CAL-DOC-006 §4.1 (State Definitions)
- **Target File**: `docs/06_Core_Integrity_Kernel_Concept.md` §4.1
- **Old Text**: "DEGRADED: Required dependency or upstream feed is unavailable or failing."
- **Replacement Text**: "DEGRADED: Relevant evidence is no longer admissible: slot arrival deadline closed without admissible evidence, evidence has hard-expired, origin instance is STALLED, or state overflow occurred. States adhere strictly to the total order DEGRADED < UNKNOWN < SUSPECT < HEALTHY."

### ERR-02: Amendment to CAL-DOC-004 §10 (Integrity Assessment Schema)
- **Target File**: `docs/04_API_and_Event_Contract.md` §10
- **Old Text**: "`decision_maturity` enum: PROVISIONAL, FINAL, REVISED."
- **Replacement Text**: "IntegrityAssessment v2.0 defines four orthogonal fields: `integrity_state`, `resolution_maturity` (OPEN, STATE_RESOLVED, TUPLE_RESOLVED, REOPENED), `finality_status` (OPEN, FINAL, FINALITY_VIOLATION), and monotonic integer `revision_number`. `decision_maturity` is retained strictly as a derived display field."

### ERR-03: Amendment to CAL-DOC-002 FR-040 and FR-041 (Analytics Classification)
- **Target File**: `docs/02_Functional_Requirements.md` §5 (FR-040, FR-041)
- **Old Text**: "System shall provide operational analytics for pipeline events and findings."
- **Replacement Text**: "Analytics shall be strictly partitioned into three non-overlapping classes: (1) Pipeline-Health Telemetry (never gated by integrity); (2) Race Metrics (always computed, carries integrity context); and (3) Gated Rule Evaluation (defers with RULE_DEFERRED when contract conditions fail)."

### ERR-04: Amendment to CAL-DOC-004 §3 (Kafka Topic Registry)
- **Target File**: `docs/04_API_and_Event_Contract.md` §3
- **Action**: Add 5 normative topics:
  - `calibre.config.contracts`: Compacted topic for versioned DEC contracts.
  - `calibre.guard.liveness`: Durable `GuardFact` (`STALLED` / `RESUMED`) stream.
  - `calibre.guard.clock`: Durable `RunClockTick` facts for deterministic expiry.
  - `calibre.core.admission_log`: Append-only admitted/quarantined atom ledger.
  - `calibre.core.verdict_history`: Append-only bitemporal verdict revision log.

### ERR-05: Amendment to CAL-DOC-002 (Addition of FR-052 through FR-062)
- **Target File**: `docs/02_Functional_Requirements.md` §5
- **Action**: Formally adopt requirements FR-052 through FR-062 from CAL-DOC-007 §9.2 into CAL-DOC-002 (DEC versioning, separate corroboration reporting, audit witness sets, consumer leases, durable guard facts, reproducible content hashes, fail-closed overflow, bitemporal queries, live/replay isolation, admission grading policy).

### ERR-06: Amendment to CAL-DOC-003 (Addition of SRS Non-Functional Requirements)
- **Target File**: `docs/03_System_Requirements_Specification.md` §7
- **Action**: Add NFR-DET (R1 evaluator determinism and R2 admission-log replay determinism), NFR-STATE (active state memory caps and fail-closed overflow), NFR-CLOCK (dual-clock domains and 'Time as Data' run-clock ticks), and NFR-LEASE (verdict consumer lease contracts).

### ERR-07: Amendment to CAL-DOC-005 (Addition of Architecture Decision Records)
- **Target File**: `docs/05_System_Architecture_HLD_LLD.md` §11
- **Action**: Add:
  - ADR-008: Typed-Time and Run-Clock Fact Model
  - ADR-009: Content Hash Decoupling and Reproducibility Levels R1/R2
  - ADR-010: Fault Isolation via Intersection of Minimal Admissible Diagnoses
  - ADR-011: Live vs Replay Transport Axis and Flink Job Isolation

---

## 7. Traceability & Verification Matrix

| Item ID | Category | Severity | Target Module | Verification Test Case | Acceptance Gate |
|---|---|---|---|---|---|
| **D-01** | Defect | CRITICAL | `sia_core/evaluation/diagnosis.py` | `TC-A02`, `TC-A03`, `TC-A04` | G-EVAL |
| **D-02** | Defect | HIGH | `sia_core/evaluation/validity.py` | `TC-A05` | G-EVAL |
| **D-03** | Defect | CRITICAL | `sia_core/envelope/completion.py` | `TC-P06`, `TC-P07`, `TC-S01` | G-EVAL |
| **D-04** | Defect | HIGH | `sia_core/time/timeline.py` | `TC-P02` | G-EVAL |
| **D-05** | Defect | HIGH | `sia_core/models/states.py` | `TC-S04` | G-EVAL |
| **D-06** | Defect | CRITICAL | `flink/supervisor.py`, `sia_core/liveness/` | `TC-A11` | G-FLINK |
| **D-07** | Defect | CRITICAL | `flink/job.py`, `sia_core/verdict/lease.py` | `TC-A12` | G-FLINK |
| **D-08** | Defect | HIGH | `sia_core/identity/hasher.py` | `TC-P10`, `TC-S07` | G-SPEC, G-FLINK |
| **D-09** | Defect | CRITICAL | `sia_core/origin/manager.py` | `TC-A09` | G-EVAL |
| **D-10** | Defect | MEDIUM | `sia_core/admission/grader.py` | Unit Grader Suite | G-EVAL |
| **D-11** | Defect | LOW | `analytics/` | Architecture Review | G-FLINK |
| **D-12** | Defect | HIGH | `flink/job.py`, `sia_core/identity/keys.py` | Replay Concurrency Test | G-FLINK |
| **D-13** | Defect | CRITICAL | `sia_core/identity/jcs.py` | `TC-S06` (50 Vectors) | G-SPEC |
| **D-14** | Defect | MEDIUM | `sia_core/contracts/` | 10 DEC Validations | G-SPEC |
| **D-15** | Defect | HIGH | `sia_core/origin/inventory.py` | `TC-A10` | G-SPEC |
| **D-16** | Defect | HIGH | `sia_core/time/bitemporal.py` | `TC-S08` | G-EVAL, G-FLINK |
| **D-17** | Defect | MEDIUM | `PROJECT.md` | Milestone Audit | G-SPEC |
| **D-18** | Defect | HIGH | `sia_core/envelope/clique.py` | `TC-S01` | G-EVAL |
| **D-19** | Defect | MEDIUM | `sia_core/verdict/witness.py` | `TC-A07` | G-EVAL |
| **D-20** | Defect | HIGH | `flink/job.py`, Spike S-1 | Spike S-1 Verification | G-FLINK |
| **C-01..09** | Conflict | HIGH | `docs/`, `docker-compose.yml`, contracts | Schema & Topic Validation | G-SPEC, G-FLINK |
| **O-01..11** | Open Item | MEDIUM | Config, benchmarks, legal | Benchmark Suite & Captures | G-FLINK, Freeze |
| **ERR-01..07** | Baseline | HIGH | `docs/01`..`docs/06` | Document Review | Final Acceptance |

---

## 8. Conclusion

The formal errata, conflicts, open items, and baseline amendments documented in this catalog establish the normative mathematical and architectural foundation for CALIBRE SIA v2.2.1. All subsequent implementation phases shall adhere strictly to the resolutions formulated in this document.
