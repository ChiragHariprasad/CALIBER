# CALIBRE Scoped Integrity Algebra (SIA) — Test Catalogue
**Document ID:** CAL-TEST-001  
**Governing Standard:** CAL-DOC-007 §16 & CAL-REP-001  
**Total Tests:** 741 tests across 11 test modules  
**Status:** 100% PASSING (0 Failures, 0 XFAILS, 0 Regressions)  
**Execution Time:** 21.57s  
**Date:** 2026-11-01  

---

## 1. Executive Test Summary

The Scoped Integrity Algebra (SIA) test suite enforces zero-tolerance verification across all algebraic invariants, canonical serialization, fault isolation theorems, streaming topologies, and durability layers.

| Test Layer | Module | Count | Pass Rate | Scope |
|:---|:---|:---:|:---:|:---|
| **Adversarial Scenarios** | `tests/test_30_adversarial_scenarios.py` | 30 | 100% | 30 mandatory adversarial cases from CAL-DOC-007 §16.4 |
| **G4 & Leases Adversarial** | `tests/test_g4_adversarial.py` | 36 | 100% | Bron-Kerbosch $M_{\max}=10$, hitting sets, $C_{\max}=32$, dual leases |
| **RFC 8785 JCS Adversarial** | `tests/test_rfc8785_adversarial.py` | 27 | 100% | Surrogates, float quantization, duplicate keys, whitespace |
| **Contract AST Adversarial** | `tests/test_adversarial_m1.py` | 66 | 100% | AST recursion depth, rules V-01..V-10, Byzantine pigeonhole |
| **Evaluator Unit & Invariants** | `tests/test_evaluator.py` | 26 | 100% | 4-state lattice, pointwise timelines, G4-V, G4-I, completions |
| **Golden Fixture Schema** | `tests/test_golden_fixtures.py` | 101 | 100% | Schema validation and integrity across FX-01 .. FX-20 |
| **Cross-Language Vectors** | `tests/test_rfc8785_vectors.py` | 123 | 100% | 55 RFC 8785 SHA-256 vectors against Python & Java canonical spec |
| **Flink Spike S-1 Empirical** | `tests/test_spike_s1_adversarial.py` | 16 | 100% | OS socket loopback, tracemalloc heap, Table 7 constants |
| **Contract Compile-Time** | `tests/test_contracts.py` | 33 | 100% | DEC YAML compilation, grammar AST, formula validation |
| **Streaming Verification** | `tests/test_flink_streaming.py` | 40 | 100% | Keyed operator, watermark timers, triple-sink routing, DLQ |
| **End-to-End Pipeline** | `tests/test_e2e_pipeline.py` | 17 | 100% | 11-stage pipeline, persistence, dashboard, analytics |
| **Golden Fixture Parity** | `tests/test_eval_parity.py` | 26 | 100% | **20/20 bit-for-bit SHA-256 verdict hash parity** |

---

## 2. 30 Mandatory Adversarial Scenarios (CAL-DOC-007 §16.4)

All 30 scenarios specified in CAL-DOC-007 §16.4 are implemented and verified in `tests/test_30_adversarial_scenarios.py`:

| Scenario ID | Test Function | Scenario Description | Expected Outcome | Verified Result |
|:---|:---|:---|:---|:---:|
| **TC-P01** | `test_tc_p01_monotonicity_state_and_timeline` | Lattice meet/join laws and non-increasing timeline steps | Commutative/absorptive; step to SUSPECT/DEGRADED | **PASS** |
| **TC-P02** | `test_tc_p02_scope_locality` | Scope locality (Invariant G2: multi-DEC independent evaluation) | Independent local judgments, zero leakage | **PASS** |
| **TC-P03** | `test_tc_p03_permutation_invariance` | Evidence atom arrival order permutation invariance | Bit-for-bit identical hash and state | **PASS** |
| **TC-P04** | `test_tc_p04_duplicate_idempotence` | Duplicate identical atoms in input stream | Invariant hash; uninflated $\kappa_{\text{src}}=0$ | **PASS** |
| **TC-P05** | `test_tc_p05_envelope_soundness` | Bounded envelope evaluation across pending completions | $\text{lower} \le \text{actual} \le \text{upper}$ | **PASS** |
| **TC-P06** | `test_tc_p06_early_collapse_soundness` | Definitive child failure in AND formula (Obligation P5) | Early collapse to STATE_RESOLVED | **PASS** |
| **TC-P07** | `test_tc_p07_valid_witness_references` | Validation that witness references exist in facts/atoms | Non-empty binding witness & audit set | **PASS** |
| **TC-P08** | `test_tc_p08_fail_closed_evaluation` | Missing reference fact or malformed timestamp syntax | Conversion refused, fail-closed UNKNOWN | **PASS** |
| **TC-P09** | `test_tc_p09_canonical_hash_identity` | RFC 8785 canonical serialization and SHA-256 | Deterministic 64-character hex hash | **PASS** |
| **TC-P10** | `test_tc_p10_bitemporal_revision_consistency` | In-window late arrival generating linked revision $H_1 \to H_2$ | Monotonic revision increment; prev hash pointer | **PASS** |
| **TC-A01** | `test_tc_a01_two_disagreeing_sources` | Independent sources $\|v_1 - v_2\| > \eta$ | Non-transitive clique split, SUSPECT | **PASS** |
| **TC-A02** | `test_tc_a02_star_graph_ambiguity_f2_excludes_nothing` | Star conflict graph with budget $f=2$ (D-01) | $\bigcap \mathcal{D}_{\min} = \emptyset$, exclude nothing | **PASS** |
| **TC-A03** | `test_tc_a03_unique_intersection_diagnosis_isolates_fault` | Graph with unique minimal hitting set (D-01) | Exclude $\bigcap \mathcal{D}_{\min} = \{A\}$, rest HEALTHY | **PASS** |
| **TC-A04** | `test_tc_a04_fault_budget_exceeded` | Conflicting origins exceeding budget $f$ (D-01) | `FAULT_BUDGET_EXCEEDED` witness, SUSPECT | **PASS** |
| **TC-A05** | `test_tc_a05_2f_plus_1_adversarial_graphs` | $2f+1$ adversarial resilience with 1 Byzantine liar (D-02) | Tolerates faulty node, maintains HEALTHY | **PASS** |
| **TC-A06** | `test_tc_a06_or_branches_coordinate_borrowing_prevention` | OR formula coordinate borrowing prevention | Disjoint evaluation paths isolated | **PASS** |
| **TC-A07** | `test_tc_a07_k_of_n_ties_canonical_tie_break` | Equal rank candidates in $k$-of-$n$ (D-19) | Deterministic canonical tie-break | **PASS** |
| **TC-A08** | `test_tc_a08_one_origin_many_derived_atoms_deduplication` | Fan-out from single source origin | Single origin credited, uninflated depth | **PASS** |
| **TC-A09** | `test_tc_a09_two_builds_of_one_parser_family_supersession` | Re-parse of same raw record (Defect D-09) | Supersession applied, $\kappa_{\text{proc}}$ uninflated | **PASS** |
| **TC-A10** | `test_tc_a10_two_providers_sharing_one_upstream` | Feeds sharing upstream timing infrastructure (D-15) | $\kappa_{\text{src}} = 0$, limitation witness attached | **PASS** |
| **TC-A11** | `test_tc_a11_silent_source_stationary_watermark_expiry` | Stalled feed with stationary watermark (D-06) | `RunClockTick` drives deterministic expiry | **PASS** |
| **TC-A12** | `test_tc_a12_guard_unavailable_consumer_lease_expiry` | Guard outage / lease expiry across clock domains (D-07) | Consumer lease elapsed, fail-closed UNKNOWN | **PASS** |
| **TC-S01** | `test_tc_s01_candidate_atom_overflow_with_conflict` | Moon-Moser clique candidate count $> M_{\max}=10$ (D-18) | Fail-closed $\le$ UNKNOWN, overflow witness | **PASS** |
| **TC-S02** | `test_tc_s02_envelope_completion_overflow` | Pending role combinations $> C_{\max}=32$ (D-03) | Conservative envelope [D, H], capped witness | **PASS** |
| **TC-S03** | `test_tc_s03_late_atom_inside_lateness_window_revises` | In-window late arrival introducing conflict | Revision emitted, $H_1 \to H_2$, state lowered | **PASS** |
| **TC-S04** | `test_tc_s04_late_atom_after_finality_quarantined` | Atom arriving after canonical finality (Invariant G11) | Atom quarantined, FINAL hash intact | **PASS** |
| **TC-S05** | `test_tc_s05_mandatory_evidence_slot_deadline_expiry` | Missing mandatory evidence slot | UNKNOWN $\to$ DEGRADED at deadline | **PASS** |
| **TC-S06** | `test_tc_s06_cross_language_rfc8785_jcs_hash_invariant` | Cross-language RFC 8785 serialization (D-13) | Identical 64-char hex SHA-256 digests | **PASS** |
| **TC-S07** | `test_tc_s07_replay_determinism_r1_and_r2` | Replay determinism R1 and log playback R2 | Bit-for-bit hash equality and FINAL replay | **PASS** |
| **TC-S08** | `test_tc_s08_bitemporal_as_of_knowledge_query` | Relational bitemporal As-Of-K knowledge queries (D-16) | State as known at historical knowledge time $K$ | **PASS** |

---

## 3. 20 Golden Fixture Families (`tests/fixtures/`)

| Fixture ID | Title | Key Algebraic Invariant | Schedules | Bit-for-Bit Hash |
|:---|:---|:---|:---:|:---:|
| `FX-01` | Mandatory Evidence Slot Deadline Expiry | UNKNOWN $\to$ DEGRADED at deadline | 1 | `00dbb61092b5...` |
| `FX-02` | Stale Evidence Aging | Soft age SUSPECT, hard age DEGRADED | 1 | `888474dfab70...` |
| `FX-03` | Hard Expired Evidence Atom | Support exclusion, audit retention | 1 | `606db26c968d...` |
| `FX-04` | Ambiguous Time Conversion | Missing reference $\implies$ fail-closed | 1 | `3f1f8688caa1...` |
| `FX-05` | Invalid Reference Fact | Malformed syntax $\implies$ fail-closed | 1 | `3e075f6c88b0...` |
| `FX-06` | Correlated Providers | Shared upstream $\implies \kappa_{\text{src}}=0$ | 1 | `4a4100e6f834...` |
| `FX-07` | Duplicated Processors | Parser deduplication $\implies \kappa_{\text{proc}}=0$ | 1 | `0def3583e266...` |
| `FX-08` | Two Builds of Same Parser Family | Version supersession (Defect D-09) | 1 | `bebc34571987...` |
| `FX-09` | Two Disagreeing Sources | Pairwise divergence $\implies$ SUSPECT | 1 | `3d1f637a861d...` |
| `FX-10` | Star-Graph Ambiguous Diagnosis | $\bigcap \mathcal{D}_{\min} = \emptyset$ excludes nothing | 1 | `7e0b876072f8...` |
| `FX-11` | Unique-Intersection Diagnosis | $\bigcap \mathcal{D}_{\min} = \{A\}$ isolates fault | 1 | `a217fc069e39...` |
| `FX-12` | Pending Origins Abstract Bounds | Envelope bounding under non-monotonicity | 1 | `2e2da737de9c...` |
| `FX-13` | Sound Early Collapse | P5 early collapse before slot deadline | 1 | `33e1ca81c6ce...` |
| `FX-14` | Late Revision | In-window revision links $H_1 \to H_2$ | 2 | `00dbb61092b5...`<br>`c75977a96263...` |
| `FX-15` | Post-Finality Atom Arrival | Quarantined; FINAL hash immutable | 1 | `631f28d49235...` |
| `FX-16` | Stationary Watermark Expiry | `RunClockTick` expiry (Defect D-06) | 1 | `d024120ca9bc...` |
| `FX-17` | Replay Virtual Clock Leases | Replay clock domain lease isolation | 1 | `2e9ddfee6b07...` |
| `FX-18` | Incompatible Fact Interpretations | Multi-DEC scope locality invariant | 1 | `7601fc9ed10b...` |
| `FX-19` | Candidate Atom Overflow | $M_{\max} = 10$ Bron-Kerbosch cap | 1 | `c92da11e64c1...` |
| `FX-20` | Cross-Language RFC 8785 JCS | UTF-16 BE, ES6 numbers, float scale 0 | 1 | `3bb456a41c78...` |

---

## 4. Test Execution Instructions

Execute individual test suites or the entire regression suite via `pytest`:

```bash
# Execute entire 741-test suite
pytest -q

# Execute all 30 mandatory adversarial scenarios
pytest tests/test_30_adversarial_scenarios.py -v

# Execute golden fixture bit-for-bit parity check
python tests/test_eval_parity.py

# Execute 55 cross-language RFC 8785 hash vectors
pytest tests/test_rfc8785_vectors.py -v

# Execute G4 theorem and clique budget suite
pytest tests/test_g4_adversarial.py -v
```
