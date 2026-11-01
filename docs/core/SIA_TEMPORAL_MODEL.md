# CALIBRE Scoped Integrity Algebra (SIA) — Temporal Model
**Document ID:** CAL-SPEC-004  
**Governing Standard:** CAL-DOC-007 §2.2, §2.5.3, §2.8  
**Implementation Modules:** `sia_core/time/types.py`, `sia_core/time/resolver.py`, `sia_core/time/timeline.py`, `sia_core/time/bitemporal.py`  
**Status:** Validated & Frozen  
**Date:** 2026-11-01  

---

## 1. Typed Time Classes & Semantic Eligibility

To prevent temporal misalignment and semantic errors (such as converting elapsed lap durations into absolute timestamps), all timestamps carry explicit type tags and unit representations:

| Time Class | Unit | Semantic Description | Eligible for Semantic Time DEC? |
|:---|:---:|:---|:---:|
| **`ABSOLUTE_UTC`** | ISO-8601 | Standard wall-clock or UTC instant | **Yes** |
| **`ABSOLUTE_WITH_OFFSET`** | ISO-8601 | UTC instant with explicit timezone offset | **Yes** |
| **`SESSION_ELAPSED`** | seconds | Elapsed seconds since session $t_0$ | **Yes** (session decisions) |
| **`LAP_RELATIVE`** | seconds | Elapsed seconds since current lap start | **Yes** (lap decisions) |
| **`LAP_DURATION`** | seconds | **Physical duration quantity, NOT a timestamp** | **NO** (Conversion forbidden) |
| **`RECEIPT_TIME_ONLY`** | UTC | Ingress packet arrival timestamp | **NO** (Operational metadata) |
| **`PROCESSING_TIME`** | UTC | Pipeline evaluation timestamp | **NO** (Operational metadata) |
| **`UNKNOWN`** | — | Unparsed or missing time semantics | **NO** (Fail-closed) |

---

## 2. Legal Coordinate Conversions & Uncertainty Propagation

Temporal conversions are strictly deterministic and require matching reference facts:

```text
               +----------------------+
               |    LAP_RELATIVE      |
               +----------+-----------+
                          |
                          | Requires: matching lap_start fact (lST)
                          v
               +----------------------+
               |   SESSION_ELAPSED    |
               +----------+-----------+
                          |
                          | Requires: session_t0 fact (t0_date)
                          v
               +----------------------+
               |     ABSOLUTE_UTC     |
               +----------------------+
```

### 2.1 Conversion Rules & Inheritance
1. **`LAP_RELATIVE` $\to$ `SESSION_ELAPSED`**: Requires `ReferenceFact` of type `lap_start_reference` matching `(session_id, driver_id, lap_number)`.
   * Result value: $v_{\text{session}} = v_{\text{lap}} + v_{\text{lap\_start}}$
   * Uncertainty: $[u_{\min, 1} + u_{\min, 2}, u_{\max, 1} + u_{\max, 2}]$ (Minkowski interval addition)
   * Origin set: $O_{\text{derived}} = O_{\text{atom}} \cup O_{\text{reference}}$
   * Quality grade: $\min(\text{grade}_{\text{atom}}, \text{grade}_{\text{reference}})$
2. **`SESSION_ELAPSED` $\to$ `ABSOLUTE_UTC`**: Requires `ReferenceFact` of type `session_t0_reference`.
   * Result value: $t_{\text{abs}} = t_0 + v_{\text{elapsed}}$
3. **`ABSOLUTE_UTC` $\to$ `SESSION_ELAPSED`**: Requires `ReferenceFact` of type `session_t0_reference`.
   * Result value: $v_{\text{elapsed}} = t_{\text{abs}} - t_0$
4. **Forbidden Conversions (Invariant G10)**:
   * $\text{LAP\_DURATION} \to \text{any timestamp}$ is **strictly rejected**.
   * $\text{RECEIPT\_TIME\_ONLY} \to \text{semantic timestamp}$ is **strictly rejected**.
   * Missing or corrupted reference facts **never fabricate guesses**; evaluation immediately fails closed to `UNKNOWN`.

---

## 3. Pointwise Composed Validity Timelines $\sigma(t)$

Rather than reducing valid horizons to a scalar `healthy_until` timestamp (which breaks under OR and $k$-of-$n$ compositions when one branch expires but others remain valid), validity is modelled as a **non-increasing step function** $\sigma(t) : \mathbb{R} \to \{D, U, S, H\}$.

### 3.1 Pointwise Composition Operators
Parent timelines are composed pointwise using the lattice operators:
$$\sigma_{\text{AND}}(t) = \min(\sigma_1(t), \sigma_2(t))$$
$$\sigma_{\text{OR}}(t) = \max(\sigma_1(t), \sigma_2(t))$$
$$\sigma_{k\text{-of-}n}(t) = \text{the } k\text{-th largest value of } \{\sigma_1(t), \dots, \sigma_n(t)\}$$

Because min, max, and rank operators preserve monotonicity, all composed parent timelines are guaranteed to be non-increasing step functions.

### 3.2 Synchronous Activation Frontiers
Per CAL-DOC-007 §2.5.3, the activation start boundary of a composed tuple is synchronized across conjuncts:
* $\text{Start}(\sigma_{\text{AND}}) = \max(\text{start}_1, \dots, \text{start}_n)$
* $\text{Start}(\sigma_{\text{OR}}) = \min(\text{start}_1, \dots, \text{start}_n)$
* $\text{Start}(\sigma_{k\text{-of-}n}) = k\text{-th sorted start instant}$

---

## 4. "Time is Data": Run-Clock Fact Architecture

In event-time streaming architectures, stationary watermarks during source stalls prevent event-time timers from advancing, leaving verdicts indefinitely in stale `HEALTHY` states inside the engine (Defect D-06).

To resolve this deterministically without synthetic watermarks:
1. An independent supervisor emits durable, sequenced `RunClockTick{run_id, clock_domain, clock_utc, seq, emitter_id}` events.
2. The evaluation instant $t_{\text{eval}}$ is explicitly supplied as data:
   $$t_{\text{eval}} = \max(\text{frontier}_{\text{evidence}}, \text{latest RunClockTick})$$
3. Expiry and deadline evaluations use $t_{\text{eval}}$, enabling silent sources to decay to `UNKNOWN` or `DEGRADED` deterministically during live runs and historical replays.
