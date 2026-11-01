# Scoped Integrity Algebra (SIA) Mathematical Specification v1.0
**Document ID:** CAL-SPEC-001  
**Governing Standard:** CAL-DOC-007 v2.2 (Amending CAL-DOC-006)  
**Status:** Normative / Validated  
**Target Runtime:** CALIBRE Core Pipeline (Python 3.13 / Apache Flink 1.19 / Apache Kafka 7.7.0)

---

## 1. Mathematical Foundations & The 4-State Total Order Lattice

The Scoped Integrity Algebra operates over a 4-state total order bounded lattice:
$$\mathcal{L} = (\{\text{DEGRADED}, \text{UNKNOWN}, \text{SUSPECT}, \text{HEALTHY}\}, \le, \wedge, \vee)$$

### 1.1 Total Order Definition
$$\text{DEGRADED} (0) < \text{UNKNOWN} (1) < \text{SUSPECT} (2) < \text{HEALTHY} (3)$$

* **$\text{HEALTHY} (\top)$**: All contract conditions (freshness, completeness, agreement, corroboration) are affirmatively satisfied under declared fault assumptions.
* **$\text{SUSPECT}$**: Evidence is present and fresh, but exhibits statistical or domain divergence exceeding tolerance $\eta$, or unmodelled correlation prevents independent verification.
* **$\text{UNKNOWN}$**: Evidence is missing, unresolvable in time, corrupted, ambiguous, or the decision window is open prior to slot deadline.
* **$\text{DEGRADED} (\bot)$**: A mandatory evidence slot deadline has expired without admissible data, hard expiry occurred, or persistent fault occurred.

### 1.2 Lattice Operations
* **Meet ($\wedge$, Logical AND)**:
  $$a \wedge b = \min(a, b)$$
  * Identity: $\top = \text{HEALTHY}$ ($a \wedge \text{HEALTHY} = a$)
  * Annihilator: $\bot = \text{DEGRADED}$ ($a \wedge \text{DEGRADED} = \text{DEGRADED}$)
* **Join ($\vee$, Logical OR)**:
  $$a \vee b = \max(a, b)$$
  * Identity: $\bot = \text{DEGRADED}$ ($a \vee \text{DEGRADED} = a$)
  * Annihilator: $\top = \text{HEALTHY}$ ($a \vee \text{HEALTHY} = \text{HEALTHY}$)
* **$k$-of-$n$ Composition**:
  For multiset of child states $S = \{s_1, \dots, s_n\}$ sorted descending ($s_{(1)} \ge s_{(2)} \ge \dots \ge s_{(n)}$):
  $$\text{k-of-n}(S) = s_{(k)}$$

---

## 2. Agreement Graphs & Theorem G4-V Search Bounding

### 2.1 Agreement Graph Definition
For evidence atoms $A = \{a_1, \dots, a_m\}$ assigned to a role with agreement tolerance $\eta$ and metric $d(x, y)$:
* Graph $G = (A, E)$ where edge $(a_i, a_j) \in E \iff d(a_i, a_j) \le \eta$.
* A valid support set is a maximal clique in $G$.

### 2.2 Theorem G4-V (Search Budget Bounding)
To guarantee real-time $O(1)$ determinism and protect against Moon-Moser graph constructions ($K_{3,3,\dots,3}$ generating $3^{m/3}$ maximal cliques):
* Clique search is bounded by $M_{\max} = 10$.
* If clique enumeration exceeds $M_{\max}$, the search terminates fail-closed:
  $$\text{exceeded} = \text{True} \implies \text{verdict} \le \text{UNKNOWN}, \quad \text{witness} = \text{"SEARCH\_BUDGET\_EXCEEDED"}$$

---

## 3. Fault Isolation & Theorem G4-I ($\bigcap \mathcal{D}_{\min}$)

### 3.1 Theorem G4-I Formulation
Let $\mathcal{C}$ be the set of conflict edges in disagreement graph $\overline{G}$. An admissible diagnosis $D \subseteq \mathcal{O}$ is a minimal hitting set of conflict origin edges such that $|D| \le f$ (assumed fault budget).

To prevent blaming innocent agreeing sources under ambiguous conflicts (Star-Graph Defect D-01):
$$\mathcal{O}_{\text{excluded}} = \bigcap_{D \in \mathcal{D}_{\min}} D$$

### 3.2 Star-Graph Resolution Proof
Consider center $C$ in conflict with leaves $L_1, L_2$, where $L_1$ and $L_2$ agree ($E = \{(C, L_1), (C, L_2)\}$):
1. **Budget $f=1$**:
   * Admissible diagnoses: $\mathcal{D}_{\min} = \{\{C\}\}$.
   * $\bigcap \mathcal{D}_{\min} = \{C\}$. Center is uniquely isolated and excluded. Leaves remain $\text{HEALTHY}$.
2. **Budget $f=2$**:
   * Admissible diagnoses: $\mathcal{D}_{\min} = \{\{C\}, \{L_1, L_2\}\}$.
   * $\bigcap \mathcal{D}_{\min} = \emptyset$.
   * Result: **No origins are excluded**. System emits $\text{"DIAGNOSIS\_AMBIGUOUS"}$ and degrades to $\text{SUSPECT}$.

---

## 4. Abstract Completion Envelopes & Obligations P4 / P5

For decision contracts with $k$ pending evidence roles prior to deadline:

### 4.1 $3^k$ Quotient Enumeration
* Each pending role can resolve to one of 3 abstract states: $\{\text{DEGRADED}, \text{SUSPECT}, \text{HEALTHY}\}$ ($\text{UNKNOWN}$ is quotiented).
* When $3^k \le C_{\max} = 32$, evaluate all $3^k$ completions to compute envelope:
  $$[\sigma_{\min}, \sigma_{\max}] = \left[\min_{c} f(c), \max_{c} f(c)\right]$$
* If $3^k > C_{\max}$, cap fail-closed: $[\text{DEGRADED}, \text{HEALTHY}]$ with witness $\text{"ENVELOPE\_CAPPED"}$.

### 4.2 Obligation P5 (Sound Early Collapse)
If $\sigma_{\min} == \sigma_{\max}$:
* Formula has collapsed algebraically (e.g. $\text{DEGRADED} \wedge X \equiv \text{DEGRADED}$, $\text{HEALTHY} \vee X \equiv \text{HEALTHY}$).
* Resolution maturity immediately advances to $\text{STATE\_RESOLVED}$ without waiting for slot deadlines.

---

## 5. Dual-Domain Leases & Temporal Grounding

### 5.1 Dual-Domain Leases
Verdicts are issued with explicit consumer validity leases:
$$\text{lease\_until} = t_{\text{eval}} + \min_{r \in \text{roles}} (\text{slot.deadline\_s} + \text{slot.allowed\_lateness\_s})$$
* **$\text{LIVE\_UTC}$**: Lease expires against wall-clock / supervisor guard heartbeat. If supervisor stalls, consumer lease expires, forcing downstream display to fail-closed $\text{UNKNOWN}$.
* **$\text{REPLAY\_VIRTUAL}$**: Lease is bounded strictly to virtual run-clock ticks, completely isolated from system wall-clock jitter.

### 5.2 "Time is Data" Principle (Defect D-06)
Event-time watermarks and run-clock ticks are decoupled:
$$t_{\text{eval}} = \max(\text{watermark}, \text{run\_clock\_tick.clock\_utc})$$
* Stationary watermarks (e.g. during red flags or source stalls) do not stall time passage; durable `RunClockTick` facts advance $t_{\text{eval}}$ and drive deterministic age degradation.

---

## 6. RFC 8785 Canonical Identity & Deterministic Hashing

Every verdict computes a cryptographically stable SHA-256 hash:
$$\text{verdict\_hash} = \text{SHA-256}(\text{JCS}(\text{verdict\_dict} \setminus \{\text{verdict\_hash}, \text{correlation\_id}\}))$$
* **RFC 8785 JSON Canonicalization Scheme (JCS)**: Keys sorted by UTF-16 code units, numbers formatted per ECMAScript 6 Section 7.1.12.1.
* **Float Quantization**: Quantized via `Decimal` round-half-even to contract tolerance $\eta$.
* **Replay Determinism R1 & R2**: Guaranteed across languages (Python, Java, C++).
