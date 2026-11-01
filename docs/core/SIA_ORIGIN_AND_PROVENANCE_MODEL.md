# CALIBRE Scoped Integrity Algebra (SIA) — Origin and Provenance Model
**Document ID:** CAL-SPEC-005  
**Governing Standard:** CAL-DOC-007 §2.3, §2.4  
**Implementation Modules:** `sia_core/origin/manager.py`, `sia_core/evaluation/diagnosis.py`, `sia_core/envelope/clique.py`  
**Status:** Validated & Frozen  
**Date:** 2026-11-01  

---

## 1. Three-Tier Origin Taxonomy

To prevent false claims of corroboration (e.g., when two feeds share the same upstream timing server or when two parser versions process the same raw packet), origin tracking operates across three distinct tiers:

```text
+--------------------------------------------------------------+
|                        ORIGIN CLASS                          |
| (Category: Upstream provider, clock domain, processor family) |
+------------------------------+-------------------------------+
                               |
                               v
+--------------------------------------------------------------+
|                       ORIGIN INSTANCE                        |
| (Concrete acquisition session, connection endpoint, feed ID) |
+------------------------------+-------------------------------+
                               |
                               v
+--------------------------------------------------------------+
|                     ORIGIN-HEALTH CELL                       |
| (Dynamic liveness state of one instance during an epoch)     |
+--------------------------------------------------------------+
```

1. **Origin Class:** Broad category for independence analysis (e.g., `PITWALL_TIMINGDATA`, `F1_LIVETIMING_UPSTREAM`).
2. **Origin Instance:** Concrete ingestion instance (e.g., `PITWALL_TIMINGDATA / session S / conn epoch 12`).
3. **Origin-Health Cell:** Dynamic state cell for an instance within an epoch (e.g., `epoch 12 / STALLED / rev 3`). A stall of instance A never marks instance B stalled.

---

## 2. Origin-Disjoint Corroboration Depth ($\kappa$)

Corroboration depth on axis $x \in \{\text{source}, \text{processing}\}$ measures true independent observations:
$$\kappa_x = \max_{Q \text{ pairwise agreeing and } x\text{-disjoint}} (|Q| - 1)$$

* $\kappa_x = 0$: Exactly 1 independent origin observation.
* $\kappa_x = 1$: Exactly 2 independent agreeing observations.
* $\kappa_x = 2$: Exactly 3 independent agreeing observations.

### 2.1 Agreement is Non-Transitive (Clique Search)
Because measurement agreement within tolerance $\eta$ ($\|v_a - v_b\| \le \eta$) is **not transitive** ($a \approx b \land b \approx c \not\implies a \approx c$), finding the maximum agreeing support set is a **maximum clique problem**, not connected components.

Exact search uses Bron-Kerbosch with Tomita pivoting up to safety bound $M_{\max} = 10$. If candidate atoms exceed $M_{\max}$, the evaluator reports the conservative lower bound, emits a `SEARCH_BUDGET_EXCEEDED` witness, and strictly caps the leaf state at $\le \text{UNKNOWN}$.

---

## 3. Theorem G4-V: Value Validity Bounding (Proved)

*Assumptions:*
* **A1:** At most $f$ origin classes on axis $x$ are faulty. An atom is *honest* iff none of its origins is faulty.
* **A2:** Honest atoms satisfy $\|v - x^*\| \le \varepsilon_h$ where $x^*$ is true ground truth.
* **A3:** Origin attribution is complete (no unmodelled common cause).
* **A4:** Atoms agree iff $\|v_a - v_b\| \le \eta$.

**Theorem G4-V:** If $Q$ is pairwise agreeing and pairwise origin-disjoint on axis $x$ with $|Q| \ge f + 1$, then $Q$ contains at least one honest atom $h$, and every $q \in Q$ satisfies:
$$|v_q - x^*| \le \eta + \varepsilon_h$$

*Proof:* Disjointness guarantees each faulty origin class affects at most one atom in $Q$. Since $|Q| \ge f + 1$ and at most $f$ origins are faulty, at least one atom $h \in Q$ is honest. By triangle inequality:
$$|v_q - x^*| \le |v_q - v_h| + |v_h - x^*| \le \eta + \varepsilon_h \quad \blacksquare$$

---

## 4. Theorem G4-I: Fault Isolation via Minimal Hitting Sets (Corrected & Proved)

*Lemma C (Completeness):* If $\eta \ge 2\varepsilon_h$ and A1–A3 hold, any two honest atoms agree:
$$|v_a - v_b| \le |v_a - x^*| + |x^* - v_b| \le 2\varepsilon_h \le \eta$$

*Consequence:* Every disagreeing pair $(a, b)$ contains at least one dishonest atom. Thus, the true faulty origin set $F$ forms a hitting set for all conflicting pairs.

**Definition:** An *admissible diagnosis* is any origin set $D$ with $|D| \le f$ such that for every conflicting pair $(a, b)$, $D \cap (O(a) \cup O(b)) \ne \emptyset$. Let $\mathcal{D}_{\min}$ be the set of all inclusion-minimal admissible diagnoses.

**Theorem G4-I:** If $\mathcal{D}_{\min} \ne \emptyset$, every origin class in the intersection $\bigcap \mathcal{D}_{\min}$ is guaranteed to be faulty.

*Proof:* The true faulty set $F$ hits all conflict pairs and has $|F| \le f$. Thus, $F$ must contain at least one inclusion-minimal hitting set $D' \in \mathcal{D}_{\min}$ ($D' \subseteq F$). Therefore:
$$\bigcap \mathcal{D}_{\min} \subseteq D' \subseteq F \quad \blacksquare$$

### 4.1 Corrected Evaluator Algorithm
1. Enumerate all conflict pairs $(a, b)$ where $\|v_a - v_b\| > \eta$.
2. Compute $\mathcal{D}_{\min}$ via bounded search tree (cost $\mathcal{O}(b^f)$ with $f \le 2$).
3. **If $\mathcal{D}_{\min} = \emptyset$:** Budget exceeded or unmodelled common cause $\implies$ result is `SUSPECT` with witness `FAULT_BUDGET_EXCEEDED`; **exclude nothing**.
4. **If $\bigcap \mathcal{D}_{\min} = \emptyset$:** Multiple competing minimal explanations $\implies$ result is `SUSPECT` with witness `DIAGNOSIS_AMBIGUOUS`; **exclude nothing**.
5. **If $\bigcap \mathcal{D}_{\min} \ne \emptyset$:** Exclude only the intersection $\bigcap \mathcal{D}_{\min}$. Retain survivor clique under G4-V.

*Resolution of Defect D-01:* The previous v2.1 rule ("exclude if minimum diagnosis is unique") was unsound for star graphs (center $A$, leaves $B, C$, budget $f=2$). The unique minimum diagnosis $\{A\}$ competed with $\{B, C\}$. The intersection is empty, so excluding $A$ would have wrongly discarded an honest origin. Theorem G4-I correctly excludes nothing.

---

## 5. Build Supersession (Defect D-09)

To prevent parser redeployments from masquerading as independent corroboration:
* Processing origin identity is defined as `processor_family_id` + implementation lineage.
* A new build or transform version of the same family is **not a new origin**.
* If two atoms share $(\text{raw\_event\_ids}, \text{role\_id}, \text{processor\_family\_id})$ and differ only in build or transform version, the newer version carries `supersedes_atom_id` and replaces the older atom in active evaluation. The older atom remains in audit history.
