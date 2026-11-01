# CALIBRE Scoped Integrity Algebra (SIA) — Event and State Models
**Document ID:** CAL-SPEC-003  
**Governing Standard:** CAL-DOC-007 §2.1, §2.5, §2.6  
**Implementation Modules:** `sia_core/models/states.py`, `sia_core/models/evidence.py`, `sia_core/models/frontier.py`  
**Status:** Validated & Frozen  
**Date:** 2026-11-01  

---

## 1. The 4-State Integrity Admissibility Lattice

Integrity evaluation in CALIBRE SIA does **not** collapse evidence health into a single scalar probability or numerical confidence score. Instead, evidence admissibility is ordered as a 4-state total order lattice:

$$\text{DEGRADED} < \text{UNKNOWN} < \text{SUSPECT} < \text{HEALTHY}$$

| State | Ordinal | Slot-Based Semantic Definition |
|:---|:---:|:---|
| **`DEGRADED`** | 0 | Relevant evidence is no longer admissible: hard-expired, origin STALLED, evidence slot closed without an admissible observation, or incomplete beyond hard limits. |
| **`UNKNOWN`** | 1 | No sufficient basis *yet*: a mandatory slot is open within its arrival deadline, or a mandatory reference fact is unresolved. |
| **`SUSPECT`** | 2 | Evidence exists and was admitted, but carries conflict ($\|v_1 - v_2\| > \eta$), unmodelled common cause, weak corroboration, soft expiry, or an unisolated fault. |
| **`HEALTHY`** | 3 | All mandatory contractual conditions are satisfied under the declared fault assumptions. (Conditional evidence admissibility, not ground truth guarantee). |

---

## 2. Truth Tables & Algebraic Operators

The lattice operators are defined over the total order:
* **$\text{AND}(a, b) = \min(a, b)$** (Greatest Lower Bound / Meet)
* **$\text{OR}(a, b) = \max(a, b)$** (Least Upper Bound / Join)
* **$k\text{-of-}n(s_1, \dots, s_n) = \text{the } k\text{-th largest state}$** ($\text{OR} = 1\text{-of-}n$, $\text{AND} = n\text{-of-}n$)

### 2.1 Complete 16-Cell Meet (AND) Truth Table

| $\text{AND}$ | `DEGRADED` | `UNKNOWN` | `SUSPECT` | `HEALTHY` |
|:---:|:---:|:---:|:---:|:---:|
| **`DEGRADED`** | `DEGRADED` | `DEGRADED` | `DEGRADED` | `DEGRADED` |
| **`UNKNOWN`** | `DEGRADED` | `UNKNOWN` | `UNKNOWN` | `UNKNOWN` |
| **`SUSPECT`** | `DEGRADED` | `UNKNOWN` | `SUSPECT` | `SUSPECT` |
| **`HEALTHY`** | `DEGRADED` | `UNKNOWN` | `SUSPECT` | `HEALTHY` |

*Identity element for meet:* `HEALTHY` ($\text{AND}() = \text{HEALTHY}$, $\text{AND}(x, \text{HEALTHY}) = x$).  
*Definitive failure rule:* $\text{AND}(\text{UNKNOWN}, \text{DEGRADED}) = \text{DEGRADED}$ (a conjunct has definitively failed).

### 2.2 Complete 16-Cell Join (OR) Truth Table

| $\text{OR}$ | `DEGRADED` | `UNKNOWN` | `SUSPECT` | `HEALTHY` |
|:---:|:---:|:---:|:---:|:---:|
| **`DEGRADED`** | `DEGRADED` | `UNKNOWN` | `SUSPECT` | `HEALTHY` |
| **`UNKNOWN`** | `UNKNOWN` | `UNKNOWN` | `SUSPECT` | `HEALTHY` |
| **`SUSPECT`** | `SUSPECT` | `SUSPECT` | `SUSPECT` | `HEALTHY` |
| **`HEALTHY`** | `HEALTHY` | `HEALTHY` | `HEALTHY` | `HEALTHY` |

*Identity element for join:* `DEGRADED` ($\text{OR}() = \text{DEGRADED}$, $\text{OR}(x, \text{DEGRADED}) = x$).  
*Open slot rule:* $\text{OR}(\text{UNKNOWN}, \text{DEGRADED}) = \text{UNKNOWN}$ (an open slot can still satisfy the disjunction).

---

## 3. Core Event Models

### 3.1 RawSourceEnvelope
Represents unmodified raw ingress from live feeds, telemetry archives, or replay sockets:
```python
@dataclass(frozen=True, slots=True)
class RawSourceEnvelope:
    provider: str
    feed: str
    source_record_key: Optional[str] = None
    session_id: Optional[str] = None
    scope: Dict[str, Any] = field(default_factory=dict)
    source_time: Dict[str, Any] = field(default_factory=dict)
    payload: Dict[str, Any] = field(default_factory=dict)
    payload_sha256: str = ""
    received_at_utc: Optional[str] = None
    raw_event_id: str = ""
    identity_stability: str = "REPLAY_STABLE"
```

### 3.2 EvidenceAtom
The fundamental unit of evidence admitted into SIA evaluation:
```python
@dataclass(frozen=True, slots=True)
class EvidenceAtom:
    evidence_atom_id: str
    role_id: str
    value: Any
    unit: str = "s"
    raw_event_ids: Sequence[str] = field(default_factory=tuple)
    processor_family_id: str = "generic_parser_family"
    transform_version: str = "1.0.0"
    scope_key: str = ""
    time: Dict[str, Any] = field(default_factory=dict)
    quality_grade: str = "HEALTHY"
    source_origin_set: Sequence[str] = field(default_factory=tuple)
    processing_origin_set: Sequence[str] = field(default_factory=tuple)
    supersedes_atom_id: Optional[str] = None
    reference_atom_ids: Sequence[str] = field(default_factory=tuple)
    origin_instance_refs: Sequence[str] = field(default_factory=tuple)
    transport_event_time_utc: str = ""
    transport_time_basis: str = "EVENT_UTC"
```

### 3.3 ReferenceFact
Durable facts providing ground temporal reference for coordinate transformations:
```python
@dataclass(frozen=True, slots=True)
class ReferenceFact:
    fact_id: str
    fact_type: str
    session_id: Optional[str] = None
    driver_id: Optional[str] = None
    lap_number: Optional[int] = None
    value: Any = None
    time_class: str = "SESSION_ELAPSED"
    grade: str = "HEALTHY"
    origin_set: Sequence[str] = field(default_factory=tuple)
    validity_window: Optional[Dict[str, str]] = None
    rejection_reason: Optional[str] = None
```

### 3.4 GuardFact & RunClockTick
Durable supervisory events feeding source liveness and time progression without synthetic watermarks:
```python
@dataclass(frozen=True, slots=True)
class GuardFact:
    fact_id: str
    origin_instance_id: str
    epoch: int = 1
    status: str = "RESUMED"  # STALLED | RESUMED
    observed_gap: float = 0.0
    detection_clock: str = ""

@dataclass(frozen=True, slots=True)
class RunClockTick:
    run_id: str
    clock_domain: str        # LIVE_UTC | REPLAY_VIRTUAL
    clock_utc: str
    seq: int
    emitter_id: str
```
