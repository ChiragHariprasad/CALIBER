# Decision Evidence Contract (DEC) Schema Specification v1.0
**Document ID:** CAL-SPEC-002  
**Governing Standard:** CAL-DOC-007 v2.2  
**Status:** Normative / Validated  
**File Format:** YAML / JSON  

---

## 1. Overview & Schema Architecture

A Decision Evidence Contract (DEC) defines the deterministic evaluation cone for a specific domain decision. Evidence not declared in a decision's contract cone cannot affect that decision.

### 1.1 Structural Schema

```yaml
contract_id: str                      # Globally unique identifier (snake_case)
version: str                          # Semantic version string (e.g., "1.0")
epoch: int                            # Monotonically increasing revision integer
effective_from: str                   # ISO-8601 UTC timestamp
decision_type: str                    # Domain categorization (e.g., "timing_consistency")

decision_scope:
  scope_class: str                    # DRIVER_LAP | DRIVER_SESSION | SESSION | CAR_FRAME
  scope_key_pattern: str              # Format pattern (e.g., "{session_id}|{driver_id}|lap{lap_number}")

formula: str                          # Boolean formula expression: AND(...), OR(...), k-of-n(...)

roles:
  <role_name>:
    required: bool                    # Mandatory vs optional role
    description: str                  # Semantic definition
    time_class_required: str          # ABSOLUTE_UTC | SESSION_ELAPSED | LAP_RELATIVE
    admissible_source_origins: [str]  # Disjoint provider classes allowed
    admissible_processing_origins: [str] # Distinct processor classes allowed
    
    agreement:
      metric: str                     # abs_diff | relative_diff | discrete_equality
      unit: str                       # Measurement unit (e.g., "s", "kph", "rpm")
      tolerance_eta: float            # Maximal acceptable divergence threshold
      calibration_ref: str            # Traceable calibration standard ID
      honest_noise_epsilon_h: float   # Expected physical sensor noise
      
    freshness:
      soft_age_s: float | null        # Age at which evidence steps to SUSPECT
      hard_age_s: float | null        # Age at which evidence steps to DEGRADED
      latency_allowance_s: float      # Network transport delay allowance
      
    corroboration:
      source: str                     # INFORMATIONAL | MANDATORY
      processing: str                 # INFORMATIONAL | MANDATORY
      source_min_depth: int           # Required distinct source origin classes
      processing_min_depth: int       # Required distinct parser families
      
    fault_model:
      source_fault_budget: int        # Assumed Byzantine faulty sources (f)
      processing_fault_budget: int    # Assumed faulty processors
      
    slots:
      trigger: str                    # lap_completed | event_driven | periodic
      max_obs_per_slot: int           # Maximum evidence atoms admitted per slot
      deadline_s: float               # Relative deadline before slot expires to DEGRADED
      allowed_lateness_s: float       # Window for accepting late revisions

simultaneity_window_s: float          # Maximal temporal skew allowed across roles
finality_condition: str               # SLOT_WINDOW_CLOSE | EARLY_COLLAPSE_OR_LATENESS
tie_break: str                        # CANONICAL_ORDER
explanation_template_id: str          # Downstream explanation template
limitations: [str]                    # Declared unmodelled common causes or assumptions
```

---

## 2. Compile-Time Validation Rules (V-01 through V-10)

Before admission into the Flink runtime registry, every DEC must pass static verification:

| Rule ID | Name | Constraint Enforced |
| :--- | :--- | :--- |
| **V-01** | Role Reference Completeness | Every role named in `formula` must exist in `roles`, and vice versa. |
| **V-02** | Valid Metric & Tolerance | `tolerance_eta > 0.0` for difference metrics; discrete metrics require valid enums. |
| **V-03** | Temporal Class Consistency | `time_class_required` must have a defined conversion path in normative time resolver. |
| **V-04** | Freshness Monotonicity | If both specified: `soft_age_s < hard_age_s`. |
| **V-05** | Slot Window Invariant | `allowed_lateness_s >= 0.0` and `deadline_s > 0.0`. |
| **V-06** | Corroboration Depth | `source_min_depth <= len(admissible_source_origins)`. |
| **V-07** | Fault Budget Bound | `source_fault_budget < len(admissible_source_origins)`. |
| **V-08** | Disjoint Origin Classes | No origin identifier may appear in multiple disjoint classes. |
| **V-09** | Valid Scope Pattern | Coordinate tokens in `scope_key_pattern` must match `scope_class` coordinates. |
| **V-10** | Formula Grammar Validity | AST parses cleanly into valid `AndNode`, `OrNode`, `KOfNNode`, and `RoleLeafNode`. |

---

## 3. Normative Contracts Catalog

CALIBRE provides 11 pre-validated contracts grounded in real Formula 1 telemetry:

1. `dec_01_lap_time_consistency.yaml` (TracingInsights vs Timing Data alignment)
2. `dec_02_telemetry_session_alignment.yaml` (Vehicle speed vs Session reference)
3. `dec_03_race_control_incident_timing.yaml` (Race Control messages vs Track time)
4. `dec_04_session_status_transition.yaml` (Green / Yellow / Red Flag state transitions)
5. `dec_05_pit_lane_timing_evidence.yaml` (Pit entry and exit beam consistency)
6. `dec_06_weather_strategy_advisory.yaml` (Track temp, rainfall, and ambient weather)
7. `dec_07_tyre_condition_advisory.yaml` (Stint degradation and tyre wear)
8. `dec_08_source_integrity_assessment.yaml` (Supervisor guard liveness assessment)
9. `dec_09_pit_entry_speed_evidence.yaml` (Pit lane speed limiter adherence)
10. `dec_10_vehicle_sensor_anomaly.yaml` (Throttle, RPM, and brake physical bounds)
11. `dec_11_track_limit_crossing.yaml` (Car position vs Track boundary coordinates)
