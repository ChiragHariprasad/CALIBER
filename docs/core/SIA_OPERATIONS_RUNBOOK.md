# Scoped Integrity Algebra (SIA) Operations Runbook
**Document ID:** CAL-OPS-001  
**Target Systems:** CALIBRE Streaming Core, Apache Flink 1.19, Apache Kafka 7.7.0  
**Classification:** Operational / Production  

---

## 1. System Architecture & Topology Overview

CALIBRE SIA operates as an event-driven streaming kernel embedded in Apache Flink:

```text
[Providers: Pitwall / TracingInsights / MOCK]
                      |
                      v
            [Kafka Ingestion Layer]
                      |
        +-------------+-------------+
        | calibre.evidence.*        |
        | calibre.guard.liveness    |
        | calibre.guard.clock       |
        | calibre.config.contracts  |
        +-------------+-------------+
                      |
                      v
       [Flink Decision Integrity Operator]
                      |
        +-------------+-------------+
        | calibre.core.verdict_hist |
        | calibre.out.database      |
        | calibre.out.dashboard     |
        | calibre.out.analytics     |
        +-------------+-------------+
        | Side Outputs:             |
        | - calibre.dlq             |
        | - calibre.side.quarantine |
        | - calibre.side.overflow   |
        +---------------------------+
```

---

## 2. Topic Inventory & Consumer Groups

| Topic | Keying Semantic | Retention | Primary Consumer Group |
| :--- | :--- | :--- | :--- |
| `calibre.evidence.*` | `event_id` | 7 Days | `calibre-flink-core-group` |
| `calibre.guard.liveness` | `origin_instance_id` | 7 Days | `calibre-flink-core-group` |
| `calibre.guard.clock` | `clock_domain` | 7 Days | `calibre-flink-core-group` |
| `calibre.config.contracts` | `contract_id` | Compacted | `calibre-flink-core-group` |
| `calibre.core.verdict_history` | `decision_instance_id` | 30 Days | `calibre-persistence-sink-group` |
| `calibre.out.database` | `correlation_id` | 30 Days | `calibre-persistence-sink-group` |
| `calibre.out.dashboard` | `correlation_id` | 1 Day | `calibre-dashboard-gateway-group`|
| `calibre.out.analytics` | `correlation_id` | 7 Days | `calibre-analytics-service-group`|
| `calibre.dlq` | `correlation_id` | 30 Days | Ops DLQ Monitor |
| `calibre.side.quarantine` | `decision_instance_id` | 30 Days | Integrity Audit Monitor |
| `calibre.side.overflow` | `decision_instance_id` | 30 Days | Performance Audit Monitor |

---

## 3. Standard Operational Procedures

### 3.1 Service Lifecycle & Startup Sequence
1. Start Zookeeper & Kafka broker:
   ```powershell
   docker-compose up -d zookeeper kafka
   ```
2. Start Apache Flink JobManager & TaskManager:
   ```powershell
   docker-compose up -d flink-jobmanager flink-taskmanager
   ```
3. Verify cluster health:
   ```powershell
   curl -s http://127.0.0.1:8081/overview
   ```
4. Start Streaming Kernel:
   ```powershell
   python -m flink.job
   ```

### 3.2 Dynamic Contract Upgrades
* Contracts are versioned and can be upgraded without cluster downtime by publishing to `calibre.config.contracts`.
* Flink dynamically parses and admits the updated contract AST into memory while preserving in-flight decision state.

---

## 4. Incident Response & Troubleshooting Runbooks

### 4.1 Malformed Envelopes & DLQ Alerts
* **Symptom**: Messages accumulating on `calibre.dlq`.
* **Root Cause**: Upstream agent emitting payloads missing required schema fields (`event_id`, `correlation_id`, `domain`, `evidence`).
* **Triage**:
  ```powershell
  python -c "from test_flink_streaming import poll_matching_message; print(poll_matching_message('calibre.dlq', 'trace-id', timeout_sec=5.0))"
  ```
* **Resolution**: Inspect `stage` and `error_code` inside the DLQ envelope. If caused by a misconfigured parser, restart the offending agent.

### 4.2 Source Feed Stall & Guard Heartbeat Outage
* **Symptom**: Downstream verdicts transitioning from `HEALTHY` to `SUSPECT` and `DEGRADED`.
* **Root Cause**: Upstream Pitwall or timing websocket stalled; supervisor emitted `GuardFact(status="STALLED")`.
* **Resolution**: Check external telemetry provider status. The system will automatically recover and transition back to `HEALTHY` upon receipt of `GuardFact(status="RESUMED")`.

### 4.3 Search Budget Overflow Alerts (`calibre.side.overflow`)
* **Symptom**: Messages on `calibre.side.overflow` with `SEARCH_BUDGET_EXCEEDED` ($M_{\max} = 10$) or `ENVELOPE_CAPPED` ($C_{\max} = 32$).
* **Root Cause**: Highly adversarial input graph with excessive maximal cliques (e.g. Moon-Moser graph) or more than 3 pending roles.
* **Resolution**: The system safely fails closed to $\le \text{UNKNOWN}$. Inspect the raw atoms for sensor glitch storms or adversarial input flooding.

### 4.4 Post-Finality Quarantined Evidence (`calibre.side.quarantine`)
* **Symptom**: Atoms routed to `calibre.side.quarantine`.
* **Root Cause**: Evidence arrived after slot window lateness period elapsed and verdict was sealed `FINAL` (Invariant G11).
* **Resolution**: Expected behavior ensuring historical decision immutability. No operational intervention required unless network latencies systematically exceed contract `allowed_lateness_s`.

---

## 5. Replay & Recovery Procedures (R1 & R2)

1. **Replay Determinism R1 (Pure Function Invariance)**:
   * To verify that code updates preserve identical verdicts, execute:
     ```powershell
     python tests/test_eval_parity.py
     ```
   * Must achieve 20/20 bit-for-bit SHA-256 verdict hash matches.
2. **Replay Determinism R2 (Recorded Admission Stream Playback)**:
   * Execute historical regression fixtures:
     ```powershell
     pytest tests/test_persistence_durability_and_replay.py -k "test_06"
     ```
   * Asserts that replaying recorded raw events reproduces exact historical `verdict_hash`.
