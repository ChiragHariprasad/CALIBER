# CALIBRE Operator Demo Runbook

This runbook guides an engineer or race operations evaluator through a clean startup, live system verification, and demonstration of the complete CALIBRE pipeline.

---

## 1. Prerequisites & Environment Setup

CALIBRE requires:
- **Python 3.11+** (Windows host with dependencies installed: `fastapi`, `uvicorn`, `aiokafka`, `kafka-python-ng`, `httpx`, `websockets`, `pytest`)
- **Apache Kafka 4.3.1 KRaft** (running inside WSL2 Fedora or local broker on port `9092`)
- **Web Browser** (Chrome / Edge / Firefox) pointing to `http://localhost:8001`

---

## 2. Clean System Startup

### Step 2.1: Start the Real Kafka Broker (WSL2 KRaft)
If the Kafka broker is not already active, launch it in a separate terminal:
```bash
wsl -d FedoraLinux-42 -e /home/chirag/kafka/bin/kafka-server-start.sh /home/chirag/kafka/config/server.properties
```
*Expected Output:*
```text
[KafkaServer id=1] started (org.apache.kafka.server.KafkaServer)
INFO [BrokerServer id=1] Transition from STARTING to STARTED (org.apache.kafka.server.BrokerServer)
```

### Step 2.2: Launch the FastAPI Backend & Pitwall UI Server
From the repository root (`E:\CALIBER`), run:
```powershell
python -m uvicorn calibre.api.main:app --host 127.0.0.1 --port 8001
```
*Expected Output:*
```text
INFO:     Started server process [PID]
INFO:     Waiting for application startup.
INFO:     [STARTUP] Kafka topics provisioned successfully on localhost:9092
INFO:     [STARTUP] StreamProcessor started in background
INFO:     Application startup complete.
INFO:     Uvicorn running on http://127.0.0.1:8001
```

### Step 2.3: Verify System Health
Open a terminal and verify dependency health:
```powershell
curl http://127.0.0.1:8001/api/v1/health
```
*Expected Response:*
```json
{
  "status": "healthy",
  "dependencies": {
    "api": {"status": "healthy", "latency_ms": 1.2},
    "kafka": {"status": "healthy", "broker": "localhost:9092", "topics_count": 18, "error": null},
    "storage": {"status": "healthy", "backend": "session_jsonl"},
    "websocket": {"status": "healthy", "active_clients": 0}
  }
}
```

---

## 3. Step-by-Step Operator Demonstration

### Step 3.1: Open the Pitwall Engineering Workspace
1. In your browser, navigate to: **`http://localhost:8001/`**
2. Observe the global status bar:
   - **OPERATING MODE:** Badge indicates `HISTORICAL_REPLAY` or `SIMULATION` (never a fake `LIVE`).
   - **KAFKA STATUS:** `CONNECTED (18 TOPICS)`.
   - **BROKER LATENCY:** `1.8 ms`.

---

### Step 3.2: Select a Historical Race & Start Playback (Race Control)
1. Click **Race Control** in the left navigation sidebar.
2. Select **Session 11731 (British Grand Prix 2024 - Silverstone)** from the dropdown.
3. Choose Target Driver: **VER**.
4. Set Speed: Click **5x** or **10x** for accelerated playback.
5. Click **START REPLAY**.
   - *Alternative CLI command:*
     ```powershell
     curl -X POST http://127.0.0.1:8001/api/v1/replay -H "Content-Type: application/json" -d "{\"session_id\":\"11731\",\"driver\":\"VER\",\"speed\":5.0}"
     ```
6. **Observable Output:**
   - The playback timeline scrubber begins advancing.
   - Operating mode switches to `HISTORICAL_REPLAY`.
   - The multi-car timing tower updates lap times and sector intervals in real time.
   - Live telemetry gauges (Speed, Throttle, RPM) reflect the car's dynamic telemetry.

---

### Step 3.3: Observe Real Kafka Streaming Throughput (Streaming Operations)
1. Click **Streaming Operations** in the sidebar.
2. Observe the real-time telemetry metrics:
   - **Broker Status:** `CONNECTED (localhost:9092)`.
   - **Event Throughput:** Displays current events-per-second (e.g. `20.4 EPS`).
   - **Consumer Lag:** `0`.
   - **Kafka Topic Table:**
     - `calibre.raw.telemetry` (10 partitions)
     - `calibre.normalized.telemetry` (10 partitions)
     - `calibre.race.events` (3 partitions)
     - `f1.verdicts` (3 partitions)
3. *CLI Proof Command:*
   ```powershell
   python -c "import httpx; print(httpx.get('http://127.0.0.1:8001/api/v1/streaming/status').json())"
   ```

---

### Step 3.4: Inspect & Inject Sensor Anomalies (Sensor Integrity)
1. Click **Sensor Integrity** in the sidebar.
2. The screen shows all active error signals and recent anomaly history.
3. Test Fault Injection:
   - Click **INJECT SPEED SPIKE** (+120 km/h jump).
   - *Or execute via CLI:*
     ```powershell
     curl -X POST http://127.0.0.1:8001/api/v1/live/integrity/scenarios -H "Content-Type: application/json" -d "{\"fault_type\":\"spike\",\"driver\":\"VER\",\"signal\":\"speed_kph\",\"magnitude\":130.0}"
     ```
4. **Observable Output:**
   - A critical red banner appears: `[SPIKE_ANOMALY] VER sudden speed spike to 410.0 km/h`.
   - Detection method: `synthetic_fault_injection` / `physical_bounds_check`.
   - Provenance tag explicitly reads **`simulated`** (never claimed as real observed telemetry).
   - Downstream pit lane speed verdicts are automatically downgraded to **`DEFER`**.

---

### Step 3.5: Inspect Reference Performance & Delta Diagnosis (Performance Twin)
1. Click **Performance Twin** in the sidebar.
2. Observe the reference model details:
   - **Model Version:** `2.4.1-spatial-envelope`.
   - **Baseline Selection:** `Pole Lap (85.819s)`.
   - **Spatial Uncertainty Envelope:** `±0.045s`.
3. View the sector delta cards:
   - Sector 1: `+0.120s` (High-speed entry).
   - Sector 2: `+0.340s` (Traction deficit exiting Becketts).
   - Sector 3: `-0.050s` (Slipstream advantage).
4. Inspect the **Diagnoses & Root Cause Trail**:
   - `TRACTION_LOSS`: Exit speed 8 km/h below reference; early wheelspin suspected.
   - `BRAKING_POINT`: Braking initiated 12m deeper into Vale chicane.

---

### Step 3.6: Safety Car Event & Tyre Strategy Replanning (Strategy & Alerts)
1. Click **Strategy & Alerts** in the sidebar.
2. Observe the Tyre Degradation curve and predicted cliff horizon (e.g. Lap 22 for Medium compound).
3. Inject or await Safety Car deployment:
   - The radio transcript displays: `[Race Engineer -> VER] (HIGH) Yellow flag! Hazard on track, maintain delta.`
   - Track status switches to `SAFETY_CAR`.
   - Pit lane loss automatically adjusts from `21.5s` to `11.5s`.
   - Tyre Strategy Agent immediately triggers a replan:
     - **Recommendation:** `BOX BOX, BOX THIS LAP FOR HARD`.
     - **Podium Probability:** Updates to reflect cheap pit stop advantage.

---

### Step 3.7: Audit Chronological Events (Session Audit)
1. Click **Session Audit** in the sidebar.
2. Filter by topic or driver.
3. Click any event row to open the raw JSON payload inspector and confirm the canonical envelope:
   - `schema_version`: `"1.0.0"`
   - `event_id`: e.g. `EVT-REP-11731-000042`
   - `operating_mode`: `"HISTORICAL_REPLAY"`
   - `field_origins`: `{"speed_kph": "OBSERVED", "degradation": "DERIVED"}`

---

## 4. Automated Verification Script

To re-run the complete automated end-to-end vertical slice test programmatically:
```powershell
python calibre/scripts/verify_vertical_slice.py
```
*Expected Output Summary:*
```text
=== [1/6] VERIFYING DEPENDENCY HEALTH & SESSIONS === [OK]
=== [2/6] PREPARING REAL KAFKA CONSUMER === [OK]
=== [3/6] STARTING HISTORICAL REPLAY VIA REST API === [OK]
=== [4/6] VERIFYING REAL KAFKA EVENT PUBLICATION & INGESTION === [OK] (5 events verified)
=== [5/6] VERIFYING WEBSOCKET STREAMING DISSEMINATION === [OK] (3 frames received)
=== [6/6] VERIFYING API PROVENANCE LABELS & STATE === [OK]
=======================================================
VERTICAL SLICE VERIFICATION COMPLETE: ALL 6 STAGES PROVEN
=======================================================
```
