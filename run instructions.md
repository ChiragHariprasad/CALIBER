### Repository Overview

This repository contains two primary modules:
1. **[`calibre`](file:///e:/CALIBER/calibre/README.md)**: Real-time F1 Race-Engineering Intelligence, Monte Carlo strategy simulation, rule evaluation stream pipeline, and React telemetry dashboard.
2. **[`sat-sa`](file:///e:/CALIBER/sat-sa/README.md)**: Supervisory Analytics Tool for SOC Assessment (Smart India Hackathon 2026), featuring DuckDB batch analytics, an Ed25519 Merkle-chain TRUST ledger, and interactive Streamlit/React dashboards.

---

### Prerequisites

- **Python 3.10+** (tested on Python 3.13)
- **Node.js 18+** & **npm** (for the React dashboards)
- **Docker & Docker Compose** *(optional, only needed for CALIBRE's Kafka + PostgreSQL pipeline)*
- **Git**

---

### Step 1: Clone the Repository

```bash
git clone https://github.com/ChiragHariprasad/CALIBER.git
cd CALIBER
```

---

### Option A: Running CALIBRE (F1 Race Engineering & Strategy Engine)

#### 1. Instant Replay Simulation (Zero Infrastructure Needed)
To immediately test the race-engineering tactical state machine and strategy optimizer:

```bash
# Optional: create and activate a virtual environment
python -m venv venv
# On Windows PowerShell:
.\venv\Scripts\Activate.ps1
# On Linux/macOS:
# source venv/bin/activate

pip install -r calibre/requirements.txt

# Run the CLI race replay engine (scenarios: synthetic-undercut, safety-car, tyre-cliff, weather-crossover, false-driver-blame)
python calibre/scripts/replay_race.py --scenario synthetic-undercut --speed 10 --laps 5
```

#### 2. Live Race Day Intelligence Engine (`calibre.live.race_day`)
Run real-time multi-agent race intelligence (telemetry quality monitoring, racing line sector tracking, tyre degradation modeling, and live JSONL session logging):

- **Mock / Simulation Mode (Recommended when testing offline or without an API key):**
  ```powershell
  # On Windows PowerShell:
  $env:PYTHONPATH="E:\CALIBER;E:\CALIBER\calibre"; python -m calibre.live.race_day --mock --driver VER --laps 56
  # On Linux/macOS:
  PYTHONPATH=".:calibre" python -m calibre.live.race_day --mock --driver VER --laps 56
  ```

- **OpenF1 Live Mode:**
  > **Note on OpenF1 401 Unauthorized:** During active Formula 1 race sessions, OpenF1 restricts free public API access across all endpoints to reduce load. To run during live F1 sessions, pass an OpenF1 bearer access token:
  ```powershell
  # On Windows PowerShell:
  $env:PYTHONPATH="E:\CALIBER;E:\CALIBER\calibre"; python -m calibre.live.race_day --session-key 11731 --driver VER --api-token "YOUR_TOKEN"
  # Or export the environment variable:
  $env:OPENF1_TOKEN="YOUR_TOKEN"
  python -m calibre.live.race_day --session-key 11731 --driver VER
  ```

#### 3. Running the Full Stack (React Dashboard + FastAPI Backend)

**Terminal 1 — Backend API:**
```bash
cd calibre

# Set PYTHONPATH to the repository root so package imports resolve cleanly
# On Windows PowerShell:
$env:PYTHONPATH="..;."
# On Linux/macOS:
# export PYTHONPATH="..:."

uvicorn api.main:app --reload --host 0.0.0.0 --port 8001
```
> The API will be live at `http://localhost:8001` (Docs: `http://localhost:8001/docs`). It automatically runs in offline/simulation mode if Kafka and PostgreSQL are not running.

**Terminal 2 — React Race Telemetry UI:**
```bash
cd calibre
npm install
npm run dev
```
> Open `http://localhost:3001` in your browser to interact with the live telemetry dashboard, strategy matrix, tyre degradation plots, and replay scenarios.

#### 3. Running with Docker Compose (Kafka + Postgres + Stream Processor)
For the full bitemporal stream-processing pipeline:

```bash
cd calibre
cp .env.example .env

# Build and start Kafka (KRaft), PostgreSQL, API, and Stream Processor
docker compose up --build

# In a separate terminal, trigger the event replay producer:
docker compose --profile replay up calibre-replay
```

#### 4. Running CALIBRE Unit Tests
```bash
pytest calibre/tests -v
```

---

### Option B: Running SAT-SA (Supervisory Analytics Tool for SOC Assessment)

All dependencies for SAT-SA are CPU-only and run completely offline.

#### 1. Setup & Installation
```bash
cd sat-sa

# Create virtual environment (recommended)
python -m venv venv
# Windows:
.\venv\Scripts\Activate.ps1
# Linux/macOS:
# source venv/bin/activate

pip install -r requirements.txt
```

#### 2. Generate the Synthetic Benchmark Dataset (40 CSEs)
```bash
python -m satsa generate --output-dir ./data/sample --num-cses 40 --seed 42
```

#### 3. Run Batch Assessment
```bash
python -m satsa run --input-dir ./data/sample/CSE-101 --entity-id CSE-101 --entity-name "State Bank Ops" --sector Banking
```

#### 4. Verify the TRUST Cryptographic Ledger
Validates the Ed25519 signature chain and Merkle inclusion roots:
```bash
python -m satsa verify --ledger-path ./trust_ledger.db
```

#### 5. Launch the Dashboards

**Choice 1: Streamlit Dashboard (Regulator Triage View):**
```bash
streamlit run ui/app.py
```
> Opens at `http://localhost:8501`.

**Choice 2: React Vite Dashboard + FastAPI Backend:**
- **Backend:**
  ```bash
  uvicorn satsa.api.main:app --reload --host 0.0.0.0 --port 8000
  ```
- **Frontend:**
  ```bash
  npm install
  npm run dev
  ```
  > Opens at `http://localhost:3000`.

#### 6. Run Validation Benchmark & Test Suite
```bash
# Automated benchmark harness (< 10s execution across 40 CSEs)
python validation/harness.py

# Complete unit & integration test suite (20 tests)
python -m pytest tests -v
```

---

### Quick Reference of Ports

| Service | Port | Directory | Command |
|---|---|---|---|
| **CALIBRE Dashboard** | `3001` | `calibre/` | `npm run dev` |
| **CALIBRE API** | `8001` (or `8000`) | `calibre/` | `uvicorn api.main:app --port 8001` |
| **SAT-SA React UI** | `3000` | `sat-sa/` | `npm run dev` |
| **SAT-SA Streamlit UI** | `8501` | `sat-sa/` | `streamlit run ui/app.py` |
| **SAT-SA API** | `8000` | `sat-sa/` | `uvicorn satsa.api.main:app --port 8000` |