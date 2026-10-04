# CALIBRE

**Calibration-Aware, Lineage-Indexed, Bitemporal Rule Evaluation**
over real-time Formula 1 event streams.

---

## What is CALIBRE?

CALIBRE is a real-time F1 stream-analytics system that:

1. **Consumes / replays** F1 event data (OpenF1 or synthetic)
2. **Evaluates FIA-style rules** in streaming fashion
3. **Attaches measurement uncertainty** to every evaluation
4. **Detects possible common-cause sensor faults** (CUSUM-based)
5. **Produces `BREACH` / `CLEAR` / `DEFER` verdicts** — never auto-penalties
6. **Supports future correction and audit workflows** (bitemporal schema)

It is a **decision-support system**, not an automatic penalty system.

---

## Architecture

```
OpenF1 / CSV / Sample Data
        │
        ▼
┌──────────────────┐
│  Replay Producer │  (replay/)
└──────┬───────────┘
       │ Kafka
       ▼
┌──────────────────┐
│ Stream Processor │  (streaming/)
│                  │
│  ┌─────────────┐ │
│  │Rule Monitor │ │  (rules/)        ← pit-lane speed
│  └──────┬──────┘ │
│         ▼        │
│  ┌─────────────┐ │
│  │ Uncertainty │ │  (uncertainty/)  ← RSS uncertainty
│  │   Engine    │ │
│  └──────┬──────┘ │
│         ▼        │
│  ┌─────────────┐ │
│  │ Integrity   │ │  (integrity/)   ← CUSUM detector
│  │  Detector   │ │
│  └──────┬──────┘ │
│         ▼        │
│  ┌─────────────┐ │
│  │  Verdict    │ │  (verdict/)     ← BREACH / CLEAR / DEFER
│  │   Engine    │ │
│  └─────────────┘ │
└──────┬───────────┘
       │
       ▼
┌──────────────────┐
│   PostgreSQL     │  (storage/)     ← bitemporal verdicts
└──────┬───────────┘
       │
       ▼
┌──────────────────┐
│    FastAPI        │  (api/)        ← /verdicts, /metrics, /health
└──────────────────┘
```

---

## Folder Structure

```
calibre/
├── README.md
├── docker-compose.yml
├── Dockerfile
├── .env.example
├── requirements.txt
├── Makefile
├── setup.cfg
│
├── config/
│   └── settings.py              # Pydantic-settings, env-var driven
│
├── data/
│   ├── raw/                     # Downloaded OpenF1 data
│   ├── processed/               # Transformed data
│   └── sample/                  # Sample datasets
│
├── replay/
│   ├── producer.py              # Kafka replay producer
│   ├── loader.py                # JSON / CSV / sample loader
│   └── sample_events.py         # Built-in demo data
│
├── schemas/
│   ├── telemetry.py             # TelemetryEvent
│   ├── timing.py                # TimingEvent
│   ├── race_control.py          # RaceControlEvent
│   ├── verdict.py               # Verdict + VerdictValue enum
│   └── correction.py            # Correction + CorrectionType enum
│
├── streaming/
│   ├── kafka_topics.py          # Centralised topic management
│   ├── consumer.py              # Async Kafka consumer
│   └── processor.py             # Pipeline orchestrator + metrics
│
├── rules/
│   ├── base.py                  # RuleMonitor ABC + RuleResult
│   └── pit_lane_speed.py        # Pit-lane speed monitor (60 km/h)
│
├── uncertainty/
│   └── engine.py                # RSS uncertainty + normalised margin
│
├── integrity/
│   └── detector.py              # Per-sector CUSUM detector
│
├── verdict/
│   └── engine.py                # Combines rules + uncertainty + integrity
│
├── corrections/
│   └── models.py                # Correction ORM (RETRO / PROSPECTIVE / WHAT_IF)
│
├── storage/
│   ├── database.py              # Async/sync SQLAlchemy engine
│   └── models.py                # ORM: telemetry, timing, verdicts, alerts, params
│
├── api/
│   ├── main.py                  # FastAPI app + lifespan
│   └── routes.py                # Endpoints
│
└── tests/
    ├── test_rules.py
    ├── test_uncertainty.py
    └── test_verdict.py
```

---

## Quick Start

### Option 1: Docker Compose (recommended)

```bash
cd calibre

# Start infrastructure + API + processor
docker compose up --build

# In a second terminal, run the replay producer
docker compose --profile replay up calibre-replay
```

The API is available at `http://localhost:8000`.

### Option 2: Local development

```bash
cd calibre
pip install -r requirements.txt

# Start Kafka and PostgreSQL (e.g. via Docker or local install)
docker compose up -d kafka postgres

# Initialise database tables
python -m storage.database --init

# Create Kafka topics
python -m streaming.kafka_topics

# Start the stream processor
python -m streaming.processor &

# Start the API
uvicorn api.main:app --reload --port 8000 &

# Run the replay
python -m replay.producer --speed 10
```

---

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health` | Liveness probe |
| `GET` | `/metrics` | Processing counters |
| `GET` | `/verdicts` | Recent verdicts (limit, default 50) |
| `GET` | `/sessions/{session_key}/verdicts` | Verdicts for a session |
| `GET` | `/integrity/alerts` | Recent integrity alerts |
| `POST` | `/replay/start?speed=10` | Start replay + processor |

### Example: Query verdicts

```bash
curl http://localhost:8000/verdicts | python -m json.tool
```

### Example verdict JSON

```json
{
  "verdict_id": "a1b2c3d4-...",
  "timestamp": "2024-05-26T14:31:00+00:00",
  "session_key": 9999,
  "driver_number": 4,
  "rule_id": "pit_lane_speed",
  "measured_value": 67.8,
  "threshold": 60.0,
  "uncertainty": 1.58,
  "confidence_margin": -4.94,
  "verdict": "BREACH",
  "parameter_version": "v1.0",
  "valid_from": "2024-05-26T14:31:00+00:00",
  "recorded_at": "2024-05-26T14:31:00+00:00"
}
```

---

## Running Tests

```bash
cd calibre
pytest tests/ -v
```

Tests do **not** require Kafka or PostgreSQL — they test the pure computation logic.

---

## Sample Data Coverage

The built-in sample events produce all three verdict types:

| Driver | Speed (km/h) | Verdict | Reason |
|--------|-------------|---------|--------|
| 1 (VER) | 48.2 | **CLEAR** | Well below 60 km/h limit |
| 1 (VER) | 55.0 | **CLEAR** | Below limit |
| 1 (VER) | 59.5 | **DEFER** | Within uncertainty band |
| 4 (NOR) | 67.8 | **BREACH** | Well above limit |
| 4 (NOR) | 42.0 | **CLEAR** | Well below limit |
| 16 (LEC) | 61.2 | **BREACH** | Above limit |
| 44 (HAM) | 51.3 | **CLEAR** | Below limit |

---

## What is Implemented Now

- [x] Pydantic schemas for all event types
- [x] Kafka topic management (5 topics, driver-partitioned)
- [x] Replay producer with configurable speed
- [x] Built-in deterministic sample data
- [x] Pit-lane speed rule monitor
- [x] Root-sum-square uncertainty engine
- [x] Per-sector CUSUM integrity detector
- [x] Verdict engine (rules + uncertainty + integrity)
- [x] PostgreSQL schema with bitemporal columns
- [x] Correction model (RETRO / PROSPECTIVE / WHAT_IF)
- [x] FastAPI with health, metrics, verdicts, alerts endpoints
- [x] Docker Compose (Kafka, PostgreSQL, API, processor)
- [x] Unit tests for rules, uncertainty, verdicts
- [x] Structured metrics (events, verdicts, latency)

## Intentionally Deferred (Future Research Modules)

- [ ] **Full FIA rule library** – track limits, DRS/overtake mode, SC/VSC compliance, penalty points
- [ ] **Timing-system twin** – real FIA timing-loop data is not publicly available
- [ ] **Mixed-effects decomposition** – `residual = sector_effect + car_effect + noise`
- [ ] **Bitemporal correction engine** – lineage-indexed selective replay
- [ ] **Flink integration** – replace Python stream processor with Flink for scale
- [ ] **TimescaleDB hypertables** – time-series optimised storage
- [ ] **Redis caching** – hot-path caching for real-time queries
- [ ] **Frontend dashboard** – live verdict feed, integrity heatmap
- [ ] **Full OpenF1 ingestion pipeline** – live + historical data
- [ ] **What-If scenario engine** – counterfactual analysis with different parameters

---

## Important Notes on Data Provenance

> **All timing-loop observations in the sample data are marked `source="simulation"`.**
>
> Real FIA Official Timing System loop-crossing data is not publicly available.
> The simulated timing events are synthetic and clearly labelled. They exist
> solely to exercise the integrity detection pipeline. Do not treat them as
> real FIA timing data.

---

## Configuration

All settings are driven by environment variables. See `.env.example` for the full list.

Key parameters:

| Variable | Default | Description |
|----------|---------|-------------|
| `PIT_LANE_SPEED_LIMIT_KMH` | 60.0 | Regulatory speed limit |
| `MEASUREMENT_UNCERTAINTY` | 1.5 | Sensor σ (km/h) |
| `PARAMETER_UNCERTAINTY` | 0.5 | Regulatory-parameter σ |
| `UNCERTAINTY_K` | 2.0 | Coverage factor for verdict classification |
| `CUSUM_THRESHOLD` | 5.0 | CUSUM decision boundary |
| `CUSUM_DRIFT` | 0.5 | CUSUM allowable drift |

---

## License

Research project – not yet published.
