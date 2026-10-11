"""
CALIBRE — API Contract Validation Suite (Workstream F, 03_API_Contract.md)
===========================================================================
Validates status codes, schemas, operating mode tagging, and provenance
for all /api/v1 REST endpoints.
"""

import pytest
from fastapi.testclient import TestClient

from calibre.api.main import app

client = TestClient(app)


def test_contract_root_health():
    r = client.get("/health")
    assert r.status_code == 200
    data = r.json()
    assert data["status"] == "ok"
    assert data["service"] == "calibre"


def test_contract_api_health():
    r = client.get("/api/v1/health")
    assert r.status_code == 200
    data = r.json()
    assert "dependencies" in data
    assert "kafka" in data["dependencies"]
    assert "api" in data["dependencies"]
    assert "checked_at" in data


def test_contract_streaming_status():
    r = client.get("/api/v1/streaming/status")
    assert r.status_code == 200
    data = r.json()
    assert "broker_connected" in data
    assert "topics_count" in data
    assert "processing_latency_ms" in data


def test_contract_sessions_list():
    r = client.get("/api/v1/sessions")
    assert r.status_code == 200
    data = r.json()
    assert isinstance(data, list)
    assert len(data) >= 2
    first = data[0]
    assert "session_id" in first
    assert "meeting_name" in first
    assert "circuit_short_name" in first


def test_contract_session_detail_and_404():
    # Valid session
    r = client.get("/api/v1/sessions/11731")
    assert r.status_code == 200
    data = r.json()
    assert data["session_id"] == "11731"

    # Missing session
    r_bad = client.get("/api/v1/sessions/non_existent_session_999999")
    assert r_bad.status_code == 404


def test_contract_replay_lifecycle():
    # 1. Start Replay
    r_start = client.post("/api/v1/replay", json={
        "session_id": "11731",
        "driver": "VER",
        "speed": 5.0,
    })
    assert r_start.status_code == 201
    rep = r_start.json()
    assert "replay_id" in rep
    rep_id = rep["replay_id"]
    assert rep["operating_mode"] == "HISTORICAL_REPLAY"

    # 2. Get Replay Status
    r_status = client.get(f"/api/v1/replay/{rep_id}")
    assert r_status.status_code == 200
    assert r_status.json()["replay_id"] == rep_id

    # 3. Pause
    r_pause = client.post(f"/api/v1/replay/{rep_id}/pause")
    assert r_pause.status_code == 200
    assert r_pause.json()["status"] == "PAUSED"

    # 4. Resume
    r_resume = client.post(f"/api/v1/replay/{rep_id}/resume")
    assert r_resume.status_code == 200
    assert r_resume.json()["status"] == "PLAYING"

    # 5. Speed Adjustment
    r_speed = client.patch(f"/api/v1/replay/{rep_id}/speed", json={"speed": 10.0})
    assert r_speed.status_code == 200
    assert r_speed.json()["speed"] == 10.0

    # 6. Stop
    r_stop = client.post(f"/api/v1/replay/{rep_id}/stop")
    assert r_stop.status_code == 200
    assert r_stop.json()["status"] == "STOPPED"


def test_contract_live_race():
    r = client.get("/api/v1/live/race")
    assert r.status_code == 200
    data = r.json()
    assert "current_lap" in data
    assert "positions" in data
    assert "provenance" in data
    assert data["provenance"]["positions"] == "observed"


def test_contract_reference_performance():
    r = client.get("/api/v1/live/reference-performance")
    assert r.status_code == 200
    data = r.json()
    assert "model_version" in data
    assert "baseline_selection" in data
    assert "exclusions" in data
    assert "uncertainty_s" in data


def test_contract_driver_performance_and_diagnostics():
    r_perf = client.get("/api/v1/live/drivers/VER/performance")
    assert r_perf.status_code == 200
    assert "sectors" in r_perf.json()

    r_diag = client.get("/api/v1/live/drivers/VER/diagnostics")
    assert r_diag.status_code == 200
    assert "diagnoses" in r_diag.json()


def test_contract_integrity_and_scenarios():
    # List integrity
    r = client.get("/api/v1/live/integrity")
    assert r.status_code == 200
    assert "active_anomalies" in r.json()

    # Fault injection scenario
    r_fault = client.post("/api/v1/live/integrity/scenarios", json={
        "fault_type": "spike",
        "driver": "VER",
        "signal": "speed_kph",
        "magnitude": 130.0,
    })
    assert r_fault.status_code == 200
    finding = r_fault.json()["finding"]
    assert finding["category"] == "SPIKE_ANOMALY"
    assert finding["is_synthetic"] is True


def test_contract_verdicts_and_pit_monitor():
    r_v = client.get("/api/v1/live/verdicts")
    assert r_v.status_code == 200
    assert "stats" in r_v.json()

    r_pm = client.get("/api/v1/live/pit_monitor")
    assert r_pm.status_code == 200
    assert "configured_limit_kmh" in r_pm.json()


def test_contract_strategy_and_alerts():
    r_s = client.get("/api/v1/live/strategy")
    assert r_s.status_code == 200
    assert "candidates" in r_s.json()

    r_a = client.get("/api/v1/live/alerts")
    assert r_a.status_code == 200
    assert "alerts" in r_a.json()

    r_r = client.get("/api/v1/live/radio")
    assert r_r.status_code == 200
    assert "target_driver" in r_r.json()
