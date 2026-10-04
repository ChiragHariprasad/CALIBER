"""
E2E Test: SAT-SA REST API Contract (§3 Step 6, §16)
===================================================
Tests typed API response endpoints using FastAPI TestClient.
"""
import pytest
from pathlib import Path
from fastapi.testclient import TestClient

from satsa.api.main import app, state


@pytest.fixture(scope="module")
def client():
    # Load test data from data/e2e
    data_dir = Path("./data/e2e")
    if data_dir.exists():
        state.load_from_directory(data_dir)
    return TestClient(app)


def test_api_health(client):
    res = client.get("/api/v1/health")
    assert res.status_code == 200
    data = res.json()
    assert data["status"] == "ok"
    assert data["version"] == "1.0.0"


def test_api_runs(client):
    res = client.get("/api/v1/runs")
    assert res.status_code == 200
    runs = res.json()
    assert isinstance(runs, list)
    if runs:
        run = runs[0]
        assert "run_id" in run
        assert "entity_count" in run
        assert "total_findings" in run


def test_api_entities(client):
    res = client.get("/api/v1/entities")
    assert res.status_code == 200
    entities = res.json()
    assert isinstance(entities, list)
    if entities:
        ent = entities[0]
        assert "cse_id" in ent
        assert "sai_score" in ent
        assert "priority_tier" in ent
        assert 0.0 <= ent["sai_score"] <= 100.0

        # Test single entity detail
        res_detail = client.get(f"/api/v1/entities/{ent['cse_id']}")
        assert res_detail.status_code == 200
        detail = res_detail.json()
        assert detail["cse_id"] == ent["cse_id"]


def test_api_findings_and_evidence(client):
    res = client.get("/api/v1/findings?limit=10")
    assert res.status_code == 200
    findings = res.json()
    assert isinstance(findings, list)
    if findings:
        f = findings[0]
        assert "finding_id" in f
        assert "severity" in f
        assert "confidence" in f
        assert "rationale" in f

        # Test finding detail
        res_f = client.get(f"/api/v1/findings/{f['finding_id']}")
        assert res_f.status_code == 200
        f_detail = res_f.json()
        assert f_detail["finding_id"] == f["finding_id"]

        # Test evidence drill-down
        res_ev = client.get(f"/api/v1/findings/{f['finding_id']}/evidence")
        assert res_ev.status_code == 200
        assert isinstance(res_ev.json(), list)


def test_api_review_queue(client):
    res = client.get("/api/v1/review-queue")
    assert res.status_code == 200
    assert isinstance(res.json(), list)


def test_api_validation(client):
    res = client.get("/api/v1/validation")
    assert res.status_code == 200
    val = res.json()
    assert "v1_precision" in val
    assert "v1_recall" in val
    assert "v2_spearman" in val


def test_api_ledger_and_verify(client):
    res = client.get("/api/v1/ledger/default")
    assert res.status_code in [200, 404]

    res_v = client.post("/api/v1/ledger/default/verify")
    assert res_v.status_code in [200, 404]
    if res_v.status_code == 200:
        v_data = res_v.json()
        assert v_data["status"] in ["VERIFIED", "FAILED"]
