"""
SAT-SA — FastAPI Application (§16)
====================================
Typed REST API serving assessment results, findings, evidence,
review queues, validation metrics, and TRUST ledger data.

All endpoints use Pydantic response models. No arbitrary dicts.
"""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import List, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from satsa.evidence.models import Finding, Dimension, SignalType
from satsa.ledger.trust import AssessmentLedger
from satsa.store.duckdb_store import DuckDBStore

logger = logging.getLogger(__name__)


# ── Response Models ─────────────────────────────────────────────────────

class HealthResponse(BaseModel):
    status: str = "ok"
    version: str = "1.0.0"


class RunSummary(BaseModel):
    run_id: str
    period: str
    seed: int
    entity_count: int
    total_findings: int
    ledger_blocks: int
    ledger_verified: bool
    runtime_seconds: float


class EntitySummary(BaseModel):
    cse_id: str
    entity_name: str
    sector: str
    sai_score: float
    priority_tier: str
    noisy_or_p_failure: float
    findings_count: int
    critical_findings: int
    high_findings: int
    merkle_root: str


class EntityDetail(EntitySummary):
    findings: List[Finding] = Field(default_factory=list)


class FindingSummary(BaseModel):
    finding_id: str
    cse_id: str
    dimension: str
    signal_type: str
    signal_card_id: str
    metric: str
    observed: float
    expected: float
    severity: float
    confidence: float
    evidence_count: int
    rationale: str
    status: str


class FindingDetail(BaseModel):
    finding_id: str
    cse_id: str
    dimension: str
    signal_type: str
    signal_card_id: str
    baseline_type: str
    stratum: str
    metric: str
    observed: float
    expected: float
    p_value: Optional[float]
    q_value: Optional[float]
    severity: float
    confidence: float
    evidence_count: int
    data_completeness: float
    rationale: str
    evidence_records: List[str]
    ruleset_version: str
    status: str


class ReviewQueueItem(BaseModel):
    finding_id: str
    signal_code: str
    finding_title: str
    severity: float
    record_id: str
    sampling_rationale: str
    priority_score: float


class NegativeSpaceItem(BaseModel):
    cse_id: str
    category: str
    expected_count: float
    observed_count: float
    signal_card_id: str
    severity: float
    confidence: float
    rationale: str


class ValidationSummary(BaseModel):
    v1_precision: float = 0.0
    v1_recall: float = 0.0
    v1_f1: float = 0.0
    v2_spearman: float = 0.0
    v2_ndcg10: float = 0.0
    v3_lift_vs_random: float = 0.0
    v4_false_positive_rate: float = 0.0
    v7_explainability_coverage: float = 0.0
    records_processed: int = 0
    runtime_seconds: float = 0.0


class LedgerBlock(BaseModel):
    block_id: int
    block_type: str
    timestamp: str
    payload_hash: str
    payload: dict
    previous_hash: str
    block_hash: str
    signature: str


class LedgerVerifyResponse(BaseModel):
    total_blocks: int
    chain_valid: bool
    signatures_valid: bool
    tampered_blocks: List[int]
    errors: List[str]
    status: str  # "VERIFIED" or "FAILED"


# ── Application State ──────────────────────────────────────────────────

class AppState:
    """Holds loaded run data for the API."""

    def __init__(self):
        self.run_data: Optional[dict] = None
        self.entities: dict[str, dict] = {}
        self.findings: List[Finding] = []
        self.findings_by_id: dict[str, Finding] = {}
        self.findings_by_cse: dict[str, List[Finding]] = {}
        self.review_queue: List[dict] = []
        self.ledger: Optional[AssessmentLedger] = None
        self.validation: Optional[dict] = None
        self.data_dir: Optional[Path] = None

    def load_from_directory(self, data_dir: Path):
        """Load run results from a data directory."""
        self.data_dir = Path(data_dir)

        # Load run result
        run_result_path = self.data_dir / "run_result.json"
        if run_result_path.exists():
            self.run_data = json.loads(run_result_path.read_text())
            for entity in self.run_data.get("entities", []):
                self.entities[entity["cse_id"]] = entity

        # Load review queue
        rq_path = self.data_dir / "review_queue.json"
        if rq_path.exists():
            try:
                self.review_queue = json.loads(rq_path.read_text(encoding="utf-8"))
            except Exception:
                pass

        # Load all findings from individual CSE runs
        # We need to re-run or load cached findings
        self._load_findings()

        # Load ledger
        ledger_path = self.data_dir / "trust_ledger.db"
        if ledger_path.exists():
            self.ledger = AssessmentLedger(storage_path=ledger_path)

        # Load validation if exists
        val_path = self.data_dir / "validation_summary.json"
        if val_path.exists():
            self.validation = json.loads(val_path.read_text())

    def _load_findings(self):
        """Load cached findings from run artifacts."""
        findings_path = self.data_dir / "all_findings.json"
        if findings_path.exists():
            findings_data = json.loads(findings_path.read_text())
            for fd in findings_data:
                f = Finding(**fd)
                self.findings.append(f)
                self.findings_by_id[f.finding_id] = f
                if f.cse_id not in self.findings_by_cse:
                    self.findings_by_cse[f.cse_id] = []
                self.findings_by_cse[f.cse_id].append(f)


state = AppState()


# ── FastAPI App ────────────────────────────────────────────────────────

app = FastAPI(
    title="SAT-SA API",
    description="Supervisory Analytics Tool for SOC Assessment — REST API",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_data_dir() -> Path:
    """Resolve data directory from environment or default."""
    import os
    return Path(os.environ.get("SATSA_DATA_DIR", "./data/e2e"))


@app.on_event("startup")
async def startup():
    data_dir = get_data_dir()
    if data_dir.exists():
        state.load_from_directory(data_dir)
        logger.info("Loaded data from %s", data_dir)


# ── Endpoints ──────────────────────────────────────────────────────────

@app.get("/api/v1/health", response_model=HealthResponse)
async def health():
    return HealthResponse()


@app.get("/api/v1/runs", response_model=List[RunSummary])
async def list_runs():
    if not state.run_data:
        return []
    return [RunSummary(**{
        k: state.run_data[k]
        for k in RunSummary.model_fields.keys()
        if k in state.run_data
    })]


@app.get("/api/v1/runs/{run_id}", response_model=RunSummary)
async def get_run(run_id: str):
    if not state.run_data or state.run_data.get("run_id") != run_id:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")
    return RunSummary(**{
        k: state.run_data[k]
        for k in RunSummary.model_fields.keys()
        if k in state.run_data
    })


@app.get("/api/v1/entities", response_model=List[EntitySummary])
async def list_entities():
    return [EntitySummary(**e) for e in state.entities.values()]


@app.get("/api/v1/entities/{cse_id}", response_model=EntityDetail)
async def get_entity(cse_id: str):
    entity = state.entities.get(cse_id)
    if not entity:
        raise HTTPException(status_code=404, detail=f"Entity {cse_id} not found")
    findings = state.findings_by_cse.get(cse_id, [])
    return EntityDetail(**entity, findings=findings)


@app.get("/api/v1/findings", response_model=List[FindingSummary])
async def list_findings(
    cse_id: Optional[str] = Query(None),
    dimension: Optional[str] = Query(None),
    signal_type: Optional[str] = Query(None),
    min_severity: float = Query(0.0),
    limit: int = Query(100, le=500),
    offset: int = Query(0),
):
    results = state.findings
    if cse_id:
        results = [f for f in results if f.cse_id == cse_id]
    if dimension:
        results = [f for f in results if f.dimension.value == dimension]
    if signal_type:
        results = [f for f in results if f.signal_type.value == signal_type]
    if min_severity > 0:
        results = [f for f in results if f.severity >= min_severity]
    results = sorted(results, key=lambda f: f.severity, reverse=True)
    page = results[offset:offset + limit]
    return [FindingSummary(
        finding_id=f.finding_id,
        cse_id=f.cse_id,
        dimension=f.dimension.value,
        signal_type=f.signal_type.value,
        signal_card_id=f.signal_card_id,
        metric=f.metric,
        observed=f.observed,
        expected=f.expected,
        severity=f.severity,
        confidence=f.confidence,
        evidence_count=f.evidence_count,
        rationale=f.rationale,
        status=f.status.value,
    ) for f in page]


@app.get("/api/v1/findings/{finding_id}", response_model=FindingDetail)
async def get_finding(finding_id: str):
    f = state.findings_by_id.get(finding_id)
    if not f:
        raise HTTPException(status_code=404, detail=f"Finding {finding_id} not found")
    return FindingDetail(
        finding_id=f.finding_id,
        cse_id=f.cse_id,
        dimension=f.dimension.value,
        signal_type=f.signal_type.value,
        signal_card_id=f.signal_card_id,
        baseline_type=f.baseline_type.value,
        stratum=f.stratum,
        metric=f.metric,
        observed=f.observed,
        expected=f.expected,
        p_value=f.p_value,
        q_value=f.q_value,
        severity=f.severity,
        confidence=f.confidence,
        evidence_count=f.evidence_count,
        data_completeness=f.data_completeness,
        rationale=f.rationale,
        evidence_records=f.evidence_records,
        ruleset_version=f.ruleset_version,
        status=f.status.value,
    )


@app.get("/api/v1/findings/{finding_id}/evidence", response_model=List[str])
async def get_finding_evidence(finding_id: str):
    f = state.findings_by_id.get(finding_id)
    if not f:
        raise HTTPException(status_code=404, detail=f"Finding {finding_id} not found")
    return f.evidence_records


@app.get("/api/v1/review-queue", response_model=List[ReviewQueueItem])
async def get_review_queue():
    return state.review_queue


@app.get("/api/v1/negative-space", response_model=List[NegativeSpaceItem])
async def get_negative_space():
    neg_space = [
        f for f in state.findings
        if f.signal_type == SignalType.NEGATIVE_SPACE
    ]
    return [NegativeSpaceItem(
        cse_id=f.cse_id,
        category=f.stratum,
        expected_count=f.expected,
        observed_count=f.observed,
        signal_card_id=f.signal_card_id,
        severity=f.severity,
        confidence=f.confidence,
        rationale=f.rationale,
    ) for f in neg_space]


@app.get("/api/v1/validation", response_model=ValidationSummary)
async def get_validation():
    if state.validation:
        return ValidationSummary(**state.validation)
    return ValidationSummary()


@app.get("/api/v1/ledger/{run_id}", response_model=List[LedgerBlock])
async def get_ledger(run_id: str):
    if not state.ledger:
        raise HTTPException(status_code=404, detail="Ledger not available")
    chain = state.ledger.get_chain()
    return [LedgerBlock(**b) for b in chain]


@app.post("/api/v1/ledger/{run_id}/verify", response_model=LedgerVerifyResponse)
async def verify_ledger(run_id: str):
    if not state.ledger:
        raise HTTPException(status_code=404, detail="Ledger not available")
    report = state.ledger.verify_chain()
    status = "VERIFIED" if report["chain_valid"] and report["signatures_valid"] else "FAILED"
    return LedgerVerifyResponse(
        total_blocks=report["total_blocks"],
        chain_valid=report["chain_valid"],
        signatures_valid=report["signatures_valid"],
        tampered_blocks=report.get("tampered_blocks", []),
        errors=report.get("errors", []),
        status=status,
    )


_root_dir = Path(__file__).resolve().parent.parent.parent
_dist_dir = _root_dir / "dist"
if not _dist_dir.exists():
    _dist_dir = Path(__file__).parent.parent / "ui-web" / "dist"
if _dist_dir.exists():
    app.mount("/", StaticFiles(directory=str(_dist_dir), html=True), name="static")
