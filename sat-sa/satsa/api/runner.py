"""
SAT-SA — Assessment Runner & Orchestrator (§5.1, §7)
===================================================
Orchestrates complete end-to-end evaluation of an entity submission:
Ingest -> All 12 Signal Detectors -> Peer Baselines -> SAI Scoring -> Examiner Sampling -> TRUST Ledger Block.
"""
from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Any, Optional
import time
import hashlib

from satsa.ingest.pipeline import IngestPipeline
from satsa.signals.core_signals import ALL_CORE_SIGNALS
from satsa.scoring.engine import ScoringEngine, EntityScoreResult
from satsa.sampling.sampler import PriorityRecordSampler, ExaminerSampleBatch
from satsa.ledger.trust import AssessmentLedger
from satsa.ledger.merkle import MerkleTree
from satsa.evidence.models import Finding, Dimension, SignalType, BaselineType


class AssessmentRunner:
    """
    Main orchestrator for SAT-SA batch evaluation.
    """

    def __init__(
        self,
        ledger: Optional[AssessmentLedger] = None,
        scoring_engine: Optional[ScoringEngine] = None,
        sampler: Optional[PriorityRecordSampler] = None,
    ):
        self.ledger = ledger or AssessmentLedger()
        self.scoring_engine = scoring_engine or ScoringEngine()
        self.sampler = sampler or PriorityRecordSampler()

    def run_assessment(
        self,
        entity_id: str,
        entity_name: str,
        sector: str,
        submission_dir: Path,
        sample_size: int = 15,
        seed: int = 42
    ) -> Dict[str, Any]:
        """
        Execute full SAT-SA assessment on an entity submission.
        """
        start_time = time.time()
        submission_dir = Path(submission_dir)

        # 1. Ingest submission into DuckDB store
        pipeline = IngestPipeline()
        store, missing_mandatory = pipeline.process_submission(submission_dir)

        # 2. Record Submission Block in TRUST Ledger
        file_hashes = {}
        for fpath in submission_dir.glob("*.parquet"):
            with open(fpath, "rb") as f:
                file_hashes[fpath.name] = hashlib.sha256(f.read()).hexdigest()

        sub_block = self.ledger.record_submission(
            entity_id=entity_id,
            entity_name=entity_name,
            sector=sector,
            submission_path=str(submission_dir),
            file_hashes=file_hashes
        )

        # 3. Load tables as Polars DataFrames and execute all 12 Signal Detectors
        table_data = store.get_all_tables_as_polars(entity_id)
        all_findings: List[Finding] = []
        detector_runtimes = {}

        for DetectorCls in ALL_CORE_SIGNALS:
            detector = DetectorCls()
            t0 = time.time()
            try:
                findings = detector.detect(entity_id, table_data)
                all_findings.extend(findings)
                detector_runtimes[detector.signal_id] = round(time.time() - t0, 4)
            except Exception as ex:
                all_findings.append(Finding(
                    finding_id=f"ERR-{detector.signal_id}",
                    cse_id=entity_id,
                    dimension=Dimension.SECURITY_OPERATIONS,
                    signal_type=SignalType.ANOMALY,
                    signal_card_id=detector.signal_id,
                    baseline_type=BaselineType.PEER,
                    stratum="error",
                    metric="execution_error",
                    observed=1.0,
                    expected=0.0,
                    severity=0.2,
                    confidence=0.5,
                    evidence_count=0,
                    data_completeness=1.0,
                    rationale=f"Signal execution error: {str(ex)}"
                ))

        # 4. Compute SAI Score & Priority Tier
        score_result = self.scoring_engine.calculate_sai(
            entity_id=entity_id,
            entity_name=entity_name,
            sector=sector,
            findings=all_findings,
            missing_mandatory_tables=missing_mandatory
        )

        # 5. Priority Examiner Record Sampling
        sample_batch = self.sampler.draw_sample(
            entity_id=entity_id,
            findings=all_findings,
            sample_size=sample_size,
            seed=seed
        )

        # 6. Build Merkle Tree of Findings
        finding_jsons = [f.model_dump_json(exclude={"created_at"}) for f in all_findings]
        merkle_tree = MerkleTree.build(finding_jsons)

        # 7. Record Assessment Run Block in TRUST Ledger
        run_block = self.ledger.record_assessment_run(
            submission_block_hash=sub_block.header.block_hash,
            findings=[f.model_dump() for f in all_findings],
            sai_score=score_result.sai_score,
            priority_tier=score_result.priority_tier
        )

        elapsed = round(time.time() - start_time, 3)

        return {
            "entity_id": entity_id,
            "entity_name": entity_name,
            "sector": sector,
            "score": score_result,
            "sample_batch": sample_batch,
            "merkle_root": merkle_tree.root,
            "submission_block_hash": sub_block.header.block_hash,
            "run_block_hash": run_block.header.block_hash,
            "elapsed_seconds": elapsed,
            "findings_count": len(all_findings),
            "findings": all_findings,
            "missing_mandatory_tables": missing_mandatory,
        }
