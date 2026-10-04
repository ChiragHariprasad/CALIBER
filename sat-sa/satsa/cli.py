"""
SAT-SA — Command Line Interface (§7.2, §9.1)
============================================
Offline-first CLI providing commands:
  satsa generate  — Generate multi-CSE dataset with failure scenarios & ground truth
  satsa run       — Execute offline batch assessment on entity dataset(s)
  satsa verify    — Verify TRUST ledger cryptographically (Merkle root + Ed25519 hash chain)
"""
from __future__ import annotations

import argparse
import json
import sys
import time
import uuid
from pathlib import Path
from typing import List

# Ensure root sat-sa directory is on sys.path
SATSA_ROOT = Path(__file__).resolve().parent.parent
if str(SATSA_ROOT) not in sys.path:
    sys.path.insert(0, str(SATSA_ROOT))

from generator.generate import SyntheticDataGenerator
from satsa.api.runner import AssessmentRunner
from satsa.ledger.trust import AssessmentLedger


def main():
    parser = argparse.ArgumentParser(
        prog="satsa",
        description="SAT-SA — Supervisory Analytics Tool for SOC Assessment"
    )
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Command: satsa generate
    cmd_gen = subparsers.add_parser("generate", help="Generate multi-CSE synthetic dataset")
    cmd_gen.add_argument("--output-dir", type=str, default="./data/sample",
                         help="Directory to save generated dataset")
    cmd_gen.add_argument("--num-cses", type=int, default=40,
                         help="Number of CSE entities to generate")
    cmd_gen.add_argument("--months", type=int, default=3,
                         help="Months of data to generate")
    cmd_gen.add_argument("--seed", type=int, default=42,
                         help="RNG seed for reproducibility")

    # Command: satsa run (batch mode — processes all CSE dirs under input-dir)
    cmd_run = subparsers.add_parser("run",
                                    help="Run SAT-SA batch assessment on all entities in a directory")
    cmd_run.add_argument("--input-dir", type=str, required=True,
                         help="Directory containing CSE submission subdirectories")
    cmd_run.add_argument("--period", type=str, default="2026-Q3",
                         help="Assessment period label")
    cmd_run.add_argument("--seed", type=int, default=42,
                         help="Hold-out seed for reproducibility")
    cmd_run.add_argument("--output", type=str, default=None,
                         help="Output JSON path for run result (default: input-dir/run_result.json)")

    # Command: satsa verify
    cmd_verify = subparsers.add_parser("verify",
                                       help="Cryptographically verify TRUST ledger")
    cmd_verify.add_argument("--run", type=str, default=None,
                            help="Run ID to verify (looks for ledger in run artifacts)")
    cmd_verify.add_argument("--ledger-path", type=str, default=None,
                            help="Explicit path to TRUST ledger database")

    args = parser.parse_args()

    if args.command == "generate":
        _cmd_generate(args)
    elif args.command == "run":
        _cmd_run(args)
    elif args.command == "verify":
        _cmd_verify(args)
    else:
        parser.print_help()


def _cmd_generate(args):
    """Generate synthetic multi-CSE dataset."""
    print(f"Generating {args.num_cses} CSE dataset (months={args.months}, seed={args.seed})...")
    gen = SyntheticDataGenerator(
        n_cses=args.num_cses,
        months=args.months,
        seed=args.seed,
        output_dir=Path(args.output_dir),
    )
    gen.generate()
    print(f"[SUCCESS] Dataset generated at: {Path(args.output_dir).resolve()}")


def _cmd_run(args):
    """Run batch assessment on all CSE subdirectories."""
    input_dir = Path(args.input_dir)
    if not input_dir.exists():
        print(f"[ERROR] Input directory not found: {input_dir}")
        sys.exit(1)

    # Discover CSE directories
    cse_dirs = sorted([
        d for d in input_dir.iterdir()
        if d.is_dir() and d.name.startswith("CSE-")
    ])

    if not cse_dirs:
        print(f"[ERROR] No CSE-* directories found in {input_dir}")
        sys.exit(1)

    # Load profiles if available
    profiles_path = input_dir / "profiles.json"
    profiles_by_id = {}
    if profiles_path.exists():
        profiles_list = json.loads(profiles_path.read_text())
        profiles_by_id = {p["cse_id"]: p for p in profiles_list}

    # Setup ledger
    run_id = f"RUN-{uuid.uuid4().hex[:8].upper()}"
    ledger_path = input_dir / "trust_ledger.db"
    ledger = AssessmentLedger(storage_path=ledger_path)
    runner = AssessmentRunner(ledger=ledger)

    print(f"\n{'='*60}")
    print(f"SAT-SA BATCH ASSESSMENT")
    print(f"{'='*60}")
    print(f"Run ID        : {run_id}")
    print(f"Period        : {args.period}")
    print(f"Seed          : {args.seed}")
    print(f"Input         : {input_dir.resolve()}")
    print(f"CSEs found    : {len(cse_dirs)}")
    print(f"{'='*60}\n")

    start_time = time.time()
    all_results = []
    total_findings = 0
    total_records = 0
    all_findings_list = []
    all_review_items = []

    for i, cse_dir in enumerate(cse_dirs):
        cse_id = cse_dir.name
        profile = profiles_by_id.get(cse_id, {})

        result = runner.run_assessment(
            entity_id=cse_id,
            entity_name=profile.get("entity_name", f"Entity {cse_id}"),
            sector=profile.get("sector", "Unknown"),
            submission_dir=cse_dir,
            seed=args.seed,
        )

        sai = result["score"].sai_score
        tier = result["score"].priority_tier
        n_findings = result["findings_count"]
        total_findings += n_findings

        # Collect findings and review samples
        for f in result.get("findings", []):
            all_findings_list.append(f.model_dump())
            all_review_items.append({
                "finding_id": f.finding_id,
                "signal_code": f.signal_card_id,
                "finding_title": f.rationale[:80],
                "severity": f.severity,
                "record_id": f.evidence_records[0] if f.evidence_records else f"REC-{f.finding_id[:8]}",
                "sampling_rationale": f"High priority anomaly detected in stratum {f.stratum}",
                "priority_score": round(f.severity * f.confidence, 3),
            })

        status = "WARN" if "Tier 1" in tier or "Tier 2" in tier else "OK  "
        print(f"  [{i+1:3d}/{len(cse_dirs)}] [{status}] {cse_id:10s}  "
              f"SAI={sai:5.1f}  Findings={n_findings:3d}  {tier}")

        all_results.append({
            "cse_id": cse_id,
            "entity_name": result["entity_name"],
            "sector": result["sector"],
            "sai_score": result["score"].sai_score,
            "priority_tier": result["score"].priority_tier,
            "noisy_or_p_failure": result["score"].noisy_or_p_failure,
            "findings_count": n_findings,
            "critical_findings": result["score"].critical_findings_count,
            "high_findings": result["score"].high_findings_count,
            "merkle_root": result["merkle_root"],
            "run_block_hash": result["run_block_hash"],
            "elapsed_seconds": result["elapsed_seconds"],
        })

    elapsed = round(time.time() - start_time, 3)

    # Verify ledger
    verify_report = ledger.verify_chain()

    # Build run result
    run_result = {
        "run_id": run_id,
        "period": args.period,
        "seed": args.seed,
        "entity_count": len(cse_dirs),
        "total_findings": total_findings,
        "ledger_blocks": verify_report["total_blocks"],
        "ledger_verified": verify_report["chain_valid"] and verify_report["signatures_valid"],
        "runtime_seconds": elapsed,
        "ledger_path": str(ledger_path.resolve()),
        "entities": all_results,
    }

    # Save result to input-dir/run_result.json and artifacts/e2e/run_result.json
    output_path = Path(args.output) if args.output else (input_dir / "run_result.json")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(run_result, indent=2, default=str), encoding="utf-8")

    # Save all_findings.json and review_queue.json
    (input_dir / "all_findings.json").write_text(json.dumps(all_findings_list, indent=2, default=str), encoding="utf-8")
    (input_dir / "review_queue.json").write_text(json.dumps(all_review_items, indent=2, default=str), encoding="utf-8")

    # Also save to artifacts/e2e and runs index
    artifacts_e2e = SATSA_ROOT / "artifacts" / "e2e"
    artifacts_e2e.mkdir(parents=True, exist_ok=True)
    (artifacts_e2e / "run_result.json").write_text(json.dumps(run_result, indent=2, default=str), encoding="utf-8")
    (artifacts_e2e / "all_findings.json").write_text(json.dumps(all_findings_list, indent=2, default=str), encoding="utf-8")
    (artifacts_e2e / "review_queue.json").write_text(json.dumps(all_review_items, indent=2, default=str), encoding="utf-8")

    runs_index_file = SATSA_ROOT / "artifacts" / "runs.json"
    runs_index_file.parent.mkdir(parents=True, exist_ok=True)
    runs_index = {}
    if runs_index_file.exists():
        try:
            runs_index = json.loads(runs_index_file.read_text(encoding="utf-8"))
        except Exception:
            runs_index = {}
    runs_index[run_id] = {
        "run_id": run_id,
        "period": args.period,
        "ledger_path": str(ledger_path.resolve()),
        "output_path": str(output_path.resolve()),
        "timestamp": time.time(),
    }
    runs_index_file.write_text(json.dumps(runs_index, indent=2), encoding="utf-8")

    print(f"\n{'='*60}")
    print(f"ASSESSMENT COMPLETE")
    print(f"{'='*60}")
    print(f"Run ID          : {run_id}")
    print(f"Entities        : {len(cse_dirs)}")
    print(f"Total Findings  : {total_findings}")
    print(f"Ledger Blocks   : {verify_report['total_blocks']}")
    print(f"Ledger Verified : {'YES' if run_result['ledger_verified'] else 'NO'}")
    print(f"Runtime         : {elapsed}s")
    print(f"Result saved    : {output_path.resolve()}")
    print(f"{'='*60}")

    ledger.close()


def _cmd_verify(args):
    """Verify TRUST ledger integrity."""
    ledger_path = None
    if args.ledger_path:
        ledger_path = Path(args.ledger_path)
    elif args.run:
        # Check runs index
        runs_index_file = SATSA_ROOT / "artifacts" / "runs.json"
        if runs_index_file.exists():
            try:
                runs_index = json.loads(runs_index_file.read_text(encoding="utf-8"))
                if args.run in runs_index:
                    ledger_path = Path(runs_index[args.run]["ledger_path"])
            except Exception:
                pass
        
        # Check artifacts/e2e/run_result.json
        e2e_result = SATSA_ROOT / "artifacts" / "e2e" / "run_result.json"
        if not ledger_path and e2e_result.exists():
            try:
                data = json.loads(e2e_result.read_text(encoding="utf-8"))
                if data.get("run_id") == args.run:
                    ledger_path = Path(data.get("ledger_path", "./data/e2e/trust_ledger.db"))
            except Exception:
                pass

        if not ledger_path:
            # Try to find ledger in common locations
            for candidate in [
                Path(f"./data/e2e/trust_ledger.db"),
                Path(f"./data/sample/trust_ledger.db"),
                Path(f"./data/validation_run/trust_ledger.db"),
                SATSA_ROOT / "data" / "e2e" / "trust_ledger.db",
            ]:
                if candidate.exists():
                    ledger_path = candidate
                    break
        if not ledger_path:
            print("[ERROR] Could not find trust_ledger.db. Use --ledger-path.")
            sys.exit(1)
    else:
        ledger_path = Path("./trust_ledger.db")

    if not ledger_path.exists():
        print(f"[ERROR] Ledger not found: {ledger_path}")
        sys.exit(1)

    print(f"Verifying TRUST Ledger: {ledger_path}")
    ledger = AssessmentLedger(storage_path=ledger_path)
    report = ledger.verify_chain()

    print(f"\n{'='*60}")
    print("TRUST LEDGER CRYPTOGRAPHIC VERIFICATION REPORT")
    print(f"{'='*60}")
    print(f"Total Blocks     : {report['total_blocks']}")
    print(f"Hash Chain Valid : {'YES [VERIFIED]' if report['chain_valid'] else 'NO [CORRUPTED]'}")
    print(f"Signatures Valid : {'YES [VERIFIED]' if report['signatures_valid'] else 'NO [INVALID]'}")

    if report['tampered_blocks']:
        print(f"\n[ALERT] Tampered blocks detected: {report['tampered_blocks']}")
        for err in report.get('errors', []):
            print(f"  • {err}")
        print(f"\n{'='*60}")
        print("VERIFICATION FAILED")
        print(f"{'='*60}")
        ledger.close()
        sys.exit(1)
    else:
        print(f"\n{'='*60}")
        print("VERIFIED")
        print(f"{'='*60}")

    ledger.close()


if __name__ == "__main__":
    main()
