"""
SAT-SA — Synthetic Data Generator (§4.2)
==========================================
Generates a multi-CSE dataset with planted failure scenarios,
benign look-alikes, and ground truth.

**NEVER imports satsa/ analytics code** — the generator is a
separate package to prevent circular validation.

Usage::
    python -m generator --cses 40 --months 3 --seed 2026
    python -m generator --cses 5 --months 1 --seed 42 --quick
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import random
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, List, Optional

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)

# ── CSE profiles ─────────────────────────────────────────────────────────

SECTORS = [
    "Banking", "Telecom", "Energy", "Healthcare",
    "Government", "Defence", "IT_Services", "Transport",
]

CATEGORIES = [
    "malware", "phishing", "brute_force", "lateral_movement",
    "data_exfiltration", "ransomware", "insider_threat",
    "web_attack", "dos", "recon",
]

ASSET_TYPES = ["server", "workstation", "network_device", "scada", "iot", "cloud_vm"]

NOTE_TEMPLATES_HEALTHY = [
    "Investigated {category} alert on {asset}. Confirmed {disposition}. Remediation applied.",
    "Alert triaged. Source IP {src_ip} blocked at firewall. Case closed.",
    "Escalated to Tier 3. Root cause identified: {root_cause}. Patch applied.",
    "Correlated with {n} related alerts. Malware sample collected and analysed.",
    "User awareness follow-up completed. No further action required.",
    "Network segment isolated. Full forensic image captured for evidence.",
    "SIEM rule tuned to reduce false positives from {category} on {asset_type}.",
    "Incident response plan activated. Containment achieved within SLA.",
    "Threat intel enrichment complete. IOCs shared with peer organisations.",
    "Post-incident review completed. Lessons learned documented.",
]

NOTE_TEMPLATES_WEAK = [
    "Closed.",
    "No action needed.",
    "FP.",
]


@dataclass
class CSEProfile:
    """Hidden profile of a CSE — drives generation parameters."""
    cse_id: str
    sector: str
    n_assets: int
    maturity: float          # 0.0 (weak) to 1.0 (strong)
    n_analysts: int
    shift_model: str         # "24x7" | "office_hours"
    sla_critical_hours: int
    sla_high_hours: int
    note_bank: list[str] = field(default_factory=list)
    failure_scenarios: list[dict] = field(default_factory=list)
    is_benign_lookalike: bool = False
    lookalike_type: Optional[str] = None


@dataclass
class GroundTruth:
    """Planted truth for each CSE, for validation."""
    cse_id: str
    scenarios: list[dict]     # [{scenario, dose, start, end, affected_assets, affected_records}]
    is_benign: bool
    benign_type: Optional[str]


# ── Failure scenario injectors ───────────────────────────────────────────

def _inject_f1_fast_critical_closure(alerts: list[dict], dose: str, rng: np.random.Generator) -> list[str]:
    """F1: Critical alerts closed suspiciously fast."""
    affected = []
    speed_factor = {"mild": 0.3, "moderate": 0.15, "severe": 0.05}[dose]
    for a in alerts:
        if a["severity"] == "CRITICAL" and a["closed_ts"] and rng.random() < 0.6:
            created = datetime.fromisoformat(a["created_ts"])
            original_close = datetime.fromisoformat(a["closed_ts"])
            diff = (original_close - created).total_seconds()
            new_diff = max(60, diff * speed_factor)
            a["closed_ts"] = (created + timedelta(seconds=new_diff)).isoformat()
            affected.append(a["alert_id"])
    return affected


def _inject_f2_escalation_suppression(escalations: list[dict], alerts: list[dict],
                                        dose: str, rng: np.random.Generator) -> list[str]:
    """F2: Critical cases not escalated when they should be."""
    drop_rate = {"mild": 0.3, "moderate": 0.6, "severe": 0.9}[dose]
    affected = []
    critical_case_ids = set()
    for a in alerts:
        if a["severity"] == "CRITICAL":
            critical_case_ids.add(a.get("_case_id"))
    to_remove = []
    for i, e in enumerate(escalations):
        if e["case_id"] in critical_case_ids and rng.random() < drop_rate:
            to_remove.append(i)
            affected.append(e["esc_id"])
    for i in reversed(to_remove):
        escalations.pop(i)
    return affected


def _inject_f3_telemetry_blind_spot(telemetry: list[dict], assets: list[dict],
                                      dose: str, rng: np.random.Generator) -> list[str]:
    """F3: Log sources on critical assets stop reporting."""
    n_affected = {"mild": 1, "moderate": 3, "severe": 5}[dose]
    critical_assets = [a for a in assets if a["criticality"] == "critical"]
    chosen = rng.choice(critical_assets, min(n_affected, len(critical_assets)), replace=False)
    affected_ids = [a["asset_id"] for a in chosen]
    cutoff_idx = len(telemetry) // 2
    telemetry[cutoff_idx:] = [t for t in telemetry[cutoff_idx:]
                               if t["asset_id"] not in affected_ids]
    return affected_ids


def _inject_f5_template_notes(cases: list[dict], weak_notes: list[str],
                                dose: str, rng: np.random.Generator) -> list[str]:
    """F5: Investigation notes are copy-paste templates."""
    rate = {"mild": 0.4, "moderate": 0.7, "severe": 0.95}[dose]
    affected = []
    for c in cases:
        if c.get("closure_note") and rng.random() < rate:
            c["closure_note"] = rng.choice(weak_notes)
            affected.append(c["case_id"])
    return affected


def _inject_f6_kpi_gaming(alerts: list[dict], dose: str,
                            rng: np.random.Generator) -> list[str]:
    """F6: Closures bunch just before SLA edges."""
    rate = {"mild": 0.2, "moderate": 0.5, "severe": 0.8}[dose]
    affected = []
    sla_hours = {"CRITICAL": 4, "HIGH": 24, "MEDIUM": 72, "LOW": 168}
    for a in alerts:
        if a["closed_ts"] and rng.random() < rate:
            sla = sla_hours.get(a["severity"], 72)
            created = datetime.fromisoformat(a["created_ts"])
            # Place closure just under the SLA edge
            jitter = rng.uniform(0.85, 0.98)
            a["closed_ts"] = (created + timedelta(hours=sla * jitter)).isoformat()
            affected.append(a["alert_id"])
    return affected


def _inject_f7_analyst_overload(alerts: list[dict], n_analysts: int,
                                  dose: str, rng: np.random.Generator) -> list[str]:
    """F7: One analyst closes most cases at implausible rates."""
    concentration = {"mild": 0.5, "moderate": 0.75, "severe": 0.95}[dose]
    overloaded = f"ANALYST-001"
    affected = []
    for a in alerts:
        if a.get("analyst_id") and rng.random() < concentration:
            a["analyst_id"] = overloaded
            affected.append(a["alert_id"])
    return affected


# ── Main generator ───────────────────────────────────────────────────────

class DataGenerator:
    """
    Generates a complete synthetic multi-CSE dataset.

    Parameters
    ----------
    n_cses : int
        Number of CSEs (entities) to generate.
    months : int
        Number of months of data.
    seed : int
        Random seed for reproducibility.
    output_dir : Path
        Where to write the Parquet files.
    """

    def __init__(self, n_cses: int = 40, months: int = 3,
                 seed: int = 42, output_dir: Path = Path("data/generated")):
        self.n_cses = n_cses
        self.months = months
        self.seed = seed
        self.output_dir = output_dir
        self.rng = np.random.default_rng(seed)
        self.py_rng = random.Random(seed)

        self.profiles: list[CSEProfile] = []
        self.ground_truth: list[GroundTruth] = []

    def _generate_profiles(self) -> None:
        """Create hidden profiles for each CSE."""
        for i in range(self.n_cses):
            cse_id = f"CSE-{i+1:03d}"
            sector = SECTORS[i % len(SECTORS)]
            maturity = float(self.rng.beta(5, 2))  # skewed toward higher
            n_assets = int(self.rng.integers(20, 500))
            n_analysts = max(2, int(n_assets * maturity * 0.05))

            note_bank = list(NOTE_TEMPLATES_HEALTHY) if maturity > 0.4 else list(NOTE_TEMPLATES_WEAK)

            profile = CSEProfile(
                cse_id=cse_id,
                sector=sector,
                n_assets=n_assets,
                maturity=maturity,
                n_analysts=n_analysts,
                shift_model="24x7" if maturity > 0.6 else "office_hours",
                sla_critical_hours=4,
                sla_high_hours=24,
                note_bank=note_bank,
            )
            self.profiles.append(profile)

    def _assign_scenarios(self) -> None:
        """Assign failure scenarios to ~60% of CSEs, benign look-alikes to ~15%."""
        n = self.n_cses
        # ~60% get failure scenarios
        fail_indices = self.rng.choice(n, size=int(n * 0.6), replace=False)
        scenarios_pool = ["F1", "F2", "F3", "F5", "F6", "F7"]
        doses = ["mild", "moderate", "severe"]

        for idx in fail_indices:
            n_scenarios = self.rng.integers(1, 4)
            chosen = self.rng.choice(scenarios_pool, size=n_scenarios, replace=False)
            profile = self.profiles[idx]
            for sc in chosen:
                dose = self.rng.choice(doses)
                profile.failure_scenarios.append({
                    "scenario": sc,
                    "dose": dose,
                })

        # ~15% are benign look-alikes
        benign_types = ["maintenance_window", "auto_closure_low_sev", "small_entity", "seasonal_spike"]
        healthy_indices = [i for i in range(n) if i not in fail_indices]
        n_benign = min(int(n * 0.15), len(healthy_indices))
        if n_benign > 0:
            benign_indices = self.rng.choice(healthy_indices, size=n_benign, replace=False)
            for idx in benign_indices:
                btype = self.rng.choice(benign_types)
                self.profiles[idx].is_benign_lookalike = True
                self.profiles[idx].lookalike_type = btype
                if btype == "small_entity":
                    self.profiles[idx].n_assets = self.rng.integers(5, 15)

    def _generate_cse_data(self, profile: CSEProfile) -> dict:
        """Generate alerts, cases, escalations, assets, telemetry for one CSE."""
        rng = self.rng
        start_date = datetime(2026, 1, 1, tzinfo=timezone.utc)
        end_date = start_date + timedelta(days=30 * self.months)
        total_days = (end_date - start_date).days

        # --- Assets ---
        assets = []
        for j in range(profile.n_assets):
            crit_choices = ["critical", "high", "medium", "low"]
            crit_weights = [0.1, 0.2, 0.4, 0.3]
            criticality = rng.choice(crit_choices, p=crit_weights)
            asset = {
                "asset_id": f"{profile.cse_id}-ASSET-{j+1:04d}",
                "cse_id": profile.cse_id,
                "type": rng.choice(ASSET_TYPES),
                "criticality": criticality,
                "internet_facing": bool(rng.random() < 0.3),
                "expected_sources": [rng.choice(["syslog", "edr", "nids", "waf", "fw"])
                                     for _ in range(rng.integers(1, 4))],
            }
            assets.append(asset)

        # --- Alerts ---
        # Rate depends on assets and maturity
        base_rate = profile.n_assets * 2  # alerts per month
        alerts = []
        analysts = [f"ANALYST-{k+1:03d}" for k in range(profile.n_analysts)]

        for day_offset in range(total_days):
            day = start_date + timedelta(days=day_offset)
            # Poisson arrivals with daily seasonality
            hour_weights = np.array([0.3, 0.2, 0.2, 0.2, 0.3, 0.5, 0.8, 1.0,
                                      1.2, 1.5, 1.5, 1.3, 1.0, 1.2, 1.4, 1.5,
                                      1.3, 1.0, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3])
            daily_count = rng.poisson(base_rate / 30)
            for _ in range(daily_count):
                hour = rng.choice(24, p=hour_weights / hour_weights.sum())
                minute = rng.integers(0, 60)
                created = day.replace(hour=int(hour), minute=int(minute))

                sev_choices = ["CRITICAL", "HIGH", "MEDIUM", "LOW"]
                sev_weights = [0.05, 0.15, 0.45, 0.35]
                severity = rng.choice(sev_choices, p=sev_weights)

                asset = rng.choice(assets)
                category = rng.choice(CATEGORIES)

                # Response time depends on maturity and severity
                sev_mult = {"CRITICAL": 1, "HIGH": 2, "MEDIUM": 6, "LOW": 24}[severity]
                ack_delay = max(1, rng.exponential(sev_mult * (2 - profile.maturity))) * 60  # minutes → seconds
                close_delay = ack_delay + max(60, rng.exponential(
                    sev_mult * 60 * (2 - profile.maturity))) * 60

                # Disposition
                disp_choices = ["TRUE_POSITIVE", "FALSE_POSITIVE", "BENIGN"]
                disp_weights = [0.3, 0.4, 0.3]
                disposition = rng.choice(disp_choices, p=disp_weights)

                alert_id = f"{profile.cse_id}-ALR-{uuid.uuid4().hex[:8].upper()}"
                alert = {
                    "alert_id": alert_id,
                    "cse_id": profile.cse_id,
                    "asset_id": asset["asset_id"],
                    "category": category,
                    "severity": severity,
                    "rule_id": f"RULE-{category[:3].upper()}-{rng.integers(1,20):02d}",
                    "created_ts": created.isoformat(),
                    "ack_ts": (created + timedelta(seconds=float(ack_delay))).isoformat(),
                    "closed_ts": (created + timedelta(seconds=float(close_delay))).isoformat(),
                    "disposition": disposition,
                    "analyst_id": rng.choice(analysts),
                }
                alerts.append(alert)

        # --- Cases (group alerts into cases) ---
        cases = []
        case_alerts = {}
        alert_idx = 0
        while alert_idx < len(alerts):
            batch_size = min(rng.integers(1, 5), len(alerts) - alert_idx)
            case_id = f"{profile.cse_id}-CASE-{uuid.uuid4().hex[:8].upper()}"
            batch = alerts[alert_idx:alert_idx + batch_size]
            for a in batch:
                a["_case_id"] = case_id
            alert_ids_in_case = [a["alert_id"] for a in batch]

            first_ts = min(a["created_ts"] for a in batch)
            last_ts = max(a.get("closed_ts") or a["created_ts"] for a in batch)

            max_sev = "LOW"
            sev_order = {"CRITICAL": 4, "HIGH": 3, "MEDIUM": 2, "LOW": 1}
            for a in batch:
                if sev_order.get(a["severity"], 0) > sev_order.get(max_sev, 0):
                    max_sev = a["severity"]

            note = self.py_rng.choice(profile.note_bank).format(
                category=batch[0]["category"],
                asset=batch[0]["asset_id"],
                disposition=batch[0]["disposition"],
                src_ip=f"10.{rng.integers(0,255)}.{rng.integers(0,255)}.{rng.integers(0,255)}",
                root_cause="misconfigured_rule",
                n=len(batch),
                asset_type=rng.choice(ASSET_TYPES),
            )

            case = {
                "case_id": case_id,
                "cse_id": profile.cse_id,
                "alert_ids": alert_ids_in_case,
                "opened_ts": first_ts,
                "closed_ts": last_ts,
                "severity": max_sev,
                "status": "CLOSED",
                "closure_note": note,
                "reviewer_id": rng.choice(analysts) if profile.maturity > 0.5 else None,
                "remediation_ref": f"REM-{uuid.uuid4().hex[:6]}" if rng.random() < profile.maturity else None,
                "root_cause": rng.choice(["misconfiguration", "vulnerability", "user_error", None]),
            }
            cases.append(case)
            alert_idx += batch_size

        # --- Escalations ---
        escalations = []
        for case in cases:
            should_escalate = (case["severity"] in ("CRITICAL", "HIGH") and
                               rng.random() < (0.5 + 0.4 * profile.maturity))
            if should_escalate:
                esc = {
                    "esc_id": f"{profile.cse_id}-ESC-{uuid.uuid4().hex[:8].upper()}",
                    "case_id": case["case_id"],
                    "ts": case["opened_ts"],
                    "to_level": rng.choice(["Tier2", "Tier3", "CISO"]),
                    "recipient_action_ts": case["closed_ts"] if rng.random() < profile.maturity else None,
                }
                escalations.append(esc)

        # --- Telemetry summary ---
        telemetry = []
        for asset in assets:
            for day_offset in range(total_days):
                day = start_date + timedelta(days=day_offset)
                for src in asset["expected_sources"]:
                    count = int(rng.poisson(500 + 200 * (1 if asset["criticality"] == "critical" else 0)))
                    telemetry.append({
                        "asset_id": asset["asset_id"],
                        "source": src,
                        "date": day.strftime("%Y-%m-%d"),
                        "event_count": count,
                    })

        # --- Inject failure scenarios ---
        gt_scenarios = []
        for sc_conf in profile.failure_scenarios:
            sc = sc_conf["scenario"]
            dose = sc_conf["dose"]
            affected = []

            if sc == "F1":
                affected = _inject_f1_fast_critical_closure(alerts, dose, rng)
            elif sc == "F2":
                affected = _inject_f2_escalation_suppression(escalations, alerts, dose, rng)
            elif sc == "F3":
                affected = _inject_f3_telemetry_blind_spot(telemetry, assets, dose, rng)
            elif sc == "F5":
                affected = _inject_f5_template_notes(cases, NOTE_TEMPLATES_WEAK, dose, rng)
            elif sc == "F6":
                affected = _inject_f6_kpi_gaming(alerts, dose, rng)
            elif sc == "F7":
                affected = _inject_f7_analyst_overload(alerts, profile.n_analysts, dose, rng)

            gt_scenarios.append({
                "scenario": sc,
                "dose": dose,
                "affected_count": len(affected),
            })

        # --- Benign look-alike adjustments ---
        if profile.is_benign_lookalike and profile.lookalike_type == "auto_closure_low_sev":
            for a in alerts:
                if a["severity"] == "LOW":
                    created = datetime.fromisoformat(a["created_ts"])
                    a["closed_ts"] = (created + timedelta(minutes=1)).isoformat()
                    a["disposition"] = "FALSE_POSITIVE"

        self.ground_truth.append(GroundTruth(
            cse_id=profile.cse_id,
            scenarios=gt_scenarios,
            is_benign=profile.is_benign_lookalike,
            benign_type=profile.lookalike_type,
        ))

        # Clean up internal fields
        for a in alerts:
            a.pop("_case_id", None)

        return {
            "alerts": alerts,
            "cases": cases,
            "escalations": escalations,
            "assets": assets,
            "telemetry_summary": telemetry,
        }

    def _write_parquet(self, data: list[dict], name: str, cse_id: str) -> dict:
        """Write a list of dicts to Parquet and return file metadata."""
        cse_dir = self.output_dir / cse_id
        cse_dir.mkdir(parents=True, exist_ok=True)
        path = cse_dir / f"{name}.parquet"

        if not data:
            # Write empty file
            path.write_bytes(b"")
            return {"name": f"{name}.parquet", "rows": 0, "sha256": hashlib.sha256(b"").hexdigest()}

        table = pa.Table.from_pylist(data)
        pq.write_table(table, str(path))

        file_hash = hashlib.sha256(path.read_bytes()).hexdigest()
        return {"name": f"{name}.parquet", "rows": len(data), "sha256": file_hash}

    def _write_manifest(self, cse_id: str, file_entries: list[dict]) -> None:
        """Write manifest.json with Merkle root."""
        from satsa.ledger.merkle import build_manifest_tree

        tree = build_manifest_tree(file_entries)
        manifest = {
            "cse_id": cse_id,
            "period": f"2026-Q{(self.months - 1) // 3 + 1}",
            "files": file_entries,
            "merkle_root": tree.root,
            "submitted_at": datetime.now(timezone.utc).isoformat(),
        }
        path = self.output_dir / cse_id / "manifest.json"
        path.write_text(json.dumps(manifest, indent=2))

    def generate(self) -> Path:
        """Generate the full dataset. Returns the output directory."""
        logger.info("Generating %d CSEs × %d months (seed=%d)...",
                    self.n_cses, self.months, self.seed)

        self.output_dir.mkdir(parents=True, exist_ok=True)
        self._generate_profiles()
        self._assign_scenarios()

        for i, profile in enumerate(self.profiles):
            logger.info("[%d/%d] Generating %s (%s, maturity=%.2f, scenarios=%s)",
                        i + 1, self.n_cses, profile.cse_id, profile.sector,
                        profile.maturity,
                        [s["scenario"] for s in profile.failure_scenarios] or "healthy")

            data = self._generate_cse_data(profile)

            file_entries = []
            for table_name, records in data.items():
                entry = self._write_parquet(records, table_name, profile.cse_id)
                file_entries.append(entry)

            self._write_manifest(profile.cse_id, file_entries)

        # Write ground truth
        gt_path = self.output_dir / "ground_truth.json"
        gt_data = []
        for gt in self.ground_truth:
            gt_data.append({
                "cse_id": gt.cse_id,
                "scenarios": gt.scenarios,
                "is_benign": gt.is_benign,
                "benign_type": gt.benign_type,
            })
        gt_path.write_text(json.dumps(gt_data, indent=2))

        # Write profiles (for debugging, not used by analytics)
        profiles_path = self.output_dir / "profiles.json"
        profile_data = []
        for p in self.profiles:
            profile_data.append({
                "cse_id": p.cse_id,
                "sector": p.sector,
                "n_assets": p.n_assets,
                "maturity": round(p.maturity, 3),
                "n_analysts": p.n_analysts,
                "shift_model": p.shift_model,
                "failure_scenarios": p.failure_scenarios,
                "is_benign_lookalike": p.is_benign_lookalike,
                "lookalike_type": p.lookalike_type,
            })
        profiles_path.write_text(json.dumps(profile_data, indent=2, default=str))

        logger.info("Dataset written to %s", self.output_dir)
        logger.info("Ground truth: %s", gt_path)
        return self.output_dir

    def generate_dataset(self, output_dir: Optional[Path | str] = None, num_cses: Optional[int] = None):
        if output_dir:
            self.output_dir = Path(output_dir)
        if num_cses:
            self.n_cses = num_cses
        return self.generate()


SyntheticDataGenerator = DataGenerator


# ── CLI ──────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="SAT-SA synthetic data generator")
    parser.add_argument("--cses", type=int, default=40)
    parser.add_argument("--months", type=int, default=3)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output", type=str, default="data/generated")
    parser.add_argument("--quick", action="store_true",
                        help="Fast mode: 5 CSEs × 1 month")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)-8s %(message)s")

    if args.quick:
        args.cses = 5
        args.months = 1

    gen = DataGenerator(
        n_cses=args.cses,
        months=args.months,
        seed=args.seed,
        output_dir=Path(args.output),
    )
    gen.generate()


if __name__ == "__main__":
    main()
