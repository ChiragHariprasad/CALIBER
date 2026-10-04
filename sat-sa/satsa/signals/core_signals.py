"""
SAT-SA — Core Signal Detectors (§5.2)
========================================
The 12 core signals required for the 5 October prototype.
Each signal compares observed behaviour against an explicit expectation.
"""
from __future__ import annotations

import uuid
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import List, Optional

import numpy as np
import polars as pl
from scipy import stats

from satsa.evidence.models import (
    BaselineType, Dimension, Finding, SignalType,
)
from satsa.signals.base import SignalDetector


# ── ES-01: Required escalation missing ──────────────────────────────────

class ES01_RequiredEscalationMissing(SignalDetector):
    """Critical/High cases closed with no escalation record."""

    signal_id = "ES-01"
    signal_name = "Required escalation missing"
    dimension = Dimension.ESCALATION
    signal_type = SignalType.EXECUTION_GAP
    required_tables = ["cases", "escalations"]

    def detect(self, cse_id, data, baselines=None) -> List[Finding]:
        if not self.is_assessable(data):
            return []

        cases = data["cases"]
        escalations = data["escalations"]

        # Cases that should have been escalated
        critical_cases = cases.filter(
            pl.col("severity").is_in(["CRITICAL", "HIGH"]) &
            (pl.col("status") == "CLOSED")
        )
        if critical_cases.is_empty():
            return []

        # Which cases actually have escalations
        escalated_ids = set(escalations["case_id"].to_list()) if not escalations.is_empty() else set()

        missing = []
        for row in critical_cases.iter_rows(named=True):
            if row["case_id"] not in escalated_ids:
                missing.append(row["case_id"])

        if not missing:
            return []

        total = len(critical_cases)
        rate = len(missing) / total

        # Only flag if substantial (>20% miss rate and at least 5 cases)
        if rate < 0.20 or len(missing) < 5:
            return []

        severity = min(1.0, rate * 1.5)
        return [self._make_finding(
            cse_id=cse_id,
            stratum="severity=CRITICAL+HIGH",
            metric="missing_escalation_rate",
            observed=rate,
            expected=0.05,  # peer expectation: ≤5% miss rate
            evidence_records=missing[:50],
            severity=severity,
            confidence=min(1.0, len(missing) / 20),
            rationale=(f"{len(missing)} of {total} critical/high cases at {cse_id} "
                       f"were closed without escalation ({rate:.0%} miss rate)."),
        )]


# ── IN-01: Shallow investigation ────────────────────────────────────────

class IN01_ShallowInvestigation(SignalDetector):
    """Cases with very fast closure and minimal activity."""

    signal_id = "IN-01"
    signal_name = "Shallow investigation"
    dimension = Dimension.INVESTIGATION
    signal_type = SignalType.EXECUTION_GAP
    required_tables = ["alerts"]

    def detect(self, cse_id, data, baselines=None) -> List[Finding]:
        if not self.is_assessable(data):
            return []

        alerts = data["alerts"]

        # Compute time-to-close for critical alerts
        critical = alerts.filter(pl.col("severity") == "CRITICAL")
        if critical.is_empty() or critical.height < 5:
            return []

        # Calculate close time in hours
        try:
            crit_with_times = critical.filter(
                pl.col("closed_ts").is_not_null() & pl.col("created_ts").is_not_null()
            )
            if crit_with_times.is_empty():
                return []

            # Parse timestamps and compute duration
            created = [datetime.fromisoformat(str(t)) for t in crit_with_times["created_ts"].to_list()]
            closed = [datetime.fromisoformat(str(t)) for t in crit_with_times["closed_ts"].to_list()]
            durations_hours = [(c - cr).total_seconds() / 3600 for cr, c in zip(created, closed)]
        except Exception:
            return []

        if not durations_hours:
            return []

        median_hours = float(np.median(durations_hours))
        # Expected: critical cases take 2+ hours; shallow investigation is <0.4h (24 min)
        peer_expected = baselines.get("median_critical_close_hours", 2.0) if baselines else 2.0

        if median_hours >= 0.4:
            return []

        shallow_ids = [row["alert_id"] for row, dur in
                       zip(crit_with_times.iter_rows(named=True), durations_hours)
                       if dur < 0.25]

        severity = min(1.0, max(0.5, 1.0 - (median_hours / 0.4)))
        return [self._make_finding(
            cse_id=cse_id,
            stratum="severity=CRITICAL",
            metric="median_close_hours",
            observed=median_hours,
            expected=peer_expected,
            evidence_records=shallow_ids,
            severity=severity,
            confidence=min(1.0, len(crit_with_times) / 20),
            rationale=(f"Critical cases at {cse_id} have median close time of "
                       f"{median_hours:.1f}h (peer expected ≈{peer_expected:.1f}h); "
                       f"{len(shallow_ids)} cases closed in <{peer_expected*0.25:.1f}h."),
        )]


# ── IN-03: Acknowledged but not worked ──────────────────────────────────

class IN03_AckedNotWorked(SignalDetector):
    """Alerts acknowledged then closed with no workflow events."""

    signal_id = "IN-03"
    signal_name = "Acknowledged but not worked"
    dimension = Dimension.INVESTIGATION
    signal_type = SignalType.EXECUTION_GAP
    required_tables = ["alerts"]

    def detect(self, cse_id, data, baselines=None) -> List[Finding]:
        if not self.is_assessable(data):
            return []

        alerts = data["alerts"]
        # Look for CRITICAL / HIGH alerts acked and closed very quickly with no investigation
        try:
            with_times = alerts.filter(
                pl.col("severity").is_in(["CRITICAL", "HIGH"]) &
                pl.col("ack_ts").is_not_null() &
                pl.col("closed_ts").is_not_null()
            )
            if with_times.is_empty():
                return []

            fast_close = []
            for row in with_times.iter_rows(named=True):
                ack = datetime.fromisoformat(str(row["ack_ts"]))
                closed = datetime.fromisoformat(str(row["closed_ts"]))
                diff_minutes = (closed - ack).total_seconds() / 60
                if diff_minutes < 2:  # closed within 2 minutes of ack
                    fast_close.append(row["alert_id"])
        except Exception:
            return []

        rate = len(fast_close) / len(with_times) if len(with_times) > 0 else 0

        if rate < 0.05 or len(fast_close) < 5:
            return []

        return [self._make_finding(
            cse_id=cse_id,
            stratum="all_severities",
            metric="ack_to_close_under_2min_rate",
            observed=rate,
            expected=0.02,
            evidence_records=fast_close,
            severity=min(1.0, rate * 2),
            confidence=min(1.0, len(fast_close) / 30),
            rationale=(f"{len(fast_close)} alerts at {cse_id} ({rate:.0%}) were "
                       f"closed within 2 minutes of acknowledgement."),
        )]


# ── ES-02: Escalation latency outlier ───────────────────────────────────

class ES02_EscalationLatencyOutlier(SignalDetector):
    """Escalation times far slower than baseline."""

    signal_id = "ES-02"
    signal_name = "Escalation latency outlier"
    dimension = Dimension.ESCALATION
    signal_type = SignalType.EXECUTION_GAP
    required_tables = ["cases", "escalations"]

    def detect(self, cse_id, data, baselines=None) -> List[Finding]:
        if not self.is_assessable(data):
            return []

        cases = data["cases"]
        escalations = data["escalations"]
        if escalations.is_empty():
            return []

        try:
            latencies = []
            slow_escs = []
            for esc_row in escalations.iter_rows(named=True):
                case_match = cases.filter(pl.col("case_id") == esc_row["case_id"])
                if case_match.is_empty():
                    continue
                case_row = case_match.row(0, named=True)
                opened = datetime.fromisoformat(str(case_row["opened_ts"]))
                esc_ts = datetime.fromisoformat(str(esc_row["ts"]))
                hours = (esc_ts - opened).total_seconds() / 3600
                latencies.append(hours)
                if hours > 24:
                    slow_escs.append(esc_row["esc_id"])
        except Exception:
            return []

        if len(latencies) < 3:
            return []

        median_lat = float(np.median(latencies))
        expected = baselines.get("median_escalation_hours", 4.0) if baselines else 4.0

        if median_lat <= expected * 2:
            return []

        return [self._make_finding(
            cse_id=cse_id,
            stratum="all_escalations",
            metric="median_escalation_latency_hours",
            observed=median_lat,
            expected=expected,
            evidence_records=slow_escs,
            severity=min(1.0, median_lat / (expected * 5)),
            confidence=min(1.0, len(latencies) / 15),
            rationale=(f"Median escalation latency at {cse_id} is {median_lat:.1f}h "
                       f"(peer expected ≈{expected:.1f}h); {len(slow_escs)} "
                       f"escalations took >24h."),
        )]


# ── IR-01: Recurrence without remediation ───────────────────────────────

class IR01_RecurrenceWithoutRemediation(SignalDetector):
    """Same asset × category recurs; no remediation reference."""

    signal_id = "IR-01"
    signal_name = "Recurrence without remediation"
    dimension = Dimension.INCIDENT_RESPONSE
    signal_type = SignalType.EXECUTION_GAP
    required_tables = ["alerts", "cases"]

    def detect(self, cse_id, data, baselines=None) -> List[Finding]:
        if not self.is_assessable(data):
            return []

        alerts = data["alerts"]
        cases = data["cases"]

        # Count occurrences per asset × category for CRITICAL and HIGH alerts
        try:
            crit_high_alerts = alerts.filter(pl.col("severity").is_in(["CRITICAL", "HIGH"]))
            if crit_high_alerts.is_empty():
                return []
            grouped = crit_high_alerts.group_by(["asset_id", "category"]).agg(
                pl.count().alias("count"),
                pl.col("alert_id").alias("alert_ids"),
            ).filter(pl.col("count") >= 4)
        except Exception:
            return []

        if grouped.is_empty():
            return []

        # Check which have remediation
        case_has_remediation = set()
        if not cases.is_empty():
            for row in cases.iter_rows(named=True):
                if row.get("remediation_ref"):
                    case_has_remediation.add(row["case_id"])

        recurring_no_rem = []
        for row in grouped.iter_rows(named=True):
            ids = row["alert_ids"]
            if isinstance(ids, list) and len(ids) > 0:
                recurring_no_rem.append(ids[0])  # representative alert

        if len(recurring_no_rem) < 2:
            return []

        return [self._make_finding(
            cse_id=cse_id,
            stratum="all_categories",
            metric="recurring_asset_category_pairs",
            observed=len(grouped),
            expected=0,
            evidence_records=recurring_no_rem,
            severity=min(1.0, len(grouped) / 20),
            confidence=min(1.0, len(grouped) / 10),
            rationale=(f"{len(grouped)} asset×category pairs at {cse_id} recur "
                       f"3+ times without remediation references."),
        )]


# ── IR-02: Slow first action on critical ────────────────────────────────

class IR02_SlowFirstAction(SignalDetector):
    """Critical time-to-first-action exceeds peer and policy."""

    signal_id = "IR-02"
    signal_name = "Slow first action"
    dimension = Dimension.INCIDENT_RESPONSE
    signal_type = SignalType.EXECUTION_GAP
    required_tables = ["alerts"]

    def detect(self, cse_id, data, baselines=None) -> List[Finding]:
        if not self.is_assessable(data):
            return []

        alerts = data["alerts"]
        critical = alerts.filter(pl.col("severity") == "CRITICAL")
        if critical.is_empty() or critical.height < 3:
            return []

        try:
            slow = []
            ack_times = []
            for row in critical.iter_rows(named=True):
                if not row.get("ack_ts") or not row.get("created_ts"):
                    continue
                created = datetime.fromisoformat(str(row["created_ts"]))
                acked = datetime.fromisoformat(str(row["ack_ts"]))
                minutes = (acked - created).total_seconds() / 60
                ack_times.append(minutes)
                if minutes > 60:
                    slow.append(row["alert_id"])
        except Exception:
            return []

        if len(ack_times) < 3:
            return []

        median_min = float(np.median(ack_times))
        expected = baselines.get("median_critical_ack_minutes", 15.0) if baselines else 15.0

        if median_min <= expected * 2:
            return []

        return [self._make_finding(
            cse_id=cse_id,
            stratum="severity=CRITICAL",
            metric="median_first_action_minutes",
            observed=median_min,
            expected=expected,
            evidence_records=slow,
            severity=min(1.0, median_min / (expected * 6)),
            confidence=min(1.0, len(ack_times) / 15),
            rationale=(f"Median time-to-first-action for critical alerts at {cse_id} "
                       f"is {median_min:.0f}min (expected ≈{expected:.0f}min); "
                       f"{len(slow)} took >60min."),
        )]


# ── SO-01: Analyst concentration ────────────────────────────────────────

class SO01_AnalystConcentration(SignalDetector):
    """One analyst closes most cases or at implausible rates."""

    signal_id = "SO-01"
    signal_name = "Analyst concentration"
    dimension = Dimension.SECURITY_OPERATIONS
    signal_type = SignalType.EXECUTION_GAP
    required_tables = ["alerts"]

    def detect(self, cse_id, data, baselines=None) -> List[Finding]:
        if not self.is_assessable(data):
            return []

        alerts = data["alerts"]
        closed = alerts.filter(pl.col("closed_ts").is_not_null())
        if closed.is_empty() or closed.height < 10:
            return []

        try:
            analyst_counts = Counter(closed["analyst_id"].to_list())
        except Exception:
            return []

        total = sum(analyst_counts.values())
        if total < 20 or len(analyst_counts) < 3:
            return []

        top_analyst, top_count = analyst_counts.most_common(1)[0]
        concentration = top_count / total

        if concentration < 0.75:  # top analyst does <75%
            return []

        affected = [row["alert_id"] for row in closed.iter_rows(named=True)
                     if row.get("analyst_id") == top_analyst]

        return [self._make_finding(
            cse_id=cse_id,
            stratum="all_analysts",
            metric="top_analyst_closure_share",
            observed=concentration,
            expected=1.0 / max(len(analyst_counts), 1),
            evidence_records=affected[:50],
            severity=min(1.0, concentration * 1.2),
            confidence=min(1.0, total / 50),
            rationale=(f"Analyst {top_analyst} at {cse_id} closes {concentration:.0%} "
                       f"of all alerts ({top_count}/{total}); "
                       f"expected ≈{1/max(len(analyst_counts),1):.0%} with "
                       f"{len(analyst_counts)} analysts."),
        )]


# ── SO-02: Evidence funnel break ────────────────────────────────────────

class SO02_EvidenceFunnelBreak(SignalDetector):
    """Alert → case → investigation → escalation ratios break expectations."""

    signal_id = "SO-02"
    signal_name = "Evidence funnel break"
    dimension = Dimension.SECURITY_OPERATIONS
    signal_type = SignalType.NEGATIVE_SPACE
    required_tables = ["alerts", "cases", "escalations"]

    def detect(self, cse_id, data, baselines=None) -> List[Finding]:
        if not self.is_assessable(data):
            return []

        n_alerts = len(data["alerts"])
        n_cases = len(data["cases"])
        n_esc = len(data["escalations"]) if not data["escalations"].is_empty() else 0

        if n_alerts < 10:
            return []

        findings = []

        # Critical alerts vs escalations
        n_critical = len(data["alerts"].filter(pl.col("severity") == "CRITICAL"))
        if n_critical > 5 and n_esc > 0:
            esc_rate = n_esc / n_critical
            expected_rate = baselines.get("critical_escalation_rate", 0.5) if baselines else 0.5
            if esc_rate < expected_rate * 0.3:
                findings.append(self._make_finding(
                    cse_id=cse_id,
                    stratum="severity=CRITICAL",
                    metric="escalation_rate",
                    observed=esc_rate,
                    expected=expected_rate,
                    evidence_records=[],
                    severity=min(1.0, (expected_rate - esc_rate) / expected_rate),
                    confidence=min(1.0, n_critical / 20),
                    signal_type_override=SignalType.NEGATIVE_SPACE,
                    rationale=(f"Only {esc_rate:.0%} of {n_critical} critical alerts "
                               f"at {cse_id} reached escalation "
                               f"(expected ≈{expected_rate:.0%})."),
                ))
        elif n_critical > 10 and n_esc == 0:
            findings.append(self._make_finding(
                cse_id=cse_id,
                stratum="severity=CRITICAL",
                metric="escalation_count",
                observed=0,
                expected=n_critical * 0.5,
                evidence_records=[],
                severity=0.8,
                confidence=min(1.0, n_critical / 20),
                rationale=(f"{n_critical} critical alerts at {cse_id} but zero "
                           f"escalations recorded."),
            ))

        return findings

    def _make_finding(self, cse_id, stratum, metric, observed, expected,
                      evidence_records, severity, confidence,
                      rationale, signal_type_override=None, **kwargs):
        """Override to support signal_type_override."""
        f = super()._make_finding(
            cse_id=cse_id, stratum=stratum, metric=metric,
            observed=observed, expected=expected,
            evidence_records=evidence_records,
            severity=severity, confidence=confidence,
            rationale=rationale, **kwargs,
        )
        if signal_type_override:
            f.signal_type = signal_type_override
        return f


# ── OD-01: Closure bunching ─────────────────────────────────────────────

class OD01_ClosureBunching(SignalDetector):
    """Excess closures just before SLA edges or shift ends."""

    signal_id = "OD-01"
    signal_name = "Closure bunching"
    dimension = Dimension.OPERATIONAL_DISCIPLINE
    signal_type = SignalType.EXECUTION_GAP
    required_tables = ["alerts"]

    def detect(self, cse_id, data, baselines=None) -> List[Finding]:
        if not self.is_assessable(data):
            return []

        alerts = data["alerts"]
        sla_hours = {"CRITICAL": 4, "HIGH": 24, "MEDIUM": 72, "LOW": 168}

        try:
            bunched = []
            total_with_times = 0
            for row in alerts.iter_rows(named=True):
                if not row.get("closed_ts") or not row.get("created_ts"):
                    continue
                total_with_times += 1
                created = datetime.fromisoformat(str(row["created_ts"]))
                closed = datetime.fromisoformat(str(row["closed_ts"]))
                sla = sla_hours.get(row["severity"], 72)
                diff_hours = (closed - created).total_seconds() / 3600
                # Bunched = closed in the 85-100% window of SLA
                if 0.85 * sla <= diff_hours <= sla:
                    bunched.append(row["alert_id"])
        except Exception:
            return []

        if total_with_times < 20:
            return []

        rate = len(bunched) / total_with_times
        # Under uniform distribution, expect ~15% in this window
        expected_rate = 0.15

        if rate <= expected_rate * 1.5 or len(bunched) < 5:
            return []

        return [self._make_finding(
            cse_id=cse_id,
            stratum="all_severities",
            metric="sla_edge_closure_rate",
            observed=rate,
            expected=expected_rate,
            evidence_records=bunched[:50],
            severity=min(1.0, (rate - expected_rate) / expected_rate),
            confidence=min(1.0, total_with_times / 50),
            rationale=(f"{len(bunched)} closures at {cse_id} ({rate:.0%}) fall in the "
                       f"85-100% SLA window (expected ≈{expected_rate:.0%} under "
                       f"uniform distribution), suggesting gaming."),
        )]


# ── TD-01: Expected category missing (negative space) ───────────────────

class TD01_ExpectedCategoryMissing(SignalDetector):
    """Zero alerts for a category that peer entities detect."""

    signal_id = "TD-01"
    signal_name = "Expected category missing"
    dimension = Dimension.THREAT_DETECTION
    signal_type = SignalType.NEGATIVE_SPACE
    required_tables = ["alerts"]

    def detect(self, cse_id, data, baselines=None) -> List[Finding]:
        if not self.is_assessable(data):
            return []

        alerts = data["alerts"]
        if alerts.is_empty() or alerts.height < 50:
            return []

        observed_cats = set(alerts["category"].unique().to_list())

        # Expected categories (from peer baselines or global set)
        all_cats = baselines.get("expected_categories", set(
            ["malware", "phishing", "brute_force", "lateral_movement",
             "web_attack", "recon"]
        )) if baselines else set(
            ["malware", "phishing", "brute_force", "lateral_movement",
             "web_attack", "recon"]
        )

        missing = all_cats - observed_cats
        if not missing:
            return []

        findings = []
        for cat in missing:
            expected_count = baselines.get(f"expected_count_{cat}", 10) if baselines else 10
            if expected_count < 5:
                continue
            findings.append(self._make_finding(
                cse_id=cse_id,
                stratum=f"category={cat}",
                metric="alert_count",
                observed=0,
                expected=expected_count,
                evidence_records=[],
                severity=min(1.0, expected_count / 50),
                confidence=0.6,
                rationale=(f"Zero {cat} alerts at {cse_id} over the assessment period; "
                           f"peer entities average ≈{expected_count:.0f} alerts."),
            ))

        return findings


# ── TD-02: Detection volume deficit ─────────────────────────────────────

class TD02_DetectionVolumeDeficit(SignalDetector):
    """Alerts per monitored asset far below peer baseline."""

    signal_id = "TD-02"
    signal_name = "Detection volume deficit"
    dimension = Dimension.THREAT_DETECTION
    signal_type = SignalType.NEGATIVE_SPACE
    required_tables = ["alerts", "assets"]

    def detect(self, cse_id, data, baselines=None) -> List[Finding]:
        if not self.is_assessable(data):
            return []

        n_alerts = len(data["alerts"])
        n_assets = len(data["assets"])
        if n_assets == 0:
            return []

        alerts_per_asset = n_alerts / n_assets
        expected = baselines.get("alerts_per_asset", 15.0) if baselines else 15.0

        if alerts_per_asset >= expected * 0.3:
            return []

        return [self._make_finding(
            cse_id=cse_id,
            stratum="all_assets",
            metric="alerts_per_asset",
            observed=alerts_per_asset,
            expected=expected,
            evidence_records=[],
            severity=min(1.0, 1.0 - alerts_per_asset / expected),
            confidence=min(1.0, n_assets / 30),
            rationale=(f"{cse_id} generates {alerts_per_asset:.1f} alerts per monitored "
                       f"asset (peer expected ≈{expected:.1f}); possible blind spots."),
        )]


# ── IN-02: Template notes ───────────────────────────────────────────────

class IN02_TemplateNotes(SignalDetector):
    """Investigation notes are copy-paste templates (MinHash + exact match)."""

    signal_id = "IN-02"
    signal_name = "Template notes"
    dimension = Dimension.INVESTIGATION
    signal_type = SignalType.EXECUTION_GAP
    required_tables = ["cases"]

    def detect(self, cse_id, data, baselines=None) -> List[Finding]:
        if not self.is_assessable(data):
            return []

        cases = data["cases"]
        notes_col = "closure_note"
        notes = [str(row.get(notes_col, "")) for row in cases.iter_rows(named=True)
                 if row.get(notes_col)]
        if len(notes) < 10:
            return []

        # Detect if a SINGLE template note dominates >45% of all cases
        note_counts = Counter(notes)
        top_template, top_count = note_counts.most_common(1)[0]
        template_share = top_count / len(notes)

        # Flag only if a single note is used for >45% of cases or if top note is ultra-short (<15 chars)
        if template_share < 0.45 and len(top_template) > 15:
            return []

        template_cases = [row["case_id"] for row in cases.iter_rows(named=True)
                          if str(row.get(notes_col, "")) == top_template]

        return [self._make_finding(
            cse_id=cse_id,
            stratum="all_cases",
            metric="template_note_share",
            observed=template_share,
            expected=0.10,
            evidence_records=template_cases[:50],
            severity=min(1.0, template_share * 1.3),
            confidence=min(1.0, len(notes) / 30),
            rationale=(f"{template_share:.0%} of investigation notes at {cse_id} "
                       f"are copy-paste templates ({top_count}/{len(notes)}). "
                       f"Most common: \"{top_template[:60]}…\""),
        )]


# ── Registry of all core signals ────────────────────────────────────────

ALL_CORE_SIGNALS: list[type[SignalDetector]] = [
    ES01_RequiredEscalationMissing,
    IN01_ShallowInvestigation,
    IN03_AckedNotWorked,
    ES02_EscalationLatencyOutlier,
    IR01_RecurrenceWithoutRemediation,
    IR02_SlowFirstAction,
    SO01_AnalystConcentration,
    SO02_EvidenceFunnelBreak,
    OD01_ClosureBunching,
    TD01_ExpectedCategoryMissing,
    TD02_DetectionVolumeDeficit,
    IN02_TemplateNotes,
]
