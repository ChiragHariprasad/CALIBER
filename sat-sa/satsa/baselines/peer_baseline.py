"""
SAT-SA — Peer Baselines & Zero-Inflated Count Models (§5.3)
============================================================
Establishes baseline expectations across peer entity groups (e.g. Tier 1 banks, Payment Systems, Insurers)
and calculates adjusted z-scores for anomaly detection.

Includes zero-inflated hurdle models for count data (e.g. low-frequency high-severity incident counts).
"""
from __future__ import annotations

import math
from typing import Dict, List, Any, Optional
import numpy as np
import polars as pl


class PeerBaselineEngine:
    """
    Peer Baseline Engine calculating entity deviation metrics relative to peer peer cohorts.
    """

    def __init__(self, peer_group_col: str = "sector"):
        self.peer_group_col = peer_group_col

    def compute_peer_z_scores(
        self,
        df: pl.DataFrame,
        metric_cols: List[str],
        robust: bool = True
    ) -> pl.DataFrame:
        """
        Compute robust z-scores (using median & MAD) or standard z-scores per peer group.
        
        MAD = Median Absolute Deviation * 1.4826 (for normal consistency).
        """
        result_df = df.clone()
        
        for col in metric_cols:
            if col not in df.columns:
                continue
                
            z_col_name = f"{col}_zscore"
            
            if robust:
                # Calculate Median and MAD per peer group
                stats = df.group_by(self.peer_group_col).agg([
                    pl.col(col).median().alias(f"{col}_median"),
                    (pl.col(col) - pl.col(col).median()).abs().median().alias(f"{col}_mad")
                ])
                
                result_df = result_df.join(stats, on=self.peer_group_col, how="left")
                
                # Robust z = (x - median) / (1.4826 * MAD + 1e-6)
                result_df = result_df.with_columns(
                    ((pl.col(col) - pl.col(f"{col}_median")) / (1.4826 * pl.col(f"{col}_mad") + 1e-6))
                    .alias(z_col_name)
                ).drop([f"{col}_median", f"{col}_mad"])
            else:
                # Standard z = (x - mean) / (std + 1e-6)
                stats = df.group_by(self.peer_group_col).agg([
                    pl.col(col).mean().alias(f"{col}_mean"),
                    pl.col(col).std().alias(f"{col}_std")
                ])
                
                result_df = result_df.join(stats, on=self.peer_group_col, how="left")
                
                result_df = result_df.with_columns(
                    ((pl.col(col) - pl.col(f"{col}_mean")) / (pl.col(f"{col}_std") + 1e-6))
                    .alias(z_col_name)
                ).drop([f"{col}_mean", f"{col}_std"])
                
        return result_df


class ZeroInflatedCountModel:
    """
    Zero-Inflated Hurdle Model for low-frequency security incident / audit finding counts.
    Separates the zero-probability (structural zeros) from count distribution (Poisson/Negative Binomial).
    """

    def __init__(self, eps: float = 1e-6):
        self.eps = eps

    def fit_predict_anomaly_score(self, counts: np.ndarray) -> np.ndarray:
        """
        Calculates negative log-likelihood of observed counts given peer distribution.
        High score indicates an unexpectedly suppressed count (e.g. 0 incidents reported when 15 are expected).
        """
        counts = np.asarray(counts, dtype=float)
        n = len(counts)
        if n == 0:
            return np.array([])

        zero_prob = np.mean(counts == 0)
        non_zero_counts = counts[counts > 0]
        
        if len(non_zero_counts) > 0:
            poisson_lambda = np.mean(non_zero_counts)
        else:
            poisson_lambda = 1.0

        scores = []
        for c in counts:
            if c == 0:
                # Likelihood of reporting exactly zero incidents
                p = max(zero_prob, self.eps)
            else:
                # Likelihood under non-zero Poisson hurdle component
                p_poisson = (poisson_lambda ** c) * math.exp(-poisson_lambda) / math.factorial(min(int(c), 170))
                p = max((1.0 - zero_prob) * p_poisson, self.eps)
            
            # Anomaly score as negative log likelihood
            scores.append(-math.log(p))

        return np.array(scores)
