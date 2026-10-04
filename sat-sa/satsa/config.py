"""
SAT-SA — Configuration
=======================
All tunable knobs.  Override via environment variables or a .env file.
"""
from __future__ import annotations

from pathlib import Path
from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    """SAT-SA application settings."""

    # -- Paths ----------------------------------------------------------------
    data_dir: Path = Field(default=Path("data"), description="Root data directory")
    db_path: Path = Field(default=Path("data/satsa.duckdb"), description="DuckDB database")
    ledger_path: Path = Field(default=Path("data/ledger.sqlite"), description="TRUST ledger SQLite")

    # -- Generator ------------------------------------------------------------
    generator_seed_a: int = Field(default=42, description="Tuning seed set")
    generator_seed_b: int = Field(default=2026, description="Hold-out seed set")
    generator_n_cses: int = Field(default=40, description="Number of CSEs to generate")
    generator_months: int = Field(default=3, description="Months of data")

    # -- Analytics ------------------------------------------------------------
    fdr_alpha: float = Field(default=0.10, description="FDR control level")
    min_evidence_count: int = Field(default=5, description="Min count before flagging negative space")
    noisy_or_floor: float = Field(default=0.80, description="SAI floor = max(weighted, floor * max_dim)")

    # -- Scoring weights (8 dimensions) ---------------------------------------
    weight_td: float = 0.15   # Threat detection
    weight_in: float = 0.15   # Investigation
    weight_es: float = 0.15   # Escalation
    weight_ir: float = 0.10   # Incident response
    weight_so: float = 0.10   # Security operations
    weight_go: float = 0.10   # Governance
    weight_od: float = 0.10   # Operational discipline
    weight_cr: float = 0.15   # Cyber resilience

    # -- Sampling -------------------------------------------------------------
    review_budget: int = Field(default=100, description="Records per entity for examiner review")
    stream_concern: float = 0.40
    stream_negspace: float = 0.20
    stream_anomaly: float = 0.15
    stream_normal: float = 0.15
    stream_random: float = 0.10

    # -- Ledger ---------------------------------------------------------------
    signing_key_path: Path = Field(default=Path("data/keys/satsa_run.key"))
    cse_keys_dir: Path = Field(default=Path("data/keys/cses"))

    # -- API ------------------------------------------------------------------
    api_host: str = "0.0.0.0"
    api_port: int = 8000

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8", "env_prefix": "SATSA_"}


settings = Settings()
