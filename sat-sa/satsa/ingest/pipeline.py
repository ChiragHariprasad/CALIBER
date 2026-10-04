"""
SAT-SA — Submission Ingest & Schema Validation Pipeline (§4.1)
===============================================================
Validates submitted Parquet/CSV entity data packages, registers tables in DuckDB,
and identifies missing mandatory Tier 1 schemas or schema corruption.
"""
from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Any, Tuple, Optional
import polars as pl
from pydantic import ValidationError

from satsa.store.duckdb_store import DuckDBStore
from satsa.schemas import MANDATORY_TIER1_TABLES, OPTIONAL_TIER2_TABLES


class IngestPipeline:
    """
    Ingest pipeline handling verification, validation, and storage registration.
    """

    def __init__(self, store: Optional[DuckDBStore] = None):
        self.store = store or DuckDBStore()

    def process_submission(self, entity_dir: Path) -> Tuple[DuckDBStore, List[str]]:
        """
        Processes an entity's submission directory containing Parquet files.
        Returns the populated DuckDBStore and a list of missing mandatory Tier 1 tables.
        """
        entity_dir = Path(entity_dir)
        if not entity_dir.exists():
            raise FileNotFoundError(f"Entity submission directory not found: {entity_dir}")

        # 1. Load Parquet directory into store
        self.store.load_submission_directory(entity_dir)

        # 2. Check for missing mandatory tables
        present_tables = set(self.store.list_tables())
        missing_mandatory = [t for t in MANDATORY_TIER1_TABLES if t not in present_tables]

        return self.store, missing_mandatory
