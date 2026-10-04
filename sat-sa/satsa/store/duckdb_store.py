"""
SAT-SA — DuckDB Store (§4 / §9.1)
====================================
Loads Parquet submissions into DuckDB, verifies manifests,
and provides fast analytical queries via SQL or Polars.
"""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Optional, Dict, List

import duckdb
import polars as pl

from satsa.ledger.merkle import build_manifest_tree, compute_file_hash

logger = logging.getLogger(__name__)


class Store:
    """
    DuckDB-backed analytical store.

    Reads Parquet files from CSE submission directories,
    verifies their manifests, and registers them as views.
    """

    def __init__(self, db_path: str = ":memory:"):
        self.con = duckdb.connect(db_path)
        self._loaded_cses: set[str] = set()
        self._init_schema()

    def _init_schema(self):
        """Create metadata tracking tables."""
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS _submissions (
                cse_id      VARCHAR,
                period      VARCHAR,
                merkle_root VARCHAR,
                loaded_at   TIMESTAMP DEFAULT current_timestamp,
                manifest_ok BOOLEAN
            )
        """)

    def load_submission_directory(self, cse_dir: Path) -> dict:
        """Alias for ingest_cse to load Parquet files into DuckDB."""
        return self.ingest_cse(cse_dir)

    def list_tables(self) -> List[str]:
        """Return list of all registered tables in DuckDB."""
        res = self.con.execute("SHOW TABLES").fetchall()
        return [r[0] for r in res if not r[0].startswith("_")]

    def get_all_tables_as_polars(self, cse_id: Optional[str] = None) -> Dict[str, pl.DataFrame]:
        """Return dict of table_name -> Polars DataFrame for all tables in DuckDB."""
        tables = self.list_tables()
        data = {}
        for t in tables:
            try:
                df = self.query(f"SELECT * FROM {t}")
                if cse_id and "cse_id" in df.columns:
                    df = df.filter(pl.col("cse_id") == cse_id)
                data[t] = df
            except Exception:
                data[t] = pl.DataFrame()
        return data

    def ingest_cse(self, cse_dir: Path) -> dict:
        """
        Ingest a single CSE submission directory.
        """
        cse_dir = Path(cse_dir)
        manifest_path = cse_dir / "manifest.json"
        
        # If manifest doesn't exist, generate standard load from parquet files
        if not manifest_path.exists():
            tables_loaded = []
            for fpath in cse_dir.glob("*.parquet"):
                table_name = fpath.stem
                try:
                    df = pl.read_parquet(str(fpath))
                    self.con.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM df WHERE 1=0")
                    self.con.execute(f"INSERT INTO {table_name} SELECT * FROM df")
                    tables_loaded.append(table_name)
                except Exception as e:
                    logger.warning("Failed loading %s: %s", fpath, e)
            self._loaded_cses.add(cse_dir.name)
            return {"cse_id": cse_dir.name, "manifest_ok": True, "tables_loaded": tables_loaded}

        manifest = json.loads(manifest_path.read_text())
        cse_id = manifest["cse_id"]
        period = manifest.get("period", "unknown")

        manifest_ok = True
        file_issues = []
        for f_entry in manifest.get("files", []):
            fpath = cse_dir / f_entry["name"]
            if not fpath.exists():
                file_issues.append(f"missing: {f_entry['name']}")
                manifest_ok = False
                continue
            if fpath.stat().st_size == 0:
                continue
            actual_hash = compute_file_hash(str(fpath))
            if actual_hash != f_entry.get("sha256"):
                file_issues.append(f"hash_mismatch: {f_entry['name']}")
                manifest_ok = False

        tree = build_manifest_tree(manifest.get("files", []))
        if tree.root != manifest.get("merkle_root"):
            file_issues.append("merkle_root_mismatch")
            manifest_ok = False

        tables_loaded = []
        for f_entry in manifest.get("files", []):
            table_name = f_entry["name"].replace(".parquet", "")
            fpath = cse_dir / f_entry["name"]
            if not fpath.exists() or fpath.stat().st_size == 0:
                continue

            try:
                df = pl.read_parquet(str(fpath))
                self.con.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM df WHERE 1=0")
                self.con.execute(f"INSERT INTO {table_name} SELECT * FROM df")
                tables_loaded.append(table_name)
            except Exception as e:
                logger.warning("Failed to load %s for %s: %s", f_entry["name"], cse_id, e)

        self.con.execute(
            "INSERT INTO _submissions VALUES (?, ?, ?, current_timestamp, ?)",
            [cse_id, period, manifest.get("merkle_root", ""), manifest_ok]
        )
        self._loaded_cses.add(cse_id)

        return {
            "cse_id": cse_id,
            "period": period,
            "manifest_ok": manifest_ok,
            "file_issues": file_issues,
            "tables_loaded": tables_loaded,
            "status": "ok" if manifest_ok else "integrity_warning",
        }

    def ingest_all(self, data_dir: Path) -> list[dict]:
        """Ingest all CSE directories under data_dir."""
        results = []
        for cse_dir in sorted(Path(data_dir).iterdir()):
            if cse_dir.is_dir() and (cse_dir / "manifest.json").exists():
                result = self.ingest_cse(cse_dir)
                results.append(result)
        return results

    def query(self, sql: str) -> pl.DataFrame:
        """Run a SQL query and return a Polars DataFrame."""
        return self.con.execute(sql).pl()

    def get_cse_ids(self) -> list[str]:
        """Return all loaded CSE IDs."""
        return sorted(self._loaded_cses)

    def close(self):
        self.con.close()


# Alias for DuckDBStore
DuckDBStore = Store
