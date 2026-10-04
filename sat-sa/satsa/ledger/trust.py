"""
SAT-SA — TRUST Ledger (§7)
============================
Hash-chained, signed assessment ledger with three block types:

1. **Submission block** — proves what data was received
2. **Run block** — proves what findings came from which inputs + code
3. **Examiner block** — proves who decided what, and when

Blocks are stored in SQLite or JSON. ``satsa verify`` replays the chain.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Dict, Any

from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)
from cryptography.hazmat.primitives import serialization


def _sha256(data: str | bytes) -> str:
    if isinstance(data, str):
        data = data.encode("utf-8")
    return hashlib.sha256(data).hexdigest()


# ── Key management ───────────────────────────────────────────────────────

def generate_signing_key() -> Ed25519PrivateKey:
    """Generate a new Ed25519 signing key."""
    return Ed25519PrivateKey.generate()


def save_key(key: Ed25519PrivateKey, path: Path) -> None:
    """Save a private key to file (PEM, unencrypted for demo)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    pem = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )
    path.write_bytes(pem)


def load_key(path: Path) -> Ed25519PrivateKey:
    """Load a private key from PEM file."""
    return serialization.load_pem_private_key(path.read_bytes(), password=None)


def sign_data(key: Ed25519PrivateKey, data: str) -> str:
    """Sign data and return hex-encoded signature."""
    sig = key.sign(data.encode("utf-8"))
    return sig.hex()


def verify_signature(pub_key: Ed25519PublicKey, data: str, signature_hex: str) -> bool:
    """Verify an Ed25519 signature."""
    try:
        pub_key.verify(bytes.fromhex(signature_hex), data.encode("utf-8"))
        return True
    except Exception:
        return False


# ── Block types ──────────────────────────────────────────────────────────

@dataclass
class LedgerBlockHeader:
    block_hash: str


@dataclass
class LedgerBlock:
    """A single block in the TRUST ledger chain."""
    block_id: int
    block_type: str             # "submission" | "run" | "examiner"
    timestamp: str
    payload_hash: str           # SHA-256 of the payload JSON
    payload: dict
    previous_hash: str          # hash of previous block (or "GENESIS")
    block_hash: str = ""        # SHA-256 of (type + payload_hash + prev_hash + timestamp)
    signature: str = ""         # Ed25519 signature of block_hash

    @property
    def header(self) -> LedgerBlockHeader:
        return LedgerBlockHeader(block_hash=self.block_hash)

    def compute_hash(self) -> str:
        content = f"{self.block_type}|{self.payload_hash}|{self.previous_hash}|{self.timestamp}"
        self.block_hash = _sha256(content)
        return self.block_hash

    def sign(self, key: Ed25519PrivateKey) -> str:
        if not self.block_hash:
            self.compute_hash()
        self.signature = sign_data(key, self.block_hash)
        return self.signature


# ── Ledger store ─────────────────────────────────────────────────────────

class TrustLedger:
    """
    SQLite-backed hash-chained ledger.
    Each block is hash-chained to its predecessor and Ed25519-signed.
    """

    def __init__(self, storage_path: Optional[Path | str] = None, signing_key: Optional[Ed25519PrivateKey] = None):
        if storage_path is None:
            db_p = ":memory:"
            key_p = Path("./trust_ledger.pem")
        else:
            p = str(storage_path)
            db_p = p.replace(".json", ".db")
            key_p = Path(db_p).parent / "trust_ledger.pem"
        
        self.db_path = db_p
        
        if signing_key is not None:
            self.signing_key = signing_key
        elif key_p.exists():
            try:
                self.signing_key = load_key(key_p)
            except Exception:
                self.signing_key = generate_signing_key()
                save_key(self.signing_key, key_p)
        else:
            self.signing_key = generate_signing_key()
            if db_p != ":memory:":
                save_key(self.signing_key, key_p)

        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._init_schema()

    def _init_schema(self) -> None:
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS blocks (
                block_id     INTEGER PRIMARY KEY AUTOINCREMENT,
                block_type   TEXT    NOT NULL,
                timestamp    TEXT    NOT NULL,
                payload_hash TEXT    NOT NULL,
                payload      TEXT    NOT NULL,
                previous_hash TEXT   NOT NULL,
                block_hash   TEXT    NOT NULL,
                signature    TEXT    NOT NULL
            )
        """)
        self.conn.commit()

    def _last_hash(self) -> str:
        row = self.conn.execute(
            "SELECT block_hash FROM blocks ORDER BY block_id DESC LIMIT 1"
        ).fetchone()
        return row[0] if row else "GENESIS"

    def append_block(self, block_type: str, payload: dict) -> LedgerBlock:
        """Create, sign, and store a new block."""
        ts = datetime.now(timezone.utc).isoformat()
        payload_json = json.dumps(payload, sort_keys=True, default=str)
        payload_hash = _sha256(payload_json)

        block = LedgerBlock(
            block_id=0,
            block_type=block_type,
            timestamp=ts,
            payload_hash=payload_hash,
            payload=payload,
            previous_hash=self._last_hash(),
        )
        block.compute_hash()
        block.sign(self.signing_key)

        cursor = self.conn.execute(
            """INSERT INTO blocks
               (block_type, timestamp, payload_hash, payload, previous_hash, block_hash, signature)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (block.block_type, block.timestamp, block.payload_hash,
             payload_json, block.previous_hash, block.block_hash, block.signature),
        )
        self.conn.commit()
        block.block_id = cursor.lastrowid
        return block

    def record_submission(self, entity_id: str, entity_name: str, sector: str, submission_path: str, file_hashes: dict) -> LedgerBlock:
        return self.append_block("submission", {
            "cse_id": entity_id,
            "entity_name": entity_name,
            "sector": sector,
            "submission_path": submission_path,
            "file_hashes": file_hashes
        })

    def record_assessment_run(self, submission_block_hash: str, findings: list, sai_score: float, priority_tier: str) -> LedgerBlock:
        return self.append_block("run", {
            "submission_block_hash": submission_block_hash,
            "findings_count": len(findings),
            "sai_score": sai_score,
            "priority_tier": priority_tier
        })

    def get_chain(self) -> List[Dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT block_id, block_type, timestamp, payload_hash, payload, "
            "previous_hash, block_hash, signature FROM blocks ORDER BY block_id"
        ).fetchall()
        blocks = []
        for r in rows:
            blocks.append({
                "block_id": r[0], "block_type": r[1], "timestamp": r[2],
                "payload_hash": r[3], "payload": json.loads(r[4]),
                "previous_hash": r[5], "block_hash": r[6], "signature": r[7],
            })
        return blocks

    def verify_chain(self) -> Dict[str, Any]:
        rows = self.conn.execute(
            "SELECT block_id, block_type, timestamp, payload_hash, payload, "
            "previous_hash, block_hash, signature FROM blocks ORDER BY block_id"
        ).fetchall()
        
        pub_key = self.signing_key.public_key()
        errors = []
        tampered = []

        for i, r in enumerate(rows):
            b_id, b_type, ts, p_hash, p_str, prev_h, b_hash, sig = r

            # 1. Payload hash check
            actual_payload_hash = _sha256(p_str)
            if actual_payload_hash != p_hash:
                errors.append(f"Block {b_id}: payload tampered")
                tampered.append(b_id)

            # 2. Linkage
            expected_prev = rows[i-1][6] if i > 0 else "GENESIS"
            if prev_h != expected_prev:
                errors.append(f"Block {b_id}: chain break")
                tampered.append(b_id)

            # 3. Block hash
            content = f"{b_type}|{p_hash}|{prev_h}|{ts}"
            if b_hash != _sha256(content):
                errors.append(f"Block {b_id}: block_hash mismatch")
                tampered.append(b_id)

            # 4. Signature
            if not verify_signature(pub_key, b_hash, sig):
                errors.append(f"Block {b_id}: signature invalid")
                tampered.append(b_id)

        chain_valid = len(errors) == 0
        signatures_valid = len([e for e in errors if "signature" in e]) == 0

        return {
            "total_blocks": len(rows),
            "chain_valid": chain_valid,
            "signatures_valid": signatures_valid,
            "tampered_blocks": list(set(tampered)),
            "errors": errors
        }

    def close(self):
        self.conn.close()


AssessmentLedger = TrustLedger
