"""
SAT-SA — Merkle Tree (§7)
===========================
~150-line Merkle tree module for submission manifests,
finding roots, and inclusion proofs.

Uses SHA-256 from the ``cryptography`` library.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import List, Optional


def _sha256(data: bytes) -> str:
    """Return hex-encoded SHA-256 digest."""
    return hashlib.sha256(data).hexdigest()


def hash_leaf(item: str | bytes | dict) -> str:
    """Hash a single leaf item."""
    if isinstance(item, dict):
        item = json.dumps(item, sort_keys=True, default=str)
    if isinstance(item, str):
        item = item.encode("utf-8")
    # Domain separation: prefix leaf hashes with 0x00
    return _sha256(b"\x00" + item)


def hash_pair(left: str, right: str) -> str:
    """Hash two child hashes into a parent node."""
    # Domain separation: prefix internal nodes with 0x01
    combined = bytes.fromhex(left) + bytes.fromhex(right)
    return _sha256(b"\x01" + combined)


@dataclass
class MerkleTree:
    """
    A binary Merkle tree over a list of items.

    Supports:
    - Building the tree and computing the root
    - Generating an inclusion proof for any leaf
    - Verifying a proof against a root
    """
    leaves: List[str] = field(default_factory=list)      # leaf hashes
    levels: List[List[str]] = field(default_factory=list) # all tree levels

    @classmethod
    def build(cls, items: list[str | bytes | dict]) -> "MerkleTree":
        """Construct a Merkle tree from a list of items."""
        if not items:
            tree = cls()
            tree.leaves = []
            tree.levels = [[_sha256(b"empty")]]
            return tree

        leaf_hashes = [hash_leaf(item) for item in items]
        tree = cls()
        tree.leaves = leaf_hashes
        tree.levels = [leaf_hashes]

        current = leaf_hashes
        while len(current) > 1:
            next_level = []
            for i in range(0, len(current), 2):
                left = current[i]
                right = current[i + 1] if i + 1 < len(current) else left
                next_level.append(hash_pair(left, right))
            tree.levels.append(next_level)
            current = next_level

        return tree

    @property
    def root(self) -> str:
        """Return the Merkle root hash."""
        return self.levels[-1][0] if self.levels else _sha256(b"empty")

    def inclusion_proof(self, index: int) -> List[dict]:
        """
        Generate an inclusion proof for the leaf at *index*.

        Returns a list of {"hash": ..., "side": "left"|"right"} entries
        representing the sibling nodes needed to recompute the root.
        """
        if index < 0 or index >= len(self.leaves):
            raise IndexError(f"Leaf index {index} out of range [0, {len(self.leaves)})")

        proof = []
        idx = index
        for level in self.levels[:-1]:  # skip the root level
            if idx % 2 == 0:
                sibling_idx = idx + 1
                side = "right"
            else:
                sibling_idx = idx - 1
                side = "left"

            if sibling_idx < len(level):
                proof.append({"hash": level[sibling_idx], "side": side})
            else:
                # Odd number of nodes: duplicate self
                proof.append({"hash": level[idx], "side": side})

            idx //= 2

        return proof

    @staticmethod
    def verify_proof(leaf_hash: str, proof: List[dict], expected_root: str) -> bool:
        """Verify an inclusion proof against the expected root."""
        current = leaf_hash
        for step in proof:
            sibling = step["hash"]
            if step["side"] == "right":
                current = hash_pair(current, sibling)
            else:
                current = hash_pair(sibling, current)
        return current == expected_root


def compute_file_hash(filepath: str) -> str:
    """SHA-256 hash of a file's contents."""
    h = hashlib.sha256()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def build_manifest_tree(file_entries: list[dict]) -> MerkleTree:
    """
    Build a Merkle tree from manifest file entries.

    Each entry should be: {"name": ..., "rows": ..., "sha256": ...}
    """
    items = [
        json.dumps(entry, sort_keys=True)
        for entry in file_entries
    ]
    return MerkleTree.build(items)
