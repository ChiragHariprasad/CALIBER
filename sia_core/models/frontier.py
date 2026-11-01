"""CALIBRE SIA v2.2 - Knowledge Frontier Models per CAL-DOC-007 §2.6.1, §2.6.2."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True, slots=True)
class KnowledgeFrontier:
    """Knowledge frontier recording stream progress and run-clock state."""
    run_clock: str
    watermark: Optional[str] = None

    def to_dict(self) -> dict[str, Optional[str]]:
        return {
            "run_clock": self.run_clock,
            "watermark": self.watermark,
        }
