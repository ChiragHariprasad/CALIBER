"""CALIBRE SIA v2.2 - Run-Clock Facts and Frontier Time per CAL-DOC-007 §2.2.5."""
from __future__ import annotations

from typing import Optional, Sequence
from ..models.evidence import RunClockTick
from ..models.frontier import KnowledgeFrontier


class RunClockManager:
    """Manages explicit durable run-clock progression and knowledge frontier."""

    def __init__(self, clock_domain: str = "LIVE_UTC") -> None:
        self.clock_domain = clock_domain
        self.latest_tick: Optional[RunClockTick] = None
        self.watermark: Optional[str] = None

    def process_tick(self, tick: RunClockTick) -> None:
        """Admit a durable RunClockTick without advancing event-time watermark."""
        if self.latest_tick is None or tick.seq > self.latest_tick.seq:
            self.latest_tick = tick

    def update_watermark(self, watermark: str) -> None:
        """Advance the event-time watermark."""
        if self.watermark is None or watermark > self.watermark:
            self.watermark = watermark

    def get_frontier(self, fallback_run_clock: str = "") -> KnowledgeFrontier:
        """Compute the current knowledge frontier."""
        rc = self.latest_tick.clock_utc if self.latest_tick else fallback_run_clock
        return KnowledgeFrontier(run_clock=rc, watermark=self.watermark)

    @staticmethod
    def compute_t_eval(watermark: Optional[str], run_clock: str) -> str:
        """Evaluation instant t_eval = max(watermark, run_clock)."""
        if watermark and watermark > run_clock:
            return watermark
        return run_clock
