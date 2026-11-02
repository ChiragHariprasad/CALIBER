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

    def advance_to(self, run_id: str, clock_domain: str, clock_value: str, emitter_id: str = "event-time") -> None:
        """Advance a replay run clock monotonically from admitted source time."""
        if self.latest_tick and clock_value <= self.latest_tick.clock_utc:
            return
        seq = self.latest_tick.seq + 1 if self.latest_tick else 1
        self.process_tick(RunClockTick(
            run_id=run_id,
            clock_domain=clock_domain,
            clock_utc=clock_value,
            seq=seq,
            emitter_id=emitter_id,
        ))

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
