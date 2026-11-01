"""CALIBRE SIA v2.2 - Consumer Leases per CAL-DOC-007 §2.8.2."""
from __future__ import annotations

from typing import Optional, Tuple
from ..time.resolver import add_seconds_to_iso


class LeaseManager:
    """Computes lease validity bounds in live and replay clock domains."""

    @staticmethod
    def compute_lease(
        t_eval: str,
        next_transition_at: Optional[str],
        max_lease_s: float = 30.0,
        clock_domain: str = "LIVE_UTC",
        is_final: bool = False,
    ) -> Tuple[Optional[str], str]:
        """Compute (lease_until, lease_clock_domain).
        
        Final verdicts have lease_until = None (never expire via lease).
        Otherwise lease_until = min(next_transition_at lowering state, t_eval + max_lease_s).
        """
        if is_final:
            return None, clock_domain

        max_lease_instant = add_seconds_to_iso(t_eval, max_lease_s)
        if next_transition_at:
            lease_until = min(next_transition_at, max_lease_instant)
        else:
            lease_until = max_lease_instant

        return lease_until, clock_domain
