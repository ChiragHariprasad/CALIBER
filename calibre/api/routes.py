"""
CALIBRE – API Routes
=====================
"""

from __future__ import annotations

import asyncio
import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

try:
    from calibre.storage.database import get_session
    from calibre.storage.models import IntegrityAlertRecord, VerdictRecord
    from calibre.streaming.processor import StreamProcessor, get_metrics
except ImportError:
    from storage.database import get_session
    from storage.models import IntegrityAlertRecord, VerdictRecord
    from streaming.processor import StreamProcessor, get_metrics

logger = logging.getLogger(__name__)

router = APIRouter()

# ── Singleton processor (started on first replay request) ────────────────
_processor: StreamProcessor | None = None
_processor_task: asyncio.Task | None = None


# ── Health ───────────────────────────────────────────────────────────────

@router.get("/health")
async def health():
    """Liveness probe."""
    return {"status": "ok", "service": "calibre"}


# ── Metrics ──────────────────────────────────────────────────────────────

@router.get("/metrics")
async def metrics():
    """Simple observability counters."""
    return get_metrics()


# ── Verdicts ─────────────────────────────────────────────────────────────

@router.get("/verdicts")
async def list_verdicts(
    limit: int = Query(default=50, le=500),
    session: AsyncSession = Depends(get_session),
):
    """Return the most recent verdicts."""
    stmt = (
        select(VerdictRecord)
        .order_by(VerdictRecord.recorded_at.desc())
        .limit(limit)
    )
    result = await session.execute(stmt)
    rows = result.scalars().all()
    return [_verdict_to_dict(r) for r in rows]


@router.get("/sessions/{session_key}/verdicts")
async def session_verdicts(
    session_key: int,
    limit: int = Query(default=50, le=500),
    session: AsyncSession = Depends(get_session),
):
    """Return verdicts for a specific session."""
    stmt = (
        select(VerdictRecord)
        .where(VerdictRecord.session_key == session_key)
        .order_by(VerdictRecord.timestamp.desc())
        .limit(limit)
    )
    result = await session.execute(stmt)
    rows = result.scalars().all()
    if not rows:
        raise HTTPException(404, f"No verdicts for session {session_key}")
    return [_verdict_to_dict(r) for r in rows]


# ── Integrity alerts ────────────────────────────────────────────────────

@router.get("/integrity/alerts")
async def list_integrity_alerts(
    limit: int = Query(default=50, le=500),
    session: AsyncSession = Depends(get_session),
):
    """Return recent integrity alerts."""
    stmt = (
        select(IntegrityAlertRecord)
        .order_by(IntegrityAlertRecord.timestamp.desc())
        .limit(limit)
    )
    result = await session.execute(stmt)
    rows = result.scalars().all()
    return [
        {
            "id": r.id,
            "sector": r.sector,
            "residual": r.residual,
            "cusum": r.cusum,
            "threshold": r.threshold,
            "severity": r.severity,
            "timestamp": r.timestamp.isoformat() if r.timestamp else None,
        }
        for r in rows
    ]


# ── Replay trigger ──────────────────────────────────────────────────────

@router.post("/replay/start")
async def start_replay(speed: float = Query(default=10.0)):
    """
    Start the replay producer + stream processor in the background.

    This is a convenience endpoint for demos – in production you'd run
    the producer and processor as separate services.
    """
    global _processor, _processor_task

    if _processor_task and not _processor_task.done():
        return {"status": "already_running"}

    # Start processor
    _processor = StreamProcessor()
    _processor_task = asyncio.create_task(_processor.run())

    # Start replay in a thread to avoid blocking the event loop
    import threading
    from replay.producer import run_replay

    t = threading.Thread(target=run_replay, args=(speed,), daemon=True)
    t.start()

    return {"status": "started", "speed": speed}


# ── Helpers ──────────────────────────────────────────────────────────────

def _verdict_to_dict(r: VerdictRecord) -> dict:
    return {
        "verdict_id": r.verdict_id,
        "timestamp": r.timestamp.isoformat() if r.timestamp else None,
        "session_key": r.session_key,
        "driver_number": r.driver_number,
        "rule_id": r.rule_id,
        "measured_value": r.measured_value,
        "threshold": r.threshold,
        "uncertainty": r.uncertainty,
        "confidence_margin": r.confidence_margin,
        "verdict": r.verdict,
        "parameter_version": r.parameter_version,
        "valid_from": r.valid_from.isoformat() if r.valid_from else None,
        "recorded_at": r.recorded_at.isoformat() if r.recorded_at else None,
    }
