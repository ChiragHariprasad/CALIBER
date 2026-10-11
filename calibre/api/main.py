"""
CALIBRE – FastAPI Application (§3 03_API_Contract.md)
======================================================
Master race engineering intelligence service, real-time telemetry distribution,
Monte Carlo strategy evaluation, and Kafka streaming processor.
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
import sys
from pathlib import Path

# Ensure root paths are in sys.path
CALIBRE_ROOT = Path(__file__).resolve().parent.parent
WORKSPACE_ROOT = CALIBRE_ROOT.parent
if str(WORKSPACE_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKSPACE_ROOT))
if str(CALIBRE_ROOT) not in sys.path:
    sys.path.insert(0, str(CALIBRE_ROOT))

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from calibre.api.live_routes import live_router, set_coordinator
from calibre.api.race_routes import race_router
from calibre.streaming.kafka_topics import ensure_topics
from calibre.streaming.processor import StreamProcessor

try:
    from calibre.api.routes import router as legacy_router
except ImportError:
    from api.routes import router as legacy_router

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup / shutdown lifecycle."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s  %(message)s",
    )
    logger.info("Initializing CALIBRE Race Engineering & Streaming Services...")

    # 1. Ensure Kafka topics on broker if available
    try:
        ensure_topics()
    except Exception as ex:
        logger.warning("Could not auto-ensure Kafka topics: %s", ex)

    # 2. Attempt legacy db init if postgres is available
    try:
        from calibre.storage.database import init_db
        import calibre.storage.models  # noqa: F401
        import calibre.corrections.models  # noqa: F401
        await init_db()
        logger.info("CALIBRE PostgreSQL database connected.")
    except Exception as ex:
        logger.warning("Optional PostgreSQL storage not connected (offline mode active): %s", ex)

    # 3. Start background Kafka StreamProcessor
    processor = None
    processor_task = None
    try:
        processor = StreamProcessor()
        processor_task = asyncio.create_task(processor.run())
        logger.info("CALIBRE StreamProcessor started in background.")
    except Exception as ex:
        logger.warning("Could not start Kafka StreamProcessor: %s", ex)

    # 4. Initialize RaceCoordinator
    coordinator = None
    try:
        from calibre.live.race_coordinator import RaceCoordinator
        coordinator = RaceCoordinator(
            session_key=11731,
            target_driver="VER",
            mock_mode=True,
            use_multipoint_aggregator=True,
        )
        await coordinator.start()
        set_coordinator(coordinator)
        logger.info("CALIBRE RaceCoordinator initialized & running with MultiPoint streaming.")
    except Exception as ex:
        logger.warning("Could not auto-start RaceCoordinator: %s", ex)

    logger.info("CALIBRE Race Engineering API ready.")
    yield

    # Shutdown sequence
    if processor:
        try:
            await processor.stop()
        except Exception:
            pass
    if processor_task:
        processor_task.cancel()

    if coordinator:
        try:
            await coordinator.stop()
        except Exception:
            pass
    logger.info("CALIBRE API shutting down.")


app = FastAPI(
    title="CALIBRE",
    description="F1 Race-Engineering Intelligence, Performance Twin & Kafka Streaming Platform",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Root liveness probe (§3)
@app.get("/health")
async def root_health():
    return {"status": "ok", "service": "calibre", "mode": "HISTORICAL_REPLAY"}

# Mount routers
app.include_router(live_router)
app.include_router(race_router)
app.include_router(legacy_router)

# Mount static frontend files if built
_dist_dir = Path(__file__).resolve().parent.parent / "dist"
if _dist_dir.exists():
    app.mount("/", StaticFiles(directory=str(_dist_dir), html=True), name="static")
