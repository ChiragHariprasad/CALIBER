"""
CALIBRE – FastAPI Application
===============================
Exposes F1 race-engineering intelligence, real-time telemetry,
tyre degradation models, Monte Carlo strategy evaluations,
and legacy regulatory rule evaluation endpoints.
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from api.routes import router as legacy_router
from calibre.api.race_routes import race_router
from calibre.api.live_routes import live_router

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup / shutdown lifecycle."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s  %(message)s",
    )
    # Attempt legacy db init if postgres is available
    try:
        from storage.database import init_db
        import storage.models  # noqa: F401
        import corrections.models  # noqa: F401
        await init_db()
        logger.info("CALIBRE PostgreSQL database connected.")
    except Exception as ex:
        logger.warning("Optional PostgreSQL storage not connected (offline mode active): %s", ex)

    logger.info("CALIBRE Race Engineering API ready.")
    yield
    logger.info("CALIBRE API shutting down.")


app = FastAPI(
    title="CALIBRE",
    description="F1 Race-Engineering Intelligence & Strategy Optimization Engine",
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

app.include_router(race_router)
app.include_router(live_router)
app.include_router(legacy_router)

# Mount static frontend files if built
_dist_dir = Path(__file__).resolve().parent.parent / "dist"
if _dist_dir.exists():
    app.mount("/", StaticFiles(directory=str(_dist_dir), html=True), name="static")
