"""
CALIBRE – Database Setup
=========================
Async SQLAlchemy engine, session factory, and Base declarative class.
"""

from __future__ import annotations

import logging
import sys

from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

from config.settings import settings

logger = logging.getLogger(__name__)


class Base(DeclarativeBase):
    """Declarative base for all CALIBRE ORM models."""
    pass


# ── Async engine (for API / stream processor) ──────────────────────────
db_url = settings.database_url
if db_url.startswith("postgresql://"):
    db_url = db_url.replace("postgresql://", "postgresql+asyncpg://", 1)

try:
    async_engine = create_async_engine(
        db_url,
        echo=False,
        pool_size=5,
        max_overflow=10,
    )
except Exception as ex:
    logger.warning("Could not initialize PostgreSQL async engine (%s). Using sqlite fallback.", ex)
    try:
        async_engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    except Exception:
        async_engine = None

AsyncSessionLocal = async_sessionmaker(
    async_engine,
    class_=AsyncSession,
    expire_on_commit=False,
) if async_engine else None


async def get_session() -> AsyncSession:
    """FastAPI dependency – yields an async session."""
    if AsyncSessionLocal:
        async with AsyncSessionLocal() as session:
            yield session
    else:
        yield None


# ── Sync engine (for migrations / CLI scripts) ─────────────────────────
try:
    sync_engine = create_engine(settings.database_url_sync, echo=False)
except Exception:
    sync_engine = None



async def init_db() -> None:
    """Create all tables (idempotent)."""
    if async_engine is None:
        return
    async with async_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("Database tables created / verified.")


# Allow `python -m storage.database --init`
if __name__ == "__main__":
    import asyncio

    if "--init" in sys.argv:
        # Import models so Base knows about them
        import storage.models  # noqa: F401
        import corrections.models  # noqa: F401

        asyncio.run(init_db())
        print("✓ Database initialised.")
    else:
        print("Usage: python -m storage.database --init")
