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
async_engine = create_async_engine(
    settings.database_url,
    echo=False,
    pool_size=5,
    max_overflow=10,
)

AsyncSessionLocal = async_sessionmaker(
    async_engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


async def get_session() -> AsyncSession:
    """FastAPI dependency – yields an async session."""
    async with AsyncSessionLocal() as session:
        yield session


# ── Sync engine (for migrations / CLI scripts) ─────────────────────────
sync_engine = create_engine(settings.database_url_sync, echo=False)


async def init_db() -> None:
    """Create all tables (idempotent)."""
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
