"""Lending's database connection: the shared DATABASE_URL, tables in the ``lending`` schema."""
from __future__ import annotations

from contextlib import contextmanager
from typing import Generator, Optional

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from config.settings import get_settings

_engine: Optional[Engine] = None
_factory: Optional[sessionmaker] = None


def _session_factory() -> sessionmaker:
    global _engine, _factory
    if _factory is None:
        _engine = create_engine(str(get_settings().database_url), pool_pre_ping=True, pool_recycle=3600)
        _factory = sessionmaker(bind=_engine, autoflush=False, expire_on_commit=False)
    return _factory


def get_lending_db() -> Generator[Session, None, None]:
    """FastAPI dependency. The caller commits; the session is always closed."""
    session = _session_factory()()
    try:
        yield session
    finally:
        session.close()


@contextmanager
def lending_session() -> Generator[Session, None, None]:
    """Standalone session (background delivery, cron). Commits on exit, rolls back on error."""
    session = _session_factory()()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
