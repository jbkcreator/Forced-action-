"""
Vera's read-only database connection.

Binds to VERA_DATABASE_URL (the vera_readonly Postgres role — see
migrations/apply_vera_readonly_role.py), never to the app's normal
read/write DATABASE_URL. Every verification check Vera runs reads through
this connection.

The vera_readonly role itself holds no write grants anywhere (enforced at
the database level), and this connection additionally sets
default_transaction_read_only=on per-session as a second, independent guard —
so a write attempt fails immediately even before hitting the role's grants.

Writing a verified fact is the one exception to Vera's read-only behaviour,
and it does NOT go through this module — see src/agents/vera/facts.py, which
uses the normal app DB role (src.core.database), scoped to the single
vera_facts table.
"""
from __future__ import annotations

from contextlib import contextmanager
from typing import Generator

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session

from config.settings import get_settings


class VeraReadOnlyDatabase:
    """Singleton engine/session-factory bound to the vera_readonly role."""

    _instance = None
    _engine = None
    _session_factory: sessionmaker = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def _initialize_engine(self) -> None:
        settings = get_settings()
        if not settings.vera_database_url:
            raise RuntimeError(
                "VERA_DATABASE_URL is not set. Provision the read-only role via "
                "migrations/apply_vera_readonly_role.py and set VERA_DATABASE_URL "
                "in .env before running Vera."
            )

        self._engine = create_engine(
            settings.vera_database_url,
            pool_pre_ping=True,
            pool_recycle=3600,
            connect_args={"options": "-c default_transaction_read_only=on"},
        )
        self._session_factory = sessionmaker(
            bind=self._engine, autocommit=False, autoflush=False, expire_on_commit=False,
        )

    @contextmanager
    def session_scope(self) -> Generator[Session, None, None]:
        """Read-only session. Any write attempt raises immediately."""
        if self._session_factory is None:
            self._initialize_engine()
        session = self._session_factory()
        try:
            yield session
        finally:
            session.close()


vera_db = VeraReadOnlyDatabase()


def check_connection() -> bool:
    """True if the vera_readonly role can connect and SELECT."""
    try:
        with vera_db.session_scope() as session:
            session.execute(text("SELECT 1"))
        return True
    except Exception:
        return False
