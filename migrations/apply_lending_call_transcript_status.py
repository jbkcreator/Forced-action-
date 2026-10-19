"""T-12 audio fallback: transcript columns on ``lending.call_dispositions``.

``transcript_status``: NULL = pending, ``done`` = fetched and extracted, ``missing`` = none within
30 minutes of the call end, ``failed`` = extraction failed (never retried). A CHECK constraint
pins the values to ``TRANSCRIPT_STATUSES``. ``transcript_attempted_at``: last not-ready or failed
fetch, so a waiting call yields to never-tried ones. The partial index is built from
``TRANSCRIPT_PENDING_PREDICATE``, the same predicate the poller's queries use, so the two always
match. Existing answered calls older than the wait cap are expired by the poller on its first
enabled tick (never fetched). Idempotent; run after apply_lending_call_dispositions.py and before
setting LENDING_TRANSCRIPT_INGEST_ENABLED=true.

Usage:
    PYTHONPATH=. python migrations/apply_lending_call_transcript_status.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine

from config.lending_dispositions import TRANSCRIPT_PENDING_PREDICATE, TRANSCRIPT_STATUSES
from config.settings import get_settings
from src.lending.models import LENDING_SCHEMA

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_INDEX = "ix_lending_call_dispositions_transcript_pending"
_CHECK = "ck_lending_call_dispositions_transcript_status"


def index_ddl(schema: str) -> str:
    return (f'CREATE INDEX IF NOT EXISTS {_INDEX} ON "{schema}".call_dispositions '
            f"(transcript_attempted_at, call_ended_at) WHERE {TRANSCRIPT_PENDING_PREDICATE}")


def _check_ddl(schema: str) -> str:
    allowed = ", ".join(f"'{s}'" for s in TRANSCRIPT_STATUSES)
    return f"""
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = '{_CHECK}') THEN
                ALTER TABLE "{schema}".call_dispositions DROP CONSTRAINT {_CHECK};
            END IF;
            ALTER TABLE "{schema}".call_dispositions ADD CONSTRAINT {_CHECK}
                CHECK (transcript_status IS NULL OR transcript_status IN ({allowed}));
        END $$;
    """


def apply_to(conn: Connection, schema: str = LENDING_SCHEMA) -> None:
    table = f'"{schema}".call_dispositions'
    for ddl in (
        f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS transcript_status varchar(20)",
        f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS transcript_attempted_at timestamptz",
        f"DROP INDEX IF EXISTS \"{schema}\".{_INDEX}",  # recreated so its columns always match the query
        index_ddl(schema),
        _check_ddl(schema),
    ):
        conn.execute(text(ddl))


def apply(engine: Engine | None = None, schema: str = LENDING_SCHEMA) -> None:
    engine = engine or create_engine(get_settings().database_url, pool_pre_ping=True)
    with engine.begin() as conn:
        apply_to(conn, schema)
    logger.info("apply_lending_call_transcript_status complete (schema=%s).", schema)


if __name__ == "__main__":
    apply()
