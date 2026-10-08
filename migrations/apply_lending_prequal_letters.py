"""T-07: ``lending.prequal_letters``, one Minute-5 pre-qualification letter per lead.

The unique (lead_source, lead_ref) index is the once-per-lead guard; status/attempts drive the
retry sweep. Idempotent; run after apply_lending_compliance.py (creates the ``lending`` schema).

Usage:
    PYTHONPATH=. python migrations/apply_lending_prequal_letters.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine

from config.settings import get_settings
from src.lending.models import LENDING_SCHEMA, LendingPrequalLetter

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


# create never alters an existing table; changes after first apply go here (safe to re-run).
_ALTERS = [
    # claim-before-send adds 'sending' and 'uncertain'
    'ALTER TABLE "{t}".prequal_letters DROP CONSTRAINT IF EXISTS ck_lending_prequal_letters_status',
    'ALTER TABLE "{t}".prequal_letters ADD CONSTRAINT ck_lending_prequal_letters_status '
    "CHECK (status IN ('pending', 'sending', 'sent', 'skipped', 'failed', 'uncertain'))",
]


def apply_to(conn: Connection, schema: str = LENDING_SCHEMA) -> None:
    translated = conn.execution_options(schema_translate_map={LENDING_SCHEMA: schema})
    LendingPrequalLetter.__table__.create(translated, checkfirst=True)
    for stmt in _ALTERS:
        conn.execute(text(stmt.format(t=schema)))


def apply(engine: Engine | None = None, schema: str = LENDING_SCHEMA) -> None:
    engine = engine or create_engine(get_settings().database_url, pool_pre_ping=True)
    with engine.begin() as conn:
        apply_to(conn, schema)
    logger.info("apply_lending_prequal_letters complete (schema=%s).", schema)


if __name__ == "__main__":
    apply()
