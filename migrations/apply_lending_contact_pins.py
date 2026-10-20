"""T-06: ``pinned_at`` / ``pinned_until`` on ``lending.contacts``, the after-hours voicemail pin.

A pinned contact is called first the next morning; the pin ends at the first outbound attempt or at
``pinned_until``. Partial index on the pinned rows only. Idempotent; run after apply_lending_compliance.py.

Usage:
    PYTHONPATH=. python migrations/apply_lending_contact_pins.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection

from config.settings import get_settings
from src.lending.models import LENDING_SCHEMA

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_DDL = [
    'ALTER TABLE "{s}".contacts ADD COLUMN IF NOT EXISTS pinned_at TIMESTAMPTZ',
    'ALTER TABLE "{s}".contacts ADD COLUMN IF NOT EXISTS pinned_until TIMESTAMPTZ',
    'CREATE INDEX IF NOT EXISTS idx_lending_contacts_pinned_until ON "{s}".contacts (pinned_until) '
    "WHERE pinned_until IS NOT NULL",
]


def apply_to(conn: Connection) -> None:
    for statement in _DDL:
        conn.execute(text(statement.format(s=LENDING_SCHEMA)))


def main() -> int:
    engine = create_engine(str(get_settings().database_url))
    try:
        with engine.begin() as conn:
            apply_to(conn)
    finally:
        engine.dispose()
    logger.info("lending.contacts pin columns are in place")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
