"""R1 0.11: ``line_type_source`` + ``line_type_checked_at`` on ``lending.contacts``, written when the SMS line-type gate
looks a number up with Telnyx (counts the day's lookups for the cap; an ``unknown`` answer is retried after 30 days).

Idempotent. A short lock timeout makes it give up rather than queue live jobs behind it; re-run it if it times out.

Usage:
    PYTHONPATH=. python migrations/apply_lending_line_type_lookup.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

STATEMENTS: tuple[str, ...] = (
    "ALTER TABLE lending.contacts ADD COLUMN IF NOT EXISTS line_type_source VARCHAR(20)",
    "ALTER TABLE lending.contacts ADD COLUMN IF NOT EXISTS line_type_checked_at TIMESTAMPTZ",
    "CREATE INDEX IF NOT EXISTS idx_lending_contacts_line_type_checked_at "
    "ON lending.contacts (line_type_checked_at) WHERE line_type_source = 'telnyx'",
)


def apply_to(conn: Connection) -> None:
    """Run on ``conn``; the caller owns the transaction."""
    conn.execute(text("SET LOCAL lock_timeout = '5s'"))
    for statement in STATEMENTS:
        conn.execute(text(statement))


def main() -> None:
    engine = create_engine(get_settings().database_url, pool_pre_ping=True)
    with engine.begin() as conn:
        apply_to(conn)
    logger.info("lending.contacts line_type_source / line_type_checked_at applied")


if __name__ == "__main__":
    main()
