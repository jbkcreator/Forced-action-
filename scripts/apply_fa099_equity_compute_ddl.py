"""Apply fa099 — mortgage_amount column on deeds table.

Companion to the ORM model change for Sprint 4.4 equity compute.
Idempotent: ADD COLUMN IF NOT EXISTS. Safe to run multiple times.

Usage:
    PYTHONPATH=. python scripts/apply_fa099_equity_compute_ddl.py
"""
import logging
import sys

from sqlalchemy import text

from src.core.database import Database

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_DDL = [
    (
        "ALTER TABLE deeds ADD COLUMN IF NOT EXISTS "
        "mortgage_amount NUMERIC(12, 2)"
    ),
]


def run() -> None:
    db = Database()
    with db.session_scope() as session:
        for stmt in _DDL:
            session.execute(text(stmt))
            logger.info("OK: %s", " ".join(stmt.split())[:90])
    logger.info("fa099 DDL applied successfully.")


if __name__ == "__main__":
    run()
    sys.exit(0)