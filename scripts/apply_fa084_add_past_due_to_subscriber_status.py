"""Auto-converted from alembic migration `fa084_add_past_due_to_subscriber_status` (revision fa084_add_past_due_to_subscriber_status).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa084_add_past_due_to_subscriber_status.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
ALTER TABLE subscribers DROP CONSTRAINT check_subscriber_status;

ALTER TABLE subscribers ADD CONSTRAINT check_subscriber_status CHECK (status IN ('active', 'grace', 'churned', 'cancelled', 'paused', 'disputed', 'past_due'));
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa084_add_past_due_to_subscriber_status")


if __name__ == "__main__":
    main()
