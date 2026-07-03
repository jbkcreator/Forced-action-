"""Auto-converted from alembic migration `fa060_message_outcome_scheduled_status` (revision fa060).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa060_message_outcome_scheduled_status.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
ALTER TABLE message_outcomes DROP CONSTRAINT check_mo_send_status;

ALTER TABLE message_outcomes ADD CONSTRAINT check_mo_send_status CHECK (send_status IN ('pending_review','approved','sent','cancelled','failed','expired','scheduled'));
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa060_message_outcome_scheduled_status")


if __name__ == "__main__":
    main()
