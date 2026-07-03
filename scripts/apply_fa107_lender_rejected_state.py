"""Auto-converted from alembic migration `fa107_lender_rejected_state` (revision fa107_lender_rejected_state).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa107_lender_rejected_state.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
ALTER TABLE broker_transitions DROP CONSTRAINT IF EXISTS ck_bt_to_state;

ALTER TABLE broker_transitions ADD CONSTRAINT ck_bt_to_state CHECK (to_state IN ('unassigned', 'assigned', 'working', 'quoted', 'committed', 'lender_rejected', 'closed_won', 'closed_lost'));
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa107_lender_rejected_state")


if __name__ == "__main__":
    main()
