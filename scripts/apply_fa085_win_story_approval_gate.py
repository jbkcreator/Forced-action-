"""Auto-converted from alembic migration `fa085_win_story_approval_gate` (revision fa085_win_story_approval_gate).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa085_win_story_approval_gate.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
ALTER TABLE win_story_assets ADD COLUMN approved_by VARCHAR(100);

ALTER TABLE win_story_assets ADD COLUMN slack_message_ts VARCHAR(50);

ALTER TABLE win_story_assets ALTER COLUMN is_public SET DEFAULT false;
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa085_win_story_approval_gate")


if __name__ == "__main__":
    main()
