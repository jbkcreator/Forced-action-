"""Auto-converted from alembic migration `fa082_quora_post_tracking` (revision fa082_quora_post_tracking).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa082_quora_post_tracking.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
ALTER TABLE quora_questions ADD COLUMN post_attempts INTEGER DEFAULT '0' NOT NULL;

ALTER TABLE quora_questions ADD COLUMN error_log TEXT;

ALTER TABLE quora_questions ADD COLUMN posted_at TIMESTAMP WITH TIME ZONE;
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa082_quora_post_tracking")


if __name__ == "__main__":
    main()
