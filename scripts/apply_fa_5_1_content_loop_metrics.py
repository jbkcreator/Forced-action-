"""Auto-converted from alembic migration `fa_5_1_content_loop_metrics` (revision fa_5_1).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa_5_1_content_loop_metrics.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
ALTER TABLE quora_topics ADD COLUMN cluster VARCHAR(30);

ALTER TABLE quora_topics ADD COLUMN signup_count INTEGER DEFAULT '0' NOT NULL;

ALTER TABLE quora_topics ADD COLUMN cumulative_spend NUMERIC(10, 4) DEFAULT '0' NOT NULL;

ALTER TABLE quora_topics ADD COLUMN performance_score NUMERIC(12, 4);

ALTER TABLE quora_topics ADD COLUMN impression_count INTEGER;

ALTER TABLE quora_topics ADD COLUMN click_through_count INTEGER;
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa_5_1_content_loop_metrics")


if __name__ == "__main__":
    main()
