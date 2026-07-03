"""Auto-converted from alembic migration `5ac36f946603_add_vertical_scores_to_distress_scores` (revision 5ac36f946603).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_5ac36f946603_add_vertical_scores_to_distress_scores.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
ALTER TABLE distress_scores ADD COLUMN vertical_scores JSONB;

CREATE INDEX ix_distress_scores_vertical_scores ON distress_scores USING gin (vertical_scores);
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied 5ac36f946603_add_vertical_scores_to_distress_scores")


if __name__ == "__main__":
    main()
