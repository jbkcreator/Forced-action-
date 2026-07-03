"""Auto-converted from alembic migration `k1l2m3n4o5p6_add_scoring_run_id_to_distress_scores` (revision k1l2m3n4o5p6).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_k1l2m3n4o5p6_add_scoring_run_id_to_distress_scores.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
ALTER TABLE distress_scores ADD COLUMN scoring_run_id INTEGER;

CREATE INDEX idx_score_scoring_run_id ON distress_scores (scoring_run_id);
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied k1l2m3n4o5p6_add_scoring_run_id_to_distress_scores")


if __name__ == "__main__":
    main()
