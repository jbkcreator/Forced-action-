"""Auto-converted from alembic migration `fa108_lane_description` (revision fa108_lane_description).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa108_lane_description.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
ALTER TABLE lanes ADD COLUMN lane_description TEXT;

ALTER TABLE lanes ADD COLUMN description_tier VARCHAR(50);

ALTER TABLE lanes ADD COLUMN description_generated_at TIMESTAMP WITH TIME ZONE;
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa108_lane_description")


if __name__ == "__main__":
    main()
