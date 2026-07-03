"""Auto-converted from alembic migration `fa056_deal_outcomes_county_vertical` (revision fa056).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa056_deal_outcomes_county_vertical.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
ALTER TABLE deal_outcomes ADD COLUMN county_id VARCHAR(50);

ALTER TABLE deal_outcomes ADD COLUMN trade_vertical VARCHAR(50);

CREATE INDEX idx_deal_outcomes_county_vertical ON deal_outcomes (county_id, trade_vertical);
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa056_deal_outcomes_county_vertical")


if __name__ == "__main__":
    main()
