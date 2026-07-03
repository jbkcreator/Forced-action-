"""Auto-converted from alembic migration `fa034_learning_card_type_date_index` (revision fa034_learning_card_type_date_index).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa034_learning_card_type_date_index.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
CREATE INDEX IF NOT EXISTS idx_learning_card_type_date ON learning_cards (card_type, card_date DESC);
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa034_learning_card_type_date_index")


if __name__ == "__main__":
    main()
