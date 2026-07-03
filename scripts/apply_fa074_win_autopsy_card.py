"""Auto-converted from alembic migration `fa074_win_autopsy_card` (revision fa074_win_autopsy_card).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa074_win_autopsy_card.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
ALTER TABLE learning_cards DROP CONSTRAINT check_card_type;

ALTER TABLE learning_cards ADD CONSTRAINT check_card_type CHECK (card_type IN ('message_perf','deal_pattern','ab_result','churn_signal','pricing_test','general','autonomy_summary','kill_switch_scorecard','win_autopsy'));
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa074_win_autopsy_card")


if __name__ == "__main__":
    main()
