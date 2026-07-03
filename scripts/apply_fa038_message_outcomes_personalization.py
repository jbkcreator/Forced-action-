"""Auto-converted from alembic migration `fa038_message_outcomes_personalization` (revision fa038_message_outcomes_personalization).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa038_message_outcomes_personalization.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
ALTER TABLE message_outcomes ADD COLUMN trade_vertical VARCHAR(50);

ALTER TABLE message_outcomes ADD COLUMN county_id VARCHAR(50);

ALTER TABLE message_outcomes ADD COLUMN behavioral_segment VARCHAR(30);

ALTER TABLE message_outcomes ADD COLUMN revenue_signal_score INTEGER;

ALTER TABLE message_outcomes ADD COLUMN revenue_signal_score_band VARCHAR(20);

ALTER TABLE message_outcomes ADD COLUMN last_action_recency_band VARCHAR(30);

ALTER TABLE message_outcomes ADD COLUMN prompt_version VARCHAR(20);

ALTER TABLE message_outcomes ADD COLUMN context_snapshot JSONB;

CREATE INDEX idx_msg_outcome_vertical_segment ON message_outcomes (trade_vertical, behavioral_segment);
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa038_message_outcomes_personalization")


if __name__ == "__main__":
    main()
