"""Auto-converted from alembic migration `fa037_revenue_signal_audit` (revision fa037_revenue_signal_audit).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa037_revenue_signal_audit.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
ALTER TABLE user_segments ADD COLUMN revenue_signal_band VARCHAR(20);

ALTER TABLE user_segments ADD COLUMN revenue_signal_breakdown JSONB;

ALTER TABLE user_segments ADD COLUMN revenue_signal_updated_at TIMESTAMP WITH TIME ZONE;

ALTER TABLE user_segments ADD COLUMN last_significant_action_at TIMESTAMP WITH TIME ZONE;

ALTER TABLE user_segments ADD COLUMN revenue_signal_last_action VARCHAR(80);

ALTER TABLE user_segments ADD CONSTRAINT check_revenue_signal_band CHECK (revenue_signal_band IS NULL OR revenue_signal_band IN ('low', 'medium', 'high', 'very_high'));

CREATE TABLE revenue_signal_score_events (
    id BIGSERIAL NOT NULL, 
    subscriber_id INTEGER NOT NULL, 
    action_type VARCHAR(80), 
    old_score INTEGER, 
    new_score INTEGER NOT NULL, 
    delta INTEGER NOT NULL, 
    band VARCHAR(20), 
    breakdown JSONB, 
    metadata JSONB, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL, 
    PRIMARY KEY (id), 
    FOREIGN KEY(subscriber_id) REFERENCES subscribers (id) ON DELETE CASCADE
);

CREATE INDEX idx_rss_events_sub_time ON revenue_signal_score_events (subscriber_id, created_at DESC);

CREATE INDEX idx_rss_events_action ON revenue_signal_score_events (action_type);

CREATE INDEX idx_rss_events_created ON revenue_signal_score_events (created_at);
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa037_revenue_signal_audit")


if __name__ == "__main__":
    main()
