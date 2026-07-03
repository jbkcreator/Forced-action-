"""Auto-converted from alembic migration `fa048_subscriber_revenue_synthflow_calls` (revision fa048_subscriber_revenue_synthflow).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa048_subscriber_revenue_synthflow_calls.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
ALTER TABLE subscribers ADD COLUMN plan_price NUMERIC(10, 2);

ALTER TABLE subscribers ADD COLUMN churned_at TIMESTAMP WITH TIME ZONE;

ALTER TABLE subscribers ADD COLUMN is_trial BOOLEAN DEFAULT 'false' NOT NULL;

ALTER TABLE subscribers ADD COLUMN trial_ends_at TIMESTAMP WITH TIME ZONE;

CREATE INDEX idx_subscribers_churned_at ON subscribers (churned_at);

CREATE INDEX idx_subscribers_is_trial ON subscribers (is_trial);

CREATE TABLE synthflow_calls (
    id SERIAL NOT NULL, 
    prospect_phone VARCHAR(20) NOT NULL, 
    outcome VARCHAR(50), 
    vertical VARCHAR(50), 
    zip_code VARCHAR(10), 
    contact_id VARCHAR(100), 
    call_date DATE NOT NULL, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id)
);

CREATE INDEX idx_synthflow_calls_call_date ON synthflow_calls (call_date);

CREATE INDEX idx_synthflow_calls_outcome ON synthflow_calls (outcome);

CREATE INDEX idx_synthflow_calls_prospect_phone ON synthflow_calls (prospect_phone);
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa048_subscriber_revenue_synthflow_calls")


if __name__ == "__main__":
    main()
