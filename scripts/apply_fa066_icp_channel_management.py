"""Auto-converted from alembic migration `fa066_icp_channel_management` (revision fa066).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa066_icp_channel_management.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
ALTER TABLE subscribers ADD COLUMN icp_channel_key VARCHAR(40) DEFAULT 'contractor' NOT NULL;

CREATE INDEX idx_subscribers_icp_channel_key ON subscribers (icp_channel_key);

CREATE TABLE icp_daily_stats (
    id SERIAL NOT NULL, 
    run_date DATE NOT NULL, 
    county_id VARCHAR(50) NOT NULL, 
    icp_channel_key VARCHAR(40) NOT NULL, 
    signup_count INTEGER DEFAULT '0' NOT NULL, 
    payer_count INTEGER DEFAULT '0' NOT NULL, 
    saved_card_count INTEGER DEFAULT '0' NOT NULL, 
    sms_sent_count INTEGER DEFAULT '0' NOT NULL, 
    sms_reply_count INTEGER DEFAULT '0' NOT NULL, 
    active_subscriber_count INTEGER DEFAULT '0' NOT NULL, 
    cancel_count INTEGER DEFAULT '0' NOT NULL, 
    refund_count INTEGER DEFAULT '0' NOT NULL, 
    mrr_cents BIGINT DEFAULT '0' NOT NULL, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL, 
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL, 
    PRIMARY KEY (id), 
    CONSTRAINT uq_icp_daily_stats_date_county_channel UNIQUE (run_date, county_id, icp_channel_key)
);

CREATE INDEX idx_icp_daily_stats_channel_date ON icp_daily_stats (icp_channel_key, run_date);

CREATE TABLE icp_channel_launch_audit (
    id SERIAL NOT NULL, 
    channel_key VARCHAR(40) NOT NULL, 
    event_type VARCHAR(32) NOT NULL, 
    actor VARCHAR(100) NOT NULL, 
    is_force_activate BOOLEAN DEFAULT 'false' NOT NULL, 
    force_reason TEXT, 
    gate_snapshot JSONB, 
    prev_status VARCHAR(20), 
    new_status VARCHAR(20), 
    detail JSONB, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL, 
    PRIMARY KEY (id), 
    CONSTRAINT ck_icp_audit_event_type CHECK (event_type IN ('activated','paused','killed','force_activated','config_updated','gate_evaluated','created'))
);

CREATE INDEX idx_icp_audit_channel_created ON icp_channel_launch_audit (channel_key, created_at);

ALTER TABLE waitlist_entries DROP CONSTRAINT ck_waitlist_entries_vertical;
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa066_icp_channel_management")


if __name__ == "__main__":
    main()
