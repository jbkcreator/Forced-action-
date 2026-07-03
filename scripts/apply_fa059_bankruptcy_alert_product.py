"""Auto-converted from alembic migration `fa059_bankruptcy_alert_product` (revision fa059).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa059_bankruptcy_alert_product.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
CREATE TABLE bankruptcy_filings (
    id SERIAL NOT NULL, 
    case_number VARCHAR(60) NOT NULL, 
    chapter VARCHAR(4), 
    court VARCHAR(20) NOT NULL, 
    jurisdiction VARCHAR(40) NOT NULL, 
    filer VARCHAR(255), 
    trustee VARCHAR(255), 
    date_filed DATE, 
    docket_id VARCHAR(40), 
    nature_of_suit VARCHAR(120), 
    raw JSONB, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL, 
    PRIMARY KEY (id), 
    UNIQUE (case_number)
);

CREATE INDEX idx_bkfiling_date_filed ON bankruptcy_filings (date_filed);

CREATE INDEX idx_bkfiling_jurisdiction_chapter ON bankruptcy_filings (jurisdiction, chapter);

CREATE INDEX idx_bkfiling_created_at ON bankruptcy_filings (created_at);

CREATE TABLE bankruptcy_alert_subscriptions (
    id SERIAL NOT NULL, 
    email VARCHAR(255) NOT NULL, 
    phone VARCHAR(20), 
    name VARCHAR(255), 
    stripe_customer_id VARCHAR(100), 
    stripe_subscription_id VARCHAR(100), 
    status VARCHAR(20) DEFAULT 'trialing' NOT NULL, 
    jurisdictions JSONB, 
    chapters JSONB, 
    channel_email BOOLEAN DEFAULT true NOT NULL, 
    channel_sms BOOLEAN DEFAULT false NOT NULL, 
    trial_ends_at TIMESTAMP WITH TIME ZONE, 
    access_token VARCHAR(36) NOT NULL, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL, 
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL, 
    canceled_at TIMESTAMP WITH TIME ZONE, 
    PRIMARY KEY (id), 
    CONSTRAINT check_bkalert_sub_status CHECK (status IN ('trialing','active','past_due','canceled')), 
    UNIQUE (stripe_customer_id), 
    UNIQUE (stripe_subscription_id), 
    UNIQUE (access_token)
);

CREATE INDEX idx_bkalert_sub_status ON bankruptcy_alert_subscriptions (status);

CREATE INDEX idx_bkalert_sub_email ON bankruptcy_alert_subscriptions (email);

CREATE TABLE bankruptcy_filing_alerts (
    id SERIAL NOT NULL, 
    subscription_id INTEGER NOT NULL, 
    filing_id INTEGER NOT NULL, 
    channel VARCHAR(10) NOT NULL, 
    status VARCHAR(12) NOT NULL, 
    error TEXT, 
    sent_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL, 
    PRIMARY KEY (id), 
    CONSTRAINT uq_bkfiling_alert_dedup UNIQUE (subscription_id, filing_id, channel), 
    CONSTRAINT check_bkalert_channel CHECK (channel IN ('email','sms')), 
    CONSTRAINT check_bkalert_status CHECK (status IN ('sent','failed','suppressed')), 
    FOREIGN KEY(subscription_id) REFERENCES bankruptcy_alert_subscriptions (id) ON DELETE CASCADE, 
    FOREIGN KEY(filing_id) REFERENCES bankruptcy_filings (id) ON DELETE CASCADE
);

CREATE INDEX idx_bkfiling_alert_sent_at ON bankruptcy_filing_alerts (sent_at);

CREATE INDEX idx_bkfiling_alert_subscription ON bankruptcy_filing_alerts (subscription_id);
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa059_bankruptcy_alert_product")


if __name__ == "__main__":
    main()
