"""Auto-converted from alembic migration `fa067_supplier_intelligence` (revision fa067).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa067_supplier_intelligence.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
CREATE TABLE supplier_accounts (
    id SERIAL NOT NULL, 
    company_name VARCHAR(255) NOT NULL, 
    contact_name VARCHAR(255), 
    contact_email VARCHAR(255) NOT NULL, 
    status VARCHAR(20) DEFAULT 'active' NOT NULL, 
    counties JSONB, 
    verticals JSONB, 
    access_token VARCHAR(36) NOT NULL, 
    stripe_customer_id VARCHAR(100), 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL, 
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL, 
    PRIMARY KEY (id), 
    CONSTRAINT ck_supplier_accounts_status CHECK (status IN ('active','suspended','canceled')), 
    UNIQUE (access_token), 
    UNIQUE (stripe_customer_id)
);

CREATE INDEX idx_supplier_accounts_email ON supplier_accounts (contact_email);

CREATE INDEX idx_supplier_accounts_status ON supplier_accounts (status);

CREATE TABLE supplier_subscriptions (
    id SERIAL NOT NULL, 
    account_id INTEGER NOT NULL, 
    plan_tier VARCHAR(20) NOT NULL, 
    status VARCHAR(20) DEFAULT 'trialing' NOT NULL, 
    stripe_subscription_id VARCHAR(100), 
    stripe_price_id VARCHAR(100), 
    price_cents INTEGER, 
    trial_ends_at TIMESTAMP WITH TIME ZONE, 
    canceled_at TIMESTAMP WITH TIME ZONE, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL, 
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL, 
    PRIMARY KEY (id), 
    CONSTRAINT ck_supplier_subscriptions_status CHECK (status IN ('trialing','active','past_due','canceled')), 
    CONSTRAINT ck_supplier_subscriptions_tier CHECK (plan_tier IN ('foundation','standard','premium')), 
    FOREIGN KEY(account_id) REFERENCES supplier_accounts (id) ON DELETE CASCADE, 
    UNIQUE (stripe_subscription_id)
);

CREATE INDEX idx_supplier_subscriptions_account ON supplier_subscriptions (account_id);

CREATE INDEX idx_supplier_subscriptions_status ON supplier_subscriptions (status);

CREATE TABLE supplier_reports (
    id SERIAL NOT NULL, 
    account_id INTEGER NOT NULL, 
    county_id VARCHAR(50) NOT NULL, 
    generated_at TIMESTAMP WITH TIME ZONE, 
    report_period_start DATE, 
    report_period_end DATE, 
    sections_json JSONB, 
    data_readiness_snapshot JSONB, 
    status VARCHAR(20) DEFAULT 'pending' NOT NULL, 
    error_message TEXT, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL, 
    PRIMARY KEY (id), 
    CONSTRAINT ck_supplier_reports_status CHECK (status IN ('pending','generated','failed','exported')), 
    FOREIGN KEY(account_id) REFERENCES supplier_accounts (id) ON DELETE CASCADE
);

CREATE INDEX idx_supplier_reports_account_date ON supplier_reports (account_id, created_at);

CREATE INDEX idx_supplier_reports_status ON supplier_reports (status);

CREATE TABLE supplier_report_exports (
    id SERIAL NOT NULL, 
    report_id INTEGER NOT NULL, 
    format VARCHAR(10) NOT NULL, 
    file_path TEXT, 
    exported_at TIMESTAMP WITH TIME ZONE, 
    emailed_at TIMESTAMP WITH TIME ZONE, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL, 
    PRIMARY KEY (id), 
    CONSTRAINT ck_supplier_report_exports_format CHECK (format IN ('pdf','csv')), 
    FOREIGN KEY(report_id) REFERENCES supplier_reports (id) ON DELETE CASCADE
);

CREATE INDEX idx_supplier_report_exports_report ON supplier_report_exports (report_id);
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa067_supplier_intelligence")


if __name__ == "__main__":
    main()
