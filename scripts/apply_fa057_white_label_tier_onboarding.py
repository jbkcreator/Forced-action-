"""Auto-converted from alembic migration `fa057_white_label_tier_onboarding` (revision fa057).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa057_white_label_tier_onboarding.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
CREATE TABLE white_label_clients (
    id SERIAL NOT NULL, 
    company_name VARCHAR(255) NOT NULL, 
    company_slug VARCHAR(100) NOT NULL, 
    display_name VARCHAR(255), 
    admin_email VARCHAR(255) NOT NULL, 
    admin_name VARCHAR(255) NOT NULL, 
    status VARCHAR(30) DEFAULT 'pending_verification' NOT NULL, 
    stripe_customer_id VARCHAR(100), 
    stripe_subscription_id VARCHAR(100), 
    plan_tier VARCHAR(20), 
    plan_price_cents INTEGER, 
    trial_ends_at TIMESTAMP WITH TIME ZONE, 
    logo_url VARCHAR(500), 
    primary_color VARCHAR(7), 
    secondary_color VARCHAR(7), 
    counties_enabled JSONB, 
    verticals_enabled JSONB, 
    api_enabled BOOLEAN DEFAULT 'true' NOT NULL, 
    api_requests_per_day INTEGER DEFAULT '10000' NOT NULL, 
    verified_at TIMESTAMP WITH TIME ZONE, 
    activated_at TIMESTAMP WITH TIME ZONE, 
    churned_at TIMESTAMP WITH TIME ZONE, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id), 
    CONSTRAINT check_wl_client_status CHECK (status IN ('pending_verification','active','suspended','churned')), 
    CONSTRAINT check_wl_client_plan_tier CHECK (plan_tier IS NULL OR plan_tier IN ('standard','premium')), 
    UNIQUE (company_slug), 
    UNIQUE (stripe_customer_id), 
    UNIQUE (stripe_subscription_id)
);

CREATE UNIQUE INDEX idx_wl_client_slug ON white_label_clients (company_slug);

CREATE INDEX idx_wl_client_admin_email ON white_label_clients (admin_email);

CREATE INDEX idx_wl_client_status ON white_label_clients (status);

CREATE INDEX idx_wl_client_stripe_cid ON white_label_clients (stripe_customer_id);

CREATE TABLE white_label_users (
    id SERIAL NOT NULL, 
    client_id INTEGER NOT NULL, 
    email VARCHAR(255) NOT NULL, 
    name VARCHAR(255) NOT NULL, 
    role VARCHAR(20) DEFAULT 'member' NOT NULL, 
    password_hash VARCHAR(255), 
    is_active BOOLEAN DEFAULT 'true' NOT NULL, 
    email_verified_at TIMESTAMP WITH TIME ZONE, 
    reset_token VARCHAR(64), 
    reset_token_expires_at TIMESTAMP WITH TIME ZONE, 
    invited_by_id INTEGER, 
    last_login_at TIMESTAMP WITH TIME ZONE, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id), 
    CONSTRAINT check_wl_user_role CHECK (role IN ('admin','member')), 
    FOREIGN KEY(client_id) REFERENCES white_label_clients (id) ON DELETE CASCADE, 
    UNIQUE (email), 
    FOREIGN KEY(invited_by_id) REFERENCES white_label_users (id) ON DELETE SET NULL
);

CREATE INDEX idx_wl_user_client_id ON white_label_users (client_id);

CREATE UNIQUE INDEX idx_wl_user_email ON white_label_users (email);

CREATE INDEX idx_wl_user_reset_token ON white_label_users (reset_token);

CREATE INDEX idx_wl_user_client_email ON white_label_users (client_id, email);

CREATE TABLE white_label_api_keys (
    id SERIAL NOT NULL, 
    client_id INTEGER NOT NULL, 
    key_prefix VARCHAR(12) NOT NULL, 
    key_hash VARCHAR(64) NOT NULL, 
    label VARCHAR(100) DEFAULT 'Default' NOT NULL, 
    is_active BOOLEAN DEFAULT 'true' NOT NULL, 
    created_by INTEGER, 
    requests_today INTEGER DEFAULT '0' NOT NULL, 
    total_requests INTEGER DEFAULT '0' NOT NULL, 
    last_used_at TIMESTAMP WITH TIME ZONE, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    revoked_at TIMESTAMP WITH TIME ZONE, 
    PRIMARY KEY (id), 
    FOREIGN KEY(client_id) REFERENCES white_label_clients (id) ON DELETE CASCADE, 
    UNIQUE (key_hash), 
    FOREIGN KEY(created_by) REFERENCES white_label_users (id) ON DELETE SET NULL
);

CREATE INDEX idx_wl_api_key_prefix ON white_label_api_keys (key_prefix);

CREATE UNIQUE INDEX idx_wl_api_key_hash ON white_label_api_keys (key_hash);

CREATE INDEX idx_wl_api_key_client_active ON white_label_api_keys (client_id, is_active);

CREATE TABLE white_label_contractor_enrichments (
    id SERIAL NOT NULL, 
    client_id INTEGER NOT NULL, 
    county_id VARCHAR(50) NOT NULL, 
    vertical VARCHAR(50) NOT NULL, 
    clay_run_id VARCHAR(100), 
    data JSONB, 
    enriched_at TIMESTAMP WITH TIME ZONE, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id), 
    CONSTRAINT uq_wl_contractor_enrichment UNIQUE (client_id, county_id, vertical), 
    FOREIGN KEY(client_id) REFERENCES white_label_clients (id) ON DELETE CASCADE
);

CREATE INDEX idx_wl_enrichment_client_county ON white_label_contractor_enrichments (client_id, county_id);
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa057_white_label_tier_onboarding")


if __name__ == "__main__":
    main()
