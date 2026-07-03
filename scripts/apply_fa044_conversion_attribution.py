"""Auto-converted from alembic migration `fa044_conversion_attribution` (revision fa044_conversion_attribution).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa044_conversion_attribution.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
CREATE TABLE conversion_attribution_events (
    id BIGSERIAL NOT NULL, 
    conversion_type VARCHAR(60) NOT NULL, 
    source_table VARCHAR(80) NOT NULL, 
    source_event_id VARCHAR(120) NOT NULL, 
    subscriber_id INTEGER NOT NULL, 
    lead_id INTEGER, 
    property_id INTEGER, 
    zip_code VARCHAR(10), 
    trade VARCHAR(50), 
    wallet_tier VARCHAR(30), 
    lock_status VARCHAR(20), 
    lock_zip VARCHAR(10), 
    autopilot_tier VARCHAR(30), 
    bundle_id INTEGER, 
    bundle_type VARCHAR(50), 
    deal_size_bucket VARCHAR(20), 
    revenue_amount NUMERIC(12, 2), 
    currency VARCHAR(3) DEFAULT 'usd' NOT NULL, 
    occurred_at TIMESTAMP WITH TIME ZONE NOT NULL, 
    attribution_status VARCHAR(20), 
    attribution_confidence VARCHAR(20), 
    attribution_metadata JSONB, 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL, 
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL, 
    PRIMARY KEY (id), 
    FOREIGN KEY(subscriber_id) REFERENCES subscribers (id) ON DELETE CASCADE, 
    FOREIGN KEY(lead_id) REFERENCES sent_leads (id) ON DELETE SET NULL, 
    FOREIGN KEY(property_id) REFERENCES properties (id) ON DELETE SET NULL, 
    FOREIGN KEY(bundle_id) REFERENCES bundle_purchases (id) ON DELETE SET NULL
);

ALTER TABLE conversion_attribution_events ADD CONSTRAINT uq_attribution_source UNIQUE (source_table, source_event_id);

ALTER TABLE conversion_attribution_events ADD CONSTRAINT check_cae_conversion_type CHECK (conversion_type IN ('paid_unlock','saved_card','wallet_activation','wallet_topup','bundle_purchase','territory_lock_purchase','autopilot_lite_upgrade','autopilot_pro_upgrade','annual_upgrade','data_only_save','deal_win_reported','failed_payment_recovered'));

ALTER TABLE conversion_attribution_events ADD CONSTRAINT check_cae_lock_status CHECK (lock_status IS NULL OR lock_status IN ('locked','unlocked','unknown','not_applicable'));

ALTER TABLE conversion_attribution_events ADD CONSTRAINT check_cae_autopilot_tier CHECK (autopilot_tier IS NULL OR autopilot_tier IN ('autopilot_lite','autopilot_pro','not_applicable','unknown'));

ALTER TABLE conversion_attribution_events ADD CONSTRAINT check_cae_attribution_status CHECK (attribution_status IS NULL OR attribution_status IN ('complete','partial','unresolved'));

ALTER TABLE conversion_attribution_events ADD CONSTRAINT check_cae_attribution_confidence CHECK (attribution_confidence IS NULL OR attribution_confidence IN ('high','medium','low'));

CREATE INDEX idx_cae_subscriber_id ON conversion_attribution_events (subscriber_id);

CREATE INDEX idx_cae_lead_id ON conversion_attribution_events (lead_id);

CREATE INDEX idx_cae_zip_code ON conversion_attribution_events (zip_code);

CREATE INDEX idx_cae_trade ON conversion_attribution_events (trade);

CREATE INDEX idx_cae_wallet_tier ON conversion_attribution_events (wallet_tier);

CREATE INDEX idx_cae_lock_status ON conversion_attribution_events (lock_status);

CREATE INDEX idx_cae_autopilot_tier ON conversion_attribution_events (autopilot_tier);

CREATE INDEX idx_cae_bundle_type ON conversion_attribution_events (bundle_type);

CREATE INDEX idx_cae_deal_size_bucket ON conversion_attribution_events (deal_size_bucket);

CREATE INDEX idx_cae_conversion_type ON conversion_attribution_events (conversion_type);

CREATE INDEX idx_cae_occurred_at ON conversion_attribution_events (occurred_at);

ALTER TABLE subscribers ADD COLUMN revenue_signal_score INTEGER DEFAULT '0' NOT NULL;

ALTER TABLE subscribers ADD COLUMN revenue_signal_band VARCHAR(20);

ALTER TABLE subscribers ADD COLUMN revenue_signal_breakdown JSONB;

ALTER TABLE subscribers ADD COLUMN revenue_signal_updated_at TIMESTAMP WITH TIME ZONE;

ALTER TABLE subscribers ADD CONSTRAINT check_subscriber_revenue_signal_band CHECK (revenue_signal_band IS NULL OR revenue_signal_band IN ('low','medium','high','very_high'));

CREATE INDEX idx_subscriber_signal_score ON subscribers (revenue_signal_score);
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa044_conversion_attribution")


if __name__ == "__main__":
    main()
