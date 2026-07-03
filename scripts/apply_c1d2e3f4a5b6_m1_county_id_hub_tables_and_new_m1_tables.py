"""Auto-converted from alembic migration `c1d2e3f4a5b6_m1_county_id_hub_tables_and_new_m1_tables` (revision c1d2e3f4a5b6).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_c1d2e3f4a5b6_m1_county_id_hub_tables_and_new_m1_tables.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
ALTER TABLE properties ADD COLUMN county_id VARCHAR(50);

CREATE INDEX idx_properties_county_id ON properties (county_id);

UPDATE properties SET county_id = 'hillsborough' WHERE county_id IS NULL;

ALTER TABLE owners ADD COLUMN county_id VARCHAR(50);

CREATE INDEX idx_owners_county_id ON owners (county_id);

UPDATE owners SET county_id = 'hillsborough' WHERE county_id IS NULL;

ALTER TABLE financials ADD COLUMN county_id VARCHAR(50);

CREATE INDEX idx_financials_county_id ON financials (county_id);

UPDATE financials SET county_id = 'hillsborough' WHERE county_id IS NULL;

ALTER TABLE distress_scores ADD COLUMN county_id VARCHAR(50);

CREATE INDEX idx_distress_scores_county_id ON distress_scores (county_id);

UPDATE distress_scores SET county_id = 'hillsborough' WHERE county_id IS NULL;

CREATE TABLE founding_subscriber_counts (
    id SERIAL NOT NULL, 
    tier VARCHAR(20) NOT NULL, 
    vertical VARCHAR(50) NOT NULL, 
    county_id VARCHAR(50) NOT NULL, 
    count INTEGER DEFAULT '0' NOT NULL, 
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id), 
    CONSTRAINT uq_founding_tier_vertical_county UNIQUE (tier, vertical, county_id), 
    CONSTRAINT check_founding_tier CHECK (tier IN ('starter', 'pro', 'dominator'))
);

CREATE INDEX idx_founding_county_id ON founding_subscriber_counts (county_id);

CREATE TABLE subscribers (
    id SERIAL NOT NULL, 
    stripe_customer_id VARCHAR(100) NOT NULL, 
    stripe_subscription_id VARCHAR(100), 
    tier VARCHAR(20) NOT NULL, 
    vertical VARCHAR(50) NOT NULL, 
    county_id VARCHAR(50) NOT NULL, 
    founding_member BOOLEAN DEFAULT 'false' NOT NULL, 
    founding_price_id VARCHAR(100), 
    rate_locked_at TIMESTAMP WITHOUT TIME ZONE, 
    status VARCHAR(20) DEFAULT 'active' NOT NULL, 
    billing_date TIMESTAMP WITHOUT TIME ZONE, 
    grace_expires_at TIMESTAMP WITHOUT TIME ZONE, 
    ghl_contact_id VARCHAR(100), 
    ghl_stage INTEGER, 
    event_feed_uuid VARCHAR(36), 
    email VARCHAR(255), 
    name VARCHAR(255), 
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now() NOT NULL, 
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id), 
    CONSTRAINT check_subscriber_tier CHECK (tier IN ('starter', 'pro', 'dominator')), 
    CONSTRAINT check_subscriber_status CHECK (status IN ('active', 'grace', 'churned', 'cancelled')), 
    UNIQUE (stripe_customer_id), 
    UNIQUE (stripe_subscription_id), 
    UNIQUE (event_feed_uuid)
);

CREATE INDEX idx_subscriber_stripe_customer ON subscribers (stripe_customer_id);

CREATE INDEX idx_subscriber_stripe_sub ON subscribers (stripe_subscription_id);

CREATE INDEX idx_subscriber_county_id ON subscribers (county_id);

CREATE INDEX idx_subscriber_status ON subscribers (status);

CREATE INDEX idx_subscriber_vertical ON subscribers (vertical);

CREATE INDEX idx_subscriber_ghl_contact ON subscribers (ghl_contact_id);

CREATE INDEX idx_subscriber_event_feed_uuid ON subscribers (event_feed_uuid);

CREATE INDEX idx_subscriber_email ON subscribers (email);

CREATE TABLE zip_territories (
    id SERIAL NOT NULL, 
    zip_code VARCHAR(10) NOT NULL, 
    vertical VARCHAR(50) NOT NULL, 
    county_id VARCHAR(50) NOT NULL, 
    subscriber_id INTEGER, 
    status VARCHAR(20) DEFAULT 'available' NOT NULL, 
    locked_at TIMESTAMP WITHOUT TIME ZONE, 
    grace_expires_at TIMESTAMP WITHOUT TIME ZONE, 
    waitlist_emails VARCHAR(255)[] DEFAULT '{}', 
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id), 
    CONSTRAINT uq_zip_vertical_county UNIQUE (zip_code, vertical, county_id), 
    CONSTRAINT check_zip_status CHECK (status IN ('available', 'locked', 'grace')), 
    FOREIGN KEY(subscriber_id) REFERENCES subscribers (id)
);

CREATE INDEX idx_zip_territory_status ON zip_territories (status);

CREATE INDEX idx_zip_territory_county_id ON zip_territories (county_id);

CREATE INDEX idx_zip_territory_subscriber_id ON zip_territories (subscriber_id);

CREATE TABLE enriched_contacts (
    id SERIAL NOT NULL, 
    property_id INTEGER NOT NULL, 
    county_id VARCHAR(50) NOT NULL, 
    mobile_phone VARCHAR(20), 
    landline VARCHAR(20), 
    email VARCHAR(255), 
    mailing_address VARCHAR(255), 
    llc_owner_name VARCHAR(255), 
    relative_contacts JSONB, 
    source VARCHAR(50) NOT NULL, 
    match_success BOOLEAN DEFAULT 'false' NOT NULL, 
    ghl_contact_id VARCHAR(100), 
    ghl_synced_at TIMESTAMP WITHOUT TIME ZONE, 
    enriched_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id), 
    CONSTRAINT check_enriched_source CHECK (source IN ('batch_skip_tracing', 'idi')), 
    FOREIGN KEY(property_id) REFERENCES properties (id)
);

CREATE INDEX idx_enriched_property_id ON enriched_contacts (property_id);

CREATE INDEX idx_enriched_county_id ON enriched_contacts (county_id);

CREATE INDEX idx_enriched_match_success ON enriched_contacts (match_success);

CREATE INDEX idx_enriched_source ON enriched_contacts (source);

CREATE INDEX idx_enriched_ghl_contact ON enriched_contacts (ghl_contact_id);
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied c1d2e3f4a5b6_m1_county_id_hub_tables_and_new_m1_tables")


if __name__ == "__main__":
    main()
