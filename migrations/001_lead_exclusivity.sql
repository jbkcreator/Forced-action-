-- DDL migration for lead_pack_exclusivity feature.
-- Run with: python scripts/run_migration.py migrations/001_lead_exclusivity.sql
-- (alembic CLI unusable here — multi-head tree; apply via script.)

-- 1. Cross-trade exclusivity table
CREATE TABLE IF NOT EXISTS lead_exclusivity (
    id              SERIAL PRIMARY KEY,
    property_id     INTEGER NOT NULL,
    zip_code        VARCHAR(10) NOT NULL,
    county_id       VARCHAR(50) NOT NULL,
    sold_to_trade   VARCHAR(50) NOT NULL,
    source          VARCHAR(20) NOT NULL,
    source_id       INTEGER NOT NULL,
    exclusive_until TIMESTAMPTZ NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_property_source UNIQUE (property_id, source),
    CONSTRAINT ck_lead_exclusivity_source CHECK (source IN ('lead_pack', 'bundle'))
);

CREATE INDEX IF NOT EXISTS idx_exclusivity_zip_county
    ON lead_exclusivity (zip_code, county_id, exclusive_until);
CREATE INDEX IF NOT EXISTS idx_exclusivity_property
    ON lead_exclusivity (property_id);

-- 2. Refund columns on lead_pack_purchases
ALTER TABLE lead_pack_purchases
    ADD COLUMN IF NOT EXISTS refunded_at      TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS refund_reason    VARCHAR(100),
    ADD COLUMN IF NOT EXISTS stripe_refund_id VARCHAR(100);

-- 3. Allow 'refunded' status
ALTER TABLE lead_pack_purchases
    DROP CONSTRAINT IF EXISTS check_lead_pack_status;

ALTER TABLE lead_pack_purchases
    ADD CONSTRAINT check_lead_pack_status
    CHECK (status IN ('pending', 'delivered', 'expired', 'refunded'));
