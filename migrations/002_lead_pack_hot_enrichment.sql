-- Lead Pack Hot-Enrichment (ADR 0018)
-- Deferred fulfillment: reserve at payment ('enriching'), enrich + deliver-or-refund in sweep.
-- Apply via: python scripts/apply_lead_pack_hot_enrichment_migration.py

-- 1. Hot-Enrichment tracking columns on lead_pack_purchases
ALTER TABLE lead_pack_purchases
    ADD COLUMN IF NOT EXISTS enrichment_submitted_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS tracerfy_queue_id       VARCHAR(50);

-- 2. Allow the 'enriching' lifecycle state
ALTER TABLE lead_pack_purchases
    DROP CONSTRAINT IF EXISTS check_lead_pack_status;

ALTER TABLE lead_pack_purchases
    ADD CONSTRAINT check_lead_pack_status
    CHECK (status IN ('pending', 'enriching', 'delivered', 'expired', 'refunded'));

-- 3. Index for the fulfillment sweep's status claim query
CREATE INDEX IF NOT EXISTS idx_lead_pack_status
    ON lead_pack_purchases (status);
