"""Migration: lending_calling_pool_staging — WP-W0-1.

Wave 0 placeholder table for calling-pool extraction output.

IMPORTANT — Wave 0 location note (O1):
    This table lives in the FA database as a Wave 0 staging placeholder.
    Developer 2 owns the isolated lending schema that will become the
    final home for this data.  When O1 is resolved, the table name in
    pool_extraction._write_to_staging() and this file are the only things
    that need to change — no logic changes required.

Idempotent — safe to re-run.

Usage:
    PYTHONPATH=. python migrations/apply_lending_calling_pool_staging.py
"""

from __future__ import annotations

import logging
import sys

from sqlalchemy import text

from src.core.database import get_db_context

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


DDL = """
CREATE TABLE IF NOT EXISTS lending_calling_pool_staging (
    id                      BIGSERIAL PRIMARY KEY,
    run_id                  UUID        NOT NULL,
    pool_name               TEXT        NOT NULL
        CHECK (pool_name IN ('wholesaler_flipper', 'active_builder', 'mortgage_broker')),

    county_id               TEXT,
    county_name             TEXT,

    -- Spec §4.3 dialer display attributes
    borrower_name           TEXT,                   -- "Borrower Name"
    entity_name             TEXT,                   -- "Entity Name" (LLC / company)
    target_property_address TEXT,                   -- "Target Property Address"
    recent_permit_details   TEXT,                   -- "Recent Permit Details"

    -- Compliance / geo (Dev 2's DNC + Georgia rules)
    entity_status           TEXT
        CHECK (entity_status IN ('LLC', 'CORPORATION', 'NATURAL_PERSON', 'TRUST')),  -- NULL allowed → GA fail-closed
    parcel_id               TEXT,
    zip                     TEXT,
    state                   TEXT,                   -- 'FL' for every Wave 0 row

    -- Contact
    normalized_phone        TEXT,                   -- E.164 or NULL (see O15)
    phone_available         BOOLEAN     NOT NULL DEFAULT FALSE,
    email                   TEXT,

    -- Intent scoring (NULL when property anchor absent — see O14)
    financing_intent_score  NUMERIC(5,2),
    intent_tier             TEXT CHECK (intent_tier IN ('high', 'medium', 'low', 'unscored')),
    recommended_product     TEXT,

    -- INTERNAL ESTIMATE — not a quote, term, rate, or commitment to borrower
    estimated_loan_value    NUMERIC(14, 2),

    -- Aircall
    aircall_campaign_tag    TEXT        NOT NULL,

    -- Source identity (one per pool type, others NULL)
    buyer_entity_id         BIGINT,     -- Pool 1: references buyer_entities.id (soft ref)
    permit_number           TEXT,       -- Pool 2: references building_permits / permit_staging
    dbpr_license_number     TEXT,       -- Pool 3: reserved for future OFR registry source

    source_property_id      BIGINT,     -- NULL when no property anchor (permit_staging)
    source_table            TEXT        NOT NULL,

    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Index run_id for per-run audits and Aircall export queries
CREATE INDEX IF NOT EXISTS idx_lcps_run_id
    ON lending_calling_pool_staging (run_id);

-- Index pool_name + phone_available for Aircall export filter
CREATE INDEX IF NOT EXISTS idx_lcps_pool_phone
    ON lending_calling_pool_staging (pool_name, phone_available);

-- Index county for per-county reporting
CREATE INDEX IF NOT EXISTS idx_lcps_county
    ON lending_calling_pool_staging (county_id);

-- Index state for Dev 2's per-state (Georgia) compliance rule
CREATE INDEX IF NOT EXISTS idx_lcps_state
    ON lending_calling_pool_staging (state);
"""


def apply(session) -> None:
    logger.info("Applying lending_calling_pool_staging DDL…")
    session.execute(text(DDL))
    session.commit()
    logger.info("Done — lending_calling_pool_staging ready.")


def main() -> None:
    with get_db_context() as session:
        apply(session)


if __name__ == "__main__":
    main()
    sys.exit(0)
