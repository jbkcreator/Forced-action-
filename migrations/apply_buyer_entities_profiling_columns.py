"""
Add buyer-type classification, portfolio cadence/capacity/hold-time, and
auction-winner-resolution columns to buyer_entities/buyer_entity_links, plus
a per-auction resolution-status column on tax_deed_auctions
(HUNTER-03/04/05).

Shared across all three tasks per docs/plans/agent_lane_phase3_week3_dev_split.md
§10b ("land one migration, not three competing ones") -- H3
(buyer_type_classification.py), H4 (portfolio_profiling.py), and H5
(auction_resolution.py) all read/write these columns.

buyer_entity_links.source_table and .match_method are WIDENED, not just
added to -- see apply_buyer_entities_estate_type.py for the same
drop-and-recreate-if-missing idempiotency pattern this reuses.
source_table gains 'tax_deed_auctions' (H5's new candidate source);
match_method gains 'exact_name_only' (attached to an existing entity with no
address to corroborate the match) and 'auction_name_only_unverified' (a
brand-new low-confidence entity created from an auction win alone, per
gating.UNVERIFIED_FLOOR -- see auction_resolution.py).

Idempotent -- ADD COLUMN IF NOT EXISTS throughout; constraint widenings only
drop+recreate if the current definition doesn't already include the new
value(s).

Usage:
    PYTHONPATH=. python migrations/apply_buyer_entities_profiling_columns.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DDL = [
    # -- H3: buyer-type classification --------------------------------------
    "ALTER TABLE buyer_entities ADD COLUMN IF NOT EXISTS buyer_type VARCHAR(20);",
    "ALTER TABLE buyer_entities ADD COLUMN IF NOT EXISTS buyer_type_confidence INTEGER;",
    "ALTER TABLE buyer_entities ADD COLUMN IF NOT EXISTS buyer_type_classified_at TIMESTAMPTZ;",
    "ALTER TABLE buyer_entities ADD COLUMN IF NOT EXISTS buyer_type_evidence JSONB;",
    "ALTER TABLE buyer_entities ADD COLUMN IF NOT EXISTS buyer_type_rule_version SMALLINT;",

    # -- H4: portfolio cadence/capacity/financing/hold-time ------------------
    "ALTER TABLE buyer_entities ADD COLUMN IF NOT EXISTS cadence_purchases_per_year NUMERIC(6,2);",
    "ALTER TABLE buyer_entities ADD COLUMN IF NOT EXISTS estimated_annual_acquisition_capacity INTEGER;",
    "ALTER TABLE buyer_entities ADD COLUMN IF NOT EXISTS financing_signal VARCHAR(20);",
    "ALTER TABLE buyer_entities ADD COLUMN IF NOT EXISTS avg_hold_days INTEGER;",
    "ALTER TABLE buyer_entities ADD COLUMN IF NOT EXISTS portfolio_profiled_at TIMESTAMPTZ;",
    "ALTER TABLE buyer_entities ADD COLUMN IF NOT EXISTS portfolio_evidence JSONB;",

    # -- new-constraint guards (brand new constraints, not widenings) --------
    """
    DO $$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'check_buyer_entity_buyer_type') THEN
            ALTER TABLE buyer_entities
                ADD CONSTRAINT check_buyer_entity_buyer_type
                CHECK (buyer_type IS NULL OR buyer_type IN ('flipper', 'buy-and-hold', 'wholesaler', 'institutional'));
        END IF;
    END $$;
    """,
    """
    DO $$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'check_buyer_entity_buyer_type_confidence') THEN
            ALTER TABLE buyer_entities
                ADD CONSTRAINT check_buyer_entity_buyer_type_confidence
                CHECK (buyer_type_confidence IS NULL OR (buyer_type_confidence >= 0 AND buyer_type_confidence <= 100));
        END IF;
    END $$;
    """,
    """
    DO $$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'check_buyer_entity_financing_signal') THEN
            ALTER TABLE buyer_entities
                ADD CONSTRAINT check_buyer_entity_financing_signal
                CHECK (financing_signal IS NULL OR financing_signal IN ('cash_inferred', 'financed', 'unknown'));
        END IF;
    END $$;
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_buyer_entities_buyer_type
        ON buyer_entities (buyer_type) WHERE buyer_type IS NOT NULL;
    """,

    # -- H5: widen buyer_entity_links for tax_deed_auctions as a candidate
    # source, plus the two new conservative (no-address) match methods ------
    """
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM pg_constraint
            WHERE conname = 'check_buyer_entity_link_source_table'
              AND pg_get_constraintdef(oid) LIKE '%tax_deed_auctions%'
        ) THEN
            ALTER TABLE buyer_entity_links DROP CONSTRAINT IF EXISTS check_buyer_entity_link_source_table;
            ALTER TABLE buyer_entity_links
                ADD CONSTRAINT check_buyer_entity_link_source_table
                CHECK (source_table IN ('owners', 'deeds', 'sunbiz_snapshots', 'tax_deed_auctions'));
        END IF;
    END $$;
    """,
    """
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM pg_constraint
            WHERE conname = 'check_buyer_entity_link_match_method'
              AND pg_get_constraintdef(oid) LIKE '%auction_name_only_unverified%'
        ) THEN
            ALTER TABLE buyer_entity_links DROP CONSTRAINT IF EXISTS check_buyer_entity_link_match_method;
            ALTER TABLE buyer_entity_links
                ADD CONSTRAINT check_buyer_entity_link_match_method
                CHECK (match_method IN (
                    'sunbiz_llc_piercing', 'exact_name_address', 'fuzzy_name', 'llm_adjudicated', 'manual',
                    'exact_name_only', 'auction_name_only_unverified'
                ));
        END IF;
    END $$;
    """,

    # -- H5: per-auction resolution-status tracking (processing != verified,
    # see auction_resolution.py) --------------------------------------------
    "ALTER TABLE tax_deed_auctions ADD COLUMN IF NOT EXISTS buyer_resolution_status VARCHAR(20);",
    """
    DO $$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'check_tax_deed_buyer_resolution_status') THEN
            ALTER TABLE tax_deed_auctions
                ADD CONSTRAINT check_tax_deed_buyer_resolution_status
                CHECK (buyer_resolution_status IS NULL OR buyer_resolution_status IN ('verified', 'provisional', 'ambiguous'));
        END IF;
    END $$;
    """,
]


def main() -> None:
    settings = get_settings()
    engine = create_engine(settings.database_url, pool_pre_ping=True)

    with engine.begin() as conn:
        for i, stmt in enumerate(DDL, 1):
            logger.info("DDL step %d/%d", i, len(DDL))
            conn.execute(text(stmt))

    logger.info("buyer_entities profiling-columns migration complete.")


if __name__ == "__main__":
    main()
