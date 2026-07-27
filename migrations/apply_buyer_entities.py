"""Apply the buyer_entities + buyer_entity_links tables (HUNTER-01 — Buyer Entity Model & Resolution).

Collapses `owners` rows (one per property) and `deeds.grantee` mentions,
cross-referenced with Sunbiz LLC-piercing data already on `owners`
(managing_members, registered_agent_name, principal_address), into one
canonical buyer identity per real person/LLC. Populated by
src/services/buyer_entity_resolution.py — see
docs/plans/agent_lane_phase1_week1_dev_split.md for the full design.

confidence_score / match_confidence use a 0-100 integer scale, matching
Hunter's constitution wording ("confidence-scored 0-100", "<70 = UNVERIFIED")
— a deliberate divergence from Deed.match_confidence's existing
Numeric(4,3) 0.000-1.000 scale.

Idempotent — IF NOT EXISTS / constraint-guarded throughout.

Usage:
    PYTHONPATH=. python migrations/apply_buyer_entities.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DDL = [
    """
    CREATE TABLE IF NOT EXISTS buyer_entities (
        id                          BIGSERIAL PRIMARY KEY,
        canonical_name              TEXT NOT NULL,
        entity_type                 VARCHAR(20) NOT NULL,
        primary_mailing_address     VARCHAR(255),
        confidence_score            INTEGER NOT NULL,
        verification_status        VARCHAR(20) NOT NULL DEFAULT 'unverified',
        total_purchase_count        INTEGER NOT NULL DEFAULT 0,
        total_cash_volume           NUMERIC(14, 2) NOT NULL DEFAULT 0,
        county_id                   VARCHAR(50),
        first_seen_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
        last_updated_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
        CONSTRAINT check_buyer_entity_type
            CHECK (entity_type IN ('Individual', 'LLC', 'Trust', 'Corporate')),
        CONSTRAINT check_buyer_entity_verification_status
            CHECK (verification_status IN ('verified', 'unverified')),
        CONSTRAINT check_buyer_entity_confidence_range
            CHECK (confidence_score >= 0 AND confidence_score <= 100)
    );
    """,
    "CREATE INDEX IF NOT EXISTS idx_buyer_entities_confidence ON buyer_entities (confidence_score);",
    "CREATE INDEX IF NOT EXISTS idx_buyer_entities_county ON buyer_entities (county_id);",
    """
    CREATE TABLE IF NOT EXISTS buyer_entity_links (
        id                  BIGSERIAL PRIMARY KEY,
        buyer_entity_id     BIGINT NOT NULL REFERENCES buyer_entities(id) ON DELETE CASCADE,
        source_table        VARCHAR(30) NOT NULL,
        source_id           INTEGER NOT NULL,
        match_confidence    INTEGER NOT NULL,
        match_method        VARCHAR(30) NOT NULL,
        linked_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
        CONSTRAINT uq_buyer_entity_link_source UNIQUE (source_table, source_id),
        CONSTRAINT check_buyer_entity_link_source_table
            CHECK (source_table IN ('owners', 'deeds', 'sunbiz_snapshots')),
        CONSTRAINT check_buyer_entity_link_match_method
            CHECK (match_method IN ('sunbiz_llc_piercing', 'exact_name_address', 'fuzzy_name', 'llm_adjudicated', 'manual')),
        CONSTRAINT check_buyer_entity_link_confidence_range
            CHECK (match_confidence >= 0 AND match_confidence <= 100)
    );
    """,
    "CREATE INDEX IF NOT EXISTS idx_buyer_entity_links_entity ON buyer_entity_links (buyer_entity_id);",
    "CREATE INDEX IF NOT EXISTS idx_buyer_entity_links_source ON buyer_entity_links (source_table, source_id);",
]


def main() -> None:
    settings = get_settings()
    engine = create_engine(settings.database_url, pool_pre_ping=True)

    with engine.begin() as conn:
        for i, stmt in enumerate(DDL, 1):
            logger.info("DDL step %d/%d", i, len(DDL))
            conn.execute(text(stmt))

    logger.info("buyer_entities migration complete.")


if __name__ == "__main__":
    main()
