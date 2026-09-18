"""
WP-T2-9 Partner Mining — add ranking snapshot columns to fa_max_partners.

Idempotent (ADR 0024). Adds columns the WP-1 scaffold omitted:
  observed_transaction_count  — investor deeds the partner appeared on
  first_observed_at           — earliest qualifying deed record_date
  last_observed_at            — most recent qualifying deed record_date
  county_id                   — county the partner was sourced from

Also adds buyer_entity_id FK so a partner row links back to the shared
identity graph (WP-T2-8 / buyer_entities).

Run once:
  PYTHONPATH=. python migrations/apply_partner_ranking_snapshot.py
"""

import logging
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import create_engine, text

from config.settings import get_settings

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

_MIGRATIONS = [
    # Ranking snapshot columns
    """
    ALTER TABLE fa_max_partners
        ADD COLUMN IF NOT EXISTS observed_transaction_count INTEGER NOT NULL DEFAULT 0
    """,
    """
    ALTER TABLE fa_max_partners
        ADD COLUMN IF NOT EXISTS first_observed_at DATE
    """,
    """
    ALTER TABLE fa_max_partners
        ADD COLUMN IF NOT EXISTS last_observed_at DATE
    """,
    """
    ALTER TABLE fa_max_partners
        ADD COLUMN IF NOT EXISTS county_id VARCHAR(50)
    """,
    # Link to shared buyer_entities identity graph
    """
    ALTER TABLE fa_max_partners
        ADD COLUMN IF NOT EXISTS buyer_entity_id INTEGER
            REFERENCES buyer_entities(id) ON DELETE SET NULL
    """,
    # Indexes for ranking queries
    """
    CREATE INDEX IF NOT EXISTS ix_fa_max_partner_class_count
        ON fa_max_partners (partner_class, observed_transaction_count DESC)
    """,
    """
    CREATE INDEX IF NOT EXISTS ix_fa_max_partner_buyer_entity
        ON fa_max_partners (buyer_entity_id)
    """,
    """
    CREATE INDEX IF NOT EXISTS ix_fa_max_partner_county
        ON fa_max_partners (county_id)
    """,
]


def run() -> None:
    settings = get_settings()
    engine = create_engine(settings.DATABASE_URL)
    with engine.begin() as conn:
        for sql in _MIGRATIONS:
            conn.execute(text(sql.strip()))
            logger.info("OK: %s", sql.strip().splitlines()[0][:80])
    logger.info("apply_partner_ranking_snapshot: done")


if __name__ == "__main__":
    run()
