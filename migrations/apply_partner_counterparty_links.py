"""
WP-T2-9 Partner Mining — extend BuyerEntityLink.source_table CHECK constraint.

Adds 'deed_lender' and 'deed_wholesaler' to the allowed source_table values.
Postgres doesn't support ALTER CONSTRAINT, so we DROP + recreate.

Idempotent (ADR 0024): the DROP is conditional and the new constraint uses
the same name, so re-running is safe.

Run once:
  PYTHONPATH=. python migrations/apply_partner_counterparty_links.py
"""

import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import create_engine, text
from config.settings import get_settings

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

_NEW_ALLOWED = (
    "'owners', 'deeds', 'sunbiz_snapshots', 'tax_deed_auctions', "
    "'building_permits', 'permit_staging', "
    "'deed_lender', 'deed_wholesaler'"
)

_MIGRATIONS = [
    # Drop the old constraint (Postgres requires recreate to change it).
    """
    ALTER TABLE buyer_entity_links
        DROP CONSTRAINT IF EXISTS check_buyer_entity_link_source_table
    """,
    # Recreate with the extended allow-list.
    f"""
    ALTER TABLE buyer_entity_links
        ADD CONSTRAINT check_buyer_entity_link_source_table
        CHECK (source_table IN ({_NEW_ALLOWED}))
    """,
]


def run() -> None:
    settings = get_settings()
    engine = create_engine(settings.DATABASE_URL)
    with engine.begin() as conn:
        for sql in _MIGRATIONS:
            conn.execute(text(sql.strip()))
            logger.info("OK: %s", sql.strip().splitlines()[0][:80])
    logger.info("apply_partner_counterparty_links: done")


if __name__ == "__main__":
    run()
