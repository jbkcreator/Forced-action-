"""Auto-converted from alembic migration `fa026_drop_enriched_contacts_ghl_cols` (revision fa026_drop_ec_ghl_cols).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa026_drop_enriched_contacts_ghl_cols.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
DROP INDEX IF EXISTS idx_enriched_contacts_ghl_contact_id;

ALTER TABLE enriched_contacts DROP COLUMN ghl_contact_id;

ALTER TABLE enriched_contacts DROP COLUMN ghl_synced_at;
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa026_drop_enriched_contacts_ghl_cols")


if __name__ == "__main__":
    main()
