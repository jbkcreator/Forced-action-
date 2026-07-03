"""Auto-converted from alembic migration `fa031_enriched_contact_traced_name` (revision fa031_enriched_contact_traced_name).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa031_enriched_contact_traced_name.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
ALTER TABLE enriched_contacts ADD COLUMN traced_name VARCHAR(255);

CREATE INDEX idx_enriched_contacts_property_traced_name ON enriched_contacts (property_id, traced_name) WHERE traced_name IS NOT NULL;
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa031_enriched_contact_traced_name")


if __name__ == "__main__":
    main()
