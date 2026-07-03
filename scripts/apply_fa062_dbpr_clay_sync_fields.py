"""Auto-converted from alembic migration `fa062_dbpr_clay_sync_fields` (revision fa062).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa062_dbpr_clay_sync_fields.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
ALTER TABLE dbpr_contacts ADD COLUMN clay_synced BOOLEAN DEFAULT 'false' NOT NULL;

ALTER TABLE dbpr_contacts ADD COLUMN clay_synced_at TIMESTAMP WITH TIME ZONE;

CREATE INDEX idx_dbpr_clay_synced ON dbpr_contacts (clay_synced);
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa062_dbpr_clay_sync_fields")


if __name__ == "__main__":
    main()
