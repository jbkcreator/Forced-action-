"""Auto-converted from alembic migration `t4u5v6w7x8y9_add_sent_leads_source` (revision fa001_sentleads_source).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_t4u5v6w7x8y9_add_sent_leads_source.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
ALTER TABLE sent_leads ADD COLUMN source VARCHAR(40);

CREATE INDEX idx_sent_leads_source ON sent_leads (source);
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied t4u5v6w7x8y9_add_sent_leads_source")


if __name__ == "__main__":
    main()
