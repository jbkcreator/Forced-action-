"""Auto-converted from alembic migration `fa088_drop_enrichment_provenance` (revision fa088_drop_enrichment_provenance).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa088_drop_enrichment_provenance.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
DROP INDEX idx_enrichment_provenance_prospect_id;

DROP TABLE enrichment_provenance;
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa088_drop_enrichment_provenance")


if __name__ == "__main__":
    main()
