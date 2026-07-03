"""Auto-converted from alembic migration `fa031b_skip_trace_waterfall` (revision fa031_skip_trace_waterfall).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa031b_skip_trace_waterfall.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
ALTER TABLE enriched_contacts ADD COLUMN confidence NUMERIC(4, 3);

ALTER TABLE enriched_contacts DROP CONSTRAINT IF EXISTS check_enriched_source;

ALTER TABLE enriched_contacts ADD CONSTRAINT check_enriched_source CHECK (source IN ('batch_skip_tracing', 'idi', 'pdl'));
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa031b_skip_trace_waterfall")


if __name__ == "__main__":
    main()
