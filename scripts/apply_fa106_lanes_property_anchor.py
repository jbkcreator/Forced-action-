"""Auto-converted from alembic migration `fa106_lanes_property_anchor` (revision fa106_lanes_property_anchor).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa106_lanes_property_anchor.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
DROP INDEX IF EXISTS idx_lanes_prospect_id;

DROP INDEX IF EXISTS uq_lanes_property_lane_type;

ALTER TABLE lanes DROP COLUMN prospect_id;

ALTER TABLE lanes ALTER COLUMN property_id SET NOT NULL;

CREATE UNIQUE INDEX uq_lanes_property_lane_type ON lanes (property_id, lane_type);
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa106_lanes_property_anchor")


if __name__ == "__main__":
    main()
