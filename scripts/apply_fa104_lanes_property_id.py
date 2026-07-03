"""Auto-converted from alembic migration `fa104_lanes_property_id` (revision fa104_lanes_property_id).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa104_lanes_property_id.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
ALTER TABLE lanes ALTER COLUMN prospect_id DROP NOT NULL;

ALTER TABLE lanes ADD COLUMN property_id INTEGER;

ALTER TABLE lanes ADD CONSTRAINT fk_lanes_property_id FOREIGN KEY(property_id) REFERENCES properties (id);

CREATE INDEX ix_lanes_property_id ON lanes (property_id) WHERE property_id IS NOT NULL;

CREATE UNIQUE INDEX uq_lanes_property_lane_type ON lanes (property_id, lane_type) WHERE property_id IS NOT NULL;
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa104_lanes_property_id")


if __name__ == "__main__":
    main()
