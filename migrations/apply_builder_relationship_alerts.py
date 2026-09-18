"""WP-T2-8 Stage E — durable RELATIONSHIPS alert deduplication."""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DDL = """
CREATE TABLE IF NOT EXISTS builder_relationship_alerts (
    buyer_entity_id    BIGINT NOT NULL REFERENCES buyer_entities(id) ON DELETE CASCADE,
    pattern            VARCHAR(32) NOT NULL,
    latest_permit_date DATE NOT NULL,
    surfaced_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (buyer_entity_id, pattern, latest_permit_date)
);
"""


def main() -> None:
    engine = create_engine(get_settings().database_url, pool_pre_ping=True)
    with engine.begin() as conn:
        conn.execute(text(DDL))
    logger.info("apply_builder_relationship_alerts complete.")


if __name__ == "__main__":
    main()
