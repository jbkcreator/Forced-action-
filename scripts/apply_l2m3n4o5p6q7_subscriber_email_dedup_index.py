"""Auto-converted from alembic migration `l2m3n4o5p6q7_subscriber_email_dedup_index` (revision l2m3n4o5p6q7).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_l2m3n4o5p6q7_subscriber_email_dedup_index.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
UPDATE subscribers SET email = lower(trim(email)) WHERE email IS NOT NULL;

CREATE TEMP TABLE _sub_survivors AS
        SELECT
            lower(email)  AS norm_email,
            vertical,
            county_id,
            min(id)       AS keep_id
        FROM subscribers
        WHERE status IN ('active', 'grace')
          AND email IS NOT NULL
        GROUP BY lower(email), vertical, county_id
        HAVING count(*) > 1;

CREATE TEMP TABLE _sub_cancelled AS
        SELECT s.id AS cancelled_id, sv.keep_id AS survivor_id
        FROM subscribers s
        JOIN _sub_survivors sv
          ON lower(s.email) = sv.norm_email
         AND s.vertical     = sv.vertical
         AND s.county_id    = sv.county_id
        WHERE s.status IN ('active', 'grace')
          AND s.id <> sv.keep_id;

UPDATE zip_territories zt
        SET subscriber_id = sc.survivor_id
        FROM _sub_cancelled sc
        WHERE zt.subscriber_id = sc.cancelled_id;

INSERT INTO sent_leads (subscriber_id, property_id, sent_at)
        SELECT sc.survivor_id, sl.property_id, sl.sent_at
        FROM sent_leads sl
        JOIN _sub_cancelled sc ON sl.subscriber_id = sc.cancelled_id
        ON CONFLICT DO NOTHING;

DELETE FROM sent_leads sl
        USING _sub_cancelled sc
        WHERE sl.subscriber_id = sc.cancelled_id;

UPDATE subscribers
        SET status = 'cancelled'
        WHERE id IN (SELECT cancelled_id FROM _sub_cancelled);

DROP TABLE _sub_survivors;

DROP TABLE _sub_cancelled;

CREATE UNIQUE INDEX uq_subscriber_email_vertical_active
        ON subscribers (lower(email), vertical, county_id)
        WHERE status IN ('active', 'grace');
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied l2m3n4o5p6q7_subscriber_email_dedup_index")


if __name__ == "__main__":
    main()
