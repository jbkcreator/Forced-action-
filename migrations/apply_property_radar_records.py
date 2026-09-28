"""Create property_radar_records staging table.

PropertyRadar records live in their own table (not `properties`) because APN
is only unique per county, not globally — two counties in different states can
share the same parcel number. See src/core/models.py:PropertyRadarRecord for
the full design note.

Idempotent — IF NOT EXISTS / IF NOT EXISTS guards throughout.

Usage:
    PYTHONPATH=. python migrations/apply_property_radar_records.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DDL = [
    """
    CREATE TABLE IF NOT EXISTS property_radar_records (
        id          BIGSERIAL PRIMARY KEY,
        radar_id    VARCHAR(50)  NOT NULL,
        state_fips  VARCHAR(5)   NOT NULL,
        county_fips VARCHAR(5)   NOT NULL,
        apn         VARCHAR(100) NOT NULL,

        state        VARCHAR(2),
        county_name  VARCHAR(100),

        property_address VARCHAR(255),
        city             VARCHAR(100),
        zip              VARCHAR(10),
        property_type    VARCHAR(20),

        owner_name      VARCHAR(255),
        ownership_type  VARCHAR(50),
        mailing_address VARCHAR(255),
        mailing_city    VARCHAR(100),
        mailing_state   VARCHAR(2),
        mailing_zip     VARCHAR(10),
        principal_name  VARCHAR(255),

        lender_name       VARCHAR(255),
        loan_amount       BIGINT,
        loan_recorded_date VARCHAR(20),
        loan_term_years    VARCHAR(20),
        est_maturity_date  VARCHAR(20),
        loan_doc_number    VARCHAR(100),

        campaign VARCHAR(100),

        property_id       BIGINT,
        match_method      VARCHAR(50),
        match_confidence  INTEGER,

        status       VARCHAR(20) NOT NULL DEFAULT 'active',
        change_flags TEXT[],
        changed_at   TIMESTAMPTZ,
        prior        JSONB,

        raw           JSONB,
        first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        last_seen_at  TIMESTAMPTZ NOT NULL DEFAULT now(),

        CONSTRAINT uq_pr_state_county_apn UNIQUE (state_fips, county_fips, apn),
        CONSTRAINT uq_pr_radar_id         UNIQUE (radar_id),
        CONSTRAINT ck_pr_status           CHECK  (status IN ('active','sold','refinanced'))
    );
    """,
    "CREATE INDEX IF NOT EXISTS ix_pr_campaign    ON property_radar_records (campaign);",
    "CREATE INDEX IF NOT EXISTS ix_pr_county_fips ON property_radar_records (county_fips);",
    "CREATE INDEX IF NOT EXISTS ix_pr_property_id ON property_radar_records (property_id);",
]


def run(conn) -> None:
    for i, stmt in enumerate(DDL, 1):
        logger.info("DDL step %d/%d", i, len(DDL))
        conn.execute(text(stmt))
    logger.info("apply_property_radar_records complete.")


if __name__ == "__main__":
    engine = create_engine(str(get_settings().database_url))
    with engine.begin() as conn:
        run(conn)
