"""Apply non-buyer nurture sequence schema.

Creates non_buyer_nurture_sequences — the per-email state/suppression list
for the multi-touch nurture sequence covering free-signup, checkout-abandon,
and waitlist non-purchasers.

Idempotent -- IF NOT EXISTS guards throughout.

Usage:
    PYTHONPATH=. python migrations/apply_non_buyer_nurture.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DDL = [
    """
    CREATE TABLE IF NOT EXISTS non_buyer_nurture_sequences (
        id                     BIGSERIAL PRIMARY KEY,
        email                  VARCHAR(255) NOT NULL,
        subscriber_id          INTEGER REFERENCES subscribers(id) ON DELETE SET NULL,
        source                 VARCHAR(20) NOT NULL,
        captured_at            TIMESTAMPTZ NOT NULL,
        instantly_campaign_id  VARCHAR(100),
        instantly_lead_id      VARCHAR(100),
        status                 VARCHAR(20) NOT NULL DEFAULT 'eligible',
        eligible_at            TIMESTAMPTZ,
        enrolled_at            TIMESTAMPTZ,
        removed_at             TIMESTAMPTZ,
        removal_reason         VARCHAR(40),
        converted_at           TIMESTAMPTZ,
        created_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT ck_non_buyer_nurture_status
            CHECK (status IN ('eligible','enrolled','converted','unsubscribed','bounced','removed')),
        CONSTRAINT ck_non_buyer_nurture_removal_reason
            CHECK (removal_reason IS NULL OR removal_reason IN
                   ('paid_conversion','unsubscribe','bounce','manual','campaign_removed')),
        CONSTRAINT ck_non_buyer_nurture_source
            CHECK (source IN ('free_signup','checkout_abandon','waitlist'))
    );
    """,
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_non_buyer_nurture_email ON non_buyer_nurture_sequences (email);",
    "CREATE INDEX IF NOT EXISTS ix_non_buyer_nurture_subscriber_id ON non_buyer_nurture_sequences (subscriber_id);",
    "CREATE INDEX IF NOT EXISTS ix_non_buyer_nurture_status ON non_buyer_nurture_sequences (status);",
    "CREATE INDEX IF NOT EXISTS ix_non_buyer_nurture_eligible_at ON non_buyer_nurture_sequences (eligible_at);",
]


def main() -> None:
    settings = get_settings()
    engine = create_engine(settings.database_url, pool_pre_ping=True)

    with engine.begin() as conn:
        for i, stmt in enumerate(DDL, 1):
            logger.info("DDL step %d/%d", i, len(DDL))
            conn.execute(text(stmt))

    logger.info("non_buyer_nurture schema applied.")


if __name__ == "__main__":
    main()
