"""Apply fa097 - subscriber memory summary table.

Creates the `subscriber_memory_summary` read-model table.

Idempotent — uses IF NOT EXISTS guards on CREATE TABLE and indexes.

Usage:
    PYTHONPATH=. python scripts/apply_fa097_subscriber_memory_summary.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DDL = [
    """
    CREATE TABLE IF NOT EXISTS subscriber_memory_summary (
        subscriber_id                 INTEGER PRIMARY KEY REFERENCES subscribers(id),
        last_event_at                 TIMESTAMP WITH TIME ZONE,
        last_event_type               VARCHAR(100),
        last_stripe_event_at          TIMESTAMP WITH TIME ZONE,
        last_stripe_event_type        VARCHAR(100),
        latest_payment_state          VARCHAR(100),
        latest_checkout_state         VARCHAR(100),
        latest_crm_status             VARCHAR(100),
        latest_crm_stage              VARCHAR(100),
        last_sms_event_at             TIMESTAMP WITH TIME ZONE,
        last_sms_event_type           VARCHAR(100),
        latest_sms_state              VARCHAR(100),
        last_sms_reply_at             TIMESTAMP WITH TIME ZONE,
        sms_opted_out                 BOOLEAN NOT NULL DEFAULT FALSE,
        last_voice_event_at           TIMESTAMP WITH TIME ZONE,
        last_voice_event_type         VARCHAR(100),
        last_underwriting_event_at    TIMESTAMP WITH TIME ZONE,
        last_underwriting_event_type  VARCHAR(100),
        latest_underwriting_state     VARCHAR(100),
        latest_underwriting_milestone VARCHAR(100),
        last_lead_id                  INTEGER REFERENCES properties(id),
        updated_at                    TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_sms_last_event
        ON subscriber_memory_summary (last_sms_event_at);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_usm_summary_last_event
        ON subscriber_memory_summary (last_event_at);
    """,
]


def main() -> None:
    settings = get_settings()
    engine = create_engine(settings.database_url, pool_pre_ping=True)

    with engine.begin() as conn:
        for i, stmt in enumerate(DDL, 1):
            logger.info("Step %d/%d — executing", i, len(DDL))
            conn.execute(text(stmt.strip()))

    logger.info("fa097 complete — subscriber_memory_summary table applied.")


if __name__ == "__main__":
    main()
