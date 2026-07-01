"""Apply fa_s1_commission_ledger - Commission Ledger (WS-C).

Creates commission_splits + commission_ledger and seeds the default
`platform_50_broker_50` split. Idempotent DDL for the live Postgres database
(CREATE TABLE IF NOT EXISTS / ON CONFLICT DO NOTHING). Safe to re-run.

The default split percentages are a PLACEHOLDER pending business sign-off; splits
are config-as-data, editable without a deploy.

Usage:
    PYTHONPATH=. python scripts/apply_fa_s1_commission_ledger.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DDL = [
    """
    CREATE TABLE IF NOT EXISTS commission_splits (
        split_config_id  VARCHAR(100) PRIMARY KEY,
        name             VARCHAR(255) NOT NULL,
        parties          JSONB NOT NULL,
        is_active        BOOLEAN NOT NULL DEFAULT TRUE
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS commission_ledger (
        entry_id                UUID PRIMARY KEY DEFAULT generate_uuidv7(),
        lane_id                 UUID NOT NULL REFERENCES lanes(lane_id),
        broker_id               UUID NOT NULL REFERENCES brokers(broker_id),
        trigger_transition_id   UUID UNIQUE REFERENCES broker_transitions(transition_id),
        gross_amount_cents      BIGINT NOT NULL,
        split_config_id         VARCHAR(100) NOT NULL REFERENCES commission_splits(split_config_id),
        net_lines               JSONB NOT NULL,
        status                  VARCHAR(20) NOT NULL DEFAULT 'posted',
        posted_at               TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
        CONSTRAINT ck_cl_gross_nonneg CHECK (gross_amount_cents >= 0),
        CONSTRAINT ck_cl_status CHECK (status IN ('posted','disputed','reconciled'))
    );
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_cl_lane_id ON commission_ledger (lane_id);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_cl_broker_id ON commission_ledger (broker_id);
    """,
    # Default split — PLACEHOLDER (50/50), pending business sign-off.
    """
    INSERT INTO commission_splits (split_config_id, name, parties, is_active)
    VALUES (
        'platform_50_broker_50',
        'Platform 50 / Broker 50',
        '[{"party": "platform", "pct": 50}, {"party": "broker", "pct": 50}]'::jsonb,
        TRUE
    )
    ON CONFLICT (split_config_id) DO NOTHING;
    """,
]


def main() -> None:
    settings = get_settings()
    engine = create_engine(settings.database_url, pool_pre_ping=True)

    with engine.begin() as conn:
        for i, stmt in enumerate(DDL, 1):
            logger.info("Step %d/%d - executing", i, len(DDL))
            conn.execute(text(stmt.strip()))

    logger.info("fa_s1_commission_ledger complete - commission tables + default split applied.")


if __name__ == "__main__":
    main()
