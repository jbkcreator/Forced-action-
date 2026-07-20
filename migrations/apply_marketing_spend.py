"""Apply the marketing_spend table (Block 4 — Attribution / CAC-payback).

Manually-entered ad spend for channels with no stored cost (Meta, Google,
email/Instantly — a flat monthly subscription with no per-campaign cost via
its API). Quora (`quora_topics.cumulative_spend`) and affiliate commissions
(`affiliate_payout_ledger`) already track real cost and are read directly by
the CAC/payback compiler, not entered here.

`channel` must use the same vocabulary as the compiler's channel key
(COALESCE(subscribers.utm_source, subscribers.signup_source)) — enforced by
the admin route's allow-list, not by a DB CHECK (channel values evolve with
marketing sources, unlike the fixed signup_source allow-list).

Idempotent — IF NOT EXISTS guard.

Usage:
    PYTHONPATH=. python migrations/apply_marketing_spend.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DDL = [
    """
    CREATE TABLE IF NOT EXISTS marketing_spend (
        id              BIGSERIAL PRIMARY KEY,
        channel         TEXT NOT NULL,
        campaign_key    TEXT,
        period_start    DATE NOT NULL,
        period_end      DATE NOT NULL,
        amount_cents    INTEGER NOT NULL CHECK (amount_cents >= 0),
        currency        TEXT NOT NULL DEFAULT 'usd',
        notes           TEXT,
        created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
        CONSTRAINT ck_marketing_spend_period_order CHECK (period_end >= period_start),
        CONSTRAINT uq_marketing_spend_period UNIQUE (channel, campaign_key, period_start, period_end)
    );
    """,
    "CREATE INDEX IF NOT EXISTS idx_marketing_spend_channel ON marketing_spend (channel);",
    "CREATE INDEX IF NOT EXISTS idx_marketing_spend_period ON marketing_spend (period_start, period_end);",
]


def main() -> None:
    settings = get_settings()
    engine = create_engine(settings.database_url, pool_pre_ping=True)

    with engine.begin() as conn:
        for i, stmt in enumerate(DDL, 1):
            logger.info("DDL step %d/%d", i, len(DDL))
            conn.execute(text(stmt))

    logger.info("marketing_spend migration complete.")


if __name__ == "__main__":
    main()
