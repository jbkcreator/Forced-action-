"""
Create referral_prompt_funnel — funnel-state tracking for the proactive
referral prompt (deal-win / lead-pack-delivery triggered SMS+email nudge to
share a referral link).

Tracks prompt-shown -> link-shared -> referral-confirmed as distinct,
timestamped states per triggering event. A UNIQUE(trigger_source_table,
trigger_source_id) index makes the trigger hooks in deal_outcome_effects.py
and lead_pack_fulfillment_sweep.py idempotent against retries (insert with
ON CONFLICT DO NOTHING). Independent of ReferralEvent, which is referee-side
only and doesn't exist until someone actually redeems a code.

Idempotent — CREATE TABLE IF NOT EXISTS / CREATE INDEX IF NOT EXISTS.

Usage:
    PYTHONPATH=. python migrations/apply_referral_prompt_funnel.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DDL = [
    """
    CREATE TABLE IF NOT EXISTS referral_prompt_funnel (
        id SERIAL PRIMARY KEY,
        subscriber_id INTEGER NOT NULL REFERENCES subscribers(id),
        trigger_type VARCHAR(30) NOT NULL
            CHECK (trigger_type IN ('deal_win', 'lead_pack_delivery')),
        trigger_source_table VARCHAR(30) NOT NULL,
        trigger_source_id INTEGER NOT NULL,
        referral_code VARCHAR(20) NOT NULL,
        state VARCHAR(20) NOT NULL DEFAULT 'shown'
            CHECK (state IN ('shown', 'shared', 'confirmed', 'expired')),
        prompt_shown_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        sms_sent BOOLEAN NOT NULL DEFAULT false,
        email_sent BOOLEAN NOT NULL DEFAULT false,
        shared_at TIMESTAMPTZ,
        confirmed_at TIMESTAMPTZ,
        confirmed_referral_event_id INTEGER REFERENCES referral_events(id),
        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """,
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_rpf_source ON referral_prompt_funnel (trigger_source_table, trigger_source_id)",
    "CREATE INDEX IF NOT EXISTS idx_rpf_subscriber_shown ON referral_prompt_funnel (subscriber_id, prompt_shown_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_rpf_referral_code ON referral_prompt_funnel (referral_code)",
    "CREATE INDEX IF NOT EXISTS idx_rpf_state ON referral_prompt_funnel (state)",
]


def main() -> None:
    settings = get_settings()
    engine = create_engine(settings.database_url, pool_pre_ping=True)

    with engine.begin() as conn:
        for i, stmt in enumerate(DDL, 1):
            logger.info("DDL step %d/%d", i, len(DDL))
            conn.execute(text(stmt))

    logger.info("referral_prompt_funnel table ready.")


if __name__ == "__main__":
    main()
