"""
Add referral_events.prompt_funnel_id — links a confirmed referral back to the
exact referral_prompt_funnel row that drove it.

Set at signup time from the signed `pt` attribution token carried on a
proactive referral-prompt share link. mark_confirmed() in
referral_prompt_service.py uses it to advance the *originating* prompt to
'confirmed' instead of guessing the newest open row. Nullable — organic and
reactive signups leave it NULL.

Idempotent — ADD COLUMN IF NOT EXISTS / CREATE INDEX IF NOT EXISTS.

Usage:
    PYTHONPATH=. python migrations/apply_referral_event_prompt_funnel_id.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DDL = [
    "ALTER TABLE referral_events ADD COLUMN IF NOT EXISTS prompt_funnel_id INTEGER",
    "CREATE INDEX IF NOT EXISTS idx_referral_events_prompt_funnel "
    "ON referral_events (prompt_funnel_id) WHERE prompt_funnel_id IS NOT NULL",
]


def main() -> None:
    settings = get_settings()
    engine = create_engine(settings.database_url, pool_pre_ping=True)

    with engine.begin() as conn:
        for i, stmt in enumerate(DDL, 1):
            logger.info("DDL step %d/%d", i, len(DDL))
            conn.execute(text(stmt))

    logger.info("referral_events.prompt_funnel_id ready.")


if __name__ == "__main__":
    main()
