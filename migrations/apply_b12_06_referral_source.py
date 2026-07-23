"""Apply T-B12-06 — referral_source attribution marker on referral_events.

Tags where a referral ask originated so Tier-3 investor-to-investor referrals
are reportable separately from generic referral-link signups. The reward ladder
is unchanged — this column is attribution only. See .wayfinder/tickets/T-B12-06.md.

Idempotent — ADD COLUMN IF NOT EXISTS + backfill NULLs to 'generic'.

Usage:
    PYTHONPATH=. python migrations/apply_b12_06_referral_source.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DDL = [
    "ALTER TABLE referral_events ADD COLUMN IF NOT EXISTS referral_source VARCHAR(30);",
    "UPDATE referral_events SET referral_source = 'generic' WHERE referral_source IS NULL;",
    "ALTER TABLE referral_events ALTER COLUMN referral_source SET DEFAULT 'generic';",
    "ALTER TABLE referral_events ALTER COLUMN referral_source SET NOT NULL;",
    "CREATE INDEX IF NOT EXISTS ix_referral_events_referral_source "
    "ON referral_events (referral_source);",
]


def main() -> None:
    settings = get_settings()
    engine = create_engine(settings.database_url, pool_pre_ping=True)

    with engine.begin() as conn:
        for i, stmt in enumerate(DDL, 1):
            logger.info("DDL step %d/%d", i, len(DDL))
            conn.execute(text(stmt))

    logger.info("b12_06_referral_source complete.")


if __name__ == "__main__":
    main()
