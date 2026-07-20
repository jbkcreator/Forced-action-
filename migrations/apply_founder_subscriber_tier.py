"""Allow `founder` on the subscribers.tier CHECK constraint.

Founder checkout writes Subscriber.tier='founder'; the existing
check_subscriber_tier constraint didn't permit it, so the insert would fail.
Partial reversal of ADR 0033 (see the ADR's amendment) — recorded in ADR 0034.

Idempotent: drops and recreates the named constraint with 'founder' added.

Usage:
    PYTHONPATH=. python migrations/apply_founder_subscriber_tier.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DDL = [
    "ALTER TABLE subscribers DROP CONSTRAINT IF EXISTS check_subscriber_tier;",
    "ALTER TABLE subscribers ADD CONSTRAINT check_subscriber_tier CHECK ("
    "tier IN ('free', 'starter', 'pro', 'dominator', 'data_only', "
    "'autopilot_lite', 'autopilot_pro', 'partner', 'annual_lock', 'founder'));",
]


def main() -> None:
    settings = get_settings()
    engine = create_engine(settings.database_url, pool_pre_ping=True)
    with engine.begin() as conn:
        for i, stmt in enumerate(DDL, 1):
            logger.info("DDL step %d/%d", i, len(DDL))
            conn.execute(text(stmt))
    logger.info("founder_subscriber_tier complete.")


if __name__ == "__main__":
    main()
