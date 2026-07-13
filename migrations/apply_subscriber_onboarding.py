"""Apply the onboarding preference-step columns on subscribers.

onboarding_completed defaults TRUE so existing rows are never retroactively
gated; src/services/signup_engine.py sets it False explicitly on new email
and inbound-caller signups so first login shows the one-screen preference
step (property type + investment budget) before the dashboard.

Idempotent — IF NOT EXISTS guard.

Usage:
    PYTHONPATH=. python migrations/apply_subscriber_onboarding.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DDL = [
    "ALTER TABLE subscribers ADD COLUMN IF NOT EXISTS onboarding_completed "
    "BOOLEAN NOT NULL DEFAULT true;",
    "ALTER TABLE subscribers ADD COLUMN IF NOT EXISTS preferred_property_type VARCHAR(50);",
    "ALTER TABLE subscribers ADD COLUMN IF NOT EXISTS investment_budget_band VARCHAR(30);",
]


def main() -> None:
    settings = get_settings()
    engine = create_engine(settings.database_url, pool_pre_ping=True)

    with engine.begin() as conn:
        for i, stmt in enumerate(DDL, 1):
            logger.info("DDL step %d/%d", i, len(DDL))
            conn.execute(text(stmt))

    logger.info("subscriber_onboarding migration complete.")


if __name__ == "__main__":
    main()
