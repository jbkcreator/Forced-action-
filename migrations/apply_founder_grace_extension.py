"""Founder-tier zip_held win-back benefit — one-time grace extension.

Founders (tier == 'founder') don't get the standard 50%-off zip_held
win-back coupon (client decision, wayfinder map notion-pending-tasks,
tickets F1/F2/F3) — they get a one-time +14-day extension of their
territory's grace window instead. This column tracks whether that
one-time grant has already happened for a subscriber, so a retried
scheduler run or a second zip_held eligibility window never grants it
twice.

Idempotent. Usage:
    PYTHONPATH=. python migrations/apply_founder_grace_extension.py
"""

import logging

from sqlalchemy import text

from src.core.database import Database

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DDL = """
ALTER TABLE subscribers
ADD COLUMN IF NOT EXISTS founder_grace_extension_granted_at TIMESTAMPTZ
"""


def main() -> None:
    db = Database()
    with db.session_scope() as s:
        s.execute(text(DDL))
    logger.info("founder_grace_extension_granted_at column applied.")


if __name__ == "__main__":
    main()
