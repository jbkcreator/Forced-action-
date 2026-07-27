"""Section 7.3 — add referral_prospects table.

The one-question referral ask inside onboarding: "who is one good
contractor you know in a county we haven't opened yet?" See ReferralProspect
docstring in src/core/models.py for why this is a separate table from
WaitlistEntry (no contact info collected for the referred person).

Idempotent. Usage:
    PYTHONPATH=. python migrations/apply_referral_prospects.py
"""

import logging

from sqlalchemy import text

from src.core.database import Database

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DDL = """
CREATE TABLE IF NOT EXISTS referral_prospects (
    id                      BIGSERIAL PRIMARY KEY,
    referring_subscriber_id INTEGER NOT NULL REFERENCES subscribers(id),
    prospect_name           VARCHAR(120) NOT NULL,
    prospect_company        VARCHAR(120),
    target_county_id        VARCHAR(50) NOT NULL,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""

INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_referral_prospects_referring_subscriber_id "
    "ON referral_prospects (referring_subscriber_id)",
    "CREATE INDEX IF NOT EXISTS idx_referral_prospects_target_county_id "
    "ON referral_prospects (target_county_id)",
]


def main() -> None:
    db = Database()
    with db.session_scope() as s:
        s.execute(text(DDL))
        for stmt in INDEXES:
            s.execute(text(stmt))
    logger.info("referral_prospects table applied.")


if __name__ == "__main__":
    main()
