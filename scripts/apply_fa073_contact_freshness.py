"""Auto-converted from alembic migration `fa073_contact_freshness` (revision fa073_contact_freshness).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa073_contact_freshness.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
ALTER TABLE owners ADD COLUMN contact_info_confidence VARCHAR(20);

ALTER TABLE owners ADD COLUMN contact_info_confidence_score NUMERIC(4, 3);

ALTER TABLE owners ADD COLUMN contact_last_verified_at TIMESTAMP WITH TIME ZONE;

ALTER TABLE owners ADD COLUMN contact_next_refresh_at TIMESTAMP WITH TIME ZONE;

ALTER TABLE owners ADD COLUMN contact_refresh_status VARCHAR(20);

ALTER TABLE owners ADD COLUMN contact_refresh_reason VARCHAR(120);

CREATE INDEX idx_owner_contact_confidence ON owners (contact_info_confidence);

CREATE INDEX idx_owner_contact_next_refresh ON owners (contact_next_refresh_at);

ALTER TABLE owners ADD CONSTRAINT check_owner_contact_info_confidence CHECK (contact_info_confidence IS NULL OR contact_info_confidence IN ('high','medium','low','stale'));

ALTER TABLE owners ADD CONSTRAINT check_owner_contact_refresh_status CHECK (contact_refresh_status IS NULL OR contact_refresh_status IN ('fresh','due','queued','refreshed','failed'));

ALTER TABLE enriched_contacts ADD COLUMN verification_status VARCHAR(20);

ALTER TABLE enriched_contacts ADD COLUMN superseded_at TIMESTAMP WITH TIME ZONE;

ALTER TABLE enriched_contacts ADD COLUMN superseded_by_contact_id INTEGER;

ALTER TABLE enriched_contacts ADD CONSTRAINT fk_enriched_contacts_superseded_by FOREIGN KEY(superseded_by_contact_id) REFERENCES enriched_contacts (id);

CREATE INDEX idx_enriched_verification_status ON enriched_contacts (verification_status);

ALTER TABLE enriched_contacts ADD CONSTRAINT check_enriched_verification_status CHECK (verification_status IS NULL OR verification_status IN ('valid','invalid','unknown'));
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa073_contact_freshness")


if __name__ == "__main__":
    main()
