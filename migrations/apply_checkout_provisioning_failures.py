"""Apply the checkout_provisioning_failures table (PR #170 review fix).

Durable ops-recovery queue for a checkout where Stripe completed the charge
and subscription but ZIP-territory provisioning failed and was rolled back —
see stripe_webhooks._on_checkout_completed. Written via its own committed
session, independent of the request's main db session, so the record
survives that session's rollback. Ops must act on it (cancel/refund/
re-provision) via a monitored admin surface; this table records the fact,
it does not automate the recovery action.

Idempotent — IF NOT EXISTS guard.

Usage:
    PYTHONPATH=. python migrations/apply_checkout_provisioning_failures.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DDL = [
    """
    CREATE TABLE IF NOT EXISTS checkout_provisioning_failures (
        id                      BIGSERIAL PRIMARY KEY,
        stripe_customer_id      TEXT,
        stripe_subscription_id  TEXT,
        email                   TEXT,
        tier                    TEXT,
        vertical                TEXT,
        county_id               TEXT,
        requested_zips          JSONB,
        unclaimed_zips          JSONB,
        reason                  TEXT NOT NULL DEFAULT 'zip_territory_unavailable',
        status                  TEXT NOT NULL DEFAULT 'open',
        created_at              TIMESTAMP NOT NULL DEFAULT now(),
        resolved_at             TIMESTAMP,
        resolved_by             TEXT,
        notes                   TEXT,
        CONSTRAINT check_checkout_provisioning_status CHECK (status IN ('open', 'resolved'))
    );
    """,
    "CREATE INDEX IF NOT EXISTS idx_checkout_provisioning_status ON checkout_provisioning_failures (status);",
    "CREATE INDEX IF NOT EXISTS idx_checkout_provisioning_customer ON checkout_provisioning_failures (stripe_customer_id);",
]


def main() -> None:
    settings = get_settings()
    engine = create_engine(settings.database_url, pool_pre_ping=True)

    with engine.begin() as conn:
        for i, stmt in enumerate(DDL, 1):
            logger.info("DDL step %d/%d", i, len(DDL))
            conn.execute(text(stmt))

    logger.info("checkout_provisioning_failures migration complete.")


if __name__ == "__main__":
    main()
