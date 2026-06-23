"""Apply fa085 — M10 Lead Delivery + Free→Paid Attribution (B2).

Creates deliveries, free_to_paid_attribution, and adds customer_accounts.lead_credits
on the shared Postgres. DDL is idempotent (IF NOT EXISTS), safe to re-run. Run
directly because the alembic CLI is unusable on this repo's multi-head tree:

    python -m scripts.apply_fa085_m10_lead_delivery
"""

from __future__ import annotations

import logging

from sqlalchemy import text

from src.core.database import get_db_context

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
logger = logging.getLogger("apply_fa085")

_DDL = [
    """
    ALTER TABLE customer_accounts
        ADD COLUMN IF NOT EXISTS lead_credits JSONB NOT NULL DEFAULT '{}'::jsonb
    """,
    """
    CREATE TABLE IF NOT EXISTS deliveries (
        id                 BIGSERIAL PRIMARY KEY,
        property_id        INTEGER NOT NULL REFERENCES properties(id),
        account_id         UUID NOT NULL REFERENCES customer_accounts(account_id),
        grade              TEXT NOT NULL,
        vertical           TEXT NOT NULL,
        status             TEXT NOT NULL DEFAULT 'delivered',
        rejection_reason   TEXT,
        rejected_at        TIMESTAMPTZ,
        billing_period_end TIMESTAMPTZ,
        delivered_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
        source             TEXT NOT NULL DEFAULT 'sweep',
        created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
        CONSTRAINT uq_delivery_property_account UNIQUE (property_id, account_id),
        CONSTRAINT ck_delivery_status CHECK (status IN ('delivered','rejected')),
        CONSTRAINT ck_delivery_reason CHECK (
            rejection_reason IS NULL OR
            rejection_reason IN ('disconnected','wrong_party','deceased','duplicate','other'))
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_deliveries_account_grade_cycle ON deliveries(account_id, grade, billing_period_end)",
    "CREATE INDEX IF NOT EXISTS idx_deliveries_property            ON deliveries(property_id)",
    "CREATE INDEX IF NOT EXISTS idx_deliveries_status              ON deliveries(status)",
    "CREATE INDEX IF NOT EXISTS idx_deliveries_delivered_at        ON deliveries(delivered_at)",
    """
    CREATE TABLE IF NOT EXISTS free_to_paid_attribution (
        id                     BIGSERIAL PRIMARY KEY,
        account_id             UUID NOT NULL REFERENCES customer_accounts(account_id),
        first_free_delivery_id BIGINT REFERENCES deliveries(id),
        last_free_delivery_id  BIGINT REFERENCES deliveries(id),
        free_leads_count       INTEGER NOT NULL DEFAULT 0,
        converted_at           TIMESTAMPTZ NOT NULL,
        first_paid_plan        TEXT,
        created_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
        CONSTRAINT uq_attribution_account UNIQUE (account_id)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_attribution_account ON free_to_paid_attribution(account_id)",
]


def main() -> int:
    with get_db_context() as db:
        for stmt in _DDL:
            db.execute(text(stmt))
        db.commit()
    logger.info("fa085 applied: deliveries, free_to_paid_attribution, customer_accounts.lead_credits")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
