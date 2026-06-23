"""Apply fa084 — S1 Revenue Engine tables (B1 / M9).

Creates plans, customer_accounts, mrr_movements on the shared Postgres.
DDL is idempotent (IF NOT EXISTS), safe to re-run. Run directly because the
alembic CLI is unusable on this repo's multi-head tree:

    python -m scripts.apply_fa084_s1_revenue_engine
"""

from __future__ import annotations

import logging

from sqlalchemy import text

from src.core.database import get_db_context

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
logger = logging.getLogger("apply_fa084")

_DDL = [
    """
    CREATE TABLE IF NOT EXISTS plans (
        plan_id         TEXT PRIMARY KEY,
        name            TEXT NOT NULL,
        tier            TEXT NOT NULL,
        price_cents     INTEGER NOT NULL,
        interval        TEXT NOT NULL,
        entitlements    JSONB NOT NULL DEFAULT '{}'::jsonb,
        stripe_price_id TEXT,
        is_active       BOOLEAN NOT NULL DEFAULT TRUE,
        created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
        CONSTRAINT ck_plans_interval CHECK (interval IN ('monthly','annual','one_time','trial'))
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS customer_accounts (
        account_id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        subscriber_id          INTEGER REFERENCES subscribers(id),
        company_name           TEXT,
        contacts               JSONB NOT NULL DEFAULT '[]'::jsonb,
        status                 TEXT NOT NULL DEFAULT 'free_trial',
        plan_tier              TEXT REFERENCES plans(plan_id),
        service_area           JSONB NOT NULL DEFAULT '{}'::jsonb,
        trades                 TEXT[] NOT NULL DEFAULT '{}'::text[],
        lead_entitlement       JSONB NOT NULL DEFAULT '{}'::jsonb,
        acquisition_source     TEXT,
        stripe_customer_id     TEXT UNIQUE,
        stripe_subscription_id TEXT,
        current_period_end     TIMESTAMPTZ,
        mrr_cents              INTEGER NOT NULL DEFAULT 0,
        created_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
        converted_at           TIMESTAMPTZ,
        updated_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
        CONSTRAINT ck_ca_status CHECK (status IN ('prospect','free_trial','active','past_due','churned'))
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_ca_status        ON customer_accounts(status)",
    "CREATE INDEX IF NOT EXISTS idx_ca_subscriber_id ON customer_accounts(subscriber_id)",
    "CREATE INDEX IF NOT EXISTS idx_ca_trades_gin    ON customer_accounts USING GIN (trades)",
    """
    CREATE TABLE IF NOT EXISTS mrr_movements (
        id               BIGSERIAL PRIMARY KEY,
        account_id       UUID NOT NULL REFERENCES customer_accounts(account_id),
        movement_type    TEXT NOT NULL,
        delta_cents      INTEGER NOT NULL,
        mrr_after_cents  INTEGER NOT NULL,
        is_involuntary   BOOLEAN NOT NULL DEFAULT FALSE,
        effective_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
        stripe_event_id  TEXT UNIQUE,
        created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
        CONSTRAINT ck_mrr_type CHECK (movement_type IN ('new','expansion','contraction','churn'))
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_mrr_account   ON mrr_movements(account_id)",
    "CREATE INDEX IF NOT EXISTS idx_mrr_effective ON mrr_movements(effective_at)",
    "CREATE INDEX IF NOT EXISTS idx_mrr_type      ON mrr_movements(movement_type)",
]


def main() -> int:
    with get_db_context() as db:
        for stmt in _DDL:
            db.execute(text(stmt))
        db.commit()
    logger.info("fa084 applied: plans, customer_accounts, mrr_movements")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
