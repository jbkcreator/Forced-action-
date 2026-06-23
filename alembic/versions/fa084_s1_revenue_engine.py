"""fa084 — S1 Revenue Engine (B1 / M9): plans, customer_accounts, mrr_movements.

NOTE: this repo's alembic tree is multi-head and the CLI is unusable; the DDL is
actually applied via scripts/apply_fa084_s1_revenue_engine.py. This file is kept
for the migration record and mirrors that DDL exactly.
"""

from typing import Sequence, Union

from alembic import op

revision: str = "fa084_s1_revenue_engine"
down_revision: Union[str, Sequence[str]] = "fa083_quora_topics"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
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
    """)
    op.execute("""
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
    """)
    op.execute("CREATE INDEX IF NOT EXISTS idx_ca_status        ON customer_accounts(status)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_ca_subscriber_id ON customer_accounts(subscriber_id)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_ca_trades_gin    ON customer_accounts USING GIN (trades)")
    op.execute("""
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
    """)
    op.execute("CREATE INDEX IF NOT EXISTS idx_mrr_account   ON mrr_movements(account_id)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_mrr_effective ON mrr_movements(effective_at)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_mrr_type      ON mrr_movements(movement_type)")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS mrr_movements")
    op.execute("DROP TABLE IF EXISTS customer_accounts")
    op.execute("DROP TABLE IF EXISTS plans")
