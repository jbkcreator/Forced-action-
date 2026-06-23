"""fa085 — M10 Lead Delivery + Free→Paid Attribution (B2): deliveries,
free_to_paid_attribution, customer_accounts.lead_credits.

NOTE: this repo's alembic tree is multi-head and the CLI is unusable; the DDL is
actually applied via scripts/apply_fa085_m10_lead_delivery.py. This file is kept
for the migration record and mirrors that DDL exactly.
"""

from typing import Sequence, Union

from alembic import op

revision: str = "fa085_m10_lead_delivery"
down_revision: Union[str, Sequence[str]] = "fa084_s1_revenue_engine"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE customer_accounts ADD COLUMN IF NOT EXISTS lead_credits JSONB NOT NULL DEFAULT '{}'::jsonb")
    op.execute("""
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
    """)
    op.execute("CREATE INDEX IF NOT EXISTS idx_deliveries_account_grade_cycle ON deliveries(account_id, grade, billing_period_end)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_deliveries_property            ON deliveries(property_id)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_deliveries_status              ON deliveries(status)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_deliveries_delivered_at        ON deliveries(delivered_at)")
    op.execute("""
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
    """)
    op.execute("CREATE INDEX IF NOT EXISTS idx_attribution_account ON free_to_paid_attribution(account_id)")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS free_to_paid_attribution")
    op.execute("DROP TABLE IF EXISTS deliveries")
    op.execute("ALTER TABLE customer_accounts DROP COLUMN IF EXISTS lead_credits")
