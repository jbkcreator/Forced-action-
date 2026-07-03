"""add_sent_lead_refund_columns

Adds refund tracking columns to `sent_leads` so admin can issue and log
Stripe refunds for $4 lead-unlock purchases directly from the admin panel.

Revision ID: fa002_refund_cols
Revises:     fa001_sentleads_source
Create Date: 2026-04-29
"""

import sqlalchemy as sa
from alembic import op

revision = "fa002_refund_cols"
down_revision = "fa001_sentleads_source"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("sent_leads", sa.Column("stripe_payment_intent_id", sa.String(100), nullable=True))
    op.add_column("sent_leads", sa.Column("refunded_at", sa.DateTime, nullable=True))
    op.add_column("sent_leads", sa.Column("refund_reason", sa.String(255), nullable=True))
    op.add_column("sent_leads", sa.Column("stripe_refund_id", sa.String(100), nullable=True))
    op.create_index("idx_sent_leads_pi_id", "sent_leads", ["stripe_payment_intent_id"])


def downgrade() -> None:
    op.drop_index("idx_sent_leads_pi_id", table_name="sent_leads")
    op.drop_column("sent_leads", "stripe_refund_id")
    op.drop_column("sent_leads", "refund_reason")
    op.drop_column("sent_leads", "refunded_at")
    op.drop_column("sent_leads", "stripe_payment_intent_id")
