"""Add amount_cents to sent_leads and lead_pack_purchases.

Task 6.1 — neither table stores the amount actually charged (only
stripe_payment_intent_id), so per-subscriber margin could not compute real
revenue for lead_unlock/lead_pack purchases without a live Stripe lookup.
Populated going forward at webhook time; historical rows stay NULL (unknown,
not backfilled/guessed).

Revision ID: fa109_lead_purchase_amount_cents
Revises:     fa108_lane_description
Create Date: 2026-07-01
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "fa109_lead_purchase_amount_cents"
down_revision = "fa108_lane_description"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("sent_leads", sa.Column("amount_cents", sa.Integer(), nullable=True))
    op.add_column("lead_pack_purchases", sa.Column("amount_cents", sa.Integer(), nullable=True))


def downgrade() -> None:
    op.drop_column("lead_pack_purchases", "amount_cents")
    op.drop_column("sent_leads", "amount_cents")
