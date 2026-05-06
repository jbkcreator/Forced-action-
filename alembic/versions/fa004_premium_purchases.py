"""add_premium_purchases

Stage 5 — premium credit SKUs (report / brief / transfer / byol).
Stores both credit-paid and cash-paid premium purchases with fulfillment
status and a pointer to the produced artifact (PDF, skip-trace record).

Revision ID: fa004_premium_purchases
Revises:     fa003_phone_metadata
Create Date: 2026-04-30
"""

import sqlalchemy as sa
from alembic import op


revision = "fa004_premium_purchases"
down_revision = "fa003_phone_metadata"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "premium_purchases",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("subscriber_id", sa.Integer(), nullable=False),
        sa.Column("sku", sa.String(length=30), nullable=False),
        sa.Column("paid_via", sa.String(length=10), nullable=False),
        sa.Column("amount_cents", sa.Integer(), nullable=True),
        sa.Column("credits_spent", sa.Integer(), nullable=True),
        sa.Column("stripe_payment_intent_id", sa.String(length=100), nullable=True),
        sa.Column("property_id", sa.Integer(), nullable=True),
        sa.Column("target_address", sa.String(length=255), nullable=True),
        sa.Column("output_ref", sa.String(length=255), nullable=True),
        sa.Column("status", sa.String(length=20), nullable=False, server_default="pending"),
        sa.Column("purchased_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
        sa.Column("delivered_at", sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(["subscriber_id"], ["subscribers.id"]),
        sa.ForeignKeyConstraint(["property_id"], ["properties.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("stripe_payment_intent_id", name="uq_premium_purchase_pi"),
        sa.CheckConstraint("sku IN ('report', 'brief', 'transfer', 'byol')", name="check_premium_sku"),
        sa.CheckConstraint("paid_via IN ('credits', 'card')", name="check_premium_paid_via"),
        sa.CheckConstraint("status IN ('pending', 'delivered', 'failed')", name="check_premium_status"),
    )
    op.create_index("idx_premium_purchase_sub_sku", "premium_purchases", ["subscriber_id", "sku"])
    op.create_index("ix_premium_purchases_subscriber_id", "premium_purchases", ["subscriber_id"])
    op.create_index("ix_premium_purchases_property_id", "premium_purchases", ["property_id"])
    op.create_index(
        "ix_premium_purchases_stripe_payment_intent_id",
        "premium_purchases",
        ["stripe_payment_intent_id"],
    )


def downgrade() -> None:
    op.drop_index("ix_premium_purchases_stripe_payment_intent_id", table_name="premium_purchases")
    op.drop_index("ix_premium_purchases_property_id", table_name="premium_purchases")
    op.drop_index("ix_premium_purchases_subscriber_id", table_name="premium_purchases")
    op.drop_index("idx_premium_purchase_sub_sku", table_name="premium_purchases")
    op.drop_table("premium_purchases")
