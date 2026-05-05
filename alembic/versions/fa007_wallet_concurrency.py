"""fa007_wallet_concurrency — non-negative balance + premium status enum + dispute fields

Adds:
  1. CHECK constraint `credits_nonneg` on wallet_balances.credits_remaining ≥ 0
     (defense in depth — the real fix is row-level locking in wallet_engine).
  2. Replace check_premium_status to allow 'refunded' and 'disputed'.
  3. Subscriber dispute tracking: disputed_count + disputed_at + 'disputed' status.
  4. PremiumPurchase refund/dispute audit columns (refunded_at, refund_reason,
     refund_amount_cents, disputed_at, dispute_reason, stripe_charge_id).

Revision ID: fa007_wallet_concurrency
Revises:     fa006_referral_teams
Create Date: 2026-05-04
"""

import sqlalchemy as sa
from alembic import op


revision = "fa007_wallet_concurrency"
down_revision = "fa006_referral_teams"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ── Wallet: non-negative balance ─────────────────────────────────────
    op.create_check_constraint(
        "credits_nonneg",
        "wallet_balances",
        "credits_remaining >= 0",
    )

    # ── PremiumPurchase: extended status enum + audit columns ───────────
    op.drop_constraint("check_premium_status", "premium_purchases", type_="check")
    op.create_check_constraint(
        "check_premium_status",
        "premium_purchases",
        "status IN ('pending', 'delivered', 'failed', 'refunded', 'disputed')",
    )
    op.add_column("premium_purchases", sa.Column("refunded_at", sa.DateTime(), nullable=True))
    op.add_column("premium_purchases", sa.Column("refund_reason", sa.String(length=100), nullable=True))
    op.add_column("premium_purchases", sa.Column("refund_amount_cents", sa.Integer(), nullable=True))
    op.add_column("premium_purchases", sa.Column("disputed_at", sa.DateTime(), nullable=True))
    op.add_column("premium_purchases", sa.Column("dispute_reason", sa.String(length=100), nullable=True))
    op.add_column("premium_purchases", sa.Column("stripe_charge_id", sa.String(length=100), nullable=True))
    op.create_index(
        "idx_premium_stripe_charge_id",
        "premium_purchases",
        ["stripe_charge_id"],
        unique=False,
    )

    # ── Subscriber: dispute tracking ─────────────────────────────────────
    op.add_column(
        "subscribers",
        sa.Column("disputed_count", sa.Integer(), nullable=False, server_default="0"),
    )
    op.add_column("subscribers", sa.Column("disputed_at", sa.DateTime(), nullable=True))
    op.drop_constraint("check_subscriber_status", "subscribers", type_="check")
    op.create_check_constraint(
        "check_subscriber_status",
        "subscribers",
        "status IN ('active', 'grace', 'churned', 'cancelled', 'paused', 'disputed')",
    )


def downgrade() -> None:
    # Subscriber
    op.drop_constraint("check_subscriber_status", "subscribers", type_="check")
    op.create_check_constraint(
        "check_subscriber_status",
        "subscribers",
        "status IN ('active', 'grace', 'churned', 'cancelled', 'paused')",
    )
    op.drop_column("subscribers", "disputed_at")
    op.drop_column("subscribers", "disputed_count")

    # PremiumPurchase
    op.drop_index("idx_premium_stripe_charge_id", table_name="premium_purchases")
    op.drop_column("premium_purchases", "stripe_charge_id")
    op.drop_column("premium_purchases", "dispute_reason")
    op.drop_column("premium_purchases", "disputed_at")
    op.drop_column("premium_purchases", "refund_amount_cents")
    op.drop_column("premium_purchases", "refund_reason")
    op.drop_column("premium_purchases", "refunded_at")
    op.drop_constraint("check_premium_status", "premium_purchases", type_="check")
    op.create_check_constraint(
        "check_premium_status",
        "premium_purchases",
        "status IN ('pending', 'delivered', 'failed')",
    )

    # Wallet
    op.drop_constraint("credits_nonneg", "wallet_balances", type_="check")
