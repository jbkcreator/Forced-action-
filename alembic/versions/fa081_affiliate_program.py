"""fa081 — Affiliate Program (Stream D) schema

Four tables for external-affiliate attribution + cash-commission payout:
affiliates, affiliate_referrals, subscription_invoices, affiliate_payout_ledger.

NOTE: Alembic CLI is unusable on this repo (multi-head tree). The DDL is applied
via scripts/apply_affiliate_schema.py; this file is the version-controlled record
of the change. See docs/adr/0005 and CONTEXT.md "Affiliate Program".

Revision ID: fa081_affiliate_program
Revises: fa079_synthflow_transcript_columns
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "fa081_affiliate_program"
down_revision: Union[str, None] = "fa079_synthflow_transcript_columns"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "affiliates",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("ref_code", sa.String(40), nullable=False),
        sa.Column("name", sa.String(200), nullable=False),
        sa.Column("contact_email", sa.String(255)),
        sa.Column("contact_phone", sa.String(20)),
        sa.Column("commission_rate", sa.Numeric(5, 4), nullable=False, server_default=sa.text("0.20")),
        sa.Column("status", sa.String(20), nullable=False, server_default="active"),
        sa.Column("created_at", sa.DateTime, nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint("status IN ('active', 'disabled')", name="check_affiliate_status"),
    )
    op.create_index("ix_affiliates_ref_code", "affiliates", ["ref_code"], unique=True)

    op.create_table(
        "affiliate_referrals",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("affiliate_id", sa.Integer, sa.ForeignKey("affiliates.id"), nullable=False),
        sa.Column("subscriber_id", sa.Integer, sa.ForeignKey("subscribers.id"), nullable=False, unique=True),
        sa.Column("status", sa.String(20), nullable=False, server_default="pending"),
        sa.Column("attributed_at", sa.DateTime, nullable=False, server_default=sa.func.now()),
        sa.Column("confirmed_at", sa.DateTime),
        sa.Column("paid_tenure_start", sa.DateTime),
        sa.Column("window_end", sa.DateTime),
        sa.CheckConstraint("status IN ('pending', 'active', 'expired')", name="check_affiliate_referral_status"),
    )
    op.create_index("ix_affiliate_referrals_affiliate_id", "affiliate_referrals", ["affiliate_id"])

    op.create_table(
        "subscription_invoices",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("subscriber_id", sa.Integer, sa.ForeignKey("subscribers.id"), nullable=False),
        sa.Column("stripe_invoice_id", sa.String(255), nullable=False, unique=True),
        sa.Column("stripe_payment_intent_id", sa.String(255)),
        sa.Column("amount_collected_cents", sa.Integer, nullable=False),
        sa.Column("period_month", sa.Date, nullable=False),
        sa.Column("paid_at", sa.DateTime, nullable=False),
        sa.Column("reversed_at", sa.DateTime),
        sa.Column("reversed_reason", sa.String(20)),
        sa.CheckConstraint("reversed_reason IN ('refund', 'dispute')", name="check_subscription_invoice_reversed_reason"),
    )
    op.create_index("ix_subscription_invoices_subscriber_id", "subscription_invoices", ["subscriber_id"])
    op.create_index("ix_subscription_invoices_period_month", "subscription_invoices", ["period_month"])
    op.create_index("ix_subscription_invoices_stripe_payment_intent_id", "subscription_invoices", ["stripe_payment_intent_id"])

    op.create_table(
        "affiliate_payout_ledger",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("affiliate_id", sa.Integer, sa.ForeignKey("affiliates.id"), nullable=False),
        sa.Column("affiliate_referral_id", sa.Integer, sa.ForeignKey("affiliate_referrals.id"), nullable=False),
        sa.Column("period_month", sa.Date, nullable=False),
        sa.Column("line_type", sa.String(20), nullable=False),
        sa.Column("amount_cents", sa.Integer, nullable=False),
        sa.Column("source_invoice_id", sa.Integer, sa.ForeignKey("subscription_invoices.id")),
        sa.Column("created_at", sa.DateTime, nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint("affiliate_referral_id", "period_month", "line_type", name="uq_affiliate_ledger_period_line"),
        sa.CheckConstraint("line_type IN ('accrual', 'clawback')", name="check_affiliate_ledger_line_type"),
    )
    op.create_index("ix_affiliate_payout_ledger_affiliate_id", "affiliate_payout_ledger", ["affiliate_id"])
    op.create_index("ix_affiliate_payout_ledger_referral_id", "affiliate_payout_ledger", ["affiliate_referral_id"])


def downgrade() -> None:
    op.drop_table("affiliate_payout_ledger")
    op.drop_table("subscription_invoices")
    op.drop_table("affiliate_referrals")
    op.drop_table("affiliates")
