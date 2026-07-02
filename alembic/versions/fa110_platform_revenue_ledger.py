"""Add platform_revenue_ledger and platform_cost_attribution.

Centralized revenue/cost-attribution ledger. Every purchase-confirmation
path (lead_unlock, lead_pack, premium report/brief, subscription invoice)
writes one normalized row into platform_revenue_ledger via
src/services/revenue_ledger.py:record_revenue() instead of each reporting
function hand-joining SentLead/LeadPackPurchase/PremiumPurchase/
subscription_invoices directly. platform_cost_attribution generalizes "which
subscriber does this enrichment cost belong to" the same way, including the
zip-territory multi-vertical collision result (refreshed daily, versioned by
computed_for_date rather than overwritten in place).

Existing per-product tables (SentLead, LeadPackPurchase, PremiumPurchase,
subscription_invoices) are untouched — they remain each product's own
operational source of truth. This is a pure additive reporting layer.

Revision ID: fa110_platform_revenue_ledger
Revises:     fa109_lead_purchase_amount_cents
Create Date: 2026-07-01
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "fa110_platform_revenue_ledger"
down_revision = "fa109_lead_purchase_amount_cents"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "platform_revenue_ledger",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("subscriber_id", sa.Integer(), sa.ForeignKey("subscribers.id"), nullable=False),
        sa.Column("product_type", sa.String(length=40), nullable=False),
        sa.Column("amount_cents", sa.Integer(), nullable=False),
        sa.Column("property_id", sa.Integer(), sa.ForeignKey("properties.id"), nullable=True),
        sa.Column("source_table", sa.String(length=60), nullable=False),
        sa.Column("source_id", sa.BigInteger(), nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("refunded_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint("source_table", "source_id", name="uq_revenue_ledger_source"),
    )
    op.create_index("idx_revenue_ledger_subscriber", "platform_revenue_ledger", ["subscriber_id"])
    op.create_index("idx_revenue_ledger_property", "platform_revenue_ledger", ["property_id"])
    op.create_index("idx_revenue_ledger_occurred_at", "platform_revenue_ledger", ["occurred_at"])
    op.create_index("idx_revenue_ledger_product_type", "platform_revenue_ledger", ["product_type"])

    op.create_table(
        "platform_cost_attribution",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("enrichment_usage_log_id", sa.Integer(), sa.ForeignKey("enrichment_usage_logs.id"), nullable=False),
        sa.Column("subscriber_id", sa.Integer(), sa.ForeignKey("subscribers.id"), nullable=False),
        sa.Column("property_id", sa.Integer(), sa.ForeignKey("properties.id"), nullable=False),
        sa.Column("attribution_method", sa.String(length=40), nullable=False),
        sa.Column("attributed_cost_cents", sa.Integer(), nullable=False),
        # NULL for point-in-time (per-purchase) attributions; set for the
        # daily zip-territory refresh so each day's snapshot is versioned
        # rather than overwritten (ownership can shift day to day).
        sa.Column("computed_for_date", sa.Date(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )
    op.create_index("idx_cost_attribution_subscriber", "platform_cost_attribution", ["subscriber_id"])
    op.create_index("idx_cost_attribution_property", "platform_cost_attribution", ["property_id"])
    op.create_index(
        "idx_cost_attribution_method_date", "platform_cost_attribution",
        ["attribution_method", "computed_for_date"],
    )
    # Two partial unique indexes instead of one composite constraint: Postgres
    # treats NULL != NULL, so a plain UniqueConstraint including the nullable
    # computed_for_date column would silently allow duplicate direct-purchase
    # rows (where computed_for_date is always NULL) through.
    op.create_index(
        "uq_cost_attribution_direct_purchase",
        "platform_cost_attribution",
        ["enrichment_usage_log_id", "subscriber_id"],
        unique=True,
        postgresql_where=sa.text("attribution_method = 'direct_purchase'"),
    )
    op.create_index(
        "uq_cost_attribution_zip_territory_daily",
        "platform_cost_attribution",
        ["enrichment_usage_log_id", "subscriber_id", "computed_for_date"],
        unique=True,
        postgresql_where=sa.text("attribution_method = 'zip_territory_highest_vertical'"),
    )


def downgrade() -> None:
    op.drop_index("uq_cost_attribution_zip_territory_daily", table_name="platform_cost_attribution")
    op.drop_index("uq_cost_attribution_direct_purchase", table_name="platform_cost_attribution")
    op.drop_index("idx_cost_attribution_method_date", table_name="platform_cost_attribution")
    op.drop_index("idx_cost_attribution_property", table_name="platform_cost_attribution")
    op.drop_index("idx_cost_attribution_subscriber", table_name="platform_cost_attribution")
    op.drop_table("platform_cost_attribution")

    op.drop_index("idx_revenue_ledger_product_type", table_name="platform_revenue_ledger")
    op.drop_index("idx_revenue_ledger_occurred_at", table_name="platform_revenue_ledger")
    op.drop_index("idx_revenue_ledger_property", table_name="platform_revenue_ledger")
    op.drop_index("idx_revenue_ledger_subscriber", table_name="platform_revenue_ledger")
    op.drop_table("platform_revenue_ledger")
