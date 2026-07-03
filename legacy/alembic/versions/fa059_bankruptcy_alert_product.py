"""fa059_bankruptcy_alert_product

Stage 12: Bankruptcy Filing Alert subscription product.

Also merges the two open heads (fa058 white-label chain + fa051_predictive_churn)
into a single head so `alembic upgrade head` resolves cleanly.

Creates three tables:

  1. bankruptcy_filings — one row per unique CourtListener bankruptcy docket.
     case_number UNIQUE is the dedup key for ingestion.

  2. bankruptcy_alert_subscriptions — standalone $297/mo subscribers (attorneys,
     investors, lenders). Separate from the property `subscribers` table — no ZIP
     territory, no vertical. access_token (uuid) authenticates the status endpoint.

  3. bankruptcy_filing_alerts — dedup + audit log. UNIQUE(subscription_id,
     filing_id, channel) guarantees a subscriber is never alerted twice for the
     same filing on the same channel.

Purely additive. Safe to apply during normal hours.

Revision ID: fa059
Revises:     fa058, fa051_predictive_churn
Create Date: 2026-05-31
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB


revision: str = "fa059"
down_revision: Union[str, Sequence[str], None] = ("fa058", "fa051_predictive_churn")
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ── bankruptcy_filings ─────────────────────────────────────────────────
    op.create_table(
        "bankruptcy_filings",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("case_number", sa.String(60), nullable=False, unique=True),
        sa.Column("chapter", sa.String(4), nullable=True),
        sa.Column("court", sa.String(20), nullable=False),
        sa.Column("jurisdiction", sa.String(40), nullable=False),
        sa.Column("filer", sa.String(255), nullable=True),
        sa.Column("trustee", sa.String(255), nullable=True),
        sa.Column("date_filed", sa.Date, nullable=True),
        sa.Column("docket_id", sa.String(40), nullable=True),
        sa.Column("nature_of_suit", sa.String(120), nullable=True),
        sa.Column("raw", JSONB, nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True),
                  nullable=False, server_default=sa.text("NOW()")),
    )
    op.create_index("idx_bkfiling_date_filed", "bankruptcy_filings", ["date_filed"])
    op.create_index("idx_bkfiling_jurisdiction_chapter",
                    "bankruptcy_filings", ["jurisdiction", "chapter"])
    op.create_index("idx_bkfiling_created_at", "bankruptcy_filings", ["created_at"])

    # ── bankruptcy_alert_subscriptions ─────────────────────────────────────
    op.create_table(
        "bankruptcy_alert_subscriptions",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("email", sa.String(255), nullable=False),
        sa.Column("phone", sa.String(20), nullable=True),
        sa.Column("name", sa.String(255), nullable=True),
        sa.Column("stripe_customer_id", sa.String(100), nullable=True, unique=True),
        sa.Column("stripe_subscription_id", sa.String(100), nullable=True, unique=True),
        sa.Column("status", sa.String(20), nullable=False, server_default="trialing"),
        sa.Column("jurisdictions", JSONB, nullable=True),   # list[str]; NULL = all
        sa.Column("chapters", JSONB, nullable=True),        # list[str]; NULL = all
        sa.Column("channel_email", sa.Boolean, nullable=False, server_default=sa.true()),
        sa.Column("channel_sms", sa.Boolean, nullable=False, server_default=sa.false()),
        sa.Column("trial_ends_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("access_token", sa.String(36), nullable=False, unique=True),
        sa.Column("created_at", sa.DateTime(timezone=True),
                  nullable=False, server_default=sa.text("NOW()")),
        sa.Column("updated_at", sa.DateTime(timezone=True),
                  nullable=False, server_default=sa.text("NOW()")),
        sa.Column("canceled_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "status IN ('trialing','active','past_due','canceled')",
            name="check_bkalert_sub_status",
        ),
    )
    op.create_index("idx_bkalert_sub_status", "bankruptcy_alert_subscriptions", ["status"])
    op.create_index("idx_bkalert_sub_email", "bankruptcy_alert_subscriptions", ["email"])

    # ── bankruptcy_filing_alerts (dedup + audit) ───────────────────────────
    op.create_table(
        "bankruptcy_filing_alerts",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("subscription_id", sa.Integer,
                  sa.ForeignKey("bankruptcy_alert_subscriptions.id", ondelete="CASCADE"),
                  nullable=False),
        sa.Column("filing_id", sa.Integer,
                  sa.ForeignKey("bankruptcy_filings.id", ondelete="CASCADE"),
                  nullable=False),
        sa.Column("channel", sa.String(10), nullable=False),
        sa.Column("status", sa.String(12), nullable=False),
        sa.Column("error", sa.Text, nullable=True),
        sa.Column("sent_at", sa.DateTime(timezone=True),
                  nullable=False, server_default=sa.text("NOW()")),
        sa.UniqueConstraint("subscription_id", "filing_id", "channel",
                            name="uq_bkfiling_alert_dedup"),
        sa.CheckConstraint("channel IN ('email','sms')", name="check_bkalert_channel"),
        sa.CheckConstraint("status IN ('sent','failed','suppressed')",
                           name="check_bkalert_status"),
    )
    op.create_index("idx_bkfiling_alert_sent_at", "bankruptcy_filing_alerts", ["sent_at"])
    op.create_index("idx_bkfiling_alert_subscription",
                    "bankruptcy_filing_alerts", ["subscription_id"])


def downgrade() -> None:
    op.drop_index("idx_bkfiling_alert_subscription", table_name="bankruptcy_filing_alerts")
    op.drop_index("idx_bkfiling_alert_sent_at", table_name="bankruptcy_filing_alerts")
    op.drop_table("bankruptcy_filing_alerts")

    op.drop_index("idx_bkalert_sub_email", table_name="bankruptcy_alert_subscriptions")
    op.drop_index("idx_bkalert_sub_status", table_name="bankruptcy_alert_subscriptions")
    op.drop_table("bankruptcy_alert_subscriptions")

    op.drop_index("idx_bkfiling_created_at", table_name="bankruptcy_filings")
    op.drop_index("idx_bkfiling_jurisdiction_chapter", table_name="bankruptcy_filings")
    op.drop_index("idx_bkfiling_date_filed", table_name="bankruptcy_filings")
    op.drop_table("bankruptcy_filings")
