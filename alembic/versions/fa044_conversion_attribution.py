"""fa044_conversion_attribution

Stage 8: Outcome Attribution + Revenue Signal Scoring.

  1. Creates `conversion_attribution_events` — one row per billable conversion
     event, capturing all 8 attribution dimensions plus scoring state.
     Unique on (source_table, source_event_id) to prevent double-attribution.

  2. Adds 4 revenue-signal score columns to `subscribers` so the latest score
     is readable without joining user_segments (which holds the per-segment
     view). The audit history stays in `revenue_signal_score_events` (fa037).

Purely additive — no backfill. Score columns are nullable (or default 0) so
existing subscriber rows remain valid until the first attribution event fires.

Revision ID: fa044_conversion_attribution
Revises:     fa043_extend_tax_delinquency_fields
Create Date: 2026-05-27
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision: str = "fa044_conversion_attribution"
down_revision: Union[str, Sequence[str], None] = "fa043_extend_tax_delinquency_fields"
branch_labels = None
depends_on = None

# Canonical conversion types — also enforced in service code.
_CONVERSION_TYPES_CHECK = (
    "conversion_type IN ("
    "'paid_unlock','saved_card','wallet_activation','wallet_topup',"
    "'bundle_purchase','territory_lock_purchase','autopilot_lite_upgrade',"
    "'autopilot_pro_upgrade','annual_upgrade','data_only_save',"
    "'deal_win_reported','failed_payment_recovered'"
    ")"
)


def upgrade() -> None:
    # ── conversion_attribution_events ────────────────────────────────────
    op.create_table(
        "conversion_attribution_events",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("conversion_type", sa.String(60), nullable=False),
        sa.Column("source_table", sa.String(80), nullable=False),
        sa.Column("source_event_id", sa.String(120), nullable=False),
        sa.Column(
            "subscriber_id",
            sa.Integer,
            sa.ForeignKey("subscribers.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "lead_id",
            sa.Integer,
            sa.ForeignKey("sent_leads.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column(
            "property_id",
            sa.Integer,
            sa.ForeignKey("properties.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("zip_code", sa.String(10), nullable=True),
        sa.Column("trade", sa.String(50), nullable=True),
        sa.Column("wallet_tier", sa.String(30), nullable=True),
        sa.Column("lock_status", sa.String(20), nullable=True),
        sa.Column("lock_zip", sa.String(10), nullable=True),
        sa.Column("autopilot_tier", sa.String(30), nullable=True),
        sa.Column(
            "bundle_id",
            sa.Integer,
            sa.ForeignKey("bundle_purchases.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("bundle_type", sa.String(50), nullable=True),
        sa.Column("deal_size_bucket", sa.String(20), nullable=True),
        sa.Column("revenue_amount", sa.Numeric(12, 2), nullable=True),
        sa.Column("currency", sa.String(3), nullable=False, server_default="usd"),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("attribution_status", sa.String(20), nullable=True),
        sa.Column("attribution_confidence", sa.String(20), nullable=True),
        sa.Column("attribution_metadata", JSONB, nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
    )

    # Unique — prevent double-attribution for the same source event.
    op.create_unique_constraint(
        "uq_attribution_source",
        "conversion_attribution_events",
        ["source_table", "source_event_id"],
    )

    # Check constraints.
    op.create_check_constraint(
        "check_cae_conversion_type",
        "conversion_attribution_events",
        _CONVERSION_TYPES_CHECK,
    )
    op.create_check_constraint(
        "check_cae_lock_status",
        "conversion_attribution_events",
        "lock_status IS NULL OR lock_status IN ('locked','unlocked','unknown','not_applicable')",
    )
    op.create_check_constraint(
        "check_cae_autopilot_tier",
        "conversion_attribution_events",
        "autopilot_tier IS NULL OR autopilot_tier IN ('autopilot_lite','autopilot_pro','not_applicable','unknown')",
    )
    op.create_check_constraint(
        "check_cae_attribution_status",
        "conversion_attribution_events",
        "attribution_status IS NULL OR attribution_status IN ('complete','partial','unresolved')",
    )
    op.create_check_constraint(
        "check_cae_attribution_confidence",
        "conversion_attribution_events",
        "attribution_confidence IS NULL OR attribution_confidence IN ('high','medium','low')",
    )

    # Indexes.
    op.create_index("idx_cae_subscriber_id", "conversion_attribution_events", ["subscriber_id"])
    op.create_index("idx_cae_lead_id", "conversion_attribution_events", ["lead_id"])
    op.create_index("idx_cae_zip_code", "conversion_attribution_events", ["zip_code"])
    op.create_index("idx_cae_trade", "conversion_attribution_events", ["trade"])
    op.create_index("idx_cae_wallet_tier", "conversion_attribution_events", ["wallet_tier"])
    op.create_index("idx_cae_lock_status", "conversion_attribution_events", ["lock_status"])
    op.create_index("idx_cae_autopilot_tier", "conversion_attribution_events", ["autopilot_tier"])
    op.create_index("idx_cae_bundle_type", "conversion_attribution_events", ["bundle_type"])
    op.create_index("idx_cae_deal_size_bucket", "conversion_attribution_events", ["deal_size_bucket"])
    op.create_index("idx_cae_conversion_type", "conversion_attribution_events", ["conversion_type"])
    op.create_index("idx_cae_occurred_at", "conversion_attribution_events", ["occurred_at"])

    # ── subscribers: 4 revenue-signal score columns ───────────────────────
    op.add_column(
        "subscribers",
        sa.Column(
            "revenue_signal_score",
            sa.Integer,
            nullable=False,
            server_default="0",
        ),
    )
    op.add_column(
        "subscribers",
        sa.Column("revenue_signal_band", sa.String(20), nullable=True),
    )
    op.add_column(
        "subscribers",
        sa.Column("revenue_signal_breakdown", JSONB, nullable=True),
    )
    op.add_column(
        "subscribers",
        sa.Column(
            "revenue_signal_updated_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
    )
    op.create_check_constraint(
        "check_subscriber_revenue_signal_band",
        "subscribers",
        "revenue_signal_band IS NULL OR "
        "revenue_signal_band IN ('low','medium','high','very_high')",
    )
    op.create_index(
        "idx_subscriber_signal_score",
        "subscribers",
        ["revenue_signal_score"],
    )


def downgrade() -> None:
    # subscribers: reverse the 4 score columns + index/constraint.
    op.drop_index("idx_subscriber_signal_score", table_name="subscribers")
    op.drop_constraint("check_subscriber_revenue_signal_band", "subscribers", type_="check")
    op.drop_column("subscribers", "revenue_signal_updated_at")
    op.drop_column("subscribers", "revenue_signal_breakdown")
    op.drop_column("subscribers", "revenue_signal_band")
    op.drop_column("subscribers", "revenue_signal_score")

    # conversion_attribution_events: drop indexes, constraints, table.
    op.drop_index("idx_cae_occurred_at", table_name="conversion_attribution_events")
    op.drop_index("idx_cae_conversion_type", table_name="conversion_attribution_events")
    op.drop_index("idx_cae_deal_size_bucket", table_name="conversion_attribution_events")
    op.drop_index("idx_cae_bundle_type", table_name="conversion_attribution_events")
    op.drop_index("idx_cae_autopilot_tier", table_name="conversion_attribution_events")
    op.drop_index("idx_cae_lock_status", table_name="conversion_attribution_events")
    op.drop_index("idx_cae_wallet_tier", table_name="conversion_attribution_events")
    op.drop_index("idx_cae_trade", table_name="conversion_attribution_events")
    op.drop_index("idx_cae_zip_code", table_name="conversion_attribution_events")
    op.drop_index("idx_cae_lead_id", table_name="conversion_attribution_events")
    op.drop_index("idx_cae_subscriber_id", table_name="conversion_attribution_events")
    op.drop_table("conversion_attribution_events")
