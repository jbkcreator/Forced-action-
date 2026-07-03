"""fa016_accelerated_wallet_push — Subscriber phone/opt-out/missed-leads + wallet_push_offers

Adds the data primitives the Accelerated Wallet Push feature needs:

  1. subscribers.phone — UNIQUE NULLABLE, used by sms_commands._find_subscriber
     for inbound-SMS → Subscriber lookup.
  2. subscribers.wallet_opt_out — per-feature opt-out separate from global STOP.
  3. subscribers.missed_lead_count — denormalized counter for the "missing-leads"
     framing variant.
  4. wallet_push_offers — explicit funnel table (offered → accepted/declined →
     activated/failed/expired). Persists past the Redis pending_offer TTL so
     adoption / take_rate metrics survive.

Revision ID: fa016_accel_wallet_push
Revises:     fa015_api_telnyx
Create Date: 2026-05-13
"""

import sqlalchemy as sa
from alembic import op


revision = "fa016_accel_wallet_push"
down_revision = "fa015_api_telnyx"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ── Subscriber columns ───────────────────────────────────────────────
    op.add_column(
        "subscribers",
        sa.Column("phone", sa.String(length=20), nullable=True),
    )
    op.create_unique_constraint("uq_subscribers_phone", "subscribers", ["phone"])
    op.create_index("idx_subscribers_phone", "subscribers", ["phone"], unique=False)

    op.add_column(
        "subscribers",
        sa.Column(
            "wallet_opt_out",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
    )
    op.add_column(
        "subscribers",
        sa.Column(
            "missed_lead_count",
            sa.Integer(),
            nullable=False,
            server_default="0",
        ),
    )

    # ── wallet_push_offers ───────────────────────────────────────────────
    op.create_table(
        "wallet_push_offers",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column(
            "subscriber_id",
            sa.Integer(),
            sa.ForeignKey("subscribers.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("decision_id", sa.String(length=36), nullable=True),
        sa.Column("framing_variant", sa.String(length=20), nullable=False),
        sa.Column("ab_variant", sa.String(length=1), nullable=True),
        sa.Column(
            "tier",
            sa.String(length=20),
            nullable=False,
            server_default="starter_wallet",
        ),
        sa.Column(
            "status",
            sa.String(length=20),
            nullable=False,
            server_default="offered",
        ),
        sa.Column("stripe_subscription_id", sa.String(length=100), nullable=True),
        sa.Column(
            "offered_at",
            sa.DateTime(),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column("accepted_at", sa.DateTime(), nullable=True),
        sa.Column("declined_at", sa.DateTime(), nullable=True),
        sa.Column("activated_at", sa.DateTime(), nullable=True),
        sa.CheckConstraint(
            "status IN ('offered','accepted','declined','activated','expired','failed')",
            name="check_wallet_push_offer_status",
        ),
        sa.CheckConstraint(
            "framing_variant IN ('missing_leads','credits_ready')",
            name="check_wallet_push_framing_variant",
        ),
    )
    op.create_index(
        "idx_wallet_push_offers_subscriber_offered",
        "wallet_push_offers",
        ["subscriber_id", "offered_at"],
        unique=False,
    )
    op.create_index(
        "idx_wallet_push_offers_decision",
        "wallet_push_offers",
        ["decision_id"],
        unique=False,
    )
    op.create_index(
        "idx_wallet_push_offers_subscription",
        "wallet_push_offers",
        ["stripe_subscription_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("idx_wallet_push_offers_subscription", table_name="wallet_push_offers")
    op.drop_index("idx_wallet_push_offers_decision", table_name="wallet_push_offers")
    op.drop_index(
        "idx_wallet_push_offers_subscriber_offered", table_name="wallet_push_offers"
    )
    op.drop_table("wallet_push_offers")

    op.drop_column("subscribers", "missed_lead_count")
    op.drop_column("subscribers", "wallet_opt_out")

    op.drop_index("idx_subscribers_phone", table_name="subscribers")
    op.drop_constraint("uq_subscribers_phone", "subscribers", type_="unique")
    op.drop_column("subscribers", "phone")
