"""fa017_signup_source — Subscriber attribution columns (commercial-ladder fix).

Adds the columns needed to attribute every signup back to its source channel
so we can preserve the four signup paths (DBPR email, Cora SMS, missed call,
referral) end-to-end:

  1. signup_source       — locked enum-like column (CHECK constraint over an
                           allow-list). Required for analytics + product gating.
  2. utm_source / medium / campaign — open free-text marketing attribution.
  3. campaign_id          — internal campaign identifier (alias when utm_campaign
                            is also present from a marketing tool).
  4. attribution_token    — opaque audit field — stores the HMAC token the user
                            arrived with (e.g. missed-call signed link) for
                            debugging without re-issuing it.

Existing rows default to 'unknown' (not 'direct') so the dashboard can tell
the difference between "we genuinely don't know" and "they came in cold".

Revision ID: fa017_signup_source
Revises:     fa016_accel_wallet_push
Create Date: 2026-05-13
"""

import sqlalchemy as sa
from alembic import op


revision = "fa017_signup_source"
down_revision = "fa016_accel_wallet_push"
branch_labels = None
depends_on = None


# Kept in sync with the CHECK constraint and the whitelist in signup_engine.
_ALLOWED_SOURCES = (
    "direct",
    "landing_page",
    "dbpr_email",
    "cora_sms",
    "missed_call",
    "referral",
    "admin",
    "unknown",
)


def upgrade() -> None:
    # Add columns nullable=True first so the backfill statement works on big
    # tables without rewriting every row twice.
    op.add_column(
        "subscribers",
        sa.Column("signup_source", sa.String(length=30), nullable=True),
    )
    op.add_column(
        "subscribers",
        sa.Column("utm_source", sa.String(length=100), nullable=True),
    )
    op.add_column(
        "subscribers",
        sa.Column("utm_medium", sa.String(length=100), nullable=True),
    )
    op.add_column(
        "subscribers",
        sa.Column("utm_campaign", sa.String(length=100), nullable=True),
    )
    op.add_column(
        "subscribers",
        sa.Column("campaign_id", sa.String(length=50), nullable=True),
    )
    op.add_column(
        "subscribers",
        sa.Column("attribution_token", sa.String(length=200), nullable=True),
    )

    # Backfill existing rows to 'unknown' (we don't have the data to claim
    # any specific channel). Then make the column NOT NULL with the default.
    op.execute("UPDATE subscribers SET signup_source = 'unknown' WHERE signup_source IS NULL")
    op.alter_column(
        "subscribers",
        "signup_source",
        existing_type=sa.String(length=30),
        nullable=False,
        server_default="direct",
    )

    op.create_check_constraint(
        "check_subscriber_signup_source",
        "subscribers",
        f"signup_source IN ({','.join(repr(s) for s in _ALLOWED_SOURCES)})",
    )
    op.create_index(
        "idx_subscriber_signup_source",
        "subscribers",
        ["signup_source"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("idx_subscriber_signup_source", table_name="subscribers")
    op.drop_constraint(
        "check_subscriber_signup_source", "subscribers", type_="check"
    )
    op.drop_column("subscribers", "attribution_token")
    op.drop_column("subscribers", "campaign_id")
    op.drop_column("subscribers", "utm_campaign")
    op.drop_column("subscribers", "utm_medium")
    op.drop_column("subscribers", "utm_source")
    op.drop_column("subscribers", "signup_source")
