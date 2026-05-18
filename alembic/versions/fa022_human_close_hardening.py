"""fa022 — human_close: posted_at + retry cols + price/vertical gate metadata

Adds Slack retry tracking (posted_at, post_attempts, last_post_error) and
value-gate snapshot columns (target_tier_price_cents, vertical) to
human_close_escalations. Also creates a partial index for the nightly
retry sweep.

Revision ID: fa022
Revises:     fa021
Create Date: 2026-05-18
"""

import sqlalchemy as sa
from alembic import op

revision = "fa022"
down_revision = ("fa018_phone_normalize_backfill", "fa021")
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "human_close_escalations",
        sa.Column("posted_at", sa.DateTime, nullable=True),
    )
    op.add_column(
        "human_close_escalations",
        sa.Column("post_attempts", sa.Integer, nullable=False, server_default="0"),
    )
    op.add_column(
        "human_close_escalations",
        sa.Column("last_post_error", sa.String(200), nullable=True),
    )
    op.add_column(
        "human_close_escalations",
        sa.Column("target_tier_price_cents", sa.Integer, nullable=False, server_default="0"),
    )
    op.add_column(
        "human_close_escalations",
        sa.Column("vertical", sa.String(40), nullable=True),
    )
    op.create_index(
        "idx_hce_retry",
        "human_close_escalations",
        ["post_attempts", "routed_at"],
        postgresql_where=sa.text("posted_at IS NULL AND post_attempts < 3"),
    )


def downgrade() -> None:
    op.drop_index("idx_hce_retry", table_name="human_close_escalations")
    op.drop_column("human_close_escalations", "vertical")
    op.drop_column("human_close_escalations", "target_tier_price_cents")
    op.drop_column("human_close_escalations", "last_post_error")
    op.drop_column("human_close_escalations", "post_attempts")
    op.drop_column("human_close_escalations", "posted_at")
