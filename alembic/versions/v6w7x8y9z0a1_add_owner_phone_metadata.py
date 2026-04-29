"""add_owner_phone_metadata

Adds `phone_metadata` JSONB column to `owners` so we can surface skip-trace
quality data (line type, carrier, reachability score, source) on every
displayed phone — turning the BatchData metadata we already pay for into
a subscriber-visible "Mobile · Verified" badge instead of throwing it away.

Forward-compatible: same column will hold Twilio Lookup results when that
secondary verification path is added.

Revision ID: fa003_phone_metadata
Revises:     fa002_refund_cols
Create Date: 2026-04-29
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "fa003_phone_metadata"
down_revision = "fa002_refund_cols"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "owners",
        sa.Column("phone_metadata", postgresql.JSONB, nullable=True),
    )
    op.create_index(
        "idx_owner_phone_metadata",
        "owners",
        ["phone_metadata"],
        postgresql_using="gin",
    )


def downgrade() -> None:
    op.drop_index("idx_owner_phone_metadata", table_name="owners")
    op.drop_column("owners", "phone_metadata")
