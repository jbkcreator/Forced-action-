"""fa027 — add variant_id column to agent_decisions

Adds an indexed variant_id column to agent_decisions so A/B attribution
can be queried directly (without unpacking the summary JSONB).
Format: "{test_name}:{variant}" e.g. "accelerated_wallet_push_framing:a"

Revision ID: fa027_agent_decisions_variant_id
Revises:     92b002bd99cd
Create Date: 2026-05-20
"""

import sqlalchemy as sa
from alembic import op

revision = "fa027_agent_decisions_variant_id"
down_revision = "92b002bd99cd"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "agent_decisions",
        sa.Column("variant_id", sa.String(80), nullable=True),
    )
    op.create_index(
        "ix_agent_decisions_variant_id",
        "agent_decisions",
        ["variant_id"],
    )


def downgrade() -> None:
    op.drop_index("ix_agent_decisions_variant_id", table_name="agent_decisions")
    op.drop_column("agent_decisions", "variant_id")
