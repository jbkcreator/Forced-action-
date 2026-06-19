"""Quora topic rotation pool — quora_topics + quora_settings tables

Revision ID: fa083_quora_topics
Revises: fa082_quora_post_tracking
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "fa083_quora_topics"
down_revision: Union[str, Sequence[str]] = "fa082_quora_post_tracking"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "quora_topics",
        sa.Column("id",          sa.Integer(),                    primary_key=True, autoincrement=True),
        sa.Column("keyword",     sa.Text(),                       nullable=False),
        sa.Column("is_active",   sa.Boolean(),                    nullable=False, server_default="true"),
        sa.Column("last_run_at", sa.DateTime(timezone=True),      nullable=True),
        sa.Column("created_at",  sa.DateTime(timezone=True),      nullable=False,
                  server_default=sa.text("NOW()")),
        sa.UniqueConstraint("keyword", name="uq_quora_topics_keyword"),
    )
    op.create_index("idx_quora_topics_active_last_run", "quora_topics",
                    ["is_active", "last_run_at"])

    op.create_table(
        "quora_settings",
        sa.Column("id",            sa.Integer(), primary_key=True),
        sa.Column("cooldown_days", sa.Integer(), nullable=False, server_default="1"),
    )
    # Seed the single-row settings record
    op.execute("INSERT INTO quora_settings (id, cooldown_days) VALUES (1, 1)")


def downgrade() -> None:
    op.drop_table("quora_settings")
    op.drop_index("idx_quora_topics_active_last_run", table_name="quora_topics")
    op.drop_table("quora_topics")
