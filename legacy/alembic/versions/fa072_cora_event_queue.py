"""fa072 - cora_event_queue durable fallback table

Revision ID: fa072_cora_event_queue
Revises: fa071_agent_decision_override_reason_code
Create Date: 2026-06-05
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision: str = "fa072_cora_event_queue"
down_revision: Union[str, Sequence[str], None] = "fa071_agent_decision_override_reason_code"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "cora_event_queue",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("event_type", sa.Text(), nullable=False),
        sa.Column("subscriber_id", sa.Integer(), nullable=True),
        sa.Column("payload", JSONB(), nullable=False, server_default="{}"),
        sa.Column("idempotency_key", sa.Text(), nullable=True, unique=True),
        sa.Column("status", sa.Text(), nullable=False, server_default="pending"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
    )
    op.create_index(
        "idx_cora_event_queue_status_created",
        "cora_event_queue",
        ["status", "created_at"],
    )


def downgrade() -> None:
    op.drop_index("idx_cora_event_queue_status_created", table_name="cora_event_queue")
    op.drop_table("cora_event_queue")
