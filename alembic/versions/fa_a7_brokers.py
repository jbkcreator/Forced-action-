"""create brokers table (Layer 1 — Broker State Machine identity)

Revision ID: fa_a7_brokers
Revises: fa_a6_underwriting_feedback
Create Date: 2026-06-29
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import UUID as PG_UUID

revision: str = "fa_a7_brokers"
down_revision: Union[str, Sequence[str], None] = "fa_a6_underwriting_feedback"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "brokers",
        sa.Column(
            "broker_id",
            PG_UUID(as_uuid=False),
            primary_key=True,
            server_default=sa.text("generate_uuidv7()"),
        ),
        sa.Column("email", sa.String(), nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("password_hash", sa.String(), nullable=True),
        sa.Column(
            "role",
            sa.String(20),
            nullable=False,
            server_default="broker",
        ),
        sa.Column(
            "is_active",
            sa.Boolean(),
            nullable=False,
            server_default="true",
        ),
        sa.Column("reset_token", sa.String(), nullable=True),
        sa.Column("reset_token_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_login_at", sa.DateTime(timezone=True), nullable=True),
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
        sa.UniqueConstraint("email", name="brokers_email_key"),
        sa.CheckConstraint("role = 'broker'", name="ck_brokers_role"),
    )
    op.create_index("ix_brokers_reset_token", "brokers", ["reset_token"])
    op.create_index("ix_brokers_is_active", "brokers", ["is_active"])


def downgrade() -> None:
    op.drop_index("ix_brokers_is_active", table_name="brokers")
    op.drop_index("ix_brokers_reset_token", table_name="brokers")
    op.drop_table("brokers")
