"""fa039_remove_sandbox_outbox

Drops the sandbox_outbox table and all its indexes. The sandbox outbox
infrastructure (capture gate in sms_compliance, sandbox router endpoints,
scenario test helpers) has been removed — the table is no longer written
to or read from anywhere in the codebase.

Safe to run during business hours: this is a pure DROP TABLE. No data is
needed — the table was only used for dev/test scenario assertions.

Revision ID: fa039_remove_sandbox_outbox
Revises:     fa038_message_outcomes_personalization
Create Date: 2026-05-26
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB


revision: str = "fa039_remove_sandbox_outbox"
down_revision: Union[str, Sequence[str], None] = "fa038_message_outcomes_personalization"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_index("idx_sandbox_outbox_campaign_created", table_name="sandbox_outbox")
    op.drop_index("idx_sandbox_outbox_sub_created", table_name="sandbox_outbox")
    op.drop_index("ix_sandbox_outbox_created_at", table_name="sandbox_outbox")
    op.drop_index("ix_sandbox_outbox_decision_id", table_name="sandbox_outbox")
    op.drop_index("ix_sandbox_outbox_campaign", table_name="sandbox_outbox")
    op.drop_index("ix_sandbox_outbox_subscriber_id", table_name="sandbox_outbox")
    op.drop_table("sandbox_outbox")


def downgrade() -> None:
    op.create_table(
        "sandbox_outbox",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("channel", sa.String(20), nullable=False),
        sa.Column("to_number", sa.String(64), nullable=True),
        sa.Column("body", sa.Text(), nullable=False),
        sa.Column("campaign", sa.String(100), nullable=True),
        sa.Column("variant_id", sa.String(100), nullable=True),
        sa.Column("subscriber_id", sa.Integer(), sa.ForeignKey("subscribers.id"), nullable=True),
        sa.Column("decision_id", sa.String(36), nullable=True),
        sa.Column("compliance_allowed", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column("compliance_reason", sa.String(60), nullable=True),
        sa.Column("would_have_delivered", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column("sandbox_flag", sa.String(40), nullable=False, server_default="telnyx_sandbox"),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint(
            "channel IN ('sms', 'voice', 'email')",
            name="check_sandbox_outbox_channel",
        ),
    )
    op.create_index("ix_sandbox_outbox_subscriber_id", "sandbox_outbox", ["subscriber_id"])
    op.create_index("ix_sandbox_outbox_campaign", "sandbox_outbox", ["campaign"])
    op.create_index("ix_sandbox_outbox_decision_id", "sandbox_outbox", ["decision_id"])
    op.create_index("ix_sandbox_outbox_created_at", "sandbox_outbox", ["created_at"])
    op.create_index("idx_sandbox_outbox_sub_created", "sandbox_outbox", ["subscriber_id", "created_at"])
    op.create_index("idx_sandbox_outbox_campaign_created", "sandbox_outbox", ["campaign", "created_at"])
