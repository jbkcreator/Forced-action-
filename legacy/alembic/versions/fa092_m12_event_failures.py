"""M12 DLQ + Event Consumer Reliability — event_failures table

Revision ID: fa092_m12_event_failures
Revises: fa091_m8_sms_prospect_gate
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import UUID

revision: str = "fa092_m12_event_failures"
down_revision: Union[str, Sequence[str]] = "fa091_m8_sms_prospect_gate"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "event_failures",
        sa.Column("event_id", UUID(as_uuid=True), sa.ForeignKey("events.event_id"), nullable=False),
        sa.Column("consumer", sa.String(100), nullable=False),
        sa.Column("retry_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("failed_permanently", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("last_attempt_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("event_id", "consumer"),
    )
    op.create_index(
        "idx_event_failures_consumer_permanent",
        "event_failures",
        ["consumer", "failed_permanently"],
    )


def downgrade() -> None:
    op.drop_index("idx_event_failures_consumer_permanent", table_name="event_failures")
    op.drop_table("event_failures")
