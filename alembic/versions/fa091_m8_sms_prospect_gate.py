"""M8 SMS Path Rewire — prospect_id on sms_send_logs + new DLQ reasons

Revision ID: fa091_m8_sms_prospect_gate
Revises: fa090_events_prospect_id_not_null
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import UUID

revision: str = "fa091_m8_sms_prospect_gate"
down_revision: Union[str, Sequence[str]] = "fa090_events_prospect_id_not_null"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add prospect_id to sms_send_logs for traceability
    op.add_column(
        "sms_send_logs",
        sa.Column(
            "prospect_id",
            UUID(as_uuid=True),
            sa.ForeignKey("prospects.prospect_id", ondelete="SET NULL"),
            nullable=True,
        ),
    )
    op.create_index(
        "idx_sms_send_logs_prospect_id",
        "sms_send_logs",
        ["prospect_id"],
    )

    # Extend DLQ reason CHECK to include prospect-level block reasons
    op.drop_constraint("check_dlq_reason", "sms_dead_letters", type_="check")
    op.create_check_constraint(
        "check_dlq_reason",
        "sms_dead_letters",
        "reason IN ("
        "'opt_out', 'delivery_failed', 'error', 'unresolvable', "
        "'quiet_hours', 'no_opt_in', 'subscriber_sms_frequency_cap', "
        "'do_not_text_tag', "
        "'prospect_not_contactable', 'prospect_sms_consent_withdrawn'"
        ")",
    )


def downgrade() -> None:
    op.drop_index("idx_sms_send_logs_prospect_id", table_name="sms_send_logs")
    op.drop_column("sms_send_logs", "prospect_id")

    op.drop_constraint("check_dlq_reason", "sms_dead_letters", type_="check")
    op.create_check_constraint(
        "check_dlq_reason",
        "sms_dead_letters",
        "reason IN ("
        "'opt_out', 'delivery_failed', 'error', 'unresolvable', "
        "'quiet_hours', 'no_opt_in', 'subscriber_sms_frequency_cap'"
        ")",
    )
