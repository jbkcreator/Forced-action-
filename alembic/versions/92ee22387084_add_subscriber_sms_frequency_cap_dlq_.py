"""add_subscriber_sms_frequency_cap_dlq_reason

Revision ID: 92ee22387084
Revises: fa027_agent_decisions_variant_id
Create Date: 2026-05-20 17:37:36.429072

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '92ee22387084'
down_revision: Union[str, Sequence[str], None] = 'fa027_agent_decisions_variant_id'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.drop_constraint("check_dlq_reason", "sms_dead_letters", type_="check")
    op.create_check_constraint(
        "check_dlq_reason",
        "sms_dead_letters",
        "reason IN ('opt_out', 'delivery_failed', 'error', 'unresolvable', 'quiet_hours', 'no_opt_in', 'subscriber_sms_frequency_cap')",
    )


def downgrade() -> None:
    op.drop_constraint("check_dlq_reason", "sms_dead_letters", type_="check")
    op.create_check_constraint(
        "check_dlq_reason",
        "sms_dead_letters",
        "reason IN ('opt_out', 'delivery_failed', 'error', 'unresolvable', 'quiet_hours', 'no_opt_in')",
    )
