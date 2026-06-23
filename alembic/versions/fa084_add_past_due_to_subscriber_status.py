"""Add past_due to subscribers.status CHECK constraint

subscribers.status previously allowed: active | grace | churned | cancelled | paused | disputed
Stripe fires invoice.payment_failed with status=past_due, which the webhook handler was silently
remapping to 'active' because the DB would reject past_due. This migration adds the missing value
so past_due can be stored directly, enabling the delivery-throttle logic in M9.

Revision ID: fa084_add_past_due_to_subscriber_status
Revises: fa083_quora_topics
Create Date: 2026-06-22
"""
from typing import Sequence, Union

from alembic import op

revision: str = "fa084_add_past_due_to_subscriber_status"
down_revision: Union[str, None] = "fa083_quora_topics"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_VALID_OLD = "('active', 'grace', 'churned', 'cancelled', 'paused', 'disputed')"
_VALID_NEW = "('active', 'grace', 'churned', 'cancelled', 'paused', 'disputed', 'past_due')"


def upgrade() -> None:
    op.drop_constraint("check_subscriber_status", "subscribers", type_="check")
    op.create_check_constraint(
        "check_subscriber_status",
        "subscribers",
        f"status IN {_VALID_NEW}",
    )


def downgrade() -> None:
    # Any rows currently past_due must be moved before downgrading; the
    # constraint drop+recreate is instantaneous on Postgres (no table scan).
    op.execute(
        "UPDATE subscribers SET status = 'grace' WHERE status = 'past_due'"
    )
    op.drop_constraint("check_subscriber_status", "subscribers", type_="check")
    op.create_check_constraint(
        "check_subscriber_status",
        "subscribers",
        f"status IN {_VALID_OLD}",
    )
