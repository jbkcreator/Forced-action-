"""widen api_usage_logs.service check constraint to include 'telnyx'

The Twilio → Telnyx hard-cut migration introduces new rows with
`service='telnyx'` in api_usage_logs. The existing CHECK constraint
`service IN ('claude', 'twilio', 'stripe')` would reject these inserts
with `CheckViolation`. Widen the allowed set to include both 'telnyx'
(forward) and keep 'twilio' (legacy historical rows) so existing data
remains valid.

Revision ID: fa015_api_telnyx
Revises:     fa014_webhook_events
Create Date: 2026-05-11
"""

from alembic import op


revision = "fa015_api_telnyx"
down_revision = "fa014_webhook_events"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_constraint("check_api_service", "api_usage_logs", type_="check")
    op.create_check_constraint(
        "check_api_service",
        "api_usage_logs",
        "service IN ('claude', 'telnyx', 'stripe', 'twilio')",
    )


def downgrade() -> None:
    op.drop_constraint("check_api_service", "api_usage_logs", type_="check")
    op.create_check_constraint(
        "check_api_service",
        "api_usage_logs",
        "service IN ('claude', 'twilio', 'stripe')",
    )
