"""widen sms_dead_letters.reason check constraint to include quiet_hours and no_opt_in

The sms_compliance.send_sms gate writes reason='quiet_hours' to the DLQ when
TCPA quiet hours block a send, but the previous CHECK constraint only allowed
four values — causing every quiet-hours flush to crash and silently lose the row.
Add 'quiet_hours' now and pre-emptively add 'no_opt_in' for the V4 opt-in gate.

Revision ID: fa020_dlq_reason_widen
Revises:     fa019_dbpr_contacts
Create Date: 2026-05-14
"""

from alembic import op


revision = "fa020_dlq_reason_widen"
down_revision = "fa019_dbpr_contacts"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_constraint("check_dlq_reason", "sms_dead_letters", type_="check")
    op.create_check_constraint(
        "check_dlq_reason",
        "sms_dead_letters",
        "reason IN ('opt_out', 'delivery_failed', 'error', 'unresolvable', 'quiet_hours', 'no_opt_in')",
    )


def downgrade() -> None:
    op.drop_constraint("check_dlq_reason", "sms_dead_letters", type_="check")
    op.create_check_constraint(
        "check_dlq_reason",
        "sms_dead_letters",
        "reason IN ('opt_out', 'delivery_failed', 'error', 'unresolvable')",
    )
