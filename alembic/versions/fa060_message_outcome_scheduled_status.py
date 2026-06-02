"""fa060_message_outcome_scheduled_status

Stage 12 — add 'scheduled' to the message_outcomes.send_status check constraint.

The bankruptcy-alert post-signup invite writes a message_outcomes row in a
'scheduled' state (due at T + BANKRUPTCY_INVITE_DELAY_MINUTES) that the invite
sweep later flips to 'sent'. The fa047 constraint
(check_mo_send_status) didn't include 'scheduled', so the insert was rejected.

Purely a constraint widening — no data change. Existing rows are unaffected
(their statuses are all still in the allowed set).

Revision ID: fa060
Revises:     fa059
Create Date: 2026-06-01
"""

from typing import Sequence, Union

from alembic import op


revision: str = "fa060"
down_revision: Union[str, Sequence[str], None] = "fa059"
branch_labels = None
depends_on = None

_OLD = "send_status IN ('pending_review','approved','sent','cancelled','failed','expired')"
_NEW = "send_status IN ('pending_review','approved','sent','cancelled','failed','expired','scheduled')"


def upgrade() -> None:
    op.drop_constraint("check_mo_send_status", "message_outcomes", type_="check")
    op.create_check_constraint("check_mo_send_status", "message_outcomes", _NEW)


def downgrade() -> None:
    op.drop_constraint("check_mo_send_status", "message_outcomes", type_="check")
    op.create_check_constraint("check_mo_send_status", "message_outcomes", _OLD)
