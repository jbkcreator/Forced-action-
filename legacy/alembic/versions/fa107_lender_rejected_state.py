"""Add lender_rejected to broker_transitions ck_bt_to_state check constraint.

The ck_bt_to_state constraint was created in fa099 without lender_rejected.
Drop and recreate it to include the new state.

Revision ID: fa107_lender_rejected_state
Revises:     fa106_lanes_property_anchor
Create Date: 2026-06-30
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "fa107_lender_rejected_state"
down_revision = "fa106_lanes_property_anchor"
branch_labels = None
depends_on = None

_STATES_OLD = (
    "unassigned", "assigned", "working",
    "quoted", "committed", "closed_won", "closed_lost",
)
_STATES_NEW = (
    "unassigned", "assigned", "working",
    "quoted", "committed", "lender_rejected", "closed_won", "closed_lost",
)


def upgrade() -> None:
    op.execute(sa.text("ALTER TABLE broker_transitions DROP CONSTRAINT IF EXISTS ck_bt_to_state"))
    state_list = ", ".join(f"'{s}'" for s in _STATES_NEW)
    op.execute(sa.text(
        f"ALTER TABLE broker_transitions ADD CONSTRAINT ck_bt_to_state "
        f"CHECK (to_state IN ({state_list}))"
    ))


def downgrade() -> None:
    op.execute(sa.text("ALTER TABLE broker_transitions DROP CONSTRAINT IF EXISTS ck_bt_to_state"))
    state_list = ", ".join(f"'{s}'" for s in _STATES_OLD)
    op.execute(sa.text(
        f"ALTER TABLE broker_transitions ADD CONSTRAINT ck_bt_to_state "
        f"CHECK (to_state IN ({state_list}))"
    ))
