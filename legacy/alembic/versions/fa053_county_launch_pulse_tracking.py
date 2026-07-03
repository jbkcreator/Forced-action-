"""fa053_county_launch_pulse_tracking

Revision ID: fa053
Revises: 5e27255260d1
Create Date: 2026-05-29

Adds revenue_pulse_sent_at to expansion_candidates (T+24 idempotency guard).
Extends ck_county_launch_audit_event to include 'waitlist_notified' and
'revenue_pulse_sent' — previously these event types were blocked by the
CHECK constraint.
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = 'fa053'
down_revision: Union[str, None] = '5e27255260d1'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        'expansion_candidates',
        sa.Column('revenue_pulse_sent_at', sa.DateTime(timezone=True), nullable=True),
    )

    op.drop_constraint('ck_county_launch_audit_event', 'county_launch_audit', type_='check')
    op.create_check_constraint(
        'ck_county_launch_audit_event',
        'county_launch_audit',
        "event_type IN ("
        "'evaluated','posted','approved','rejected','launch_started',"
        "'launch_aborted_gate_red','launched','cooldown_skipped',"
        "'waitlist_notified','revenue_pulse_sent'"
        ")",
    )


def downgrade() -> None:
    op.drop_constraint('ck_county_launch_audit_event', 'county_launch_audit', type_='check')
    op.create_check_constraint(
        'ck_county_launch_audit_event',
        'county_launch_audit',
        "event_type IN ("
        "'evaluated','posted','approved','rejected','launch_started',"
        "'launch_aborted_gate_red','launched','cooldown_skipped'"
        ")",
    )
    op.drop_column('expansion_candidates', 'revenue_pulse_sent_at')
