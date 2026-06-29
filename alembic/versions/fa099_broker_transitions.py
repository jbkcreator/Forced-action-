"""broker_transitions table — Layer 3A of Broker State Machine

Append-only work-state audit log for brokers on Loan Lanes.
Each row records one state transition; latest to_state is current state.

Uses IF NOT EXISTS so the migration is safe to run against a DB where these
objects were already created directly (e.g. dev environment bootstrapped via
direct SQL).

Revision ID: fa099_broker_transitions
Revises:     fa098
Create Date: 2026-06-29
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "fa099_broker_transitions"
down_revision = "fa098"
branch_labels = None
depends_on = None

_STATES = (
    "unassigned", "assigned", "working",
    "quoted", "committed", "closed_won", "closed_lost",
)
_STATE_LIST = ", ".join(f"'{s}'" for s in _STATES)


def upgrade() -> None:
    op.execute(sa.text(f"""
        CREATE TABLE IF NOT EXISTS broker_transitions (
            transition_id   UUID        NOT NULL DEFAULT generate_uuidv7()
                                        CONSTRAINT broker_transitions_pkey PRIMARY KEY,
            lane_id         UUID        NOT NULL
                                        REFERENCES lanes(lane_id),
            prospect_id     UUID        NOT NULL
                                        REFERENCES prospects(prospect_id),
            broker_id       UUID        NOT NULL
                                        REFERENCES brokers(broker_id),
            from_state      VARCHAR(50) NOT NULL,
            to_state        VARCHAR(50) NOT NULL
                                        CONSTRAINT ck_bt_to_state
                                        CHECK (to_state IN ({_STATE_LIST})),
            reason_code     VARCHAR(100) NOT NULL,
            actor           VARCHAR(255) NOT NULL,
            occurred_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """))

    op.execute(sa.text(
        "CREATE INDEX IF NOT EXISTS idx_bt_lane_id "
        "ON broker_transitions (lane_id)"
    ))
    op.execute(sa.text(
        "CREATE INDEX IF NOT EXISTS idx_bt_prospect_id "
        "ON broker_transitions (prospect_id)"
    ))
    op.execute(sa.text(
        "CREATE INDEX IF NOT EXISTS idx_bt_broker_id "
        "ON broker_transitions (broker_id)"
    ))
    op.execute(sa.text(
        "CREATE INDEX IF NOT EXISTS idx_bt_lane_occurred "
        "ON broker_transitions (lane_id, occurred_at DESC)"
    ))


def downgrade() -> None:
    op.execute(sa.text("DROP INDEX IF EXISTS idx_bt_lane_occurred"))
    op.execute(sa.text("DROP INDEX IF EXISTS idx_bt_broker_id"))
    op.execute(sa.text("DROP INDEX IF EXISTS idx_bt_prospect_id"))
    op.execute(sa.text("DROP INDEX IF EXISTS idx_bt_lane_id"))
    op.execute(sa.text("DROP TABLE IF EXISTS broker_transitions"))
