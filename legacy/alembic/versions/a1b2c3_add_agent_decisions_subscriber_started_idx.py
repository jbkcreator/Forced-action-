"""add_agent_decisions_subscriber_started_idx

Revision ID: a1b2c3_timeline_idx
Revises: fa005_concierge_chat
Create Date: 2026-05-28

Adds composite index (subscriber_id, started_at DESC) on agent_decisions to
support the per-subscriber Cora touch timeline query. Built CONCURRENTLY so
it cannot run inside a transaction — apply via scripts/apply_timeline_index.py
rather than `alembic upgrade head`.
"""
from typing import Sequence, Union

revision: str = 'a1b2c3_timeline_idx'
down_revision: Union[str, Sequence[str], None] = 'fa005_concierge_chat'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Applied via scripts/apply_timeline_index.py (CONCURRENTLY requires autocommit).
    pass


def downgrade() -> None:
    pass
