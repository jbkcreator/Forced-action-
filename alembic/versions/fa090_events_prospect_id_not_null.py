"""Enforce prospect_id NOT NULL on events — spec §1.2 requires it on every event

Revision ID: fa090_events_prospect_id_not_null
Revises: fa089_prospect_merge_events
"""
from typing import Sequence, Union

from alembic import op

revision: str = "fa090_events_prospect_id_not_null"
down_revision: Union[str, Sequence[str]] = "fa089_prospect_merge_events"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column("events", "prospect_id", nullable=False)


def downgrade() -> None:
    op.alter_column("events", "prospect_id", nullable=True)
