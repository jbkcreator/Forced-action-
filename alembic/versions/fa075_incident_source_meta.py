"""Add source_meta JSONB to incidents table

Revision ID: fa075_incident_source_meta
Revises: fa074_win_autopsy_card
Create Date: 2026-06-09
"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

revision: str = "fa075_incident_source_meta"
down_revision: Union[str, Sequence[str], None] = "fa074_win_autopsy_card"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("incidents", sa.Column("source_meta", JSONB, nullable=True))


def downgrade() -> None:
    op.drop_column("incidents", "source_meta")
