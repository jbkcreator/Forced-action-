"""owners.contactability_detail — cross-source triangulation evidence (ADR 0015)

Revision ID: fa078_contactability_detail
Revises: fa077_master_weekly_refresh

Record-only migration per house convention: DDL is applied by
scripts/apply_contactability_detail_migration.py, never `alembic upgrade`
(multiple heads exist; the script is idempotent).
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision: str = "fa078_contactability_detail"
down_revision: Union[str, None] = "fa077_master_weekly_refresh"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("owners", sa.Column("contactability_detail", JSONB, nullable=True))


def downgrade() -> None:
    op.drop_column("owners", "contactability_detail")
