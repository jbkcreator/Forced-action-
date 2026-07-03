"""owners.contactability_detail — cross-source triangulation evidence (ADR 0015)

Revision ID: fa078_contactability_detail
Revises: (none — standalone branch; DDL applied by scripts/apply_contactability_detail_migration.py)

Record-only migration per house convention: DDL is applied by
scripts/apply_contactability_detail_migration.py, never `alembic upgrade`
(multiple heads exist; the script is idempotent).

Note: down_revision was fa077_master_weekly_refresh but that file was never
committed. Set to None so Alembic can build its revision map.
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision: str = "fa078_contactability_detail"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("owners", sa.Column("contactability_detail", JSONB, nullable=True))


def downgrade() -> None:
    op.drop_column("owners", "contactability_detail")
