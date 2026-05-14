"""fa017_owner_manager_fields — Add manager_name and manager_title to owners

Stores the LLC manager/member identity extracted from the Sunbiz Officers section.
This is the free enrichment layer — manager name + title written from Sunbiz filing,
no paid skip-trace required. Used as input for future BatchData/IDI lookups.

Revision ID: fa017_owner_manager_fields
Revises:     fa016_accel_wallet_push
Create Date: 2026-05-13
"""

import sqlalchemy as sa
from alembic import op

revision = "fa017_owner_manager_fields"
down_revision = "fa016_accel_wallet_push"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("owners", sa.Column("manager_name", sa.String(255), nullable=True))
    op.add_column("owners", sa.Column("manager_title", sa.String(50), nullable=True))


def downgrade() -> None:
    op.drop_column("owners", "manager_title")
    op.drop_column("owners", "manager_name")
