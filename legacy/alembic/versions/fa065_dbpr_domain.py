"""fa065_dbpr_domain — add domain to dbpr_contacts

Revision ID: fa065
Revises:     fa064
Create Date: 2026-06-02
"""

import sqlalchemy as sa
from alembic import op

revision = "fa065"
down_revision = "fa064"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "dbpr_contacts",
        sa.Column("domain", sa.String(255), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("dbpr_contacts", "domain")
