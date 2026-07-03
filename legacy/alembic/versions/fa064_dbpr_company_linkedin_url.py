"""fa064_dbpr_company_linkedin_url — add company_linkedin_url to dbpr_contacts

Revision ID: fa064
Revises:     fa063
Create Date: 2026-06-02
"""

import sqlalchemy as sa
from alembic import op

revision = "fa064"
down_revision = "fa063"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "dbpr_contacts",
        sa.Column("company_linkedin_url", sa.String(500), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("dbpr_contacts", "company_linkedin_url")
