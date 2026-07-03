"""fa063_dbpr_work_email_linkedin — add work_email and linkedin_url to dbpr_contacts

Revision ID: fa063
Revises:     fa062
Create Date: 2026-06-02
"""

import sqlalchemy as sa
from alembic import op

revision = "fa063"
down_revision = "fa062"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "dbpr_contacts",
        sa.Column("work_email", sa.String(200), nullable=True),
    )
    op.add_column(
        "dbpr_contacts",
        sa.Column("linkedin_url", sa.String(500), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("dbpr_contacts", "linkedin_url")
    op.drop_column("dbpr_contacts", "work_email")
