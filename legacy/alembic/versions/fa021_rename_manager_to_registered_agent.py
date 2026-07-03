"""fa021 — Rename manager_name/manager_title to registered_agent_name/registered_agent_address

Sunbiz scraper now targets the legally-appointed registered agent exclusively
rather than the Officers section. Widens the address column from String(50) to
String(500) to accommodate multi-line addresses.

Revision ID: fa021
Revises:     fa020_dlq_reason_widen
Create Date: 2026-05-14
"""

import sqlalchemy as sa
from alembic import op

revision = "fa021"
down_revision = "fa020_dlq_reason_widen"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column("owners", "manager_name", new_column_name="registered_agent_name")
    op.alter_column(
        "owners",
        "manager_title",
        new_column_name="registered_agent_address",
        existing_type=sa.String(50),
        type_=sa.String(500),
    )


def downgrade() -> None:
    op.alter_column(
        "owners",
        "registered_agent_address",
        new_column_name="manager_title",
        existing_type=sa.String(500),
        type_=sa.String(50),
    )
    op.alter_column("owners", "registered_agent_name", new_column_name="manager_name")
