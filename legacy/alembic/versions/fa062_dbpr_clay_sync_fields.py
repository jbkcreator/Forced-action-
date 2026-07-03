"""fa062_dbpr_clay_sync_fields — add clay_synced and clay_synced_at to dbpr_contacts

Tracks whether each certified contractor has been pushed to the Clay CRM
webhook so the daily sync task can skip already-sent rows.

Also merges the fa057_synthflow_inbound branch (broken down_revision in that
file caused it to float as an unmerged head; fa061 was the DB state).

Revision ID: fa062
Revises:     fa061, fa057_synthflow_inbound
Create Date: 2026-06-02
"""

import sqlalchemy as sa
from alembic import op

revision = "fa062"
down_revision = ("fa061", "fa057_synthflow_inbound")
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "dbpr_contacts",
        sa.Column("clay_synced", sa.Boolean, nullable=False, server_default="false"),
    )
    op.add_column(
        "dbpr_contacts",
        sa.Column("clay_synced_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("idx_dbpr_clay_synced", "dbpr_contacts", ["clay_synced"])


def downgrade() -> None:
    op.drop_index("idx_dbpr_clay_synced", table_name="dbpr_contacts")
    op.drop_column("dbpr_contacts", "clay_synced_at")
    op.drop_column("dbpr_contacts", "clay_synced")
