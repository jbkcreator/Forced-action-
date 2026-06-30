"""fa098_loan_lane_lenders_claim_tracking

Add lender tracking and claim metadata to the existing Loan Lane schema.

Revision ID: fa098
Revises:     fa097_subscriber_memory_summary
Create Date: 2026-06-29
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID as PG_UUID


revision = "fa098"
down_revision = "fa097_subscriber_memory_summary"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "lenders",
        sa.Column("lender_id", PG_UUID(as_uuid=True), primary_key=True, server_default=sa.text("generate_uuidv7()")),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("is_cleared", sa.Boolean, nullable=False, server_default=sa.text("false")),
        sa.Column("is_active", sa.Boolean, nullable=False, server_default=sa.text("true")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("NOW()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("NOW()")),
        sa.UniqueConstraint("name", name="uq_lenders_name"),
    )
    op.create_index("ix_lenders_name", "lenders", ["name"])

    op.add_column("lanes", sa.Column("lender_id", PG_UUID(as_uuid=True), nullable=True))
    op.add_column("lanes", sa.Column("claimed_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column(
        "lanes",
        sa.Column("last_activity_at", sa.DateTime(timezone=True), nullable=True, server_default=sa.text("NOW()")),
    )
    op.create_foreign_key("fk_lanes_lender_id_lenders", "lanes", "lenders", ["lender_id"], ["lender_id"])


def downgrade() -> None:
    op.drop_constraint("fk_lanes_lender_id_lenders", "lanes", type_="foreignkey")
    op.drop_column("lanes", "last_activity_at")
    op.drop_column("lanes", "claimed_at")
    op.drop_column("lanes", "lender_id")
    op.drop_index("ix_lenders_name", table_name="lenders")
    op.drop_table("lenders")
