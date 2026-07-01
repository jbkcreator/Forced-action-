"""Add outbound staging columns to enriched_contacts (fa5.3)

Revision ID: fa_outbound_pacing
Revises: fa_a4_enrichment_anomaly
Create Date: 2026-06-30
"""
from alembic import op
import sqlalchemy as sa

revision = "fa_outbound_pacing"
down_revision = "fa_a4_enrichment_anomaly"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "enriched_contacts",
        sa.Column("outbound_queued_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "enriched_contacts",
        sa.Column("first_touch_sent_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "enriched_contacts",
        sa.Column("outbound_terminal", sa.Boolean(), nullable=True, server_default=sa.text("FALSE")),
    )
    op.create_index(
        "idx_ec_outbound_queued",
        "enriched_contacts",
        ["outbound_queued_at"],
        postgresql_where=sa.text("outbound_queued_at IS NOT NULL"),
    )


def downgrade() -> None:
    op.drop_index("idx_ec_outbound_queued", table_name="enriched_contacts")
    op.drop_column("enriched_contacts", "outbound_terminal")
    op.drop_column("enriched_contacts", "first_touch_sent_at")
    op.drop_column("enriched_contacts", "outbound_queued_at")
