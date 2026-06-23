"""Add merge_events table and merged_into_id on prospects

Revision ID: fa089_prospect_merge_events
Revises: fa088_drop_enrichment_provenance
"""
from typing import Sequence, Union

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB, UUID as PG_UUID
from alembic import op

revision: str = "fa089_prospect_merge_events"
down_revision: Union[str, Sequence[str]] = "fa088_drop_enrichment_provenance"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Self-referencing merge pointer on prospects
    op.add_column(
        "prospects",
        sa.Column("merged_into_id", PG_UUID(as_uuid=True), nullable=True),
    )
    op.create_foreign_key(
        "fk_prospects_merged_into_id",
        "prospects", "prospects",
        ["merged_into_id"], ["prospect_id"],
        ondelete="SET NULL",
    )
    op.create_index(
        "idx_prospects_merged_into",
        "prospects", ["merged_into_id"],
        postgresql_where=sa.text("merged_into_id IS NOT NULL"),
    )

    # Merge audit log
    op.create_table(
        "merge_events",
        sa.Column("merge_id", PG_UUID(as_uuid=True), primary_key=True,
                  server_default=sa.text("generate_uuidv7()")),
        sa.Column("surviving_id", PG_UUID(as_uuid=True), nullable=False),
        sa.Column("merged_id", PG_UUID(as_uuid=True), nullable=False),
        sa.Column("field_decisions", JSONB(), nullable=False,
                  server_default=sa.text("'{}'::jsonb")),
        sa.Column("merged_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("NOW()")),
        sa.ForeignKeyConstraint(
            ["surviving_id"], ["prospects.prospect_id"],
            name="fk_merge_events_surviving_id",
        ),
        sa.ForeignKeyConstraint(
            ["merged_id"], ["prospects.prospect_id"],
            name="fk_merge_events_merged_id",
        ),
    )
    op.create_index("idx_merge_events_surviving_id", "merge_events", ["surviving_id"])
    op.create_index("idx_merge_events_merged_id", "merge_events", ["merged_id"])


def downgrade() -> None:
    op.drop_index("idx_merge_events_merged_id", table_name="merge_events")
    op.drop_index("idx_merge_events_surviving_id", table_name="merge_events")
    op.drop_table("merge_events")
    op.drop_index("idx_prospects_merged_into", table_name="prospects")
    op.drop_constraint("fk_prospects_merged_into_id", "prospects", type_="foreignkey")
    op.drop_column("prospects", "merged_into_id")
