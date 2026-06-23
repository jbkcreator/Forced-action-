"""Drop enrichment_provenance — superseded by enriched_contacts join via property_id

Revision ID: fa088_drop_enrichment_provenance
Revises: fa087_m1_shared_backbone
"""
from typing import Sequence, Union

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB, UUID as PG_UUID
from alembic import op

revision: str = "fa088_drop_enrichment_provenance"
down_revision: Union[str, Sequence[str]] = "fa087_m1_shared_backbone"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_index("idx_enrichment_provenance_prospect_id",
                  table_name="enrichment_provenance")
    op.drop_table("enrichment_provenance")


def downgrade() -> None:
    op.create_table(
        "enrichment_provenance",
        sa.Column("id", PG_UUID(as_uuid=True), primary_key=True,
                  server_default=sa.text("generate_uuidv7()")),
        sa.Column("prospect_id", PG_UUID(as_uuid=True), nullable=False),
        sa.Column("field_name", sa.String(), nullable=False),
        sa.Column("source", sa.String(), nullable=False),
        sa.Column("cost_cents", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("confidence", sa.Numeric(5, 4), nullable=True),
        sa.Column("acquired_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("NOW()")),
        sa.ForeignKeyConstraint(
            ["prospect_id"], ["prospects.prospect_id"],
            name="fk_enrichment_provenance_prospect_id",
            ondelete="CASCADE",
        ),
        sa.CheckConstraint(
            "source IN ('voter','appraiser','tracerfy','batchdata','idi')",
            name="ck_enrichment_provenance_source",
        ),
        sa.CheckConstraint(
            "confidence IS NULL OR (confidence >= 0 AND confidence <= 1)",
            name="ck_enrichment_provenance_confidence",
        ),
    )
    op.create_index(
        "idx_enrichment_provenance_prospect_id",
        "enrichment_provenance", ["prospect_id"],
    )
