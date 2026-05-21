"""fa029 — add normalized_address column to properties

Adds a canonical `normalized_address String(255)` column alongside the
existing raw `address` column, plus a btree index and a pg_trgm GIN index
mirroring the ones on `address`. Both the property ingestion path
(MasterPropertyLoader) and the match-time waterfall in BaseLoader read this
column so address normalization is now a single shared layer.

Also merges the two open fa028 heads.

Revision ID: fa029_add_normalized_address
Revises:     fa028_merge_scoring_indexes, fa028_unmatched_tiered_confidence
Create Date: 2026-05-21
"""

import logging
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

logger = logging.getLogger(__name__)

revision: str = "fa029_add_normalized_address"
down_revision: Union[str, Sequence[str], None] = (
    "fa028_merge_scoring_indexes",
    "fa028_unmatched_tiered_confidence",
)
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _trgm_installed(conn) -> bool:
    result = conn.execute(
        sa.text("SELECT 1 FROM pg_extension WHERE extname = 'pg_trgm'")
    ).fetchone()
    return result is not None


def upgrade() -> None:
    op.add_column(
        "properties",
        sa.Column("normalized_address", sa.String(length=255), nullable=True),
    )
    op.create_index(
        "idx_property_normalized_address",
        "properties",
        ["normalized_address"],
    )

    conn = op.get_bind()
    if _trgm_installed(conn):
        op.execute(
            "CREATE INDEX IF NOT EXISTS idx_property_normalized_address_trgm "
            "ON properties USING gin(normalized_address gin_trgm_ops)"
        )
    else:
        logger.warning(
            "pg_trgm extension not installed — skipping trigram index on "
            "properties.normalized_address. Re-run this migration after the "
            "extension is added to create the index."
        )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_property_normalized_address_trgm")
    op.drop_index("idx_property_normalized_address", table_name="properties")
    op.drop_column("properties", "normalized_address")
