"""fa031 — add traced_name to enriched_contacts (multi-heir probate enrichment)

The skip-trace pipeline currently produces at most one EnrichedContact row per
property: the assessor owner or, when the property has filing-derived parties,
the highest-priority filing party (probate heir / eviction landlord / etc.).

Multi-heir probate filings store every heir in `legal_proceedings.meta_data->'heirs'`
but only the first heir is enriched today (the one stored in `secondary_party`).
This migration adds a `traced_name` column so the table can hold multiple rows
per property — one per traced individual — once the multi-heir extension is
turned on via `MULTI_HEIR_ENRICHMENT_ENABLED`.

The column is nullable so existing rows (single-trace era) keep working. New
single-heir rows can leave it NULL or fill it; multi-heir rows MUST fill it so
duplicates per (property_id, traced_name) can be detected on re-runs.

Revision ID: fa031_enriched_contact_traced_name
Revises:     fa57e134df37
Create Date: 2026-05-22
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "fa031_enriched_contact_traced_name"
down_revision: Union[str, Sequence[str], None] = "fa57e134df37"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "enriched_contacts",
        sa.Column("traced_name", sa.String(length=255), nullable=True),
    )
    # Partial index — most rows will remain NULL (single-trace path); only
    # multi-heir rows need fast (property_id, traced_name) dedup lookups.
    op.create_index(
        "idx_enriched_contacts_property_traced_name",
        "enriched_contacts",
        ["property_id", "traced_name"],
        postgresql_where=sa.text("traced_name IS NOT NULL"),
    )


def downgrade() -> None:
    op.drop_index(
        "idx_enriched_contacts_property_traced_name",
        table_name="enriched_contacts",
    )
    op.drop_column("enriched_contacts", "traced_name")
