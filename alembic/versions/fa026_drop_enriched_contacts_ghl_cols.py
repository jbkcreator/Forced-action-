"""fa026 — drop dead ghl_contact_id / ghl_synced_at from enriched_contacts

These columns were never written to by any code path. GHL contact IDs for
properties live on properties.gohighlevel_contact_id. The enriched_contacts
columns were placeholders that were never wired up.

Revision ID: fa026_drop_ec_ghl_cols
Revises:     fa025_merge
Create Date: 2026-05-19
"""

import sqlalchemy as sa
from alembic import op

revision = "fa026_drop_ec_ghl_cols"
down_revision = "fa025_merge"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_index("idx_enriched_contacts_ghl_contact_id", table_name="enriched_contacts", if_exists=True)
    op.drop_column("enriched_contacts", "ghl_contact_id")
    op.drop_column("enriched_contacts", "ghl_synced_at")


def downgrade() -> None:
    op.add_column("enriched_contacts", sa.Column("ghl_synced_at", sa.DateTime(), nullable=True))
    op.add_column("enriched_contacts", sa.Column("ghl_contact_id", sa.String(100), nullable=True))
    op.create_index("idx_enriched_contacts_ghl_contact_id", "enriched_contacts", ["ghl_contact_id"])
