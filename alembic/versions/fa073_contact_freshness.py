"""fa073 - contact freshness lifecycle

Revision ID: fa073_contact_freshness
Revises: fa072_cora_event_queue
Create Date: 2026-06-08
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "fa073_contact_freshness"
down_revision: Union[str, Sequence[str], None] = "fa072_cora_event_queue"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("owners", sa.Column("contact_info_confidence", sa.String(20), nullable=True))
    op.add_column("owners", sa.Column("contact_info_confidence_score", sa.Numeric(4, 3), nullable=True))
    op.add_column("owners", sa.Column("contact_last_verified_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("owners", sa.Column("contact_next_refresh_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("owners", sa.Column("contact_refresh_status", sa.String(20), nullable=True))
    op.add_column("owners", sa.Column("contact_refresh_reason", sa.String(120), nullable=True))
    op.create_index("idx_owner_contact_confidence", "owners", ["contact_info_confidence"])
    op.create_index("idx_owner_contact_next_refresh", "owners", ["contact_next_refresh_at"])
    op.create_check_constraint(
        "check_owner_contact_info_confidence",
        "owners",
        "contact_info_confidence IS NULL OR contact_info_confidence IN ('high','medium','low','stale')",
    )
    op.create_check_constraint(
        "check_owner_contact_refresh_status",
        "owners",
        "contact_refresh_status IS NULL OR contact_refresh_status IN ('fresh','due','queued','refreshed','failed')",
    )

    op.add_column("enriched_contacts", sa.Column("verification_status", sa.String(20), nullable=True))
    op.add_column("enriched_contacts", sa.Column("superseded_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("enriched_contacts", sa.Column("superseded_by_contact_id", sa.Integer(), nullable=True))
    op.create_foreign_key(
        "fk_enriched_contacts_superseded_by",
        "enriched_contacts",
        "enriched_contacts",
        ["superseded_by_contact_id"],
        ["id"],
    )
    op.create_index("idx_enriched_verification_status", "enriched_contacts", ["verification_status"])
    op.create_check_constraint(
        "check_enriched_verification_status",
        "enriched_contacts",
        "verification_status IS NULL OR verification_status IN ('valid','invalid','unknown')",
    )


def downgrade() -> None:
    op.drop_constraint("check_enriched_verification_status", "enriched_contacts", type_="check")
    op.drop_index("idx_enriched_verification_status", table_name="enriched_contacts")
    op.drop_constraint("fk_enriched_contacts_superseded_by", "enriched_contacts", type_="foreignkey")
    op.drop_column("enriched_contacts", "superseded_by_contact_id")
    op.drop_column("enriched_contacts", "superseded_at")
    op.drop_column("enriched_contacts", "verification_status")

    op.drop_constraint("check_owner_contact_refresh_status", "owners", type_="check")
    op.drop_constraint("check_owner_contact_info_confidence", "owners", type_="check")
    op.drop_index("idx_owner_contact_next_refresh", table_name="owners")
    op.drop_index("idx_owner_contact_confidence", table_name="owners")
    op.drop_column("owners", "contact_refresh_reason")
    op.drop_column("owners", "contact_refresh_status")
    op.drop_column("owners", "contact_next_refresh_at")
    op.drop_column("owners", "contact_last_verified_at")
    op.drop_column("owners", "contact_info_confidence_score")
    op.drop_column("owners", "contact_info_confidence")
