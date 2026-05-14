"""fa019_dbpr_contacts — DBPR contractor contact registry

One row per unique DBPR license number. Weekly file from myfloridalicense.com
replaces all source data; loader upserts on license_number so enrichment
(email/phone from BatchData) and email campaign state survive each sync.

Revision ID: fa019_dbpr_contacts
Revises:     fa018_nws_alerts_table, 13847230a4d6 (merge)
Create Date: 2026-05-14
"""

import sqlalchemy as sa
from alembic import op

revision = "fa019_dbpr_contacts"
down_revision = ("fa018_nws_alerts_table", "13847230a4d6")
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "dbpr_contacts",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),

        # Identity
        sa.Column("license_number", sa.String(30), unique=True, nullable=False),
        sa.Column("license_type_code", sa.String(10), nullable=False),
        sa.Column("license_type_desc", sa.String(60), nullable=True),
        sa.Column("full_name", sa.String(200), nullable=False),
        sa.Column("address", sa.String(255), nullable=True),
        sa.Column("city", sa.String(100), nullable=True),
        sa.Column("state", sa.String(5), nullable=True, server_default="FL"),
        sa.Column("zip_code", sa.String(10), nullable=True),
        sa.Column("county_id", sa.String(50), nullable=True),
        sa.Column("license_expiry", sa.Date, nullable=True),
        sa.Column("data_source", sa.String(20), nullable=False, server_default="certified"),

        # Vertical
        sa.Column("vertical", sa.String(50), nullable=True),

        # Enrichment
        sa.Column("email", sa.String(200), nullable=True),
        sa.Column("phone", sa.String(20), nullable=True),
        sa.Column("enrichment_status", sa.String(20), nullable=False, server_default="pending"),
        sa.Column("enrichment_attempted_at", sa.DateTime(timezone=True), nullable=True),

        # Email campaign
        sa.Column("email_status", sa.String(20), nullable=False, server_default="not_sent"),
        sa.Column("email_sent_at", sa.DateTime(timezone=True), nullable=True),

        # Signup tracking
        sa.Column("subscriber_id", sa.Integer, sa.ForeignKey("subscribers.id"), nullable=True),
        sa.Column("signed_up_at", sa.DateTime(timezone=True), nullable=True),

        # Sync metadata
        sa.Column("last_synced_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("NOW()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("NOW()"), nullable=False),

        # Constraints
        sa.CheckConstraint(
            "enrichment_status IN ('pending', 'enriched', 'failed', 'skipped')",
            name="check_dbpr_enrichment_status",
        ),
        sa.CheckConstraint(
            "email_status IN ('not_sent', 'sent', 'bounced', 'signed_up', 'opted_out')",
            name="check_dbpr_email_status",
        ),
        sa.CheckConstraint(
            "data_source IN ('certified', 'registered')",
            name="check_dbpr_data_source",
        ),
    )

    op.create_index("ix_dbpr_license_number", "dbpr_contacts", ["license_number"], unique=True)
    op.create_index("ix_dbpr_zip_code", "dbpr_contacts", ["zip_code"])
    op.create_index("ix_dbpr_county_id", "dbpr_contacts", ["county_id"])
    op.create_index("ix_dbpr_vertical", "dbpr_contacts", ["vertical"])
    op.create_index("ix_dbpr_county_vertical", "dbpr_contacts", ["county_id", "vertical"])
    op.create_index("ix_dbpr_enrichment_status", "dbpr_contacts", ["enrichment_status"])
    op.create_index("ix_dbpr_email_status", "dbpr_contacts", ["email_status"])
    op.create_index("ix_dbpr_subscriber_id", "dbpr_contacts", ["subscriber_id"])
    op.create_index("ix_dbpr_last_synced", "dbpr_contacts", ["last_synced_at"])


def downgrade() -> None:
    op.drop_index("ix_dbpr_last_synced", table_name="dbpr_contacts")
    op.drop_index("ix_dbpr_subscriber_id", table_name="dbpr_contacts")
    op.drop_index("ix_dbpr_email_status", table_name="dbpr_contacts")
    op.drop_index("ix_dbpr_enrichment_status", table_name="dbpr_contacts")
    op.drop_index("ix_dbpr_county_vertical", table_name="dbpr_contacts")
    op.drop_index("ix_dbpr_vertical", table_name="dbpr_contacts")
    op.drop_index("ix_dbpr_county_id", table_name="dbpr_contacts")
    op.drop_index("ix_dbpr_zip_code", table_name="dbpr_contacts")
    op.drop_index("ix_dbpr_license_number", table_name="dbpr_contacts")
    op.drop_table("dbpr_contacts")
