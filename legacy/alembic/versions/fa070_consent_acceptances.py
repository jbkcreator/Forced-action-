"""
add_consent_acceptances

Revision ID: fa070_consent_acceptances
Revises: fa075_incident_source_meta
Create Date: 2026-06-04 16:30:00.000000
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

# revision identifiers, used by Alembic.
revision: str = "fa070_consent_acceptances"
down_revision: Union[str, None] = "fa075_incident_source_meta"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "consent_acceptances",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("subscriber_id", sa.Integer(), sa.ForeignKey("subscribers.id"), nullable=True, index=True),
        sa.Column("waitlist_entry_id", sa.BigInteger(), sa.ForeignKey("waitlist_entries.id"), nullable=True, index=True),
        sa.Column("phone", sa.String(20), nullable=True, index=True),
        sa.Column("email", sa.String(255), nullable=False, index=True),

        # T&C acceptance (always required)
        sa.Column("terms_version", sa.String(20), nullable=False),
        sa.Column("privacy_version", sa.String(20), nullable=False),
        sa.Column("accepted_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("source_flow", sa.String(30), nullable=False, server_default="waitlist"),
        sa.Column("ip_address", sa.String(45), nullable=True),
        sa.Column("user_agent", sa.Text(), nullable=True),

        # Scroll-to-bottom timestamps
        sa.Column("modal_opened_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("modal_scrolled_to_end_at", sa.DateTime(timezone=True), nullable=True),

        # Accepted-text hash (SHA-256)
        sa.Column("accepted_text_hash", sa.String(64), nullable=False),

        # TCPA marketing consent (optional)
        sa.Column("tcpa_consent_text", sa.Text(), nullable=True),
        sa.Column("tcpa_consent_version", sa.String(20), nullable=True),
        sa.Column("tcpa_checked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("consent_scope", sa.String(30), nullable=True),
        sa.Column("not_condition_of_purchase_ack", sa.Boolean(), nullable=True),

        # Audit
        sa.Column("county_id", sa.String(50), nullable=True, server_default="hillsborough"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),

        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(
            "source_flow IN ('waitlist','signup','checkout','county_launch','free_signup')",
            name="ck_consent_source_flow",
        ),
        sa.CheckConstraint(
            "consent_scope IS NULL OR consent_scope IN ('marketing','waitlist_notify','lead_alerts')",
            name="ck_consent_scope",
        ),
    )

    op.create_index("idx_consent_email", "consent_acceptances", ["email"])
    op.create_index("idx_consent_accepted_at", "consent_acceptances", ["accepted_at"])
    op.create_index("idx_consent_subscriber", "consent_acceptances", ["subscriber_id"])
    op.create_index("idx_consent_waitlist", "consent_acceptances", ["waitlist_entry_id"])

    # Update SmsOptIn source CHECK constraint to include 'consent_form'
    op.execute(
        "ALTER TABLE sms_opt_ins DROP CONSTRAINT IF EXISTS check_opt_in_source"
    )
    op.create_check_constraint(
        "check_opt_in_source",
        "sms_opt_ins",
        "source IN ('double_opt_in','manual','import','widget','waitlist_form',"
        "'synthflow_inbound','missed_call_inbound','consent_form')",
    )


def downgrade() -> None:
    op.drop_table("consent_acceptances")

    # Restore the original SmsOptIn source CHECK constraint
    op.execute(
        "ALTER TABLE sms_opt_ins DROP CONSTRAINT IF EXISTS check_opt_in_source"
    )
    op.create_check_constraint(
        "check_opt_in_source",
        "sms_opt_ins",
        sa.schema.CheckConstraint(
            "source IN ('double_opt_in','manual','import','widget','waitlist_form',"
            "'synthflow_inbound','missed_call_inbound')",
            name="check_opt_in_source",
        ),
    )