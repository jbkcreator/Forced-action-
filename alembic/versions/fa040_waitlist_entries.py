"""fa040_waitlist_entries

Creates the waitlist_entries table (proper replacement for the
ZipTerritory.waitlist_emails array) and extends sms_opt_ins.source
constraint to include 'waitlist_form'.

NOTE: alembic CLI is unusable (multi-head tree). Do NOT run this via
`alembic upgrade head`. Apply DDL via:
    python scripts/apply_waitlist_migration.py

Revision ID: fa040_waitlist_entries
Revises:     fa039_remove_sandbox_outbox
Create Date: 2026-05-28
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "fa040_waitlist_entries"
down_revision: Union[str, Sequence[str], None] = "fa039_remove_sandbox_outbox"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "waitlist_entries",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("zip_code", sa.String(10), nullable=False),
        sa.Column("vertical", sa.String(50), nullable=False),
        sa.Column("county_id", sa.String(50), nullable=False),
        sa.Column("name", sa.String(120), nullable=False),
        sa.Column("email", sa.String(255), nullable=False),
        sa.Column("phone_e164", sa.String(20), nullable=True),
        sa.Column("sms_opt_in", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("waitlist_type", sa.String(20), nullable=False, server_default="sold_out"),
        sa.Column("signup_ip", sa.String(45), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.func.now()),
        sa.Column("notified_email_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("notified_sms_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("reactivation_decision_id", sa.String(36), nullable=True),
        sa.Column("status", sa.String(20), nullable=False, server_default="waiting"),
        sa.CheckConstraint(
            "status IN ('waiting','notified','converted','expired','opted_out','lost')",
            name="ck_waitlist_entries_status",
        ),
        sa.CheckConstraint(
            "waitlist_type IN ('coming_soon','sold_out')",
            name="ck_waitlist_entries_type",
        ),
        sa.CheckConstraint(
            "vertical IN ('roofing','restoration','public_adjusters',"
            "'wholesalers','fix_flip','attorneys')",
            name="ck_waitlist_entries_vertical",
        ),
        sa.UniqueConstraint(
            "zip_code", "vertical", "county_id", "email",
            name="uq_waitlist_zip_vert_county_email",
        ),
    )
    op.create_index("ix_waitlist_county_status", "waitlist_entries",
                    ["county_id", "status"])
    op.create_index("ix_waitlist_county_type_status", "waitlist_entries",
                    ["county_id", "waitlist_type", "status"])
    op.create_index("ix_waitlist_zip_vertical", "waitlist_entries",
                    ["zip_code", "vertical"])

    # Extend sms_opt_ins.source check constraint to include waitlist_form.
    op.drop_constraint("check_opt_in_source", "sms_opt_ins", type_="check")
    op.create_check_constraint(
        "check_opt_in_source",
        "sms_opt_ins",
        "source IN ('double_opt_in','manual','import','widget','waitlist_form')",
    )


def downgrade() -> None:
    op.drop_index("ix_waitlist_zip_vertical", table_name="waitlist_entries")
    op.drop_index("ix_waitlist_county_type_status", table_name="waitlist_entries")
    op.drop_index("ix_waitlist_county_status", table_name="waitlist_entries")
    op.drop_table("waitlist_entries")

    op.drop_constraint("check_opt_in_source", "sms_opt_ins", type_="check")
    op.create_check_constraint(
        "check_opt_in_source",
        "sms_opt_ins",
        "source IN ('double_opt_in','manual','import','widget')",
    )
