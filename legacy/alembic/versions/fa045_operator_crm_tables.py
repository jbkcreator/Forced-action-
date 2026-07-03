"""fa045_operator_crm_tables

Adds subscriber_notes (operator notes on subscribers) and
deal_pipeline_events (audit trail for DealOutcome.pipeline_stage changes).

Revision ID: fa045_operator_crm
Revises:     fa044_conversion_attribution
Create Date: 2026-05-29
"""

import sqlalchemy as sa
from alembic import op


revision = "fa045_operator_crm"
down_revision = "fa044_conversion_attribution"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "subscriber_notes",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("subscriber_id", sa.Integer,
                  sa.ForeignKey("subscribers.id"), nullable=False),
        sa.Column("author_email", sa.String(255), nullable=False),
        sa.Column("body", sa.Text, nullable=False),
        sa.Column("pinned", sa.Boolean, nullable=False, server_default=sa.text("false")),
        sa.Column("created_at", sa.DateTime, nullable=False,
                  server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("updated_at", sa.DateTime, nullable=False,
                  server_default=sa.text("CURRENT_TIMESTAMP")),
    )
    op.create_index("idx_subscriber_notes_sub_pinned", "subscriber_notes",
                    ["subscriber_id", "pinned", "created_at"])

    op.create_table(
        "deal_pipeline_events",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("deal_id", sa.Integer,
                  sa.ForeignKey("deal_outcomes.id"), nullable=False),
        sa.Column("from_stage", sa.String(30), nullable=True),
        sa.Column("to_stage", sa.String(30), nullable=False),
        sa.Column("changed_by", sa.String(255), nullable=False),
        sa.Column("note", sa.Text, nullable=True),
        sa.Column("created_at", sa.DateTime, nullable=False,
                  server_default=sa.text("CURRENT_TIMESTAMP")),
    )
    op.create_index("idx_deal_pipeline_events_deal_created",
                    "deal_pipeline_events", ["deal_id", "created_at"])


def downgrade() -> None:
    op.drop_index("idx_deal_pipeline_events_deal_created",
                  table_name="deal_pipeline_events")
    op.drop_table("deal_pipeline_events")
    op.drop_index("idx_subscriber_notes_sub_pinned", table_name="subscriber_notes")
    op.drop_table("subscriber_notes")
