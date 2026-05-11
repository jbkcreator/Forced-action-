"""add webhook_events

Unified audit log across all webhook and vendor-callback sources:
Stripe, GHL inbound/outbound, Synthflow, BatchData polls, NWS alerts,
Twilio inbound. Best-effort writes; not used as an idempotency lock
(Stripe keeps its dedicated stripe_webhook_events for that). Stores
sanitized summaries only — no raw PII.

Revision ID: fa014_webhook_events
Revises:     fa013_phone_deliv
Create Date: 2026-05-11
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql


revision = "fa014_webhook_events"
down_revision = "fa013_phone_deliv"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "webhook_events",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("source", sa.String(30), nullable=False),
        sa.Column("event_type", sa.String(80), nullable=False),
        sa.Column(
            "direction", sa.String(10), nullable=False,
            server_default="inbound",
        ),
        sa.Column("source_event_id", sa.String(120), nullable=True),
        sa.Column(
            "status", sa.String(20), nullable=False,
            server_default="received",
        ),
        sa.Column("status_detail", sa.Text(), nullable=True),
        sa.Column(
            "subscriber_id", sa.Integer(),
            sa.ForeignKey("subscribers.id"), nullable=True,
        ),
        sa.Column(
            "property_id", sa.Integer(),
            sa.ForeignKey("properties.id"), nullable=True,
        ),
        sa.Column("payload_summary", postgresql.JSONB, nullable=True),
        sa.Column("duration_ms", sa.Integer(), nullable=True),
        sa.Column(
            "processed_at", sa.DateTime(timezone=True),
            server_default=sa.func.now(), nullable=False,
        ),
        sa.Column(
            "created_at", sa.DateTime(timezone=True),
            server_default=sa.func.now(), nullable=False,
        ),
        sa.CheckConstraint(
            "direction IN ('inbound', 'outbound')",
            name="check_webhook_event_direction",
        ),
        sa.CheckConstraint(
            "status IN ('received', 'processed', 'failed', 'duplicate', 'skipped')",
            name="check_webhook_event_status",
        ),
    )
    op.create_index("ix_webhook_events_source", "webhook_events", ["source"])
    op.create_index("ix_webhook_events_event_type", "webhook_events", ["event_type"])
    op.create_index("ix_webhook_events_source_event_id", "webhook_events", ["source_event_id"])
    op.create_index("ix_webhook_events_subscriber_id", "webhook_events", ["subscriber_id"])
    op.create_index("ix_webhook_events_property_id", "webhook_events", ["property_id"])
    op.create_index("ix_webhook_events_processed_at", "webhook_events", ["processed_at"])
    op.create_index(
        "idx_webhook_events_source_processed",
        "webhook_events",
        ["source", "processed_at"],
    )


def downgrade() -> None:
    for name in (
        "idx_webhook_events_source_processed",
        "ix_webhook_events_processed_at",
        "ix_webhook_events_property_id",
        "ix_webhook_events_subscriber_id",
        "ix_webhook_events_source_event_id",
        "ix_webhook_events_event_type",
        "ix_webhook_events_source",
    ):
        op.drop_index(name, table_name="webhook_events")
    op.drop_table("webhook_events")
