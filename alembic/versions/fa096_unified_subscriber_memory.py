"""fa096 - unified subscriber memory table

Creates the `unified_subscriber_memory` audit-spine table aggregating
all external-interaction events (Stripe, GHL, SMS, Synthflow, underwriting).

NOTE: the alembic CLI is unusable in this tree (divergent multi-head history).
Apply operationally via:

    PYTHONPATH=. python scripts/apply_fa096_unified_subscriber_memory.py
"""
from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "fa096_unified_subscriber_memory"
down_revision: Union[str, Sequence[str], None] = "fa095_feedback_ritual_shared_queue_refactor"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "unified_subscriber_memory",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True,
                  server_default=sa.text("gen_random_uuid()")),
        sa.Column("subscriber_id", sa.String(100), nullable=False),
        sa.Column("property_id", sa.Integer, sa.ForeignKey("properties.id"),
                  nullable=True),
        sa.Column("stream_source", sa.String(50), nullable=False),
        sa.Column("event_type", sa.String(100), nullable=False),
        sa.Column("event_payload", postgresql.JSONB, nullable=False,
                  server_default="{}"),
        sa.Column("created_at", sa.DateTime(timezone=True),
                  server_default=sa.text("now()"), nullable=False),
    )

    op.create_check_constraint(
        "ck_usm_stream_source",
        "unified_subscriber_memory",
        sa.text("stream_source IN ('STRIPE', 'GHL', 'SMS', 'SYNTHFLOW', 'UNDERWRITING')"),
    )

    op.create_index(
        "idx_usm_subscriber_created",
        "unified_subscriber_memory",
        ["subscriber_id", "created_at"],
    )

    op.create_index(
        "idx_usm_property",
        "unified_subscriber_memory",
        ["property_id"],
        postgresql_where=sa.text("property_id IS NOT NULL"),
    )

    op.create_index(
        "idx_usm_stream_source",
        "unified_subscriber_memory",
        ["stream_source", "created_at"],
    )

    op.create_index(
        "idx_usm_event_type",
        "unified_subscriber_memory",
        ["event_type"],
    )


def downgrade() -> None:
    op.drop_table("unified_subscriber_memory")