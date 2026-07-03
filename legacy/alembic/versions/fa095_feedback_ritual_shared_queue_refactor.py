"""fa095 - feedback ritual shared queue refactor

Follow-on to fa094_a6_cora_training_overrides.

Extends the shared `cora_training_overrides` table so it can store both:
  - A6 property-level Teaching Corrections
  - 4.3 Cora-Touch Feedback Ritual rows

Changes:
  - rename `subject_id` -> `subject_ref` and widen to string
  - make `correction_reason` nullable (ritual rows are queued pre-review)
  - add 4.3 review/snapshot columns
  - replace the old A6-only unique index with:
      * A6 partial unique index on active property corrections
      * 4.3 partial unique index on one row per Cora Touch

NOTE: the alembic CLI is unusable in this tree (divergent multi-head history).
Apply operationally via:

    PYTHONPATH=. python scripts/apply_fa095_feedback_ritual_queue_refactor.py
"""
from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "fa095_feedback_ritual_shared_queue_refactor"
down_revision: Union[str, Sequence[str], None] = "fa094_a6_cora_training_overrides"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.alter_column(
        "cora_training_overrides",
        "subject_id",
        new_column_name="subject_ref",
        existing_type=sa.Integer(),
        type_=sa.String(length=80),
        postgresql_using="subject_id::varchar",
        existing_nullable=False,
    )

    op.alter_column(
        "cora_training_overrides",
        "correction_reason",
        existing_type=sa.String(length=40),
        nullable=True,
    )

    op.add_column(
        "cora_training_overrides",
        sa.Column("corrected_output", sa.Text(), nullable=True),
    )
    op.add_column(
        "cora_training_overrides",
        sa.Column("review_outcome", sa.String(length=30), nullable=True),
    )
    op.add_column(
        "cora_training_overrides",
        sa.Column("reviewed_by", sa.String(length=120), nullable=True),
    )
    op.add_column(
        "cora_training_overrides",
        sa.Column("reviewed_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "cora_training_overrides",
        sa.Column("snapshot_payload", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    )
    op.add_column(
        "cora_training_overrides",
        sa.Column("source_metadata", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    )

    op.execute("DROP INDEX IF EXISTS uq_cora_override_active")
    op.drop_index("idx_cora_overrides_subject_active", table_name="cora_training_overrides")

    op.create_index(
        "idx_cora_overrides_subject_active",
        "cora_training_overrides",
        ["subject_type", "subject_ref", "dampener_active"],
    )

    op.execute(
        """
        CREATE UNIQUE INDEX uq_cora_override_active
        ON cora_training_overrides (
            subject_ref,
            correction_reason,
            COALESCE(signal_type, '')
        )
        WHERE dampener_active
          AND subject_type = 'property'
          AND correction_reason IS NOT NULL
        """
    )
    op.execute(
        """
        CREATE UNIQUE INDEX uq_cora_feedback_ritual_subject
        ON cora_training_overrides (subject_type, subject_ref)
        WHERE source = 'feedback_ritual'
        """
    )

    op.create_check_constraint(
        "ck_cora_overrides_review_outcome",
        "cora_training_overrides",
        "review_outcome IS NULL OR review_outcome IN ('approved', 'needs_correction', 'discarded')",
    )


def downgrade() -> None:
    op.drop_constraint("ck_cora_overrides_review_outcome", "cora_training_overrides", type_="check")
    op.execute("DROP INDEX IF EXISTS uq_cora_feedback_ritual_subject")
    op.execute("DROP INDEX IF EXISTS uq_cora_override_active")
    op.drop_index("idx_cora_overrides_subject_active", table_name="cora_training_overrides")

    op.drop_column("cora_training_overrides", "source_metadata")
    op.drop_column("cora_training_overrides", "snapshot_payload")
    op.drop_column("cora_training_overrides", "reviewed_at")
    op.drop_column("cora_training_overrides", "reviewed_by")
    op.drop_column("cora_training_overrides", "review_outcome")
    op.drop_column("cora_training_overrides", "corrected_output")

    op.alter_column(
        "cora_training_overrides",
        "correction_reason",
        existing_type=sa.String(length=40),
        nullable=False,
    )

    op.alter_column(
        "cora_training_overrides",
        "subject_ref",
        new_column_name="subject_id",
        existing_type=sa.String(length=80),
        type_=sa.Integer(),
        postgresql_using="subject_ref::integer",
        existing_nullable=False,
    )

    op.create_index(
        "idx_cora_overrides_subject_active",
        "cora_training_overrides",
        ["subject_type", "subject_id", "dampener_active"],
    )

    op.execute(
        """
        CREATE UNIQUE INDEX uq_cora_override_active
        ON cora_training_overrides (
            subject_id,
            correction_reason,
            COALESCE(signal_type, '')
        )
        WHERE dampener_active
        """
    )
