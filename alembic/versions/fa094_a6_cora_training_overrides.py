"""A6: add cora_training_overrides table (teaching corrections + fine-tuning queue)

Revision ID: fa094_a6_cora_training_overrides
Revises: fa_s5_enhancement_loops
Create Date: 2026-06-24

ADR 0006: dampener = type-keyed gate reuse, not multiplier
ADR 0007: shared polymorphic schema, row-as-queue
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "fa094_a6_cora_training_overrides"
down_revision = "fa_s5_enhancement_loops"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "cora_training_overrides",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("source", sa.String(30), nullable=False),
        sa.Column("subject_type", sa.String(30), nullable=False),
        sa.Column("subject_id", sa.Integer(), nullable=False),
        sa.Column(
            "closer_call_id",
            sa.Integer(),
            sa.ForeignKey("closer_calls.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("correction_reason", sa.String(40), nullable=False),
        sa.Column("signal_type", sa.String(40), nullable=True),
        sa.Column("note", sa.Text(), nullable=True),
        sa.Column("dampener_active", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column("queue_status", sa.String(20), nullable=False, server_default="pending"),
        sa.Column("created_by", sa.String(120), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint(
            "source IN ('closer_teach', 'feedback_ritual')",
            name="ck_cora_overrides_source",
        ),
        sa.CheckConstraint(
            "queue_status IN ('pending', 'exported', 'discarded')",
            name="ck_cora_overrides_queue_status",
        ),
    )

    # Hot-path read: CDS engine fetches active corrections per property at score time.
    op.create_index(
        "idx_cora_overrides_subject_active",
        "cora_training_overrides",
        ["subject_type", "subject_id", "dampener_active"],
    )

    # Queue consumer reads pending rows.
    op.create_index(
        "idx_cora_overrides_queue_status",
        "cora_training_overrides",
        ["queue_status"],
    )

    # Idempotency: a re-fired identical active correction must not create a duplicate.
    # signal_type is nullable — Postgres treats two NULLs as distinct in unique indexes,
    # so COALESCE to '' to make (non_residential, '', ...) unique.
    op.execute("""
        CREATE UNIQUE INDEX uq_cora_override_active
        ON cora_training_overrides (
            subject_id,
            correction_reason,
            COALESCE(signal_type, '')
        )
        WHERE dampener_active
    """)


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS uq_cora_override_active")
    op.drop_index("idx_cora_overrides_queue_status", "cora_training_overrides")
    op.drop_index("idx_cora_overrides_subject_active", "cora_training_overrides")
    op.drop_table("cora_training_overrides")
