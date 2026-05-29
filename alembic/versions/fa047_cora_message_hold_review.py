"""fa047_cora_message_hold_review

Adds the Cora hold-and-review workflow columns.

message_outcomes
  send_status        VARCHAR(20)  NOT NULL DEFAULT 'sent'
                     CHECK IN (pending_review, approved, sent, cancelled, failed, expired)
  requires_review    BOOLEAN      NOT NULL DEFAULT false
  review_reason      VARCHAR(255) nullable
  scheduled_send_at  TIMESTAMPTZ  nullable
  approved_at        TIMESTAMPTZ  nullable
  approved_by        VARCHAR(100) nullable
  cancelled_at       TIMESTAMPTZ  nullable
  cancelled_by       VARCHAR(100) nullable
  cancel_reason      VARCHAR(255) nullable
  decision_id        VARCHAR(36)  nullable  (soft ref to agent_decisions.decision_id)

  Indexes:
    idx_mo_send_status             (send_status)
    idx_mo_decision_id             (decision_id)
    idx_mo_pending_review_queue    partial — used by the /cora-messages/pending query:
      (created_at DESC, id DESC) WHERE send_status='pending_review'
        AND requires_review=true AND message_type='sms' AND cancelled_at IS NULL

sms_send_logs
  message_outcome_id  INTEGER  nullable  FK → message_outcomes.id ON DELETE SET NULL
  Indexes:
    idx_sml_message_outcome_id     (message_outcome_id)

Purely additive — all new columns are nullable or carry a server_default.
Existing rows receive send_status='sent', requires_review=false, all other cols NULL.
Safe to apply during business hours.

Revision ID: fa047_cora_message_hold_review
Revises:     fa046_add_court_dockets_source_type
Create Date: 2026-05-28
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "fa047_cora_message_hold_review"
down_revision: Union[str, Sequence[str], None] = "fa046_add_court_dockets_source_type"
branch_labels = None
depends_on = None

_SEND_STATUS_CHECK = (
    "send_status IN ('pending_review','approved','sent','cancelled','failed','expired')"
)


def upgrade() -> None:
    # ── message_outcomes: hold/review lifecycle columns ───────────────────────

    op.add_column(
        "message_outcomes",
        sa.Column(
            "send_status",
            sa.String(20),
            nullable=False,
            server_default="sent",
        ),
    )
    op.add_column(
        "message_outcomes",
        sa.Column(
            "requires_review",
            sa.Boolean,
            nullable=False,
            server_default=sa.false(),
        ),
    )
    op.add_column(
        "message_outcomes",
        sa.Column("review_reason", sa.String(255), nullable=True),
    )
    op.add_column(
        "message_outcomes",
        sa.Column("scheduled_send_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "message_outcomes",
        sa.Column("approved_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "message_outcomes",
        sa.Column("approved_by", sa.String(100), nullable=True),
    )
    op.add_column(
        "message_outcomes",
        sa.Column("cancelled_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "message_outcomes",
        sa.Column("cancelled_by", sa.String(100), nullable=True),
    )
    op.add_column(
        "message_outcomes",
        sa.Column("cancel_reason", sa.String(255), nullable=True),
    )
    # Soft reference to agent_decisions.decision_id — VARCHAR not FK so
    # decisions can be pruned independently without cascading to message rows.
    op.add_column(
        "message_outcomes",
        sa.Column("decision_id", sa.String(36), nullable=True),
    )

    op.create_check_constraint(
        "check_mo_send_status",
        "message_outcomes",
        _SEND_STATUS_CHECK,
    )

    # General-purpose lookup indexes
    op.create_index("idx_mo_send_status",  "message_outcomes", ["send_status"])
    op.create_index("idx_mo_decision_id",  "message_outcomes", ["decision_id"])

    # Partial index for the pending-review admin queue.
    # Covers all four WHERE predicates so the query is an index-only scan
    # ordered by (created_at DESC, id DESC) with zero heap fetches.
    op.create_index(
        "idx_mo_pending_review_queue",
        "message_outcomes",
        ["created_at", "id"],
        postgresql_where=sa.text(
            "send_status = 'pending_review'"
            " AND requires_review = true"
            " AND message_type = 'sms'"
            " AND cancelled_at IS NULL"
        ),
        postgresql_ops={"created_at": "DESC", "id": "DESC"},
    )

    # ── sms_send_logs: approval linkage FK ───────────────────────────────────

    op.add_column(
        "sms_send_logs",
        sa.Column(
            "message_outcome_id",
            sa.Integer,
            sa.ForeignKey("message_outcomes.id", ondelete="SET NULL"),
            nullable=True,
        ),
    )
    op.create_index(
        "idx_sml_message_outcome_id",
        "sms_send_logs",
        ["message_outcome_id"],
    )


def downgrade() -> None:
    # sms_send_logs
    op.drop_index("idx_sml_message_outcome_id", table_name="sms_send_logs")
    op.drop_column("sms_send_logs", "message_outcome_id")

    # message_outcomes — indexes first, then constraint, then columns
    op.drop_index("idx_mo_pending_review_queue", table_name="message_outcomes")
    op.drop_index("idx_mo_decision_id",          table_name="message_outcomes")
    op.drop_index("idx_mo_send_status",          table_name="message_outcomes")
    op.drop_constraint("check_mo_send_status",   "message_outcomes", type_="check")
    op.drop_column("message_outcomes", "decision_id")
    op.drop_column("message_outcomes", "cancel_reason")
    op.drop_column("message_outcomes", "cancelled_by")
    op.drop_column("message_outcomes", "cancelled_at")
    op.drop_column("message_outcomes", "approved_by")
    op.drop_column("message_outcomes", "approved_at")
    op.drop_column("message_outcomes", "scheduled_send_at")
    op.drop_column("message_outcomes", "review_reason")
    op.drop_column("message_outcomes", "requires_review")
    op.drop_column("message_outcomes", "send_status")
