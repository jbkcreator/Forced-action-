"""fa036_cora_autonomy_scorecard

Adds the schema for the Weekly Cora Autonomy Scorecard:

  1. `cora_playbook` table — minimal lifecycle (recommended → adopted /
     rejected → retired). Source-tracking columns + unique partial index
     on `source_key` prevent duplicate rows for the same A/B test or
     metric breach.

  2. Eight new columns on `agent_decisions`:
       - autonomy_class    — classification at decision time
       - was_autonomous    — sticky flag, never cleared after first TRUE
       - requires_approval — explicit human-approval gate
       - approved_at / approved_by
       - overridden_at / overridden_by / override_reason
       - playbook_id       — nullable FK to cora_playbook(id)

  3. Widens `learning_cards.check_card_type` to include `autonomy_summary`.

Source of truth: FA-2B-v9-FINAL §Weekly Cora Autonomy Scorecard.
Plan: ~/.claude/plans/the-bronze-at-2-34-vs-ultra-platinum-at-happy-pretzel.md

Production note: purely additive — new table + nullable cols + widened CHECK.
Safe during business hours. The FK between agent_decisions.playbook_id and
cora_playbook.id, plus the reverse cora_playbook.decision_id → agent_decisions,
both use ON DELETE SET NULL so neither side cascades.

Revision ID: fa036_cora_autonomy_scorecard
Revises:     fa035_widen_baseline_metric_cols
Create Date: 2026-05-25
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB


revision: str = "fa036_cora_autonomy_scorecard"
down_revision: Union[str, Sequence[str], None] = "fa035_widen_baseline_metric_cols"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ── cora_playbook ────────────────────────────────────────────────────
    op.create_table(
        "cora_playbook",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("name", sa.String(120), nullable=False),
        sa.Column("description", sa.Text, nullable=True),
        sa.Column("pattern_json", JSONB, nullable=False),
        sa.Column("authored_by", sa.String(80), nullable=False),
        sa.Column(
            "authored_at", sa.DateTime(timezone=True),
            nullable=False, server_default=sa.text("NOW()"),
        ),
        sa.Column(
            "status", sa.String(20),
            nullable=False, server_default="recommended",
        ),
        sa.Column("adopted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("adopted_by", sa.String(80), nullable=True),
        sa.Column("rejected_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("rejected_by", sa.String(80), nullable=True),
        sa.Column("rejection_reason", sa.Text, nullable=True),
        sa.Column("retired_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("retired_by", sa.String(80), nullable=True),
        # decision_id is VARCHAR(36) to match agent_decisions.decision_id
        # (which is String(36), not PG UUID type — confirmed in audit).
        sa.Column("decision_id", sa.String(36), nullable=True),
        # Source-tracking columns (correction #2 from plan review) — dedupe
        # repeat recommendations for the same A/B test / metric breach.
        sa.Column("source_type", sa.String(40), nullable=True),
        sa.Column("source_id", sa.String(80), nullable=True),
        sa.Column("source_key", sa.String(160), nullable=True),
        sa.Column(
            "created_at", sa.DateTime(timezone=True),
            nullable=False, server_default=sa.text("NOW()"),
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True),
            nullable=False, server_default=sa.text("NOW()"),
        ),
        sa.CheckConstraint(
            "status IN ('recommended','adopted','rejected','retired')",
            name="check_cora_playbook_status",
        ),
        sa.ForeignKeyConstraint(
            ["decision_id"], ["agent_decisions.decision_id"],
            ondelete="SET NULL",
            name="fk_cora_playbook_decision",
        ),
    )
    op.create_index("idx_cora_playbook_status", "cora_playbook", ["status"])
    op.create_index(
        "idx_cora_playbook_authored", "cora_playbook",
        ["authored_by", "authored_at"],
    )
    # Partial index — only rows with a non-null adopted_at.
    op.execute(sa.text("""
        CREATE INDEX idx_cora_playbook_adopted
            ON cora_playbook(adopted_at)
            WHERE adopted_at IS NOT NULL
    """))
    # Unique partial index on source_key — prevents duplicate playbooks for
    # the same source. NULL source_key allowed and uncounted.
    op.execute(sa.text("""
        CREATE UNIQUE INDEX idx_cora_playbook_source_key_unique
            ON cora_playbook(source_key)
            WHERE source_key IS NOT NULL
    """))

    # ── agent_decisions: 8 new columns ───────────────────────────────────
    op.add_column(
        "agent_decisions",
        sa.Column("autonomy_class", sa.String(32), nullable=True),
    )
    # was_autonomous: sticky flag (correction #3). Set TRUE on first
    # 'autonomous' classification; never cleared. Metric 2 denominator
    # queries on was_autonomous=TRUE so overridden rows still count.
    op.add_column(
        "agent_decisions",
        sa.Column(
            "was_autonomous", sa.Boolean,
            nullable=False, server_default=sa.text("FALSE"),
        ),
    )
    op.add_column(
        "agent_decisions",
        sa.Column(
            "requires_approval", sa.Boolean,
            nullable=False, server_default=sa.text("FALSE"),
        ),
    )
    op.add_column(
        "agent_decisions",
        sa.Column("approved_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "agent_decisions",
        sa.Column("approved_by", sa.String(80), nullable=True),
    )
    op.add_column(
        "agent_decisions",
        sa.Column("overridden_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "agent_decisions",
        sa.Column("overridden_by", sa.String(80), nullable=True),
    )
    op.add_column(
        "agent_decisions",
        sa.Column("override_reason", sa.Text, nullable=True),
    )
    # playbook_id: nullable FK (correction #4) — links a decision to the
    # playbook that drove it. Enables Metric 4 (adopted_at → first
    # autonomous application).
    op.add_column(
        "agent_decisions",
        sa.Column("playbook_id", sa.BigInteger, nullable=True),
    )
    op.create_foreign_key(
        "fk_agent_decisions_playbook",
        "agent_decisions", "cora_playbook",
        ["playbook_id"], ["id"],
        ondelete="SET NULL",
    )

    op.create_check_constraint(
        "check_agent_autonomy_class",
        "agent_decisions",
        "autonomy_class IS NULL OR autonomy_class IN ("
        "'autonomous','approval_required','approved',"
        "'rejected','overridden','recommendation_only'"
        ")",
    )

    op.execute(sa.text("""
        CREATE INDEX idx_agent_decisions_autonomy_class
            ON agent_decisions(autonomy_class)
            WHERE autonomy_class IS NOT NULL
    """))
    op.execute(sa.text("""
        CREATE INDEX idx_agent_decisions_overridden
            ON agent_decisions(overridden_at)
            WHERE overridden_at IS NOT NULL
    """))
    op.execute(sa.text("""
        CREATE INDEX idx_agent_decisions_was_autonomous
            ON agent_decisions(was_autonomous, started_at)
            WHERE was_autonomous = TRUE
    """))
    op.execute(sa.text("""
        CREATE INDEX idx_agent_decisions_playbook
            ON agent_decisions(playbook_id)
            WHERE playbook_id IS NOT NULL
    """))

    # ── learning_cards: widen card_type CHECK to allow autonomy_summary ──
    op.drop_constraint("check_card_type", "learning_cards", type_="check")
    op.create_check_constraint(
        "check_card_type",
        "learning_cards",
        "card_type IN ("
        "'message_perf','deal_pattern','ab_result',"
        "'churn_signal','pricing_test','general',"
        "'autonomy_summary'"
        ")",
    )


def downgrade() -> None:
    # learning_cards constraint — restore the pre-fa036 list
    op.drop_constraint("check_card_type", "learning_cards", type_="check")
    op.create_check_constraint(
        "check_card_type",
        "learning_cards",
        "card_type IN ("
        "'message_perf','deal_pattern','ab_result',"
        "'churn_signal','pricing_test','general'"
        ")",
    )

    # agent_decisions: indexes → FK → check → columns
    op.execute(sa.text("DROP INDEX IF EXISTS idx_agent_decisions_playbook"))
    op.execute(sa.text("DROP INDEX IF EXISTS idx_agent_decisions_was_autonomous"))
    op.execute(sa.text("DROP INDEX IF EXISTS idx_agent_decisions_overridden"))
    op.execute(sa.text("DROP INDEX IF EXISTS idx_agent_decisions_autonomy_class"))
    op.drop_constraint(
        "check_agent_autonomy_class", "agent_decisions", type_="check",
    )
    op.drop_constraint(
        "fk_agent_decisions_playbook", "agent_decisions", type_="foreignkey",
    )
    for col in (
        "playbook_id", "override_reason", "overridden_by", "overridden_at",
        "approved_by", "approved_at", "requires_approval",
        "was_autonomous", "autonomy_class",
    ):
        op.drop_column("agent_decisions", col)

    # cora_playbook: drop indexes → drop table
    op.execute(sa.text("DROP INDEX IF EXISTS idx_cora_playbook_source_key_unique"))
    op.execute(sa.text("DROP INDEX IF EXISTS idx_cora_playbook_adopted"))
    op.drop_index("idx_cora_playbook_authored", table_name="cora_playbook")
    op.drop_index("idx_cora_playbook_status", table_name="cora_playbook")
    op.drop_table("cora_playbook")
