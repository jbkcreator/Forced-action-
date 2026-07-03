"""fa_a5_pre_decision_snapshots

Revision ID: fa_a5_pre_decision_snapshots
Revises: fa_a3_scoring_weight_overrides
Create Date: 2026-06-24

Creates pre_decision_snapshots table for A5 Counterfactual Memory Engine.
Captures full pre-routing context (all 6 vertical scores, pricing cohort,
Cora graph, pitch variant) at the moment a deal outcome is created.
Resolution (funded/lost) is written back when the deal closes.
Broker fields are nullable stubs, wirable when broker layer arrives.
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID, JSONB

revision = "fa_a5_pre_decision_snapshots"
down_revision = "fa_a3_scoring_weight_overrides"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "pre_decision_snapshots",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("property_id", sa.Integer(), sa.ForeignKey("properties.id", ondelete="SET NULL"), nullable=True),
        sa.Column("prospect_id", UUID(as_uuid=True), sa.ForeignKey("prospects.prospect_id", ondelete="SET NULL"), nullable=True),
        sa.Column("deal_outcome_id", sa.Integer(), sa.ForeignKey("deal_outcomes.id", ondelete="SET NULL"), nullable=True),

        # Decision context at routing time
        sa.Column("snapshot_ts", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("selected_vertical", sa.String(50), nullable=True),
        sa.Column("lead_tier", sa.String(30), nullable=True),
        sa.Column("final_cds_score", sa.Numeric(5, 2), nullable=True),
        sa.Column("distress_types", JSONB(), nullable=True),

        # Roads not taken
        sa.Column("all_vertical_scores", JSONB(), nullable=True),
        sa.Column("runner_up_verticals", JSONB(), nullable=True),

        # Routing snapshot
        sa.Column("pricing_cohort_id", sa.Integer(), nullable=True),  # soft ref, no FK
        sa.Column("pricing_snapshot", JSONB(), nullable=True),
        sa.Column("cora_graph", sa.String(100), nullable=True),
        sa.Column("pitch_variant", sa.String(100), nullable=True),

        # Full context blob
        sa.Column("raw_context", JSONB(), nullable=True),

        # Resolution
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("outcome_status", sa.String(30), nullable=True),

        # Broker stub
        sa.Column("broker_id", sa.Integer(), nullable=True),
        sa.Column("alternative_brokers", JSONB(), nullable=True),

        # Engine tracking
        sa.Column("counterfactual_run", sa.Boolean(), nullable=False, server_default=sa.text("FALSE")),
        sa.Column("counterfactual_run_at", sa.DateTime(timezone=True), nullable=True),

        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    )

    # Idempotency: one snapshot per deal_outcome
    op.create_index(
        "uq_pds_deal_outcome",
        "pre_decision_snapshots",
        ["deal_outcome_id"],
        unique=True,
        postgresql_where=sa.text("deal_outcome_id IS NOT NULL"),
    )
    op.create_index("idx_pds_property_id", "pre_decision_snapshots", ["property_id"])
    op.create_index("idx_pds_snapshot_ts", "pre_decision_snapshots", ["snapshot_ts"], postgresql_ops={"snapshot_ts": "DESC"})
    op.create_index("idx_pds_selected_vertical", "pre_decision_snapshots", ["selected_vertical"])
    op.create_index("idx_pds_outcome_status", "pre_decision_snapshots", ["outcome_status"])
    # Partial index: engine pickup queue for future A5b comparison engine
    op.create_index(
        "idx_pds_pending_cf",
        "pre_decision_snapshots",
        ["id"],
        postgresql_where=sa.text("counterfactual_run = FALSE AND outcome_status IS NOT NULL"),
    )


def downgrade() -> None:
    op.drop_index("idx_pds_pending_cf", table_name="pre_decision_snapshots")
    op.drop_index("idx_pds_outcome_status", table_name="pre_decision_snapshots")
    op.drop_index("idx_pds_selected_vertical", table_name="pre_decision_snapshots")
    op.drop_index("idx_pds_snapshot_ts", table_name="pre_decision_snapshots")
    op.drop_index("idx_pds_property_id", table_name="pre_decision_snapshots")
    op.drop_index("uq_pds_deal_outcome", table_name="pre_decision_snapshots")
    op.drop_table("pre_decision_snapshots")
