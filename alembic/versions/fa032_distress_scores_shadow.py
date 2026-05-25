"""fa032_distress_scores_shadow

Adds `distress_scores_shadow` — a mirror of `distress_scores` for the
Stage E shadow rescore phase of the CDS cross-county retune.

Shadow scoring writes here instead of `distress_scores` so the live
subscriber feed, GHL push, and Cora flows are unaffected while we test
proposed weight values from the Stage C fit artifact. The validation
report task (src/tasks/scoring_validation_report.py) reads both tables
to compare tier distributions and event rates per county before the
Stage F cutover swaps the live weights.

Schema mirrors `distress_scores` exactly (same columns, same constraints,
same indexes) so the engine can write to either table interchangeably.
The shadow table can be dropped after the 60-day post-cutover monitoring
window in Stage F is over.

Revision ID: fa032_distress_scores_shadow
Revises:     fa031_enriched_contact_traced_name
Create Date: 2026-05-25
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB


revision: str = "fa032_distress_scores_shadow"
down_revision: Union[str, Sequence[str], None] = "fa031_enriched_contact_traced_name"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "distress_scores_shadow",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column(
            "property_id", sa.Integer,
            sa.ForeignKey("properties.id"),
            nullable=False, index=True,
        ),
        sa.Column("vertical_scores", JSONB, nullable=True),
        sa.Column(
            "score_date", sa.DateTime,
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.Column("final_cds_score", sa.Numeric(5, 2), nullable=True),
        sa.Column("lead_tier", sa.String(50), nullable=True),
        sa.Column("distress_types", JSONB, nullable=True),
        sa.Column("urgency_level", sa.String(20), nullable=True),
        sa.Column("multiplier", sa.Numeric(4, 2), nullable=True),
        sa.Column("factor_scores", JSONB, nullable=True),
        sa.Column("qualified", sa.Boolean, server_default=sa.text("false"), nullable=True),
        sa.Column(
            "county_id", sa.String(50),
            server_default="hillsborough", nullable=True, index=True,
        ),
        sa.Column("scoring_run_id", sa.Integer, nullable=True),
        sa.CheckConstraint(
            "urgency_level IN ('Immediate', 'High', 'Medium', 'Low')",
            name="check_urgency_level_shadow",
        ),
        sa.CheckConstraint(
            "lead_tier IN ('Ultra Platinum', 'Platinum', 'Gold', 'Silver', 'Bronze')",
            name="check_lead_tier_shadow",
        ),
    )

    # Indexes — mirror the live table so validation queries run with the same plan
    op.create_index("idx_shadow_score_date",          "distress_scores_shadow", ["score_date"])
    op.create_index("idx_shadow_score_final_cds",     "distress_scores_shadow", ["final_cds_score"])
    op.create_index("idx_shadow_score_lead_tier",     "distress_scores_shadow", ["lead_tier"])
    op.create_index("idx_shadow_score_qualified",     "distress_scores_shadow", ["qualified"])
    op.create_index("idx_shadow_score_county_id",     "distress_scores_shadow", ["county_id"])
    op.create_index(
        "idx_shadow_score_distress_types",
        "distress_scores_shadow", ["distress_types"],
        postgresql_using="gin",
    )
    op.create_index("idx_shadow_score_scoring_run_id", "distress_scores_shadow", ["scoring_run_id"])


def downgrade() -> None:
    op.drop_index("idx_shadow_score_scoring_run_id", table_name="distress_scores_shadow")
    op.drop_index("idx_shadow_score_distress_types", table_name="distress_scores_shadow")
    op.drop_index("idx_shadow_score_county_id",      table_name="distress_scores_shadow")
    op.drop_index("idx_shadow_score_qualified",      table_name="distress_scores_shadow")
    op.drop_index("idx_shadow_score_lead_tier",      table_name="distress_scores_shadow")
    op.drop_index("idx_shadow_score_final_cds",      table_name="distress_scores_shadow")
    op.drop_index("idx_shadow_score_date",           table_name="distress_scores_shadow")
    op.drop_table("distress_scores_shadow")
