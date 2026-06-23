"""M6 Lead Quality Truth Engine — verdicts, grade_thresholds (seeded), cohort_rates

Revision ID: fa091_m6_truth_engine
Revises: fa090_events_prospect_id_not_null

NOTE: this repo has historically carried multiple Alembic heads. Run `alembic heads`
before applying; if a parallel head exists, create a merge migration first and rebase
down_revision onto the M1 backbone tip.
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB, UUID as PG_UUID

revision: str = "fa091_m6_truth_engine"
down_revision: Union[str, Sequence[str]] = "fa090_events_prospect_id_not_null"
branch_labels = None
depends_on = None

_GRADE_CHECK = "grade IN ('Ultra','Platinum','Gold','Silver','Bronze','sub_grade')"
_CHANNEL_CHECK = (
    "routed_channel IN ('loan_lane','contractor_subscription','storm_retainer',"
    "'data_pack_bulk','free_hand_delivered','recycle_suppress')"
)


def upgrade() -> None:
    # ── grade_thresholds (config — tunable without deploy, spec §3.1a) ──────────
    op.create_table(
        "grade_thresholds",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("grade", sa.String(), nullable=False),
        sa.Column("cds_min", sa.Integer(), nullable=True),
        sa.Column("cds_max", sa.Integer(), nullable=True),
        sa.Column("contactability_min", sa.Numeric(5, 4), nullable=True),
        sa.Column("requires_mobile_consent", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("NOW()")),
        sa.UniqueConstraint("grade", name="uq_grade_thresholds_grade"),
        sa.CheckConstraint(_GRADE_CHECK, name="ck_grade_thresholds_grade"),
    )

    # Seed §3.1a thresholds. CDS on 0–100 (spec's 0–1 values recorded in notes);
    # contactability_min on 0–1 (dormant until contactability rates exist).
    op.execute("""
        INSERT INTO grade_thresholds
            (grade, cds_min, cds_max, contactability_min, requires_mobile_consent, notes)
        VALUES
            ('Ultra',     85, NULL, 0.4000, true,  'spec §3.1a: CDS>=0.85, contactability>=40%, validated mobile+consent'),
            ('Platinum',  70,   84, 0.2500, false, 'spec §3.1a: CDS 0.70-0.84, contactability>=25%'),
            ('Gold',      50,   69, 0.1200, false, 'spec §3.1a: CDS 0.50-0.69, contactability>=12% (binding floor)'),
            ('Silver',    30,   49, 0.0500, false, 'spec §3.1a: CDS 0.30-0.49, contactability>=5%'),
            ('Bronze',    15,   29, NULL,   false, 'spec §3.1a: CDS 0.15-0.29, any contactability'),
            ('sub_grade', NULL, 14, NULL,   false, 'spec §3.1a: CDS<0.15, any — recycle/suppress')
    """)

    # ── verdicts (Truth Engine output, spec §4.5) ───────────────────────────────
    op.create_table(
        "verdicts",
        sa.Column("verdict_id", PG_UUID(as_uuid=True), primary_key=True,
                  server_default=sa.text("generate_uuidv7()")),
        sa.Column("prospect_id", PG_UUID(as_uuid=True),
                  sa.ForeignKey("prospects.prospect_id"), nullable=False),
        sa.Column("grade", sa.String(), nullable=False),
        sa.Column("contributing_factors", JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("contactability_flag", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("routed_channel", sa.String(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("NOW()")),
        sa.CheckConstraint(_GRADE_CHECK, name="ck_verdicts_grade"),
        sa.CheckConstraint(_CHANNEL_CHECK, name="ck_verdicts_routed_channel"),
    )
    op.create_index("idx_verdicts_prospect_id", "verdicts", ["prospect_id"])
    op.create_index("idx_verdicts_created_at", "verdicts", ["created_at"])

    # ── cohort_rates (contactability cohort fallback, spec §12.1) ───────────────
    op.create_table(
        "cohort_rates",
        sa.Column("cohort_key", sa.String(), primary_key=True),
        sa.Column("contact_attempts", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("successful_contacts", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("contactability_rate", sa.Numeric(5, 4), nullable=True),
        sa.Column("sample_size", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("computed_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("NOW()")),
    )


def downgrade() -> None:
    op.drop_table("cohort_rates")
    op.drop_index("idx_verdicts_created_at", table_name="verdicts")
    op.drop_index("idx_verdicts_prospect_id", table_name="verdicts")
    op.drop_table("verdicts")
    op.drop_table("grade_thresholds")
