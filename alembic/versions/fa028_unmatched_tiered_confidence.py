"""fa028 — tiered match confidence across all destination + unmatched tables

Adds confidence scoring (0.000–1.000 Numeric) and match_method to every
loader destination table. Extends UnmatchedRecord with candidate_property_id,
match_confidence, match_method, and the pending_review match_status value.
Normalizes LegalAndLien.match_confidence from Integer (0–100) to Numeric(4,3).

Sits on a parallel branch alongside fa028_merge_scoring_indexes; both are
joined back into the trunk by fa029_add_normalized_address.

Revision ID: fa028_unmatched_tiered_confidence
Revises:     fa027_agent_decisions_variant_id
Create Date: 2026-05-21
"""

import sqlalchemy as sa
from alembic import op

revision = "fa028_unmatched_tiered_confidence"
down_revision = "fa027_agent_decisions_variant_id"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ── unmatched_records: 3 new columns + constraints ─────────────────────
    op.add_column("unmatched_records",
        sa.Column("match_confidence", sa.Numeric(4, 3), nullable=True))
    op.add_column("unmatched_records",
        sa.Column("match_method", sa.String(30), nullable=True))
    op.add_column("unmatched_records",
        sa.Column("candidate_property_id", sa.Integer(), nullable=True))
    op.create_foreign_key(
        "fk_unmatched_candidate_property", "unmatched_records",
        "properties", ["candidate_property_id"], ["id"],
    )
    op.create_index(
        "ix_unmatched_candidate_property", "unmatched_records", ["candidate_property_id"]
    )
    # Drop old check constraint if it exists under this name, then recreate
    # with pending_review included. Using raw SQL + IF NOT EXISTS guard so
    # the migration is safe to re-run regardless of prior constraint state.
    op.execute(sa.text(
        "ALTER TABLE unmatched_records "
        "DROP CONSTRAINT IF EXISTS check_unmatched_match_status"
    ))
    op.create_check_constraint(
        "check_unmatched_match_status", "unmatched_records",
        "match_status IN ('unmatched','matched','skipped','pending_review')",
    )
    op.create_check_constraint(
        "check_unmatched_match_method", "unmatched_records",
        "match_method IN ('address','owner_name','legal_desc','parcel_id') OR match_method IS NULL",
    )

    # ── legal_and_liens: Integer → Numeric(4,3), backfill existing rows ────
    op.execute(sa.text(
        "ALTER TABLE legal_and_liens "
        "ALTER COLUMN match_confidence TYPE NUMERIC(4,3) "
        "USING CASE WHEN match_confidence IS NULL THEN NULL "
        "     ELSE ROUND(match_confidence / 100.0, 3) END"
    ))

    # ── destination tables: add match_confidence + match_method ───────────
    for table in ("deeds", "legal_proceedings", "code_violations", "foreclosures"):
        op.add_column(table, sa.Column("match_confidence", sa.Numeric(4, 3), nullable=True))
        op.add_column(table, sa.Column("match_method", sa.String(30), nullable=True))


def downgrade() -> None:
    for table in ("deeds", "legal_proceedings", "code_violations", "foreclosures"):
        op.drop_column(table, "match_method")
        op.drop_column(table, "match_confidence")

    op.execute(sa.text(
        "ALTER TABLE legal_and_liens "
        "ALTER COLUMN match_confidence TYPE INTEGER "
        "USING CASE WHEN match_confidence IS NULL THEN NULL "
        "     ELSE ROUND(match_confidence * 100)::INTEGER END"
    ))

    op.execute(sa.text(
        "ALTER TABLE unmatched_records DROP CONSTRAINT IF EXISTS check_unmatched_match_method"
    ))
    op.execute(sa.text(
        "ALTER TABLE unmatched_records DROP CONSTRAINT IF EXISTS check_unmatched_match_status"
    ))
    op.create_check_constraint(
        "check_unmatched_match_status", "unmatched_records",
        "match_status IN ('unmatched','matched','skipped')",
    )
    op.drop_index("ix_unmatched_candidate_property", table_name="unmatched_records")
    op.drop_constraint(
        "fk_unmatched_candidate_property", "unmatched_records", type_="foreignkey"
    )
    op.drop_column("unmatched_records", "candidate_property_id")
    op.drop_column("unmatched_records", "match_method")
    op.drop_column("unmatched_records", "match_confidence")
