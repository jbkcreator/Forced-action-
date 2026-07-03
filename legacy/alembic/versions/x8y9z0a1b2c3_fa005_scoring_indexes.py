"""fa005_scoring_indexes

Adds high-impact indexes required for CDS scoring engine scalability:

1. distress_scores(property_id, score_date DESC) — enables fast "today's score"
   and "latest score" lookups without a full table scan per property.

2. Composite (property_id, date_added) on all 8 signal tables — enables
   --rescore-new-signals to use index-only scans instead of correlated EXISTS
   seq-scans across every signal table.

Production note:
  The migration creates indexes with IF NOT EXISTS (safe to re-run).
  For zero-lock production deployment on large tables, create the
  distress_scores index manually first with CONCURRENTLY, then run
  alembic upgrade head — the IF NOT EXISTS guard makes it a no-op:

    CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_score_property_date
        ON distress_scores (property_id, score_date DESC);

Revision ID: fa005_scoring_indexes
Revises:     fa004_referral_core_loop
Create Date: 2026-05-21
"""

import sqlalchemy as sa
from alembic import op

revision = "fa005_scoring_indexes"
down_revision = "fa004_referral_core_loop"
branch_labels = None
depends_on = None

# Signal tables that need (property_id, date_added) composite indexes
_SIGNAL_TABLES = [
    ("code_violations",    "idx_cv_pid_date_added"),
    ("legal_and_liens",    "idx_lal_pid_date_added"),
    ("deeds",              "idx_deeds_pid_date_added"),
    ("legal_proceedings",  "idx_lp_pid_date_added"),
    ("tax_delinquencies",  "idx_td_pid_date_added"),
    ("foreclosures",       "idx_fc_pid_date_added"),
    ("building_permits",   "idx_bp_pid_date_added"),
    ("incidents",          "idx_inc_pid_date_added"),
]


def upgrade() -> None:
    # Primary scoring index — (property_id, score_date DESC) covering the two
    # hot query patterns:
    #   1. SELECT … WHERE property_id=X AND score_date>=today_start AND score_date<tomorrow_start
    #   2. SELECT … WHERE property_id=X ORDER BY score_date DESC LIMIT 1
    op.execute(sa.text(
        "CREATE INDEX IF NOT EXISTS idx_score_property_date "
        "ON distress_scores (property_id, score_date DESC)"
    ))

    # Signal-table composite indexes — enables --rescore-new-signals EXISTS checks
    # to use an index scan on (property_id, date_added) rather than a full scan.
    for table, idx_name in _SIGNAL_TABLES:
        op.execute(sa.text(
            f"CREATE INDEX IF NOT EXISTS {idx_name} "
            f"ON {table} (property_id, date_added)"
        ))


def downgrade() -> None:
    op.execute(sa.text("DROP INDEX IF EXISTS idx_score_property_date"))
    for _, idx_name in _SIGNAL_TABLES:
        op.execute(sa.text(f"DROP INDEX IF EXISTS {idx_name}"))
