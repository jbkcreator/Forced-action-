"""Task 5.1 Autonomous Inbound Content Loop — fold keyword metrics onto quora_topics.

Adds the Seed Keyword performance columns directly to quora_topics rather than
building the spec's scraper_keyword_metrics / thread_clusters tables (see
docs/adr/0021). Additive + backward-compatible. Applied to the shared DB via
script per the project's db-changes-via-script convention; this file is the
record. down_revision=None matches the recent standalone-migration pattern
(fa103, fa_a4) in this multi-head tree.
"""

from alembic import op
import sqlalchemy as sa

revision = "fa_5_1"
down_revision = None
branch_labels = None
depends_on = None

_COLUMNS = [
    ("cluster", sa.String(30), True, None),
    ("signup_count", sa.Integer(), False, "0"),
    ("cumulative_spend", sa.Numeric(10, 4), False, "0"),
    ("performance_score", sa.Numeric(12, 4), True, None),
    ("impression_count", sa.Integer(), True, None),
    ("click_through_count", sa.Integer(), True, None),
]


def upgrade():
    for name, type_, nullable, server_default in _COLUMNS:
        op.add_column(
            "quora_topics",
            sa.Column(name, type_, nullable=nullable, server_default=server_default),
        )


def downgrade():
    for name, *_ in reversed(_COLUMNS):
        op.drop_column("quora_topics", name)
