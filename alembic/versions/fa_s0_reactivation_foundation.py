"""Sprint S0 reactivation foundation.

Adds subscribers.last_reactivation_attempt_at (cooldown gate) and
gold_plus_zip_snapshots (nightly Gold+ supply aggregation per ZIP).

NOTE: idx_msg_outcome_sub_sent on message_outcomes(subscriber_id, sent_at)
already exists — not duplicated here.

Run:
    alembic upgrade fa_s0_reactivation_foundation
"""

import sqlalchemy as sa
from alembic import op

revision = "fa_s0_reactivation_foundation"
down_revision = "fa005_concierge_chat"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()

    col_exists = conn.execute(sa.text(
        "SELECT EXISTS ("
        "  SELECT 1 FROM information_schema.columns"
        "  WHERE table_name='subscribers'"
        "  AND column_name='last_reactivation_attempt_at'"
        ")"
    )).scalar()
    if not col_exists:
        op.add_column(
            "subscribers",
            sa.Column(
                "last_reactivation_attempt_at",
                sa.DateTime(timezone=True),
                nullable=True,
            ),
        )
        op.create_index(
            "idx_subscriber_last_reactivation_at",
            "subscribers",
            ["last_reactivation_attempt_at"],
        )

    tbl_exists = conn.execute(sa.text(
        "SELECT EXISTS ("
        "  SELECT FROM information_schema.tables"
        "  WHERE table_name='gold_plus_zip_snapshots'"
        ")"
    )).scalar()
    if not tbl_exists:
        op.create_table(
            "gold_plus_zip_snapshots",
            sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column("zip_code", sa.String(10), nullable=False),
            sa.Column("county_id", sa.String(50), nullable=False),
            sa.Column("snapshot_date", sa.Date(), nullable=False),
            sa.Column(
                "gold_plus_lead_count",
                sa.Integer(),
                nullable=False,
                server_default="0",
            ),
            sa.Column("computed_at", sa.DateTime(timezone=True), nullable=False),
            sa.UniqueConstraint(
                "zip_code", "county_id", "snapshot_date",
                name="uq_gpzs_zip_county_date",
            ),
        )
        op.create_index(
            "idx_gpzs_zip_county_date",
            "gold_plus_zip_snapshots",
            ["zip_code", "county_id", "snapshot_date"],
        )


def downgrade() -> None:
    op.drop_index("idx_gpzs_zip_county_date", table_name="gold_plus_zip_snapshots")
    op.drop_table("gold_plus_zip_snapshots")
    op.drop_index("idx_subscriber_last_reactivation_at", table_name="subscribers")
    op.drop_column("subscribers", "last_reactivation_attempt_at")
