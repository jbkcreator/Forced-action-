"""Weekly master refresh: change-detection + downstream flag columns

Adds the columns the weekly master loader needs to detect source changes and
propagate them downstream:

  properties.source_row_hash  - md5 of canonical scraper-sourced field values,
                                written by the master loader; NULL means the row
                                predates hash tracking (treated as changed on the
                                next load, with per-field guards preventing
                                spurious downstream flags).
  properties.last_seen_at     - stamped for every parcel present in a master
                                file; rows missing from consecutive loads become
                                queryable as stale/retired. Deliberately
                                unindexed so the weekly full-county stamp stays
                                HOT-eligible.
  properties.needs_rescore    - consumed by the CDS engine's changed-property
                                collection; partial index keeps the lookup
                                O(flagged).
  owners.skip_trace_stale     - set when owner_name changes for an owner with
                                prior skip-trace data; the old contact info is
                                kept but marked as belonging to the previous
                                owner.

Revision ID: fa077_master_weekly_refresh
Revises: fa070_consent_acceptances
Create Date: 2026-06-10
"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa

revision: str = "fa077_master_weekly_refresh"
down_revision: Union[str, Sequence[str], None] = "fa070_consent_acceptances"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("properties", sa.Column("source_row_hash", sa.String(32), nullable=True))
    op.add_column("properties", sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column(
        "properties",
        sa.Column("needs_rescore", sa.Boolean(), nullable=False, server_default=sa.text("false")),
    )
    op.create_index(
        "idx_properties_needs_rescore",
        "properties",
        ["id"],
        postgresql_where=sa.text("needs_rescore"),
    )

    op.add_column(
        "owners",
        sa.Column("skip_trace_stale", sa.Boolean(), nullable=False, server_default=sa.text("false")),
    )
    op.create_index(
        "idx_owner_skip_trace_stale",
        "owners",
        ["id"],
        postgresql_where=sa.text("skip_trace_stale"),
    )


def downgrade() -> None:
    op.drop_index("idx_owner_skip_trace_stale", table_name="owners")
    op.drop_column("owners", "skip_trace_stale")
    op.drop_index("idx_properties_needs_rescore", table_name="properties")
    op.drop_column("properties", "needs_rescore")
    op.drop_column("properties", "last_seen_at")
    op.drop_column("properties", "source_row_hash")
