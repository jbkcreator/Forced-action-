"""drop ORI columns from county_sources, clean legacy keys from special_flags

Revision ID: z0a1b2c3d4e5
Revises: y9z0a1b2c3d4
Create Date: 2026-05-12

Phase-2 cleanup of the scrape_mode / ORI-collapse refactor. The previous
migration (y9z0a1b2c3d4) added new columns, backfilled them from special_flags
and ori_* fields, and synthesized CountyColumnMapping rows from the ORI data.

This migration removes the now-redundant artifacts:

1. ori_column_map, ori_book_page_col, ori_doc_type_map columns are dropped
   from county_sources. The same information lives in the synthesized
   CountyColumnMapping rows (mapping / post_processors / value_maps).

2. The promoted keys (scrape_mode, playwright_code, playwright_code_version,
   playwright_code_approved) are removed from each row's special_flags JSONB.
   Their authoritative home is now the first-class columns; leaving them in
   the JSONB would let a stale admin UI edit silently overwrite the promoted
   value.

**Apply this migration only after** the engine + UI rollout from y9z0a1b2c3d4
has been verified end-to-end:
  - python -m src.scrappers.liens.lien_engine --county-id pinellas
    produces 52/201/96/127/4 buckets matching the pre-refactor counts.
  - python -m src.scrappers.liens.lien_engine --county-id hillsborough
    produces equivalent bucket counts.
  - pytest tests/test_lien_engine.py tests/test_column_mapper_extensions.py
    tests/test_admin_playwright_code.py → all green.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "z0a1b2c3d4e5"
down_revision: Union[str, Sequence[str], None] = "y9z0a1b2c3d4"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # 1. Sweep the now-promoted keys out of special_flags so future edits to
    #    the JSONB blob can't silently overwrite the first-class columns.
    op.execute(
        sa.text(
            """
            UPDATE county_sources
               SET special_flags = special_flags
                                 - 'scrape_mode'
                                 - 'playwright_code'
                                 - 'playwright_code_version'
                                 - 'playwright_code_approved'
             WHERE special_flags IS NOT NULL
            """
        )
    )

    # 2. Drop the three deprecated ORI columns. Engines no longer read these;
    #    the equivalent transformations live in CountyColumnMapping rows.
    op.drop_column("county_sources", "ori_doc_type_map")
    op.drop_column("county_sources", "ori_book_page_col")
    op.drop_column("county_sources", "ori_column_map")


def downgrade() -> None:
    # Add the ORI columns back as nullable JSONB / String. NOTE: a roll-back
    # cannot reconstruct the original values — the backfill in y9z0a1b2c3d4
    # was a one-way migration into CountyColumnMapping. If a true rollback is
    # needed, also downgrade y9z0a1b2c3d4 which restores the original rows.
    op.add_column(
        "county_sources",
        sa.Column(
            "ori_column_map",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
    )
    op.add_column(
        "county_sources",
        sa.Column("ori_book_page_col", sa.String(length=50), nullable=True),
    )
    op.add_column(
        "county_sources",
        sa.Column(
            "ori_doc_type_map",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
    )
    # We do NOT put the promoted keys back into special_flags — those were
    # always meant to be moved out, and re-injecting them would create a
    # split-brain between the top-level columns and the JSONB.
