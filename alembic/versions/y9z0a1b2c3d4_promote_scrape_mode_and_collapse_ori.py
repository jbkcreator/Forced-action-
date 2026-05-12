"""promote scrape_mode and playwright_code, collapse ORI into ColumnMapper

Revision ID: y9z0a1b2c3d4
Revises: x8y9z0a1b2c3_cf
Create Date: 2026-05-12

What this migration does (additive only — DROP of ORI columns is a separate
follow-up revision once engines + UI have shipped against the new fields):

1. county_sources gets four first-class columns extracted from special_flags:
     - scrape_mode (enum: ai_only / playwright_only / playwright_then_ai)
     - playwright_code (TEXT)
     - playwright_code_version (VARCHAR)
     - playwright_code_approved (BOOLEAN)
   These were previously buried in the special_flags JSONB textarea, which made
   them easy to accidentally wipe (see Pinellas selector-flags incident).

2. county_column_mappings gets three new transformation fields:
     - post_processors (ordered ops applied after column rename, e.g. BookPage split)
     - value_maps (per-column value normalization, e.g. JUDGEMENT → JUDGMENT)
     - row_routing (DocType → signal bucket, only set on multi-bucket sources)

3. Backfill: existing special_flags.scrape_mode / playwright_code / etc. are
   copied to the new columns. The promoted keys stay in special_flags too for
   one release (the engine reads top-level first; the cleanup migration
   removes them after rollout).

4. For every county_sources row with ORI data populated (currently Pinellas
   liens), synthesize an is_approved=True CountyColumnMapping row from the
   ori_column_map + ori_book_page_col + ori_doc_type_map values. Hillsborough
   gets a synthesized mapping too, even though its column_map is empty, so the
   canonical row_routing config lives in one place.

5. ORI columns stay on county_sources for now — dropped in a follow-up
   migration after engines + UI cutover. This is the standard expand-contract
   schema-change pattern.
"""
from typing import Sequence, Union
import json

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# ---------------------------------------------------------------------------
# Alembic identifiers
# ---------------------------------------------------------------------------

revision: str = "y9z0a1b2c3d4"
down_revision: Union[str, Sequence[str], None] = "x8y9z0a1b2c3_cf"
branch_labels = None
depends_on = None


# ---------------------------------------------------------------------------
# Canonical row-routing config — mirrors the rules that were hardcoded in
# lien_engine.categorize_and_split_data() prior to this refactor. Applied to
# every liens-signal CountyColumnMapping synthesized below so the engine can
# stop doing this in Python.
# ---------------------------------------------------------------------------

CANONICAL_LIEN_ROW_ROUTING = {
    "column": "DocType",
    "default": "skip",
    "rules": [
        {"match_exact": ["DEED", "TAX DEED"],                     "bucket": "deeds"},
        {"match_contains": ["LIS PENDENS"],                       "bucket": "liens"},
        {"match_exact": ["JUDGMENT", "JUDGMENT LIEN"],            "bucket": "judgments"},
        {"match_exact": ["PROBATE", "PROBATE REAL PROPERTY"],     "bucket": "probate"},
        {"match_contains": ["DOMESTIC RELATIONS",
                            "DISSOLUTION OF MARRIAGE"],           "bucket": "divorce"},
        {"match_exact": ["TAX LIEN"],                             "bucket": "liens"},
        {"match_exact": ["LIEN", "FINANCING STATEMENT",
                         "CORPORATE LIEN"],                       "bucket": "liens"},
    ],
}

# Hillsborough's built-in coded → canonical map (was in lien_engine.py:65-75).
# Lives in value_maps.DocType on the Hillsborough liens mapping row after
# this migration so the engine no longer needs the constant.
HILLSBOROUGH_DOC_VALUE_MAP = {
    "(D) DEED":                                          "DEED",
    "(TAXDEED) TAX DEED":                                "DEED",
    "(DPL) DEED PLAT":                                   "DEED",
    "(JUD) JUDGMENT":                                    "JUDGMENT",
    "(CCJ) CERTIFIED COPY OF A COURT JUDGMENT":          "JUDGMENT",
    "(LN) LIEN":                                         "LIEN",
    "(LNCORPTX) CORP TAX LIEN FOR STATE OF FLORIDA":     "TAX LIEN",
    "(LP) LIS PENDENS":                                  "LIS PENDENS",
    "LIS PENDENS":                                       "LIS PENDENS",
}


# ---------------------------------------------------------------------------
# Upgrade
# ---------------------------------------------------------------------------

def upgrade() -> None:
    # 1. county_sources — add new columns
    op.add_column(
        "county_sources",
        sa.Column(
            "scrape_mode",
            sa.String(length=32),
            nullable=False,
            server_default="ai_only",
        ),
    )
    op.create_check_constraint(
        "ck_county_sources_scrape_mode",
        "county_sources",
        "scrape_mode IN ('ai_only','playwright_only','playwright_then_ai')",
    )
    op.add_column(
        "county_sources",
        sa.Column("playwright_code", sa.Text(), nullable=True),
    )
    op.add_column(
        "county_sources",
        sa.Column("playwright_code_version", sa.String(length=32), nullable=True),
    )
    op.add_column(
        "county_sources",
        sa.Column(
            "playwright_code_approved",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
    )

    # 2. county_column_mappings — add new transformation fields
    op.add_column(
        "county_column_mappings",
        sa.Column(
            "post_processors",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
    )
    op.add_column(
        "county_column_mappings",
        sa.Column(
            "value_maps",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
    )
    op.add_column(
        "county_column_mappings",
        sa.Column(
            "row_routing",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
    )

    # 3. Backfill scrape_mode + playwright_code from special_flags JSONB.
    # Existing values: 'selector' (permit Hillsborough), 'browser_use',
    # 'download', 'extract', 'download_direct' — only 'selector' maps to a
    # playwright mode; everything else is the AI agent path.
    op.execute(
        sa.text(
            """
            UPDATE county_sources
               SET scrape_mode = CASE special_flags->>'scrape_mode'
                   WHEN 'selector'        THEN 'playwright_then_ai'
                   WHEN 'browser_use'     THEN 'ai_only'
                   WHEN 'download'        THEN 'ai_only'
                   WHEN 'extract'         THEN 'ai_only'
                   WHEN 'download_direct' THEN 'ai_only'
                   ELSE 'ai_only'
                   END,
                   playwright_code          = special_flags->>'playwright_code',
                   playwright_code_version  = special_flags->>'playwright_code_version',
                   playwright_code_approved = COALESCE(
                       (special_flags->>'playwright_code_approved')::boolean,
                       false
                   )
             WHERE special_flags IS NOT NULL
            """
        )
    )

    # 4. Synthesize CountyColumnMapping rows from ORI fields.
    #
    # For every liens source, we want an is_approved=True row with:
    #   mapping         ← ori_column_map (or {} if absent)
    #   post_processors ← [{op: split_on_separator, from: ori_book_page_col, ...}] if set
    #   value_maps      ← {"DocType": ori_doc_type_map} if set
    #   row_routing     ← CANONICAL_LIEN_ROW_ROUTING
    #
    # Hillsborough also gets the HILLSBOROUGH_DOC_VALUE_MAP folded into
    # value_maps.DocType so its built-in coded types get normalized via the
    # mapping rather than via a hardcoded dict in the engine.
    #
    # Done in Python rather than SQL because the value_maps + row_routing
    # structures are nested objects that are awkward to build in SQL.
    bind = op.get_bind()

    lien_sources = bind.execute(
        sa.text(
            """
            SELECT id, county_id, ori_column_map, ori_book_page_col, ori_doc_type_map
              FROM county_sources
             WHERE signal_type = 'liens'
            """
        )
    ).all()

    for src in lien_sources:
        col_map = dict(src.ori_column_map) if src.ori_column_map else {}
        bp_col = src.ori_book_page_col
        doc_map = dict(src.ori_doc_type_map) if src.ori_doc_type_map else {}

        # Hillsborough's coded types get merged into the value_map so the engine
        # constant goes away. Pinellas overrides any key collisions.
        if src.county_id == "hillsborough":
            merged_doc_map = dict(HILLSBOROUGH_DOC_VALUE_MAP)
            merged_doc_map.update(doc_map)
            doc_map = merged_doc_map

        post_processors = []
        if bp_col:
            post_processors.append(
                {
                    "op": "split_on_separator",
                    "from": bp_col,
                    "sep": "/",
                    "into": ["Book", "Page"],
                }
            )

        value_maps = {"DocType": doc_map} if doc_map else {}

        # source_columns: keys of ori_column_map (the raw column names the
        # ColumnMapper expects to see). If empty (Hillsborough), use the
        # canonical liens schema as a self-mapping placeholder.
        if col_map:
            source_columns = sorted(col_map.keys())
        else:
            source_columns = [
                "Grantor", "Grantee", "Instrument", "Legal", "DocType",
                "Book", "Page", "RecordDate", "Filing Amt", "BookType",
            ]

        # Insert as is_approved=True, mapped_by='migration' so admins know it
        # came from the ORI backfill, not LLM or manual entry.
        bind.execute(
            sa.text(
                """
                INSERT INTO county_column_mappings (
                    source_id, source_columns, mapping,
                    post_processors, value_maps, row_routing,
                    is_approved, mapped_by, approved_by, approved_at, created_at
                ) VALUES (
                    :source_id, CAST(:source_columns AS jsonb), CAST(:mapping AS jsonb),
                    CAST(:post_processors AS jsonb),
                    CAST(:value_maps AS jsonb),
                    CAST(:row_routing AS jsonb),
                    TRUE, 'migration', 'migration', NOW(), NOW()
                )
                """
            ),
            {
                "source_id": src.id,
                "source_columns": json.dumps(source_columns),
                "mapping": json.dumps(col_map),
                "post_processors": json.dumps(post_processors) if post_processors else None,
                "value_maps": json.dumps(value_maps) if value_maps else None,
                "row_routing": json.dumps(CANONICAL_LIEN_ROW_ROUTING),
            },
        )


# ---------------------------------------------------------------------------
# Downgrade
# ---------------------------------------------------------------------------

def downgrade() -> None:
    # Drop migration-synthesized CountyColumnMapping rows (anything else is
    # admin work and stays).
    op.execute(
        sa.text(
            """
            DELETE FROM county_column_mappings
             WHERE mapped_by = 'migration'
            """
        )
    )

    # Reverse the new columns on county_column_mappings.
    op.drop_column("county_column_mappings", "row_routing")
    op.drop_column("county_column_mappings", "value_maps")
    op.drop_column("county_column_mappings", "post_processors")

    # Reverse new columns on county_sources.
    op.drop_constraint("ck_county_sources_scrape_mode", "county_sources", type_="check")
    op.drop_column("county_sources", "playwright_code_approved")
    op.drop_column("county_sources", "playwright_code_version")
    op.drop_column("county_sources", "playwright_code")
    op.drop_column("county_sources", "scrape_mode")
