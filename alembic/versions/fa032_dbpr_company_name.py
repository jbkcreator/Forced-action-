"""fa032_dbpr_company_name — add company name (DBA) to dbpr_contacts

The bulk CILB CSV extract has no business/DBA name, only the individual
qualifier (full_name). Company name is scraped per-license from
myfloridalicense.com (see docs/adr/0003). Adds the scraped value plus a
status column so the weekly job can distinguish "no DBA on this license"
(none) from "not scraped yet" (pending), and never re-scrape terminal rows.

Revision ID: fa032_dbpr_company_name
Revises:     fa031_skip_trace_waterfall
Create Date: 2026-05-27
"""

import sqlalchemy as sa
from alembic import op

revision = "fa032_dbpr_company_name"
down_revision = "fa031_skip_trace_waterfall"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Idempotent — columns/constraints may already exist if applied outside Alembic
    op.execute("ALTER TABLE dbpr_contacts ADD COLUMN IF NOT EXISTS company_name VARCHAR(255)")
    op.execute(
        "ALTER TABLE dbpr_contacts ADD COLUMN IF NOT EXISTS"
        " company_name_status VARCHAR(20) NOT NULL DEFAULT 'pending'"
    )
    op.execute(
        "ALTER TABLE dbpr_contacts ADD COLUMN IF NOT EXISTS"
        " company_name_scraped_at TIMESTAMPTZ"
    )
    op.execute(
        "ALTER TABLE dbpr_contacts DROP CONSTRAINT IF EXISTS check_dbpr_company_name_status"
    )
    op.create_check_constraint(
        "check_dbpr_company_name_status",
        "dbpr_contacts",
        "company_name_status IN ('pending', 'found', 'none', 'failed')",
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_dbpr_company_name_status"
        " ON dbpr_contacts (company_name_status)"
    )

    # Allow the company scraper to record its run under a new source_type.
    op.drop_constraint("check_run_stats_source_type", "scraper_run_stats", type_="check")
    op.create_check_constraint(
        "check_run_stats_source_type",
        "scraper_run_stats",
        "source_type IN ("
        "'lien_tcl', 'lien_ccl', 'lien_hoa', 'lien_ml', 'lien_tl', 'lien_unknown', 'lis_pendens',"
        "'judgments', 'deeds', 'evictions', 'divorce_filings', 'probate', 'bankruptcy',"
        "'violations', 'foreclosures', 'permits', 'tax_delinquencies',"
        "'roofing_permits', 'storm_damage', 'flood_damage', 'insurance_claims', 'fire_incidents',"
        "'sunbiz', 'property_appraiser', 'dbpr_company'"
        ")",
    )


def downgrade() -> None:
    op.drop_constraint("check_run_stats_source_type", "scraper_run_stats", type_="check")
    op.create_check_constraint(
        "check_run_stats_source_type",
        "scraper_run_stats",
        "source_type IN ("
        "'lien_tcl', 'lien_ccl', 'lien_hoa', 'lien_ml', 'lien_tl', 'lien_unknown', 'lis_pendens',"
        "'judgments', 'deeds', 'evictions', 'divorce_filings', 'probate', 'bankruptcy',"
        "'violations', 'foreclosures', 'permits', 'tax_delinquencies',"
        "'roofing_permits', 'storm_damage', 'flood_damage', 'insurance_claims', 'fire_incidents',"
        "'sunbiz', 'property_appraiser'"
        ")",
    )

    op.drop_index("ix_dbpr_company_name_status", table_name="dbpr_contacts")
    op.drop_constraint("check_dbpr_company_name_status", "dbpr_contacts", type_="check")
    op.drop_column("dbpr_contacts", "company_name_scraped_at")
    op.drop_column("dbpr_contacts", "company_name_status")
    op.drop_column("dbpr_contacts", "company_name")
