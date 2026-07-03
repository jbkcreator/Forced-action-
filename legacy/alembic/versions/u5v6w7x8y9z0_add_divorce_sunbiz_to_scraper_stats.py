"""add_divorce_sunbiz_to_scraper_stats_source_type

Revision ID: u5v6w7x8y9z0
Revises: t4u5v6w7x8y9
Create Date: 2026-04-27

Expand scraper_run_stats.source_type check constraint to include
'divorce_filings' and 'sunbiz'.
"""
from alembic import op

revision = 'u5v6w7x8y9z0'
down_revision = 't4u5v6w7x8y9'
branch_labels = None
depends_on = None

_ALL = (
    "'lien_tcl', 'lien_ccl', 'lien_hoa', 'lien_ml', 'lien_tl',"
    "'judgments', 'deeds', 'evictions', 'probate', 'bankruptcy',"
    "'violations', 'foreclosures', 'permits', 'tax_delinquencies',"
    "'roofing_permits', 'storm_damage', 'flood_damage', 'insurance_claims', 'fire_incidents',"
    "'lis_pendens', 'divorce_filings', 'sunbiz'"
)

_PREV = (
    "'lien_tcl', 'lien_ccl', 'lien_hoa', 'lien_ml', 'lien_tl',"
    "'judgments', 'deeds', 'evictions', 'probate', 'bankruptcy',"
    "'violations', 'foreclosures', 'permits', 'tax_delinquencies',"
    "'roofing_permits', 'storm_damage', 'flood_damage', 'insurance_claims', 'fire_incidents',"
    "'lis_pendens'"
)


def upgrade() -> None:
    op.drop_constraint('check_run_stats_source_type', 'scraper_run_stats', type_='check')
    op.create_check_constraint(
        'check_run_stats_source_type',
        'scraper_run_stats',
        f"source_type IN ({_ALL})",
    )


def downgrade() -> None:
    op.drop_constraint('check_run_stats_source_type', 'scraper_run_stats', type_='check')
    op.create_check_constraint(
        'check_run_stats_source_type',
        'scraper_run_stats',
        f"source_type IN ({_PREV})",
    )
