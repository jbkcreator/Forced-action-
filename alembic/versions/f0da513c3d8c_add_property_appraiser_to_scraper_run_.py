"""add property_appraiser to scraper_run_stats source_type constraint

Revision ID: f0da513c3d8c
Revises: 7baab3a62175
Create Date: 2026-05-22 14:59:18.887949

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'f0da513c3d8c'
down_revision: Union[str, Sequence[str], None] = '7baab3a62175'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_OLD_CHECK = (
    "source_type IN ("
    "'lien_tcl','lien_ccl','lien_hoa','lien_ml','lien_tl','lis_pendens',"
    "'judgments','deeds','evictions','divorce_filings','probate','bankruptcy',"
    "'violations','foreclosures','permits','tax_delinquencies',"
    "'roofing_permits','storm_damage','flood_damage','insurance_claims','fire_incidents'"
    ")"
)

_NEW_CHECK = (
    "source_type IN ("
    "'lien_tcl','lien_ccl','lien_hoa','lien_ml','lien_tl','lis_pendens',"
    "'judgments','deeds','evictions','divorce_filings','probate','bankruptcy',"
    "'violations','foreclosures','permits','tax_delinquencies',"
    "'roofing_permits','storm_damage','flood_damage','insurance_claims','fire_incidents',"
    "'sunbiz','property_appraiser'"
    ")"
)


def upgrade() -> None:
    op.drop_constraint("check_run_stats_source_type", "scraper_run_stats", type_="check")
    op.create_check_constraint("check_run_stats_source_type", "scraper_run_stats", _NEW_CHECK)


def downgrade() -> None:
    op.drop_constraint("check_run_stats_source_type", "scraper_run_stats", type_="check")
    op.create_check_constraint("check_run_stats_source_type", "scraper_run_stats", _OLD_CHECK)
