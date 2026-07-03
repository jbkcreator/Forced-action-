"""add lien_unknown to scraper_run_stats source_type constraint

Revision ID: fa57e134df37
Revises: y9z0a1b2c3d4_add_raw_response
Create Date: 2026-05-22 18:55:00.000000

"""
from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = 'fa57e134df37'
down_revision: Union[str, Sequence[str], None] = 'y9z0a1b2c3d4_add_raw_response'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_BASE_VALUES = (
    "'lien_tcl','lien_ccl','lien_hoa','lien_ml','lien_tl','lis_pendens',"
    "'judgments','deeds','evictions','divorce_filings','probate','bankruptcy',"
    "'violations','foreclosures','permits','tax_delinquencies',"
    "'roofing_permits','storm_damage','flood_damage','insurance_claims','fire_incidents',"
    "'sunbiz','property_appraiser'"
)

_OLD_CHECK = f"source_type IN ({_BASE_VALUES})"
_NEW_CHECK = f"source_type IN ({_BASE_VALUES},'lien_unknown')"


def upgrade() -> None:
    op.drop_constraint("check_run_stats_source_type", "scraper_run_stats", type_="check")
    op.create_check_constraint("check_run_stats_source_type", "scraper_run_stats", _NEW_CHECK)


def downgrade() -> None:
    op.drop_constraint("check_run_stats_source_type", "scraper_run_stats", type_="check")
    op.create_check_constraint("check_run_stats_source_type", "scraper_run_stats", _OLD_CHECK)
