"""fa103 - tax_deed_auctions and vacant_parcels tables

Revision ID: fa103
Revises:
Create Date: 2026-06-29
"""

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB
from alembic import op

revision = "fa103"
down_revision = None
branch_labels = None
depends_on = None

_NEW_CHECK = (
    "source_type IN ("
    "'lien_tcl', 'lien_ccl', 'lien_hoa', 'lien_ml', 'lien_tl', 'lien_unknown', 'lis_pendens',"
    "'judgments', 'deeds', 'evictions', 'divorce_filings', 'probate', 'bankruptcy',"
    "'violations', 'foreclosures', 'permits', 'tax_delinquencies',"
    "'roofing_permits', 'storm_damage', 'flood_damage', 'insurance_claims', 'fire_incidents',"
    "'sunbiz', 'property_appraiser', 'dbpr_company',"
    "'tax_deed_auction', 'vacant_land'"
    ")"
)

_OLD_CHECK = (
    "source_type IN ("
    "'lien_tcl', 'lien_ccl', 'lien_hoa', 'lien_ml', 'lien_tl', 'lien_unknown', 'lis_pendens',"
    "'judgments', 'deeds', 'evictions', 'divorce_filings', 'probate', 'bankruptcy',"
    "'violations', 'foreclosures', 'permits', 'tax_delinquencies',"
    "'roofing_permits', 'storm_damage', 'flood_damage', 'insurance_claims', 'fire_incidents',"
    "'sunbiz', 'property_appraiser', 'dbpr_company'"
    ")"
)


def upgrade() -> None:  # noqa: E501  Applied via scripts/apply_fa103_tax_deed_vacant_land.py
    op.create_table(
        "tax_deed_auctions",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("property_id", sa.Integer(), sa.ForeignKey("properties.id", ondelete="SET NULL"), nullable=True),
        sa.Column("county_id", sa.String(50), nullable=False),
        sa.Column("parcel_id", sa.String(100), nullable=True),
        sa.Column("auction_date", sa.Date(), nullable=False),
        sa.Column("case_number", sa.String(100), nullable=False),
        sa.Column("certificate_number", sa.String(50), nullable=True),
        sa.Column("certificate_year", sa.SmallInteger(), nullable=True),
        sa.Column("status", sa.String(50), nullable=True),
        sa.Column("auction_type", sa.String(50), nullable=True),
        sa.Column("opening_bid", sa.Numeric(14, 2), nullable=True),
        sa.Column("sold_amount", sa.Numeric(14, 2), nullable=True),
        sa.Column("sold_to", sa.String(255), nullable=True),
        sa.Column("raw_fields", JSONB(), nullable=True),
        sa.Column("match_method", sa.String(30), nullable=True),
        sa.Column("match_confidence", sa.Numeric(4, 3), nullable=True),
        sa.Column("scraped_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint("county_id", "auction_date", "case_number", name="uq_tax_deed_auction"),
    )
    op.create_index("ix_tax_deed_auctions_property_id", "tax_deed_auctions", ["property_id"])
    op.create_index("ix_tax_deed_auctions_county_date", "tax_deed_auctions", ["county_id", "auction_date"])
    op.create_index("ix_tax_deed_auctions_parcel_id", "tax_deed_auctions", ["parcel_id"])

    op.create_table(
        "vacant_parcels",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("property_id", sa.Integer(), sa.ForeignKey("properties.id", ondelete="SET NULL"), nullable=True),
        sa.Column("county_id", sa.String(50), nullable=False),
        sa.Column("parcel_id", sa.String(100), nullable=False),
        sa.Column("use_code", sa.String(20), nullable=True),
        sa.Column("property_use", sa.String(200), nullable=True),
        sa.Column("dor_code", sa.String(20), nullable=True),
        sa.Column("source_name", sa.String(20), nullable=False),
        sa.Column("last_verified", sa.Date(), nullable=False),
        sa.Column("scraped_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint("county_id", "parcel_id", name="uq_vacant_parcel"),
    )
    op.create_index("ix_vacant_parcels_property_id", "vacant_parcels", ["property_id"])
    op.create_index("ix_vacant_parcels_county_id", "vacant_parcels", ["county_id"])

    op.drop_constraint("check_run_stats_source_type", "scraper_run_stats", type_="check")
    op.create_check_constraint("check_run_stats_source_type", "scraper_run_stats", _NEW_CHECK)


def downgrade() -> None:
    op.drop_index("ix_vacant_parcels_county_id", table_name="vacant_parcels")
    op.drop_index("ix_vacant_parcels_property_id", table_name="vacant_parcels")
    op.drop_table("vacant_parcels")

    op.drop_index("ix_tax_deed_auctions_parcel_id", table_name="tax_deed_auctions")
    op.drop_index("ix_tax_deed_auctions_county_date", table_name="tax_deed_auctions")
    op.drop_index("ix_tax_deed_auctions_property_id", table_name="tax_deed_auctions")
    op.drop_table("tax_deed_auctions")

    op.drop_constraint("check_run_stats_source_type", "scraper_run_stats", type_="check")
    op.create_check_constraint("check_run_stats_source_type", "scraper_run_stats", _OLD_CHECK)
