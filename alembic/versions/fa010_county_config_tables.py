"""fa010_county_config_tables

Create counties, county_sources, and county_column_mappings tables.
Seed Hillsborough (from existing counties.json data) and Pinellas
(from PINELLAS_EXPANSION_RESEARCH.md) rows.

Revision ID: fa010_county_config_tables
Revises:     fa009_annual_lock_tier
Create Date: 2026-05-06
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB
from datetime import datetime, timezone

revision = 'fa010_county_config_tables'
down_revision = '97efacd72d79'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ------------------------------------------------------------------
    # counties
    # ------------------------------------------------------------------
    op.create_table(
        "counties",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("county_id", sa.String(50), nullable=False),
        sa.Column("display_name", sa.String(100), nullable=False),
        sa.Column("fips", sa.String(10), nullable=True),
        sa.Column("nws_zone", sa.String(20), nullable=True),
        sa.Column("parcel_id_format", sa.String(20), nullable=True, server_default="folio"),
        sa.Column("bankruptcy_division", sa.String(10), nullable=True),
        sa.Column("city_filer_keywords", JSONB, nullable=True, server_default="[]"),
        sa.Column("code_lien_type_map", JSONB, nullable=True, server_default="{}"),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint("county_id", name="uq_counties_county_id"),
    )
    op.create_index("idx_counties_county_id", "counties", ["county_id"])
    op.create_index("idx_counties_is_active", "counties", ["is_active"])

    # ------------------------------------------------------------------
    # county_sources
    # ------------------------------------------------------------------
    op.create_table(
        "county_sources",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column(
            "county_id",
            sa.String(50),
            sa.ForeignKey("counties.county_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("signal_type", sa.String(50), nullable=False),
        sa.Column("source_name", sa.String(100), nullable=True),
        sa.Column("url", sa.Text(), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("navigation_hint", sa.Text(), nullable=True),
        sa.Column("output_format", sa.String(20), nullable=True),
        sa.Column("date_range_available", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column("frequency", sa.String(20), nullable=True, server_default="daily"),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column("special_flags", JSONB, nullable=True, server_default="{}"),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint("county_id", "signal_type", name="uq_county_signal_source"),
    )
    op.create_index("idx_county_sources_county_id", "county_sources", ["county_id"])
    op.create_index("idx_county_sources_signal_type", "county_sources", ["signal_type"])
    op.create_index("idx_county_sources_is_active", "county_sources", ["is_active"])

    # ------------------------------------------------------------------
    # county_column_mappings
    # ------------------------------------------------------------------
    op.create_table(
        "county_column_mappings",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column(
            "source_id",
            sa.Integer(),
            sa.ForeignKey("county_sources.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("source_columns", JSONB, nullable=False),
        sa.Column("mapping", JSONB, nullable=False),
        sa.Column("is_approved", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("approved_by", sa.String(100), nullable=True),
        sa.Column("approved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("idx_county_col_mappings_source_id", "county_column_mappings", ["source_id"])
    op.create_index("idx_county_col_mappings_is_approved", "county_column_mappings", ["is_approved"])

    # ------------------------------------------------------------------
    # Seed data — Hillsborough
    # ------------------------------------------------------------------
    counties_table = sa.table(
        "counties",
        sa.column("county_id", sa.String),
        sa.column("display_name", sa.String),
        sa.column("fips", sa.String),
        sa.column("nws_zone", sa.String),
        sa.column("parcel_id_format", sa.String),
        sa.column("bankruptcy_division", sa.String),
        sa.column("city_filer_keywords", JSONB),
        sa.column("code_lien_type_map", JSONB),
        sa.column("is_active", sa.Boolean),
    )

    op.bulk_insert(counties_table, [
        {
            "county_id": "hillsborough",
            "display_name": "Hillsborough County",
            "fips": "12057",
            "nws_zone": "FLZ151",
            "parcel_id_format": "folio",
            "bankruptcy_division": "8",
            "city_filer_keywords": ["CITY OF TAMPA", "HILLSBOROUGH COUNTY"],
            "code_lien_type_map": {"TCL": "TAMPA", "CCL": None},
            "is_active": True,
        },
        {
            "county_id": "pinellas",
            "display_name": "Pinellas County",
            "fips": "12103",
            "nws_zone": "FLZ050",
            "parcel_id_format": "strap",
            "bankruptcy_division": "8",
            "city_filer_keywords": [
                "PINELLAS COUNTY",
                "CITY OF ST. PETERSBURG",
                "CITY OF CLEARWATER",
                "CITY OF LARGO",
                "CITY OF PINELLAS PARK",
                "CITY OF DUNEDIN",
                "CITY OF TARPON SPRINGS",
            ],
            "code_lien_type_map": {},
            "is_active": True,
        },
    ])

    # ------------------------------------------------------------------
    # Seed data — Hillsborough sources
    # ------------------------------------------------------------------
    sources_table = sa.table(
        "county_sources",
        sa.column("county_id", sa.String),
        sa.column("signal_type", sa.String),
        sa.column("source_name", sa.String),
        sa.column("url", sa.Text),
        sa.column("description", sa.Text),
        sa.column("navigation_hint", sa.Text),
        sa.column("output_format", sa.String),
        sa.column("date_range_available", sa.Boolean),
        sa.column("frequency", sa.String),
        sa.column("is_active", sa.Boolean),
        sa.column("special_flags", JSONB),
    )

    op.bulk_insert(sources_table, [
        # ---- Hillsborough ----
        {
            "county_id": "hillsborough",
            "signal_type": "foreclosures",
            "source_name": "Hillsborough RealForeclose",
            "url": "https://www.hillsborough.realforeclose.com/index.cfm",
            "description": "Hillsborough County foreclosure auction portal listing upcoming tax deed and mortgage foreclosure auctions.",
            "navigation_hint": "Navigate to search, set date range, extract all auction listings.",
            "output_format": "table",
            "date_range_available": True,
            "frequency": "daily",
            "is_active": True,
            "special_flags": {},
        },
        {
            "county_id": "hillsborough",
            "signal_type": "liens",
            "source_name": "Hillsborough Clerk ORI",
            "url": "https://publicaccess.hillsclerk.com/oripublicaccess/",
            "description": "Hillsborough County Official Records portal. Contains liens, judgments, deeds, lis pendens, probate, and divorce judgments.",
            "navigation_hint": "Search by recording date range. Download CSV export of all results.",
            "output_format": "csv",
            "date_range_available": True,
            "frequency": "daily",
            "is_active": True,
            "special_flags": {},
        },
        {
            "county_id": "hillsborough",
            "signal_type": "violations",
            "source_name": "Hillsborough Accela Enforcement",
            "url": "https://aca-prod.accela.com/HCFL/Cap/CapHome.aspx?module=Enforcement&TabName=Enforcement",
            "description": "Hillsborough County code enforcement and violation portal via Accela.",
            "navigation_hint": "Search by date range in Enforcement module, export CSV.",
            "output_format": "csv",
            "date_range_available": True,
            "frequency": "daily",
            "is_active": True,
            "special_flags": {},
        },
        {
            "county_id": "hillsborough",
            "signal_type": "permits",
            "source_name": "Hillsborough Accela Building",
            "url": "https://aca-prod.accela.com/HCFL/Cap/CapHome.aspx?module=Building",
            "description": "Hillsborough County building permit portal via Accela.",
            "navigation_hint": "Navigate to Building module, search by date range, download CSV export.",
            "output_format": "csv",
            "date_range_available": True,
            "frequency": "daily",
            "is_active": True,
            "special_flags": {},
        },
        {
            "county_id": "hillsborough",
            "signal_type": "court_records",
            "source_name": "Hillsborough Clerk Court",
            "url": "https://publicrec.hillsclerk.com/Civil/dailyfilings/",
            "description": "Hillsborough County clerk court records. Contains civil filings, evictions, divorces, foreclosures, probate.",
            "navigation_hint": "Search by filed date range, download results.",
            "output_format": "csv",
            "date_range_available": True,
            "frequency": "daily",
            "is_active": True,
            "special_flags": {},
        },
        {
            "county_id": "hillsborough",
            "signal_type": "tax_delinquency",
            "source_name": "Hillsborough Tax Delinquency",
            "url": "https://hillsborough.county-taxes.com",
            "description": "Hillsborough County tax delinquency and certificate data.",
            "navigation_hint": "Access tax report at /reports/real-estate or search by parcel.",
            "output_format": "csv",
            "date_range_available": False,
            "frequency": "weekly",
            "is_active": True,
            "special_flags": {},
        },
        {
            "county_id": "hillsborough",
            "signal_type": "master_data",
            "source_name": "HCPA Bulk Data",
            "url": "https://downloads.hcpafl.org/",
            "description": "Hillsborough County Property Appraiser bulk parcel data. Nightly refresh.",
            "navigation_hint": "Direct CSV download links — no browser navigation needed.",
            "output_format": "csv",
            "date_range_available": False,
            "frequency": "daily",
            "is_active": True,
            "special_flags": {},
        },
        # ---- Pinellas ----
        {
            "county_id": "pinellas",
            "signal_type": "foreclosures",
            "source_name": "Pinellas RealForeclose",
            "url": "https://pinellas.realforeclose.com/index.cfm",
            "description": "Pinellas County foreclosure auction portal listing upcoming tax deed and mortgage foreclosure auctions.",
            "navigation_hint": "Navigate to search, set date range, extract all auction listings.",
            "output_format": "table",
            "date_range_available": True,
            "frequency": "daily",
            "is_active": True,
            "special_flags": {},
        },
        {
            "county_id": "pinellas",
            "signal_type": "liens",
            "source_name": "Pinellas Clerk ORI",
            "url": "https://officialrecords.mypinellasclerk.gov",
            "description": "Pinellas County official records portal. Contains liens, judgments, deeds, lis pendens, probate, and divorce judgments.",
            "navigation_hint": "Search by recording date range. Download CSV export of all results.",
            "output_format": "csv",
            "date_range_available": True,
            "frequency": "daily",
            "is_active": True,
            "special_flags": {},
        },
        {
            "county_id": "pinellas",
            "signal_type": "violations",
            "source_name": "Pinellas GovQA (PRR)",
            "url": "https://pinellas.govqa.us",
            "description": "PRR-only source. No programmatic scraping available. Submit public records request for violation data.",
            "navigation_hint": None,
            "output_format": "csv",
            "date_range_available": False,
            "frequency": "manual",
            "is_active": True,
            "special_flags": {"prr_only": True},
        },
        {
            "county_id": "pinellas",
            "signal_type": "permits",
            "source_name": "Pinellas Accela Building",
            "url": "https://aca-prod.accela.com/PINELLAS",
            "description": "Pinellas County Accela building permit portal.",
            "navigation_hint": "Navigate to Building module, search by date range, download CSV export.",
            "output_format": "csv",
            "date_range_available": True,
            "frequency": "daily",
            "is_active": True,
            "special_flags": {},
        },
        {
            "county_id": "pinellas",
            "signal_type": "court_records",
            "source_name": "Pinellas Clerk Court",
            "url": "https://courtrecords.mypinellasclerk.gov",
            "description": "Pinellas County clerk court records. Contains evictions, divorces, foreclosures, probate.",
            "navigation_hint": "Search by filed date range, download Excel export.",
            "output_format": "excel",
            "date_range_available": True,
            "frequency": "daily",
            "is_active": True,
            "special_flags": {"style_col": "Style/Description"},
        },
        {
            "county_id": "pinellas",
            "signal_type": "tax_delinquency",
            "source_name": "Pinellas County Taxes",
            "url": "https://pinellas.county-taxes.com/public/search/property_tax",
            "description": "Pinellas County tax delinquency search portal.",
            "navigation_hint": "Search for delinquent tax accounts, extract all results.",
            "output_format": "table",
            "date_range_available": False,
            "frequency": "weekly",
            "is_active": True,
            "special_flags": {},
        },
        {
            "county_id": "pinellas",
            "signal_type": "master_data",
            "source_name": "PCPAO Bulk Data",
            "url": "https://www.pcpao.gov/tools-data/data-downloads/raw-database-files",
            "description": "Pinellas County Property Appraiser bulk data. 15 CSV tables, nightly refresh.",
            "navigation_hint": "Direct CSV download links — no browser navigation needed.",
            "output_format": "csv",
            "date_range_available": False,
            "frequency": "daily",
            "is_active": True,
            "special_flags": {
                "bulk_tables": [
                    "RP_PROPERTY_INFO",
                    "RP_EXEMPTIONS",
                    "RP_BUILDING",
                    "RP_STRUCTURAL_ELEMENTS",
                    "RP_ALL_SITE_ADDRESSES",
                    "RP_PERMITS",
                    "RP_SALES_HISTORY",
                ]
            },
        },
    ])


def downgrade() -> None:
    op.drop_table("county_column_mappings")
    op.drop_table("county_sources")
    op.drop_table("counties")
