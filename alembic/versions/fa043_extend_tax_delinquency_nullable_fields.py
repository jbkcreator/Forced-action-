"""extend tax delinquency nullable fields

Revision ID: fa043_extend_tax_delinquency_fields
Revises: fa042_parcel_id_normalized
Create Date: 2026-05-26

Adds nullable tax certificate/source snapshot fields so existing
tax_delinquencies rows can remain untouched during the schema expansion.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "fa043_extend_tax_delinquency_fields"
down_revision: Union[str, Sequence[str], None] = "fa042_parcel_id_normalized"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("tax_delinquencies", sa.Column("source_report", sa.String(length=100), nullable=True))
    op.add_column("tax_delinquencies", sa.Column("account_number", sa.String(length=50), nullable=True))
    op.add_column("tax_delinquencies", sa.Column("alternate_key", sa.String(length=50), nullable=True))
    op.add_column("tax_delinquencies", sa.Column("parcel_number", sa.String(length=100), nullable=True))
    op.add_column("tax_delinquencies", sa.Column("owner_name", sa.String(length=255), nullable=True))
    op.add_column("tax_delinquencies", sa.Column("owner_address", sa.String(length=500), nullable=True))
    op.add_column("tax_delinquencies", sa.Column("property_address", sa.String(length=500), nullable=True))
    op.add_column("tax_delinquencies", sa.Column("certificate_number", sa.String(length=50), nullable=True))
    op.add_column("tax_delinquencies", sa.Column("certificate_status", sa.String(length=50), nullable=True))
    op.add_column("tax_delinquencies", sa.Column("issued_date", sa.Date(), nullable=True))
    op.add_column("tax_delinquencies", sa.Column("bidder_number", sa.String(length=50), nullable=True))
    op.add_column("tax_delinquencies", sa.Column("certificate_buyer", sa.String(length=255), nullable=True))
    op.add_column("tax_delinquencies", sa.Column("certificate_buyer_address", sa.String(length=500), nullable=True))
    op.add_column("tax_delinquencies", sa.Column("face_amount", sa.Numeric(12, 2), nullable=True))
    op.add_column("tax_delinquencies", sa.Column("account_balance_amount", sa.Numeric(12, 2), nullable=True))
    op.add_column("tax_delinquencies", sa.Column("interest_rate", sa.Numeric(8, 4), nullable=True))
    op.add_column("tax_delinquencies", sa.Column("assessed_value", sa.Numeric(14, 2), nullable=True))
    op.add_column("tax_delinquencies", sa.Column("account_status", sa.String(length=50), nullable=True))
    op.add_column("tax_delinquencies", sa.Column("deed_status", sa.String(length=50), nullable=True))
    op.add_column("tax_delinquencies", sa.Column("date_redeemed", sa.Date(), nullable=True))
    op.add_column("tax_delinquencies", sa.Column("purchased_date", sa.Date(), nullable=True))
    op.add_column("tax_delinquencies", sa.Column("county_held", sa.Boolean(), nullable=True))
    op.add_column("tax_delinquencies", sa.Column("standard_flags", sa.String(length=255), nullable=True))
    op.add_column("tax_delinquencies", sa.Column("custom_flags", sa.String(length=255), nullable=True))
    op.add_column("tax_delinquencies", sa.Column("use_code", sa.String(length=50), nullable=True))
    op.add_column("tax_delinquencies", sa.Column("raw_source_data", postgresql.JSONB(astext_type=sa.Text()), nullable=True))
    op.add_column("tax_delinquencies", sa.Column("created_at", sa.DateTime(), nullable=True))
    op.add_column("tax_delinquencies", sa.Column("updated_at", sa.DateTime(), nullable=True))

    op.create_index(op.f("ix_tax_delinquencies_account_number"), "tax_delinquencies", ["account_number"], unique=False)
    op.create_index(op.f("ix_tax_delinquencies_alternate_key"), "tax_delinquencies", ["alternate_key"], unique=False)
    op.create_index(op.f("ix_tax_delinquencies_parcel_number"), "tax_delinquencies", ["parcel_number"], unique=False)
    op.create_index(op.f("ix_tax_delinquencies_owner_name"), "tax_delinquencies", ["owner_name"], unique=False)
    op.create_index(op.f("ix_tax_delinquencies_certificate_number"), "tax_delinquencies", ["certificate_number"], unique=False)
    op.create_index(op.f("ix_tax_delinquencies_certificate_status"), "tax_delinquencies", ["certificate_status"], unique=False)
    op.create_index(op.f("ix_tax_delinquencies_account_status"), "tax_delinquencies", ["account_status"], unique=False)
    op.create_index(op.f("ix_tax_delinquencies_deed_status"), "tax_delinquencies", ["deed_status"], unique=False)
    op.create_index("idx_tax_delinquency_county_status", "tax_delinquencies", ["county_id", "account_status"], unique=False)
    op.create_index("idx_tax_delinquency_cert_status", "tax_delinquencies", ["certificate_status"], unique=False)
    op.create_index("idx_tax_delinquency_county_account", "tax_delinquencies", ["county_id", "source_account_number"], unique=False)
    op.create_index("idx_tax_delinquency_parcel", "tax_delinquencies", ["parcel_number"], unique=False)


def downgrade() -> None:
    op.drop_index("idx_tax_delinquency_parcel", table_name="tax_delinquencies")
    op.drop_index("idx_tax_delinquency_county_account", table_name="tax_delinquencies")
    op.drop_index("idx_tax_delinquency_cert_status", table_name="tax_delinquencies")
    op.drop_index("idx_tax_delinquency_county_status", table_name="tax_delinquencies")
    op.drop_index(op.f("ix_tax_delinquencies_deed_status"), table_name="tax_delinquencies")
    op.drop_index(op.f("ix_tax_delinquencies_account_status"), table_name="tax_delinquencies")
    op.drop_index(op.f("ix_tax_delinquencies_certificate_status"), table_name="tax_delinquencies")
    op.drop_index(op.f("ix_tax_delinquencies_certificate_number"), table_name="tax_delinquencies")
    op.drop_index(op.f("ix_tax_delinquencies_owner_name"), table_name="tax_delinquencies")
    op.drop_index(op.f("ix_tax_delinquencies_parcel_number"), table_name="tax_delinquencies")
    op.drop_index(op.f("ix_tax_delinquencies_alternate_key"), table_name="tax_delinquencies")
    op.drop_index(op.f("ix_tax_delinquencies_account_number"), table_name="tax_delinquencies")

    op.drop_column("tax_delinquencies", "updated_at")
    op.drop_column("tax_delinquencies", "created_at")
    op.drop_column("tax_delinquencies", "raw_source_data")
    op.drop_column("tax_delinquencies", "use_code")
    op.drop_column("tax_delinquencies", "custom_flags")
    op.drop_column("tax_delinquencies", "standard_flags")
    op.drop_column("tax_delinquencies", "county_held")
    op.drop_column("tax_delinquencies", "purchased_date")
    op.drop_column("tax_delinquencies", "date_redeemed")
    op.drop_column("tax_delinquencies", "deed_status")
    op.drop_column("tax_delinquencies", "account_status")
    op.drop_column("tax_delinquencies", "assessed_value")
    op.drop_column("tax_delinquencies", "interest_rate")
    op.drop_column("tax_delinquencies", "account_balance_amount")
    op.drop_column("tax_delinquencies", "face_amount")
    op.drop_column("tax_delinquencies", "certificate_buyer_address")
    op.drop_column("tax_delinquencies", "certificate_buyer")
    op.drop_column("tax_delinquencies", "bidder_number")
    op.drop_column("tax_delinquencies", "issued_date")
    op.drop_column("tax_delinquencies", "certificate_status")
    op.drop_column("tax_delinquencies", "certificate_number")
    op.drop_column("tax_delinquencies", "property_address")
    op.drop_column("tax_delinquencies", "owner_address")
    op.drop_column("tax_delinquencies", "owner_name")
    op.drop_column("tax_delinquencies", "parcel_number")
    op.drop_column("tax_delinquencies", "alternate_key")
    op.drop_column("tax_delinquencies", "account_number")
    op.drop_column("tax_delinquencies", "source_report")
