"""
Database models for Distressed Property Intelligence Platform.
Implements the Hub-and-Spoke architecture with properties as the central hub.
"""

from datetime import date, datetime, timezone
from typing import List, Optional

from sqlalchemy import (
    Boolean,
    Column,
    Date,
    DateTime,
    ForeignKey,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    CheckConstraint,
    Index,
    func,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.orm import DeclarativeBase, relationship, Mapped, mapped_column


class Base(DeclarativeBase):
    """Base class for all models."""
    pass


# ============================================================================
# 1. CENTRAL HUB (Anchor)
# ============================================================================

class Property(Base):
    """
    The central hub table from which all other data radiates.
    Contains core property information and addresses.
    """
    __tablename__ = "properties"

    # Primary Key
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    # Required Fields
    parcel_id: Mapped[str] = mapped_column(String(100), unique=True, nullable=False, index=True)

    # Address Information
    address: Mapped[Optional[str]] = mapped_column(String(255))
    city: Mapped[Optional[str]] = mapped_column(String(100))
    state: Mapped[Optional[str]] = mapped_column(String(2))
    zip: Mapped[Optional[str]] = mapped_column(String(10))
    jurisdiction: Mapped[Optional[str]] = mapped_column(String(100))

    # Property Characteristics
    property_type: Mapped[Optional[str]] = mapped_column(String(50))
    year_built: Mapped[Optional[int]] = mapped_column(Integer)
    sq_ft: Mapped[Optional[float]] = mapped_column(Numeric(10, 2))
    beds: Mapped[Optional[float]] = mapped_column(Numeric(4, 1))
    baths: Mapped[Optional[float]] = mapped_column(Numeric(4, 1))
    lot_size: Mapped[Optional[float]] = mapped_column(Numeric(12, 2))

    # Geolocation (API Gap)
    lat: Mapped[Optional[float]] = mapped_column(Numeric(10, 8))
    lon: Mapped[Optional[float]] = mapped_column(Numeric(11, 8))

    # Legal Information
    legal_description: Mapped[Optional[str]] = mapped_column(Text)

    # Multi-county
    county_id: Mapped[Optional[str]] = mapped_column(String(50), default='hillsborough', index=True)

    # CRM Integration
    gohighlevel_contact_id: Mapped[Optional[str]] = mapped_column(String(100), unique=True)
    sync_status: Mapped[Optional[str]] = mapped_column(String(20), default="pending")
    last_crm_sync: Mapped[Optional[datetime]] = mapped_column(DateTime)

    # Audit Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))

    # Relationships (1:1 and 1:Many)
    owner: Mapped[Optional["Owner"]] = relationship("Owner", back_populates="property", uselist=False, cascade="all, delete-orphan")
    financial: Mapped[Optional["Financial"]] = relationship("Financial", back_populates="property", uselist=False, cascade="all, delete-orphan")
    code_violations: Mapped[List["CodeViolation"]] = relationship("CodeViolation", back_populates="property", cascade="all, delete-orphan")
    legal_and_liens: Mapped[List["LegalAndLien"]] = relationship("LegalAndLien", back_populates="property", cascade="all, delete-orphan")
    deeds: Mapped[List["Deed"]] = relationship("Deed", back_populates="property", cascade="all, delete-orphan")
    legal_proceedings: Mapped[List["LegalProceeding"]] = relationship("LegalProceeding", back_populates="property", cascade="all, delete-orphan")
    tax_delinquencies: Mapped[List["TaxDelinquency"]] = relationship("TaxDelinquency", back_populates="property", cascade="all, delete-orphan")
    foreclosures: Mapped[List["Foreclosure"]] = relationship("Foreclosure", back_populates="property", cascade="all, delete-orphan")
    building_permits: Mapped[List["BuildingPermit"]] = relationship("BuildingPermit", back_populates="property", cascade="all, delete-orphan")
    incidents: Mapped[List["Incident"]] = relationship("Incident", back_populates="property", cascade="all, delete-orphan")
    distress_scores: Mapped[List["DistressScore"]] = relationship("DistressScore", back_populates="property", cascade="all, delete-orphan")

    # Indexes
    __table_args__ = (
        Index("idx_property_address", "address"),
        Index("idx_property_city_state", "city", "state"),
        Index("idx_property_zip", "zip"),
        Index("idx_property_county_id", "county_id"),
        Index("idx_property_sync_status", "sync_status"),
        CheckConstraint("sync_status IN ('pending', 'pending_sync', 'synced', 'sync_failed', 'error')", name="check_sync_status"),
    )

    def __repr__(self):
        return f"<Property(id={self.id}, parcel_id='{self.parcel_id}', address='{self.address}')>"


# ============================================================================
# 2. PROPERTY EXTENSIONS (1:1 Relationships)
# ============================================================================

class Owner(Base):
    """
    Owner information for each property.
    One-to-one relationship with Property.
    """
    __tablename__ = "owners"

    # Primary Key
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    # Foreign Key
    property_id: Mapped[int] = mapped_column(ForeignKey("properties.id"), unique=True, nullable=False, index=True)

    # Owner Information
    owner_name: Mapped[Optional[str]] = mapped_column(Text)
    mailing_address: Mapped[Optional[str]] = mapped_column(String(255))
    owner_type: Mapped[Optional[str]] = mapped_column(String(50))
    ownership_years: Mapped[Optional[float]] = mapped_column(Numeric(5, 2))
    absentee_status: Mapped[Optional[str]] = mapped_column(String(50))

    # Contact Information (API Gap - Skip Traced)
    phone_1: Mapped[Optional[str]] = mapped_column(String(20))
    phone_2: Mapped[Optional[str]] = mapped_column(String(20))
    phone_3: Mapped[Optional[str]] = mapped_column(String(20))
    email_1: Mapped[Optional[str]] = mapped_column(String(255))
    email_2: Mapped[Optional[str]] = mapped_column(String(255))
    linkedin_url: Mapped[Optional[str]] = mapped_column(String(255))

    # Owner Intelligence (API Gap)
    employer_name: Mapped[Optional[str]] = mapped_column(String(255))
    estimated_income: Mapped[Optional[float]] = mapped_column(Numeric(12, 2))
    credit_score_tier: Mapped[Optional[str]] = mapped_column(String(50))
    skip_trace_success: Mapped[Optional[bool]] = mapped_column(Boolean, default=False)

    # Multi-county
    county_id: Mapped[Optional[str]] = mapped_column(String(50), default='hillsborough', index=True)

    # Relationship
    property: Mapped["Property"] = relationship("Property", back_populates="owner")

    # Indexes
    __table_args__ = (
        Index("idx_owner_name", "owner_name"),
        Index("idx_owner_type", "owner_type"),
        Index("idx_absentee_status", "absentee_status"),
        Index("idx_owner_county_id", "county_id"),
        CheckConstraint("owner_type IN ('Individual', 'LLC', 'Trust', 'Estate', 'Corporate')", name="check_owner_type"),
        CheckConstraint("absentee_status IN ('In-County', 'Out-of-County', 'Out-of-State')", name="check_absentee_status"),
    )

    def __repr__(self):
        return f"<Owner(id={self.id}, property_id={self.property_id}, name='{self.owner_name}')>"


class Financial(Base):
    """
    Financial information for each property.
    One-to-one relationship with Property.
    """
    __tablename__ = "financials"

    # Primary Key
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    # Foreign Key
    property_id: Mapped[int] = mapped_column(ForeignKey("properties.id"), unique=True, nullable=False, index=True)

    # County Valuations
    assessed_value_mkt: Mapped[Optional[float]] = mapped_column(Numeric(12, 2))
    assessed_value_tax: Mapped[Optional[float]] = mapped_column(Numeric(12, 2))

    # Last Sale Information
    last_sale_price: Mapped[Optional[float]] = mapped_column(Numeric(12, 2))
    last_sale_date: Mapped[Optional[datetime]] = mapped_column(Date)
    value_change_yoy: Mapped[Optional[float]] = mapped_column(Numeric(5, 2))

    # Debt Information (API Gap)
    est_mortgage_bal: Mapped[Optional[float]] = mapped_column(Numeric(12, 2))
    mtg_1: Mapped[Optional[float]] = mapped_column(Numeric(12, 2))
    mtg_2: Mapped[Optional[float]] = mapped_column(Numeric(12, 2))
    total_lien_amount: Mapped[Optional[float]] = mapped_column(Numeric(12, 2))
    total_debt: Mapped[Optional[float]] = mapped_column(Numeric(12, 2))

    # Equity Calculations (API Gap)
    est_equity: Mapped[Optional[float]] = mapped_column(Numeric(12, 2))
    equity_pct: Mapped[Optional[float]] = mapped_column(Numeric(5, 2))

    # Market Metrics
    price_per_sq_ft: Mapped[Optional[float]] = mapped_column(Numeric(8, 2))
    annual_tax_amount: Mapped[Optional[float]] = mapped_column(Numeric(10, 2))
    homestead_exempt: Mapped[Optional[bool]] = mapped_column(Boolean, default=False)

    # Investment Metrics (API Gap)
    est_repair_cost: Mapped[Optional[float]] = mapped_column(Numeric(12, 2))
    arv: Mapped[Optional[float]] = mapped_column(Numeric(12, 2))

    # Multi-county
    county_id: Mapped[Optional[str]] = mapped_column(String(50), default='hillsborough', index=True)

    # Relationship
    property: Mapped["Property"] = relationship("Property", back_populates="financial")

    # Indexes
    __table_args__ = (
        Index("idx_financial_assessed_value", "assessed_value_mkt"),
        Index("idx_financial_equity_pct", "equity_pct"),
        Index("idx_financial_county_id", "county_id"),
    )

    def __repr__(self):
        return f"<Financial(id={self.id}, property_id={self.property_id}, assessed_value_mkt={self.assessed_value_mkt})>"


# ============================================================================
# 3. DISTRESS SIGNAL TABLES (1:Many Relationships)
# ============================================================================

class CodeViolation(Base):
    """
    Code violations for properties.
    One-to-many relationship with Property.
    """
    __tablename__ = "code_violations"

    # Primary Key
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    # Foreign Key
    property_id: Mapped[int] = mapped_column(ForeignKey("properties.id"), nullable=False, index=True)

    # Violation Information
    record_number: Mapped[str] = mapped_column(String(100), unique=True, nullable=False, index=True)
    violation_type: Mapped[Optional[str]] = mapped_column(String(100))
    description: Mapped[Optional[str]] = mapped_column(Text)
    opened_date: Mapped[Optional[datetime]] = mapped_column(Date)
    status: Mapped[Optional[str]] = mapped_column(String(50))
    severity_tier: Mapped[Optional[str]] = mapped_column(String(20))
    fine_amount: Mapped[Optional[float]] = mapped_column(Numeric(10, 2))
    is_lien: Mapped[Optional[bool]] = mapped_column(Boolean, default=False)

    # Load tracking & multi-county
    date_added: Mapped[Optional[date]] = mapped_column(Date, default=date.today, index=True)
    county_id: Mapped[Optional[str]] = mapped_column(String(50), default='hillsborough', index=True)

    # Relationship
    property: Mapped["Property"] = relationship("Property", back_populates="code_violations")

    # Indexes
    __table_args__ = (
        Index("idx_violation_status", "status"),
        Index("idx_violation_severity", "severity_tier"),
        Index("idx_violation_opened_date", "opened_date"),
        CheckConstraint("severity_tier IN ('Critical', 'Major', 'Minor')", name="check_severity_tier"),
    )

    def __repr__(self):
        return f"<CodeViolation(id={self.id}, record_number='{self.record_number}', type='{self.violation_type}')>"


class LegalAndLien(Base):
    """
    Liens and Judgments table for legal claims against properties.
    Handles CCL, TCL, Mechanics Liens, Tax Liens, HOA Liens, Judgments, etc.
    One-to-many relationship with Property.
    """
    __tablename__ = "legal_and_liens"

    # Primary Key
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    # Foreign Key
    property_id: Mapped[int] = mapped_column(ForeignKey("properties.id"), nullable=False, index=True)

    # Discriminator for polymorphic behavior
    record_type: Mapped[str] = mapped_column(String(50), nullable=False, index=True)

    # Lien/Judgment Fields
    filing_date: Mapped[Optional[datetime]] = mapped_column(Date)
    amount: Mapped[Optional[float]] = mapped_column(Numeric(12, 2))
    creditor: Mapped[Optional[str]] = mapped_column(Text)  # Who filed the lien/judgment
    debtor: Mapped[Optional[str]] = mapped_column(Text)  # Property owner
    
    # Document reference fields
    instrument_number: Mapped[Optional[str]] = mapped_column(String(50), unique=True)
    book_type: Mapped[Optional[str]] = mapped_column(String(50))
    book_number: Mapped[Optional[str]] = mapped_column(String(50))
    page_number: Mapped[Optional[str]] = mapped_column(String(50))
    
    # Additional metadata
    document_type: Mapped[Optional[str]] = mapped_column(String(100))  # CCL, TCL, ML, TL, HL, Judgment
    legal_description: Mapped[Optional[str]] = mapped_column(Text)
    meta_data: Mapped[Optional[dict]] = mapped_column(JSONB)  # Additional type-specific fields

    # Match provenance — populated by the loader at insert time
    match_confidence: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    match_method: Mapped[Optional[str]] = mapped_column(String(30), nullable=True)  # legal_desc | owner_name | llm_verified | address | manual

    # Load tracking & multi-county
    date_added: Mapped[Optional[date]] = mapped_column(Date, default=date.today, index=True)
    county_id: Mapped[Optional[str]] = mapped_column(String(50), default='hillsborough', index=True)

    # Relationship
    property: Mapped["Property"] = relationship("Property", back_populates="legal_and_liens")

    # Indexes
    __table_args__ = (
        Index("idx_legal_record_type", "record_type"),
        Index("idx_legal_filing_date", "filing_date"),
        Index("idx_legal_instrument", "instrument_number"),
        Index("idx_legal_meta_data", "meta_data", postgresql_using="gin"),
        Index("idx_legal_match_method", "match_method"),
        CheckConstraint("record_type IN ('Lien', 'Judgment')", name="check_lien_record_type"),
        CheckConstraint(
            "match_method IN ('legal_desc', 'owner_name', 'llm_verified', 'address', 'manual')",
            name="check_legal_match_method",
        ),
    )

    def __repr__(self):
        return f"<LegalAndLien(id={self.id}, record_type='{self.record_type}', amount={self.amount})>"


class Deed(Base):
    """
    Property ownership transfer records (Deeds, Tax Deeds).
    Tracks all sales and transfers of property ownership.
    One-to-many relationship with Property.
    """
    __tablename__ = "deeds"

    # Primary Key
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    # Foreign Key
    property_id: Mapped[int] = mapped_column(ForeignKey("properties.id"), nullable=False, index=True)

    # Document reference
    instrument_number: Mapped[str] = mapped_column(String(50), unique=True, nullable=False)
    
    # Transfer parties
    grantor: Mapped[Optional[str]] = mapped_column(Text)  # Seller
    grantee: Mapped[Optional[str]] = mapped_column(Text)  # Buyer
    
    # Transaction details
    record_date: Mapped[Optional[datetime]] = mapped_column(Date)
    sale_price: Mapped[Optional[float]] = mapped_column(Numeric(12, 2))
    deed_type: Mapped[Optional[str]] = mapped_column(String(100))  # Deed, Tax Deed, Warranty Deed, etc.
    
    # Document reference fields
    doc_type: Mapped[Optional[str]] = mapped_column(String(100))
    book_type: Mapped[Optional[str]] = mapped_column(String(50))
    book_number: Mapped[Optional[str]] = mapped_column(String(50))
    page_number: Mapped[Optional[str]] = mapped_column(String(50))
    
    # Legal description
    legal_description: Mapped[Optional[str]] = mapped_column(Text)

    # Load tracking & multi-county
    date_added: Mapped[Optional[date]] = mapped_column(Date, default=date.today, index=True)
    county_id: Mapped[Optional[str]] = mapped_column(String(50), default='hillsborough', index=True)

    # Relationship
    property: Mapped["Property"] = relationship("Property", back_populates="deeds")

    # Indexes
    __table_args__ = (
        Index("idx_deed_record_date", "record_date"),
        Index("idx_deed_instrument", "instrument_number"),
        Index("idx_deed_grantor", "grantor"),
        Index("idx_deed_grantee", "grantee"),
    )

    def __repr__(self):
        return f"<Deed(id={self.id}, instrument='{self.instrument_number}', sale_price={self.sale_price})>"


class LegalProceeding(Base):
    """
    Legal proceedings table for probate, evictions, and bankruptcy cases.
    Handles formal court proceedings affecting properties.
    One-to-many relationship with Property.
    """
    __tablename__ = "legal_proceedings"

    # Primary Key
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    # Foreign Key
    property_id: Mapped[int] = mapped_column(ForeignKey("properties.id"), nullable=False, index=True)

    # Discriminator for polymorphic behavior
    record_type: Mapped[str] = mapped_column(String(50), nullable=False, index=True)

    # Case information
    case_number: Mapped[Optional[str]] = mapped_column(String(100), unique=True)
    filing_date: Mapped[Optional[datetime]] = mapped_column(Date)
    case_status: Mapped[Optional[str]] = mapped_column(String(100))
    
    # Parties involved
    associated_party: Mapped[Optional[str]] = mapped_column(Text)  # Decedent name, tenant name, debtor name
    secondary_party: Mapped[Optional[str]] = mapped_column(Text)  # Petitioner, landlord, etc.
    
    # Financial details (if applicable)
    amount: Mapped[Optional[float]] = mapped_column(Numeric(12, 2))
    
    # Flexible metadata bucket for type-specific fields
    meta_data: Mapped[Optional[dict]] = mapped_column(JSONB)

    # Load tracking & multi-county
    date_added: Mapped[Optional[date]] = mapped_column(Date, default=date.today, index=True)
    county_id: Mapped[Optional[str]] = mapped_column(String(50), default='hillsborough', index=True)

    # Relationship
    property: Mapped["Property"] = relationship("Property", back_populates="legal_proceedings")

    # Indexes
    __table_args__ = (
        Index("idx_proceeding_record_type", "record_type"),
        Index("idx_proceeding_filing_date", "filing_date"),
        Index("idx_proceeding_case_number", "case_number"),
        Index("idx_proceeding_meta_data", "meta_data", postgresql_using="gin"),
        CheckConstraint("record_type IN ('Probate', 'Eviction', 'Bankruptcy')", name="check_proceeding_record_type"),
    )

    def __repr__(self):
        return f"<LegalProceeding(id={self.id}, record_type='{self.record_type}', case_number='{self.case_number}')>"


class TaxDelinquency(Base):
    """
    Tax delinquency records for properties.
    One-to-many relationship with Property.
    """
    __tablename__ = "tax_delinquencies"

    # Primary Key
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    # Foreign Key
    property_id: Mapped[int] = mapped_column(ForeignKey("properties.id"), nullable=False, index=True)

    # Tax Information
    tax_year: Mapped[Optional[int]] = mapped_column(Integer)
    years_delinquent: Mapped[Optional[int]] = mapped_column(Integer)
    total_amount_due: Mapped[Optional[float]] = mapped_column(Numeric(12, 2))
    certificate_data: Mapped[Optional[str]] = mapped_column(String(255))
    deed_app_date: Mapped[Optional[datetime]] = mapped_column(Date)

    # Load tracking & multi-county
    date_added: Mapped[Optional[date]] = mapped_column(Date, default=date.today, index=True)
    county_id: Mapped[Optional[str]] = mapped_column(String(50), default='hillsborough', index=True)

    # Relationship
    property: Mapped["Property"] = relationship("Property", back_populates="tax_delinquencies")

    # Indexes and constraints
    __table_args__ = (
        Index("idx_tax_year", "tax_year"),
        Index("idx_tax_years_delinquent", "years_delinquent"),
        Index("idx_tax_deed_app_date", "deed_app_date"),
        UniqueConstraint("property_id", "tax_year", name="uq_tax_delinquency_property_year"),
    )

    def __repr__(self):
        return f"<TaxDelinquency(id={self.id}, tax_year={self.tax_year}, amount_due={self.total_amount_due})>"


class Foreclosure(Base):
    """
    Foreclosure records for properties.
    One-to-many relationship with Property.
    """
    __tablename__ = "foreclosures"

    # Primary Key
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    # Foreign Key
    property_id: Mapped[int] = mapped_column(ForeignKey("properties.id"), nullable=False, index=True)

    # Foreclosure Information
    case_number: Mapped[str] = mapped_column(String(100), unique=True, nullable=False, index=True)
    plaintiff: Mapped[Optional[str]] = mapped_column(Text)
    filing_date: Mapped[Optional[datetime]] = mapped_column(Date)
    lis_pendens_date: Mapped[Optional[datetime]] = mapped_column(Date)
    judgment_amount: Mapped[Optional[float]] = mapped_column(Numeric(12, 2))
    auction_date: Mapped[Optional[datetime]] = mapped_column(DateTime)
    case_status: Mapped[Optional[str]] = mapped_column(String(100))

    # Load tracking & multi-county
    date_added: Mapped[Optional[date]] = mapped_column(Date, default=date.today, index=True)
    county_id: Mapped[Optional[str]] = mapped_column(String(50), default='hillsborough', index=True)

    # Relationship
    property: Mapped["Property"] = relationship("Property", back_populates="foreclosures")

    # Indexes
    __table_args__ = (
        Index("idx_foreclosure_filing_date", "filing_date"),
        Index("idx_foreclosure_auction_date", "auction_date"),
        Index("idx_foreclosure_plaintiff", "plaintiff"),
        Index("idx_foreclosure_case_status", "case_status"),
    )

    def __repr__(self):
        return f"<Foreclosure(id={self.id}, case_number='{self.case_number}', plaintiff='{self.plaintiff}')>"


class BuildingPermit(Base):
    """
    Building permit records for properties.
    One-to-many relationship with Property.
    """
    __tablename__ = "building_permits"

    # Primary Key
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    # Foreign Key
    property_id: Mapped[int] = mapped_column(ForeignKey("properties.id"), nullable=False, index=True)

    # Permit Information
    permit_number: Mapped[str] = mapped_column(String(100), unique=True, nullable=False, index=True)
    permit_type: Mapped[Optional[str]] = mapped_column(String(100))
    issue_date: Mapped[Optional[datetime]] = mapped_column(Date)
    expire_date: Mapped[Optional[datetime]] = mapped_column(Date)
    status: Mapped[Optional[str]] = mapped_column(String(50))

    # Enforcement flag — True for stop work orders, after-the-fact, failed/expired/revoked/suspended
    is_enforcement_permit: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False, index=True)

    # Load tracking & multi-county
    date_added: Mapped[Optional[date]] = mapped_column(Date, default=date.today, index=True)
    county_id: Mapped[Optional[str]] = mapped_column(String(50), default='hillsborough', index=True)

    # Relationship
    property: Mapped["Property"] = relationship("Property", back_populates="building_permits")

    # Indexes
    __table_args__ = (
        Index("idx_permit_type", "permit_type"),
        Index("idx_permit_status", "status"),
        Index("idx_permit_expire_date", "expire_date"),
    )

    def __repr__(self):
        return f"<BuildingPermit(id={self.id}, permit_number='{self.permit_number}', type='{self.permit_type}')>"


class Incident(Base):
    """
    Incident records (arrests, police dispatches, fires) for properties.
    One-to-many relationship with Property.
    """
    __tablename__ = "incidents"

    # Primary Key
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    # Foreign Key
    property_id: Mapped[int] = mapped_column(ForeignKey("properties.id"), nullable=False, index=True)

    # Incident Information
    incident_type: Mapped[Optional[str]] = mapped_column(String(50))
    incident_date: Mapped[Optional[datetime]] = mapped_column(Date)
    arrest_count_12m: Mapped[Optional[int]] = mapped_column(Integer)
    crime_types: Mapped[Optional[dict]] = mapped_column(JSONB)
    problem_prop_flag: Mapped[Optional[bool]] = mapped_column(Boolean, default=False)

    # Load tracking & multi-county
    date_added: Mapped[Optional[date]] = mapped_column(Date, default=date.today, index=True)
    county_id: Mapped[Optional[str]] = mapped_column(String(50), default='hillsborough', index=True)

    # Relationship
    property: Mapped["Property"] = relationship("Property", back_populates="incidents")

    # Indexes
    __table_args__ = (
        Index("idx_incident_type", "incident_type"),
        Index("idx_incident_date", "incident_date"),
        Index("idx_incident_problem_flag", "problem_prop_flag"),
        Index("idx_incident_crime_types", "crime_types", postgresql_using="gin"),
        CheckConstraint(
            "incident_type IN ('Arrest', 'Police Dispatch', 'Fire', "
            "'roofing_permit', 'storm_damage', 'flood_damage', 'insurance_claim')",
            name="check_incident_type",
        ),
    )

    def __repr__(self):
        return f"<Incident(id={self.id}, type='{self.incident_type}', date={self.incident_date})>"


# ============================================================================
# 4. SCORING & INTELLIGENCE
# ============================================================================

class DistressScore(Base):
    """
    Distress scoring records for properties.
    Stores CDS Engine results and lead tier classifications.
    One-to-many relationship with Property (tracks scoring history).
    """
    __tablename__ = "distress_scores"

    # Primary Key
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    # Foreign Key
    property_id: Mapped[int] = mapped_column(ForeignKey("properties.id"), nullable=False, index=True)
    vertical_scores: Mapped[Optional[dict]] = mapped_column(JSONB)

    # Scoring Information
    score_date: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)
    final_cds_score: Mapped[Optional[float]] = mapped_column(Numeric(5, 2))
    lead_tier: Mapped[Optional[str]] = mapped_column(String(50))
    distress_types: Mapped[Optional[dict]] = mapped_column(JSONB)
    urgency_level: Mapped[Optional[str]] = mapped_column(String(20))
    multiplier: Mapped[Optional[float]] = mapped_column(Numeric(4, 2))
    factor_scores: Mapped[Optional[dict]] = mapped_column(JSONB)
    qualified: Mapped[Optional[bool]] = mapped_column(Boolean, default=False)

    # Multi-county
    county_id: Mapped[Optional[str]] = mapped_column(String(50), default='hillsborough', index=True)

    # Scoring batch identifier — int(UTC epoch) set at start of score_all_properties()
    scoring_run_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    # Relationship
    property: Mapped["Property"] = relationship("Property", back_populates="distress_scores")

    # Indexes
    __table_args__ = (
        Index("idx_score_date", "score_date"),
        Index("idx_score_final_cds", "final_cds_score"),
        Index("idx_score_lead_tier", "lead_tier"),
        Index("idx_score_qualified", "qualified"),
        Index("idx_score_county_id", "county_id"),
        Index("idx_score_distress_types", "distress_types", postgresql_using="gin"),
        Index("idx_score_scoring_run_id", "scoring_run_id"),
        CheckConstraint("urgency_level IN ('Immediate', 'High', 'Medium', 'Low')", name="check_urgency_level"),
        CheckConstraint("lead_tier IN ('Ultra Platinum', 'Platinum', 'Gold', 'Silver', 'Bronze')", name="check_lead_tier"),
    )

    def __repr__(self):
        return f"<DistressScore(id={self.id}, property_id={self.property_id}, score={self.final_cds_score}, tier='{self.lead_tier}')>"


# ============================================================================
# 5. M1 — SUBSCRIBER & REVENUE TABLES
# ============================================================================

class FoundingSubscriberCount(Base):
    """
    Tracks founding subscriber count per tier/vertical/county.
    Used for atomic checkout price selection and live countdown on landing page.
    """
    __tablename__ = "founding_subscriber_counts"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    tier: Mapped[str] = mapped_column(String(20), nullable=False)          # starter | pro | dominator
    vertical: Mapped[str] = mapped_column(String(50), nullable=False)      # roofing | remediation | investor
    county_id: Mapped[str] = mapped_column(String(50), nullable=False)
    count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc)
    )

    __table_args__ = (
        UniqueConstraint("tier", "vertical", "county_id", name="uq_founding_tier_vertical_county"),
        Index("idx_founding_county_id", "county_id"),
        CheckConstraint("tier IN ('starter', 'pro', 'dominator')", name="check_founding_tier"),
    )

    def __repr__(self):
        return f"<FoundingSubscriberCount(tier='{self.tier}', vertical='{self.vertical}', county='{self.county_id}', count={self.count})>"


class Subscriber(Base):
    """
    Paid subscriber record. Founding rate is locked at checkout and never overwritten.
    """
    __tablename__ = "subscribers"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    # Stripe identifiers
    stripe_customer_id: Mapped[str] = mapped_column(String(100), unique=True, nullable=False, index=True)
    stripe_subscription_id: Mapped[Optional[str]] = mapped_column(String(100), unique=True, index=True)

    # Plan details
    tier: Mapped[str] = mapped_column(String(20), nullable=False)          # starter | pro | dominator
    vertical: Mapped[str] = mapped_column(String(50), nullable=False)      # roofing | remediation | investor
    county_id: Mapped[str] = mapped_column(String(50), nullable=False)

    # Founding rate lock — set at checkout, never overwritten
    founding_member: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    founding_price_id: Mapped[Optional[str]] = mapped_column(String(100))  # Stripe price_id locked at checkout
    rate_locked_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    escalated_at: Mapped[Optional[datetime]] = mapped_column(DateTime)     # set when 6-month founding rate expires

    # Subscription state
    status: Mapped[str] = mapped_column(String(20), default='active', nullable=False)  # active | grace | churned | cancelled
    billing_date: Mapped[Optional[datetime]] = mapped_column(DateTime)
    grace_expires_at: Mapped[Optional[datetime]] = mapped_column(DateTime)

    # GHL integration
    ghl_contact_id: Mapped[Optional[str]] = mapped_column(String(100), index=True)
    ghl_stage: Mapped[Optional[int]] = mapped_column(Integer)              # 5 = paid, 7 = churned

    # Event Feed access
    event_feed_uuid: Mapped[Optional[str]] = mapped_column(String(36), unique=True, index=True)

    # Contact
    email: Mapped[Optional[str]] = mapped_column(String(255), index=True)
    name: Mapped[Optional[str]] = mapped_column(String(255))

    # ── 2B: Saved card + wallet + referral ────────────────────────────
    has_saved_card: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    stripe_payment_method_id: Mapped[Optional[str]] = mapped_column(String(100))
    referral_code: Mapped[Optional[str]] = mapped_column(String(20), unique=True, index=True)
    auto_mode_enabled: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)

    bundle_purchases = relationship("BundlePurchase", back_populates="subscriber")

    # Audit
    created_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc)
    )

    __table_args__ = (
        Index("idx_subscriber_county_id", "county_id"),
        Index("idx_subscriber_status", "status"),
        Index("idx_subscriber_vertical", "vertical"),
        CheckConstraint(
            "tier IN ('free', 'starter', 'pro', 'dominator', 'data_only', 'autopilot_lite', 'autopilot_pro', 'partner')",
            name="check_subscriber_tier",
        ),
        CheckConstraint(
            "status IN ('active', 'grace', 'churned', 'cancelled', 'paused')",
            name="check_subscriber_status",
        ),
    )

    def __repr__(self):
        return f"<Subscriber(id={self.id}, email='{self.email}', tier='{self.tier}', founding={self.founding_member})>"


class ZipTerritory(Base):
    """
    ZIP code exclusivity per vertical per county.
    One subscriber holds a ZIP per vertical at a time.
    """
    __tablename__ = "zip_territories"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    zip_code: Mapped[str] = mapped_column(String(10), nullable=False)
    vertical: Mapped[str] = mapped_column(String(50), nullable=False)
    county_id: Mapped[str] = mapped_column(String(50), nullable=False)

    # Ownership
    subscriber_id: Mapped[Optional[int]] = mapped_column(ForeignKey("subscribers.id"), nullable=True, index=True)
    status: Mapped[str] = mapped_column(String(20), default='available', nullable=False)  # available | locked | grace

    # Timing
    locked_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    grace_expires_at: Mapped[Optional[datetime]] = mapped_column(DateTime)

    # Waitlist — array of emails waiting for this ZIP to open
    waitlist_emails: Mapped[Optional[list]] = mapped_column(ARRAY(String(255)), default=list)

    # Audit
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc)
    )

    subscriber: Mapped[Optional["Subscriber"]] = relationship("Subscriber")

    __table_args__ = (
        UniqueConstraint("zip_code", "vertical", "county_id", name="uq_zip_vertical_county"),
        Index("idx_zip_territory_status", "status"),
        Index("idx_zip_territory_county_id", "county_id"),
        CheckConstraint("status IN ('available', 'locked', 'grace')", name="check_zip_status"),
    )

    def __repr__(self):
        return f"<ZipTerritory(zip='{self.zip_code}', vertical='{self.vertical}', status='{self.status}')>"


class SentLead(Base):
    """
    Tracks which property leads have been emailed to which subscriber.
    Used for 7-day duplicate suppression in the daily lead email.
    ON CONFLICT DO UPDATE refreshes sent_at so the window slides forward on resend.
    """
    __tablename__ = "sent_leads"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    subscriber_id: Mapped[int] = mapped_column(ForeignKey("subscribers.id"), nullable=False)
    property_id: Mapped[int] = mapped_column(ForeignKey("properties.id"), nullable=False)
    sent_at: Mapped[datetime] = mapped_column(
        DateTime, default=lambda: datetime.now(timezone.utc), nullable=False
    )
    source: Mapped[Optional[str]] = mapped_column(String(40), default="daily_email")

    # Refund tracking (populated for lead_unlock_payment rows only)
    stripe_payment_intent_id: Mapped[Optional[str]] = mapped_column(String(100), nullable=True, index=True)
    refunded_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    refund_reason: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    stripe_refund_id: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)

    __table_args__ = (
        UniqueConstraint("subscriber_id", "property_id", name="uq_sent_lead"),
        Index("idx_sent_lead_subscriber_sent_at", "subscriber_id", "sent_at"),
        Index("idx_sent_leads_source", "source"),
    )

    def __repr__(self):
        return f"<SentLead(subscriber_id={self.subscriber_id}, property_id={self.property_id})>"


class EnrichedContact(Base):
    """
    Skip-traced contact data from BatchSkipTracing (primary) and IDI (fallback).
    Linked to a property. Pushed to GHL on creation.
    """
    __tablename__ = "enriched_contacts"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    property_id: Mapped[int] = mapped_column(ForeignKey("properties.id"), nullable=False, index=True)
    county_id: Mapped[str] = mapped_column(String(50), nullable=False, index=True)

    # Skip-trace results
    mobile_phone: Mapped[Optional[str]] = mapped_column(String(20))
    landline: Mapped[Optional[str]] = mapped_column(String(20))
    email: Mapped[Optional[str]] = mapped_column(String(255))
    mailing_address: Mapped[Optional[str]] = mapped_column(String(255))
    llc_owner_name: Mapped[Optional[str]] = mapped_column(String(255))
    relative_contacts: Mapped[Optional[dict]] = mapped_column(JSONB)  # relative contact chain

    # Source tracking
    source: Mapped[str] = mapped_column(String(50), nullable=False)   # batch_skip_tracing | idi
    match_success: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)

    # GHL sync
    ghl_contact_id: Mapped[Optional[str]] = mapped_column(String(100), index=True)
    ghl_synced_at: Mapped[Optional[datetime]] = mapped_column(DateTime)

    # Audit
    enriched_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc))

    property: Mapped["Property"] = relationship("Property")

    __table_args__ = (
        Index("idx_enriched_match_success", "match_success"),
        Index("idx_enriched_source", "source"),
        CheckConstraint("source IN ('batch_skip_tracing', 'idi')", name="check_enriched_source"),
    )

    def __repr__(self):
        return f"<EnrichedContact(id={self.id}, property_id={self.property_id}, source='{self.source}', match={self.match_success})>"


# ============================================================================
# 6. OPERATIONAL TELEMETRY
# ============================================================================

class ScraperRunStats(Base):
    """
    Daily scraper run statistics per source type.

    One row per (run_date, source_type, county_id).
    All lien subtypes are broken out individually rather than grouped
    under a single 'liens' bucket, giving per-type visibility.

    source_type values:
      Liens       → 'lien_tcl', 'lien_ccl', 'lien_hoa', 'lien_ml', 'lien_tl'
      Judgments   → 'judgments'
      Deeds       → 'deeds'
      Evictions   → 'evictions'
      Probate     → 'probate'
      Bankruptcy  → 'bankruptcy'
      Violations  → 'violations'
      Foreclosures→ 'foreclosures'
      Permits     → 'permits'
      Tax Deliq.  → 'tax_delinquencies'
      Roofing     → 'roofing_permits'
      Storm       → 'storm_damage'
      Flood       → 'flood_damage'
      Insurance   → 'insurance_claims'
      Fire        → 'fire_incidents'
    """
    __tablename__ = "scraper_run_stats"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    # Identity
    run_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    source_type: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    county_id: Mapped[str] = mapped_column(String(50), nullable=False, default='hillsborough', index=True)

    # Core counts
    total_scraped: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    matched: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    unmatched: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    skipped: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    scored: Mapped[int] = mapped_column(Integer, default=0, nullable=False)

    # Run metadata
    run_success: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    error_type: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    error_message: Mapped[Optional[str]] = mapped_column(Text)
    duration_seconds: Mapped[Optional[float]] = mapped_column(Numeric(10, 2))

    # Audit
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=lambda: datetime.now(timezone.utc), nullable=False
    )

    __table_args__ = (
        UniqueConstraint("run_date", "source_type", "county_id", name="uq_scraper_run_stats"),
        Index("idx_run_stats_date_source", "run_date", "source_type"),
        CheckConstraint(
            "source_type IN ("
            "'lien_tcl', 'lien_ccl', 'lien_hoa', 'lien_ml', 'lien_tl',"
            "'judgments', 'deeds', 'evictions', 'probate', 'bankruptcy',"
            "'violations', 'foreclosures', 'permits', 'tax_delinquencies',"
            "'roofing_permits', 'storm_damage', 'flood_damage', 'insurance_claims', 'fire_incidents'"
            ")",
            name="check_run_stats_source_type",
        ),
    )

    def __repr__(self):
        return (
            f"<ScraperRunStats(date={self.run_date}, source='{self.source_type}', "
            f"scraped={self.total_scraped}, matched={self.matched})>"
        )


class ScraperAlertLog(Base):
    """
    Deduplication log for scraper ops alerts.

    Before sending any alert, load_validator and subscriber_email check this
    table for a recent row matching (source_type, county_id, alert_type).
    If one exists within ALERT_COOLDOWN_HOURS, the alert is suppressed.
    After sending, a row is written here.

    alert_type values:
      'scraper_error'  — scraper raised a non-no-data exception
      'zero_records'   — scraper succeeded but returned 0 records vs baseline
      'low_count'      — scraper count dropped >70% below 7-day baseline
      'health_check'   — subscriber_email health check detected stale/failed data
    """
    __tablename__ = "scraper_alert_log"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    source_type: Mapped[str] = mapped_column(String(50), nullable=False)
    county_id: Mapped[str] = mapped_column(String(50), nullable=False, default="hillsborough")
    alert_type: Mapped[str] = mapped_column(String(50), nullable=False)
    alerted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    __table_args__ = (
        Index("idx_scraper_alert_log_lookup", "source_type", "county_id", "alert_type", "alerted_at"),
    )

    def __repr__(self):
        return (
            f"<ScraperAlertLog(type='{self.alert_type}', source='{self.source_type}', "
            f"at={self.alerted_at})>"
        )


class PlatformDailyStats(Base):
    """
    Platform-level daily health metrics — one row per (run_date, county_id).

    Aggregates across all scrapers and CDS runs to give a single daily summary:
      - signals_*       : totals rolled up from scraper_run_stats at write time
      - properties_*    : CDS engine throughput for this run
      - leads_*         : new / updated / qualified counts from distress_scores
      - tier_*          : count of properties at each tier after today's run

    Written by the CDS engine at the end of each score_all_properties batch.
    Uses upsert (ON CONFLICT DO UPDATE) so partial/retry runs accumulate safely.
    """
    __tablename__ = "platform_daily_stats"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    # Identity
    run_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    county_id: Mapped[str] = mapped_column(String(50), nullable=False, default='hillsborough', index=True)

    # ── Signal pipeline (rolled up from scraper_run_stats) ────────────────────
    signals_scraped: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    signals_matched: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    signals_skipped: Mapped[int] = mapped_column(Integer, default=0, nullable=False)

    # ── CDS scoring ───────────────────────────────────────────────────────────
    properties_scored: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    properties_with_signals: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    score_runs_total: Mapped[int] = mapped_column(Integer, default=0, nullable=False)

    # ── Lead output ───────────────────────────────────────────────────────────
    leads_new: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    leads_updated: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    leads_unchanged: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    leads_qualified: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    leads_upgraded: Mapped[int] = mapped_column(Integer, default=0, nullable=False)

    # ── Tier snapshot (properties at each tier in today's batch) ─────────────
    tier_ultra_platinum: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    tier_platinum: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    tier_gold: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    tier_silver: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    tier_bronze: Mapped[int] = mapped_column(Integer, default=0, nullable=False)

    # Audit
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=lambda: datetime.now(timezone.utc), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    __table_args__ = (
        UniqueConstraint("run_date", "county_id", name="uq_platform_daily_stats"),
        Index("idx_platform_stats_date", "run_date"),
    )

    def __repr__(self):
        return (
            f"<PlatformDailyStats(date={self.run_date}, scored={self.properties_scored}, "
            f"leads_new={self.leads_new}, qualified={self.leads_qualified})>"
        )


# ============================================================================
# 7. UNMATCHED RECORDS STAGING
# ============================================================================

class UnmatchedRecord(Base):
    __tablename__ = "unmatched_records"

    id                  = mapped_column(Integer, primary_key=True, autoincrement=True)
    source_type         = mapped_column(String(50), nullable=False, index=True)   # liens, deeds, evictions, probate, etc.
    county_id           = mapped_column(String(50), nullable=False, default="hillsborough", index=True)
    raw_data            = mapped_column(JSONB, nullable=False)                     # full CSV row as dict
    instrument_number   = mapped_column(String(100), nullable=True, index=True)
    grantor             = mapped_column(Text, nullable=True)
    address_string      = mapped_column(Text, nullable=True)
    match_status        = mapped_column(String(20), nullable=False, default="unmatched", index=True)  # unmatched | matched | skipped
    match_attempted_at  = mapped_column(DateTime(timezone=True), nullable=True)
    matched_property_id = mapped_column(Integer, ForeignKey("properties.id"), nullable=True)
    date_added          = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    __table_args__ = (
        Index("ix_unmatched_source_status", "source_type", "match_status"),
        Index(
            "uq_unmatched_instrument_source_county",
            "instrument_number", "source_type", "county_id",
            unique=True,
            postgresql_where="instrument_number IS NOT NULL",
        ),
    )

    def __repr__(self):
        return f"<UnmatchedRecord(id={self.id}, source='{self.source_type}', status='{self.match_status}')>"


# ============================================================================
# 8. LEAD PACK PURCHASES
# ============================================================================

class LeadPackPurchase(Base):
    """
    Tracks $99 lead pack purchases (5 leads, 72-hour exclusivity).

    When a subscriber purchases a lead pack, the top 5 scored properties
    for their ZIP+vertical are selected, locked for 72 hours exclusively
    to that subscriber, and delivered via email immediately.
    """
    __tablename__ = "lead_pack_purchases"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    # Subscriber who purchased
    subscriber_id: Mapped[int] = mapped_column(
        ForeignKey("subscribers.id"), nullable=False, index=True
    )

    # Purchase scope
    zip_code: Mapped[str] = mapped_column(String(10), nullable=False)
    vertical: Mapped[str] = mapped_column(String(50), nullable=False)
    county_id: Mapped[str] = mapped_column(String(50), nullable=False, default="hillsborough")

    # Stripe reference (unique — prevents double-processing a webhook)
    stripe_payment_intent_id: Mapped[str] = mapped_column(
        String(100), unique=True, nullable=False, index=True
    )

    # Lifecycle
    status: Mapped[str] = mapped_column(
        String(20), default="pending", nullable=False
    )  # pending | delivered | expired

    # Timestamps
    purchased_at: Mapped[datetime] = mapped_column(
        DateTime, default=lambda: datetime.now(timezone.utc), nullable=False
    )
    delivered_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    exclusive_until: Mapped[Optional[datetime]] = mapped_column(DateTime)  # purchased_at + 72h

    # The 5 selected property IDs (set at purchase time)
    lead_ids: Mapped[Optional[list]] = mapped_column(ARRAY(Integer))

    # Relationship
    subscriber: Mapped["Subscriber"] = relationship("Subscriber")

    __table_args__ = (
        CheckConstraint(
            "status IN ('pending', 'delivered', 'expired')",
            name="check_lead_pack_status",
        ),
        Index("idx_lead_pack_zip_vertical", "zip_code", "vertical"),
        Index("idx_lead_pack_exclusive_until", "exclusive_until"),
    )

    def __repr__(self):
        return (
            f"<LeadPackPurchase(id={self.id}, subscriber_id={self.subscriber_id}, "
            f"zip={self.zip_code}, status={self.status})>"
        )


class StripeWebhookEvent(Base):
    """
    Idempotency guard for Stripe webhook events.

    Before processing any event, the handler inserts a row here keyed on the
    Stripe event ID.  The unique constraint on event_id means a second attempt
    to insert the same event raises IntegrityError — which the handler catches
    and treats as "already processed, skip".

    This prevents duplicate email sends caused by:
      - Multiple stripe listen processes forwarding the same event
      - Stripe retrying an event after a transient error
      - Multiple uvicorn workers receiving the same request concurrently
    """
    __tablename__ = "stripe_webhook_events"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    event_id: Mapped[str] = mapped_column(String(100), unique=True, nullable=False, index=True)
    event_type: Mapped[str] = mapped_column(String(100), nullable=False)
    processed_at: Mapped[datetime] = mapped_column(
        DateTime, default=lambda: datetime.now(timezone.utc), nullable=False
    )

    def __repr__(self):
        return f"<StripeWebhookEvent(event_id={self.event_id}, type={self.event_type})>"


# ══════════════════════════════════════════════════════════════════════════════
# Phase 2B Models
# ══════════════════════════════════════════════════════════════════════════════


class WalletBalance(Base):
    """Credit wallet balance per subscriber. One row per subscriber."""
    __tablename__ = "wallet_balances"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    subscriber_id: Mapped[int] = mapped_column(Integer, ForeignKey("subscribers.id"), nullable=False, unique=True, index=True)
    wallet_tier: Mapped[str] = mapped_column(String(20), nullable=False)  # starter_wallet / growth / power
    credits_remaining: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    credits_used_total: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    auto_reload_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    last_reload_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc)
    )

    subscriber = relationship("Subscriber", backref="wallet")

    __table_args__ = (
        CheckConstraint("wallet_tier IN ('starter_wallet', 'growth', 'power')", name="check_wallet_tier"),
    )

    def __repr__(self):
        return f"<WalletBalance(subscriber={self.subscriber_id}, tier={self.wallet_tier}, credits={self.credits_remaining})>"


class WalletTransaction(Base):
    """Individual credit transaction (debit, credit, reload, bonus, refund)."""
    __tablename__ = "wallet_transactions"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    subscriber_id: Mapped[int] = mapped_column(Integer, ForeignKey("subscribers.id"), nullable=False, index=True)
    wallet_id: Mapped[int] = mapped_column(Integer, ForeignKey("wallet_balances.id"), nullable=False, index=True)
    txn_type: Mapped[str] = mapped_column(String(20), nullable=False)  # credit/debit/reload/bonus/refund
    amount: Mapped[int] = mapped_column(Integer, nullable=False)       # positive = added, negative = spent
    balance_after: Mapped[int] = mapped_column(Integer, nullable=False)
    description: Mapped[Optional[str]] = mapped_column(String(255))
    stripe_charge_id: Mapped[Optional[str]] = mapped_column(String(100), index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc))

    wallet = relationship("WalletBalance", backref="transactions")

    __table_args__ = (
        CheckConstraint("txn_type IN ('credit', 'debit', 'reload', 'bonus', 'refund')", name="check_txn_type"),
        Index("idx_wallet_txn_sub_created", "subscriber_id", "created_at"),
    )

    def __repr__(self):
        return f"<WalletTransaction(id={self.id}, type={self.txn_type}, amount={self.amount})>"


class UserSegment(Base):
    """Behavioral segment classification per subscriber (1:1)."""
    __tablename__ = "user_segments"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    subscriber_id: Mapped[int] = mapped_column(Integer, ForeignKey("subscribers.id"), nullable=False, unique=True, index=True)
    segment: Mapped[str] = mapped_column(String(30), nullable=False)  # 8 buckets
    revenue_signal_score: Mapped[Optional[int]] = mapped_column(Integer, default=0)  # 0–100
    last_classified_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc))
    classification_reason: Mapped[Optional[str]] = mapped_column(String(255))
    created_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc)
    )

    subscriber = relationship("Subscriber", backref="segment")

    __table_args__ = (
        CheckConstraint(
            "segment IN ('new', 'browsing', 'engaged', 'wallet_active', 'high_intent', 'lock_candidate', 'at_risk', 'churned')",
            name="check_user_segment",
        ),
    )

    def __repr__(self):
        return f"<UserSegment(subscriber={self.subscriber_id}, segment={self.segment}, score={self.revenue_signal_score})>"


class MessageOutcome(Base):
    """
    Tracks every outbound message (SMS, email, voice) and its conversion attribution.
    Ground truth for all Cora learning — must log from Day 1.
    """
    __tablename__ = "message_outcomes"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    subscriber_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("subscribers.id"), index=True)
    message_type: Mapped[str] = mapped_column(String(20), nullable=False)  # sms/email/voice
    template_id: Mapped[Optional[str]] = mapped_column(String(100))
    variant_id: Mapped[Optional[str]] = mapped_column(String(100), index=True)  # A/B test variant
    channel: Mapped[Optional[str]] = mapped_column(String(50))  # twilio/ses/synthflow
    sent_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)
    delivered_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    opened_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    clicked_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    replied_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    conversion_type: Mapped[Optional[str]] = mapped_column(String(30))  # unlock/wallet/lock/annual/none
    conversion_within_4h: Mapped[bool] = mapped_column(Boolean, default=False)
    conversion_within_24h: Mapped[bool] = mapped_column(Boolean, default=False)
    conversion_within_48h: Mapped[bool] = mapped_column(Boolean, default=False)
    revenue_attributed: Mapped[Optional[float]] = mapped_column(Numeric(10, 2))
    created_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc))

    __table_args__ = (
        CheckConstraint("message_type IN ('sms', 'email', 'voice')", name="check_message_type"),
        Index("idx_msg_outcome_sub_sent", "subscriber_id", "sent_at"),
    )

    def __repr__(self):
        return f"<MessageOutcome(id={self.id}, type={self.message_type}, conversion={self.conversion_type})>"


class DealOutcome(Base):
    """
    Tracks confirmed deals reported by subscribers. Feeds revenue signal score,
    annual push triggers, and attribution.
    """
    __tablename__ = "deal_outcomes"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    subscriber_id: Mapped[int] = mapped_column(Integer, ForeignKey("subscribers.id"), nullable=False, index=True)
    property_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("properties.id"), index=True)
    deal_size_bucket: Mapped[Optional[str]] = mapped_column(String(20))  # 5_10k/10_25k/25k_plus/skip
    deal_amount: Mapped[Optional[float]] = mapped_column(Numeric(12, 2))
    deal_date: Mapped[Optional[date]] = mapped_column(Date)
    lead_source: Mapped[Optional[str]] = mapped_column(String(50))  # which signal drove the lead
    days_to_close: Mapped[Optional[int]] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc))

    __table_args__ = (
        CheckConstraint(
            "deal_size_bucket IN ('5_10k', '10_25k', '25k_plus', 'skip')",
            name="check_deal_size_bucket",
        ),
        Index("idx_deal_outcome_sub_date", "subscriber_id", "deal_date"),
    )

    def __repr__(self):
        return f"<DealOutcome(id={self.id}, subscriber={self.subscriber_id}, bucket={self.deal_size_bucket})>"


class LearningCard(Base):
    """
    Weekly Cora learning summary. Sunday midnight LangGraph job writes one card
    per type. Cora reads the most recent cards at the start of every decision tree.
    """
    __tablename__ = "learning_cards"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    card_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    card_type: Mapped[str] = mapped_column(String(30), nullable=False)
    summary_text: Mapped[str] = mapped_column(Text, nullable=False)
    data_json: Mapped[Optional[dict]] = mapped_column(JSONB)        # raw metrics
    action_taken: Mapped[Optional[str]] = mapped_column(String(255))  # what Cora did
    created_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc))

    __table_args__ = (
        CheckConstraint(
            "card_type IN ('message_perf', 'deal_pattern', 'ab_result', 'churn_signal', 'pricing_test', 'general')",
            name="check_card_type",
        ),
        UniqueConstraint("card_date", "card_type", name="uq_learning_card_date_type"),
    )

    def __repr__(self):
        return f"<LearningCard(date={self.card_date}, type={self.card_type})>"


class ReferralEvent(Base):
    """
    Referral chain tracking. One row per referral attempt.
    Milestone escalation: 1 ref = 5 credits, 3 refs = free month, 5 refs = lock upgrade.
    """
    __tablename__ = "referral_events"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    referrer_subscriber_id: Mapped[int] = mapped_column(Integer, ForeignKey("subscribers.id"), nullable=False, index=True)
    referee_subscriber_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("subscribers.id"), index=True)
    referral_code: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="pending")
    reward_type: Mapped[Optional[str]] = mapped_column(String(30))   # credits/free_month/lock_upgrade
    reward_value: Mapped[Optional[str]] = mapped_column(String(50))
    created_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc))
    confirmed_at: Mapped[Optional[datetime]] = mapped_column(DateTime)

    __table_args__ = (
        CheckConstraint(
            "status IN ('pending', 'confirmed', 'rewarded', 'expired')",
            name="check_referral_status",
        ),
        Index("idx_referral_referrer_status", "referrer_subscriber_id", "status"),
    )

    def __repr__(self):
        return f"<ReferralEvent(referrer={self.referrer_subscriber_id}, status={self.status})>"


class AbTest(Base):
    """A/B test definition. Cora creates and manages tests within guardrail bounds."""
    __tablename__ = "ab_tests"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    test_name: Mapped[str] = mapped_column(String(100), nullable=False, unique=True)
    segment: Mapped[Optional[str]] = mapped_column(String(30))  # target user segment
    variant_a: Mapped[dict] = mapped_column(JSONB, nullable=False)
    variant_b: Mapped[dict] = mapped_column(JSONB, nullable=False)
    traffic_pct: Mapped[int] = mapped_column(Integer, nullable=False, default=10)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="active")
    started_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc))
    ended_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    winner: Mapped[Optional[str]] = mapped_column(String(10))  # 'a' / 'b'

    __table_args__ = (
        CheckConstraint("status IN ('active', 'completed', 'rolled_back')", name="check_ab_test_status"),
        CheckConstraint("traffic_pct BETWEEN 1 AND 100", name="check_ab_traffic_pct"),
    )

    def __repr__(self):
        return f"<AbTest(name={self.test_name}, status={self.status})>"


class AbAssignment(Base):
    """Individual subscriber assignment to an A/B test variant."""
    __tablename__ = "ab_assignments"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    test_id: Mapped[int] = mapped_column(Integer, ForeignKey("ab_tests.id"), nullable=False, index=True)
    subscriber_id: Mapped[int] = mapped_column(Integer, ForeignKey("subscribers.id"), nullable=False, index=True)
    variant: Mapped[str] = mapped_column(String(10), nullable=False)  # 'a' or 'b'
    outcome: Mapped[Optional[str]] = mapped_column(String(30))  # converted/ignored/bounced
    created_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc))

    test = relationship("AbTest", backref="assignments")

    __table_args__ = (
        UniqueConstraint("test_id", "subscriber_id", name="uq_ab_assignment"),
    )


# ══════════════════════════════════════════════════════════════════════════════
# Compliance & Observability Models
# ══════════════════════════════════════════════════════════════════════════════


class SmsOptOut(Base):
    """
    TCPA suppression list. Any number in this table must never receive outbound SMS.
    Populated by inbound STOP/UNSUBSCRIBE/QUIT/CANCEL/END keywords via Twilio webhook.
    Pre-send gate in sms_compliance.can_send() checks this table (Redis in 2B-2).
    """
    __tablename__ = "sms_opt_outs"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    phone: Mapped[str] = mapped_column(String(20), nullable=False, unique=True, index=True)
    keyword_used: Mapped[Optional[str]] = mapped_column(String(20))   # STOP / UNSUBSCRIBE / etc.
    source: Mapped[str] = mapped_column(String(30), nullable=False, default="twilio_inbound")  # twilio_inbound/manual/import
    opted_out_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)

    def __repr__(self):
        return f"<SmsOptOut(phone={self.phone}, keyword={self.keyword_used})>"


class SmsDeadLetter(Base):
    """
    Dead-letter queue for SMS events that failed delivery, hit opt-out, or errored.
    Admin reviews and resolves manually via /admin/dlq endpoint.
    """
    __tablename__ = "sms_dead_letters"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    phone: Mapped[Optional[str]] = mapped_column(String(20), index=True)
    reason: Mapped[str] = mapped_column(String(50), nullable=False)   # opt_out/delivery_failed/error/unresolvable
    payload: Mapped[Optional[dict]] = mapped_column(JSONB)            # original message body + metadata
    created_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc))
    reviewed_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    reviewed_by: Mapped[Optional[str]] = mapped_column(String(100))

    __table_args__ = (
        CheckConstraint(
            "reason IN ('opt_out', 'delivery_failed', 'error', 'unresolvable')",
            name="check_dlq_reason",
        ),
        Index("idx_dlq_reviewed", "reviewed_at"),
    )

    def __repr__(self):
        return f"<SmsDeadLetter(id={self.id}, phone={self.phone}, reason={self.reason})>"


class ApiUsageLog(Base):
    """
    Per-call cost tracking for Claude, Twilio, and Stripe API usage.
    Feeds cost-reduction decisions (Haiku routing) and vendor cost dashboards.
    """
    __tablename__ = "api_usage_logs"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    service: Mapped[str] = mapped_column(String(20), nullable=False)      # claude/twilio/stripe
    model: Mapped[Optional[str]] = mapped_column(String(60))              # haiku/sonnet/opus (Claude only)
    input_tokens: Mapped[Optional[int]] = mapped_column(Integer)
    output_tokens: Mapped[Optional[int]] = mapped_column(Integer)
    cost_usd: Mapped[Optional[float]] = mapped_column(Numeric(10, 6))
    task_type: Mapped[Optional[str]] = mapped_column(String(60))          # sms_copy/classification/conversational_close/etc.
    subscriber_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("subscribers.id"), index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc))

    __table_args__ = (
        CheckConstraint("service IN ('claude', 'twilio', 'stripe')", name="check_api_service"),
        Index("idx_api_usage_service_created", "service", "created_at"),
        Index("idx_api_usage_task_created", "task_type", "created_at"),
    )

    def __repr__(self):
        return f"<ApiUsageLog(service={self.service}, model={self.model}, cost=${self.cost_usd})>"


class BundlePurchase(Base):
    """One-time bundle purchase (weekend/storm/zip_booster/monthly_reload)."""
    __tablename__ = "bundle_purchases"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    subscriber_id: Mapped[int] = mapped_column(Integer, ForeignKey("subscribers.id"), nullable=False, index=True)
    bundle_type: Mapped[str] = mapped_column(String(30), nullable=False)
    stripe_payment_intent_id: Mapped[str] = mapped_column(String(100), unique=True, index=True, nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="pending")
    zip_code: Mapped[Optional[str]] = mapped_column(String(10))
    vertical: Mapped[Optional[str]] = mapped_column(String(50))
    county_id: Mapped[str] = mapped_column(String(50), nullable=False, default="hillsborough")
    credits_awarded: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    lead_ids: Mapped[Optional[list]] = mapped_column(ARRAY(Integer))
    purchased_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc))
    expires_at: Mapped[Optional[datetime]] = mapped_column(DateTime)

    subscriber = relationship("Subscriber", back_populates="bundle_purchases")

    __table_args__ = (
        CheckConstraint(
            "bundle_type IN ('weekend', 'storm', 'zip_booster', 'monthly_reload')",
            name="check_bundle_type",
        ),
        CheckConstraint(
            "status IN ('pending', 'active', 'expired', 'cancelled')",
            name="check_bundle_status",
        ),
        Index("idx_bundle_purchase_subscriber", "subscriber_id"),
        Index("idx_bundle_type_status", "bundle_type", "status"),
    )

    def __repr__(self):
        return f"<BundlePurchase(id={self.id}, type={self.bundle_type}, status={self.status})>"


class SmsOptIn(Base):
    """
    TCPA double opt-in records. Tracks explicit consent via "Reply YES" flow.
    Required before sending proactive outbound SMS to any number.
    Pre-send gate: sms_compliance.has_opted_in() checks this table.
    """
    __tablename__ = "sms_opt_ins"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    phone: Mapped[str] = mapped_column(String(20), nullable=False, unique=True, index=True)
    subscriber_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("subscribers.id"), nullable=True, index=True
    )
    keyword_used: Mapped[Optional[str]] = mapped_column(String(20))     # YES / START / JOIN
    source: Mapped[str] = mapped_column(String(30), nullable=False, default="double_opt_in")
    opt_in_message: Mapped[Optional[str]] = mapped_column(Text)         # consent prompt text (TCPA record)
    opted_in_at: Mapped[datetime] = mapped_column(
        DateTime, default=lambda: datetime.now(timezone.utc), nullable=False
    )
    ip_address: Mapped[Optional[str]] = mapped_column(String(50))       # web opt-in source IP

    __table_args__ = (
        CheckConstraint(
            "source IN ('double_opt_in', 'manual', 'import', 'widget')",
            name="check_opt_in_source",
        ),
        Index("idx_sms_opt_in_subscriber", "subscriber_id"),
    )

    def __repr__(self):
        return f"<SmsOptIn(phone={self.phone}, source={self.source}, at={self.opted_in_at})>"


# ══════════════════════════════════════════════════════════════════════════════
# Agents — Cora LangGraph Audit Log
# ══════════════════════════════════════════════════════════════════════════════


class AgentDecision(Base):
    """
    One row per Cora graph decision. Separate from message_outcomes (which is
    outcome-focused). This is the "why did Cora do X for user Y" audit table —
    the first stop for any operational question about autonomous behaviour.
    """
    __tablename__ = "agent_decisions"

    decision_id: Mapped[str] = mapped_column(String(36), primary_key=True)   # UUID
    graph_name: Mapped[str] = mapped_column(String(60), nullable=False, index=True)
    subscriber_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("subscribers.id"), nullable=True, index=True
    )
    event_type: Mapped[Optional[str]] = mapped_column(String(60), index=True)
    started_at: Mapped[datetime] = mapped_column(
        DateTime, default=lambda: datetime.now(timezone.utc), nullable=False, index=True
    )
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    terminal_status: Mapped[Optional[str]] = mapped_column(String(20))   # completed | aborted | escalated | failed
    tokens_used: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    cost_usd: Mapped[float] = mapped_column(Numeric(10, 6), default=0, nullable=False)
    summary: Mapped[Optional[dict]] = mapped_column(JSONB)

    __table_args__ = (
        CheckConstraint(
            "terminal_status IS NULL OR terminal_status IN ('completed', 'aborted', 'escalated', 'failed')",
            name="check_agent_terminal_status",
        ),
        Index("idx_agent_decisions_graph_started", "graph_name", "started_at"),
    )

    def __repr__(self):
        return f"<AgentDecision(id={self.decision_id[:8]}, graph={self.graph_name}, status={self.terminal_status})>"


class SandboxOutbox(Base):
    """
    Capture table for would-be outbound messages during scenario tests.

    When TWILIO_SANDBOX or SYNTHFLOW_SANDBOX is true, the outbound services
    write one row here instead of (or alongside) the dry-run log. Developers
    inspect these rows via /admin/sandbox/outbox to verify message bodies,
    compliance outcomes, and graph-produced copy.

    Production with TWILIO_ENABLED=true and TWILIO_SANDBOX=false leaves this
    table empty.
    """
    __tablename__ = "sandbox_outbox"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    channel: Mapped[str] = mapped_column(String(20), nullable=False)        # sms | voice | email
    to_number: Mapped[Optional[str]] = mapped_column(String(64))            # E.164 or email
    body: Mapped[str] = mapped_column(Text, nullable=False)
    campaign: Mapped[Optional[str]] = mapped_column(String(100), index=True)
    variant_id: Mapped[Optional[str]] = mapped_column(String(100))
    subscriber_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("subscribers.id"), nullable=True, index=True
    )
    decision_id: Mapped[Optional[str]] = mapped_column(String(36), index=True)
    compliance_allowed: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    compliance_reason: Mapped[Optional[str]] = mapped_column(String(60))
    would_have_delivered: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    sandbox_flag: Mapped[str] = mapped_column(String(40), nullable=False, default="twilio_sandbox")
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=lambda: datetime.now(timezone.utc), nullable=False, index=True
    )

    __table_args__ = (
        CheckConstraint(
            "channel IN ('sms', 'voice', 'email')",
            name="check_sandbox_outbox_channel",
        ),
        Index("idx_sandbox_outbox_sub_created", "subscriber_id", "created_at"),
        Index("idx_sandbox_outbox_campaign_created", "campaign", "created_at"),
    )

    def __repr__(self):
        return f"<SandboxOutbox(id={self.id}, channel={self.channel}, to={self.to_number}, campaign={self.campaign})>"
