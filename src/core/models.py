"""
Database models for Distressed Property Intelligence Platform.
Implements the Hub-and-Spoke architecture with properties as the central hub.
"""

from datetime import datetime
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
    CheckConstraint,
    Index,
)
from sqlalchemy.dialects.postgresql import JSONB
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

    # CRM Integration
    gohighlevel_contact_id: Mapped[Optional[str]] = mapped_column(String(100), unique=True)
    sync_status: Mapped[Optional[str]] = mapped_column(String(20), default="pending")
    last_crm_sync: Mapped[Optional[datetime]] = mapped_column(DateTime)

    # Audit Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships (1:1 and 1:Many)
    owner: Mapped[Optional["Owner"]] = relationship("Owner", back_populates="property", uselist=False, cascade="all, delete-orphan")
    financial: Mapped[Optional["Financial"]] = relationship("Financial", back_populates="property", uselist=False, cascade="all, delete-orphan")
    code_violations: Mapped[List["CodeViolation"]] = relationship("CodeViolation", back_populates="property", cascade="all, delete-orphan")
    legal_and_liens: Mapped[List["LegalAndLien"]] = relationship("LegalAndLien", back_populates="property", cascade="all, delete-orphan")
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
        Index("idx_property_sync_status", "sync_status"),
        CheckConstraint("sync_status IN ('pending', 'synced', 'error')", name="check_sync_status"),
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
    owner_name: Mapped[Optional[str]] = mapped_column(String(255))
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

    # Relationship
    property: Mapped["Property"] = relationship("Property", back_populates="owner")

    # Indexes
    __table_args__ = (
        Index("idx_owner_name", "owner_name"),
        Index("idx_owner_type", "owner_type"),
        Index("idx_absentee_status", "absentee_status"),
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

    # Relationship
    property: Mapped["Property"] = relationship("Property", back_populates="financial")

    # Indexes
    __table_args__ = (
        Index("idx_financial_assessed_value", "assessed_value_mkt"),
        Index("idx_financial_equity_pct", "equity_pct"),
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
    Polymorphic table for various legal and lien records.
    Handles Probate, Eviction, HOA, Bankruptcy, etc.
    One-to-many relationship with Property.
    """
    __tablename__ = "legal_and_liens"

    # Primary Key
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    # Foreign Key
    property_id: Mapped[int] = mapped_column(ForeignKey("properties.id"), nullable=False, index=True)

    # Discriminator for polymorphic behavior
    record_type: Mapped[str] = mapped_column(String(50), nullable=False, index=True)

    # Common Fields
    filing_date: Mapped[Optional[datetime]] = mapped_column(Date)
    amount: Mapped[Optional[float]] = mapped_column(Numeric(12, 2))
    associated_party: Mapped[Optional[str]] = mapped_column(String(255))

    # Flexible metadata bucket for type-specific fields
    meta_data: Mapped[Optional[dict]] = mapped_column(JSONB)

    # Relationship
    property: Mapped["Property"] = relationship("Property", back_populates="legal_and_liens")

    # Indexes
    __table_args__ = (
        Index("idx_legal_record_type", "record_type"),
        Index("idx_legal_filing_date", "filing_date"),
        Index("idx_legal_meta_data", "meta_data", postgresql_using="gin"),
        CheckConstraint("record_type IN ('Probate', 'Eviction', 'HOA', 'Bankruptcy', 'Judgment', 'Other')", name="check_record_type"),
    )

    def __repr__(self):
        return f"<LegalAndLien(id={self.id}, record_type='{self.record_type}', amount={self.amount})>"


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

    # Relationship
    property: Mapped["Property"] = relationship("Property", back_populates="tax_delinquencies")

    # Indexes
    __table_args__ = (
        Index("idx_tax_year", "tax_year"),
        Index("idx_tax_years_delinquent", "years_delinquent"),
        Index("idx_tax_deed_app_date", "deed_app_date"),
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
    plaintiff: Mapped[Optional[str]] = mapped_column(String(255))
    filing_date: Mapped[Optional[datetime]] = mapped_column(Date)
    lis_pendens_date: Mapped[Optional[datetime]] = mapped_column(Date)
    judgment_amount: Mapped[Optional[float]] = mapped_column(Numeric(12, 2))
    auction_date: Mapped[Optional[datetime]] = mapped_column(DateTime)

    # Relationship
    property: Mapped["Property"] = relationship("Property", back_populates="foreclosures")

    # Indexes
    __table_args__ = (
        Index("idx_foreclosure_filing_date", "filing_date"),
        Index("idx_foreclosure_auction_date", "auction_date"),
        Index("idx_foreclosure_plaintiff", "plaintiff"),
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

    # Relationship
    property: Mapped["Property"] = relationship("Property", back_populates="incidents")

    # Indexes
    __table_args__ = (
        Index("idx_incident_type", "incident_type"),
        Index("idx_incident_date", "incident_date"),
        Index("idx_incident_problem_flag", "problem_prop_flag"),
        Index("idx_incident_crime_types", "crime_types", postgresql_using="gin"),
        CheckConstraint("incident_type IN ('Arrest', 'Police Dispatch', 'Fire')", name="check_incident_type"),
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

    # Scoring Information
    score_date: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    final_cds_score: Mapped[Optional[float]] = mapped_column(Numeric(5, 2))
    lead_tier: Mapped[Optional[str]] = mapped_column(String(50))
    distress_types: Mapped[Optional[dict]] = mapped_column(JSONB)
    urgency_level: Mapped[Optional[str]] = mapped_column(String(20))
    multiplier: Mapped[Optional[float]] = mapped_column(Numeric(4, 2))
    factor_scores: Mapped[Optional[dict]] = mapped_column(JSONB)
    qualified: Mapped[Optional[bool]] = mapped_column(Boolean, default=False)

    # Relationship
    property: Mapped["Property"] = relationship("Property", back_populates="distress_scores")

    # Indexes
    __table_args__ = (
        Index("idx_score_date", "score_date"),
        Index("idx_score_final_cds", "final_cds_score"),
        Index("idx_score_lead_tier", "lead_tier"),
        Index("idx_score_qualified", "qualified"),
        Index("idx_score_distress_types", "distress_types", postgresql_using="gin"),
        CheckConstraint("urgency_level IN ('Immediate', 'High', 'Medium', 'Low')", name="check_urgency_level"),
        CheckConstraint("lead_tier IN ('Ultra Platinum', 'Platinum', 'Gold', 'Silver', 'Bronze')", name="check_lead_tier"),
    )

    def __repr__(self):
        return f"<DistressScore(id={self.id}, property_id={self.property_id}, score={self.final_cds_score}, tier='{self.lead_tier}')>"
