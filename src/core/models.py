"""
Database models for Distressed Property Intelligence Platform.
Implements the Hub-and-Spoke architecture with properties as the central hub.
"""

from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, List, Optional

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Computed,
    Date,
    DateTime,
    ForeignKey,
    Integer,
    LargeBinary as sa_LargeBinary,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    CheckConstraint,
    Index,
    func,
    false as sa_false,
    text,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, UUID as PG_UUID
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
    normalized_address: Mapped[Optional[str]] = mapped_column(String(255))
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

    # HCPA Enrichment — property classification
    property_use_code: Mapped[Optional[str]] = mapped_column(String(20))       # e.g. "0100" SFR, "0200" condo
    building_condition: Mapped[Optional[str]] = mapped_column(String(20))      # Average / Fair / Poor / Good / Excellent
    building_class: Mapped[Optional[str]] = mapped_column(String(5))           # A / B / C / M
    heated_sq_ft: Mapped[Optional[float]] = mapped_column(Numeric(10, 2))      # heated living area; sq_ft may be gross
    subdivision: Mapped[Optional[str]] = mapped_column(String(255))
    hcpa_neighborhood_code: Mapped[Optional[str]] = mapped_column(String(50))
    building_details: Mapped[Optional[dict]] = mapped_column(JSONB)            # roof, walls, sub-areas, extra features
    hcpa_last_refreshed: Mapped[Optional[datetime]] = mapped_column(DateTime)  # NULL = never enriched

    # Multi-county
    county_id: Mapped[Optional[str]] = mapped_column(String(50), default='hillsborough', index=True)

    # CRM Integration
    gohighlevel_contact_id: Mapped[Optional[str]] = mapped_column(String(100), unique=True)
    sync_status: Mapped[Optional[str]] = mapped_column(String(20), default="pending")
    last_crm_sync: Mapped[Optional[datetime]] = mapped_column(DateTime)

    # Weekly master refresh (fa077)
    # source_row_hash: md5 over canonical scraper-sourced values (see
    # src/loaders/master.py HASH_VERSION). NULL = row predates hash tracking.
    # last_seen_at: stamped for every parcel present in a master file — kept
    # unindexed so the weekly full-county stamp UPDATE stays HOT-eligible.
    source_row_hash: Mapped[Optional[str]] = mapped_column(String(32))
    last_seen_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    needs_rescore: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false", nullable=False)

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
    tax_payment_history: Mapped[List["TaxPaymentHistory"]] = relationship("TaxPaymentHistory", back_populates="property", cascade="all, delete-orphan")

    # Indexes
    __table_args__ = (
        Index("idx_property_address", "address"),
        Index("idx_property_normalized_address", "normalized_address"),
        Index("idx_property_city_state", "city", "state"),
        Index("idx_property_zip", "zip"),
        Index("idx_property_county_id", "county_id"),
        Index("idx_property_sync_status", "sync_status"),
        Index("idx_property_hcpa_refreshed", "hcpa_last_refreshed"),
        Index("idx_property_building_condition", "building_condition"),
        Index("idx_property_building_details", "building_details", postgresql_using="gin"),
        Index("idx_properties_needs_rescore", "id", postgresql_where=text("needs_rescore")),
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

    # Per-phone metadata from skip-trace providers (BatchData, IDI, Twilio Lookup).
    # Keyed by slot ("phone_1" / "phone_2" / "phone_3") with shape:
    #   { "type": "mobile|landline|voip", "carrier": str, "score": int 0-100,
    #     "reachable": bool, "source": "batch_data|idi|twilio_lookup" }
    phone_metadata: Mapped[Optional[dict]] = mapped_column(JSONB)

    # Owner Intelligence (API Gap)
    employer_name: Mapped[Optional[str]] = mapped_column(String(255))
    estimated_income: Mapped[Optional[float]] = mapped_column(Numeric(12, 2))
    credit_score_tier: Mapped[Optional[str]] = mapped_column(String(50))
    skip_trace_success: Mapped[Optional[bool]] = mapped_column(Boolean, default=False)
    # Set by the weekly master refresh when owner_name changes for an owner with
    # prior trace data — phones/emails are kept but belong to the previous owner.
    skip_trace_stale: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false", nullable=False)

    # Direct-mail fallback (fa077): set true when the skip-trace waterfall ends
    # in a MISS but a usable mailing address exists (tax-collector billing
    # address, voter mailing address, or appraiser mailing). Consumed by a
    # future mail-house export — no mail vendor is integrated yet.
    direct_mail_eligible: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")

    # Contact freshness (fa073) — written by src/services/contact_freshness.py.
    # Columns existed in the DB since fa073 but were unmapped here, so ORM
    # writes silently no-opped (ADR 0015). DB-side check constraints:
    #   contact_info_confidence IN ('high','medium','low','stale')
    #   contact_refresh_status  IN ('fresh','due','queued','refreshed','failed')
    contact_info_confidence: Mapped[Optional[str]] = mapped_column(String(20))
    contact_info_confidence_score: Mapped[Optional[float]] = mapped_column(Numeric(4, 3))
    contact_last_verified_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    contact_next_refresh_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    contact_refresh_status: Mapped[Optional[str]] = mapped_column(String(20))
    contact_refresh_reason: Mapped[Optional[str]] = mapped_column(String(120))

    # Set by the master weekly refresh (fa077) when contact data predates the
    # latest refresh cycle. Unmapped until ADR 0015 (same drift as fa073 cols).
    skip_trace_stale: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")

    # Cross-source triangulation evidence (ADR 0015) — written by
    # src/services/contact_triangulation.py alongside the freshness columns.
    # Shape: {matched_phone, person, sources: [...], corroboration,
    #         email_corroboration, matched_email, rule_fired, computed_at,
    #         prev_label}
    # DDL applied via scripts/apply_contactability_detail_migration.py
    # (alembic fa078 file is the record — never `alembic upgrade`).
    contactability_detail: Mapped[Optional[dict]] = mapped_column(JSONB)

    # Sunbiz registered agent — populated by Sunbiz Playwright scraper (LLC owners only)
    registered_agent_name: Mapped[Optional[str]] = mapped_column(String(255))
    registered_agent_address: Mapped[Optional[str]] = mapped_column(String(500))

    # Sunbiz LLC piercing — populated by expanded Sunbiz scraper (fa031).
    # managing_members shape: [{name, address, role, title}]. JSONB GIN-indexed
    # so "what LLCs does this person manage?" is an indexable query without a
    # separate canonical-entity table (graph schema deferred to v2).
    sunbiz_doc_number: Mapped[Optional[str]] = mapped_column(Text)
    principal_address: Mapped[Optional[str]] = mapped_column(Text)
    registered_agent_email: Mapped[Optional[str]] = mapped_column(String(255))
    entity_status: Mapped[Optional[str]] = mapped_column(String(20))   # ACTIVE | INACTIVE | DISSOLVED
    formation_date: Mapped[Optional[date]] = mapped_column(Date)
    managing_members: Mapped[Optional[list]] = mapped_column(JSONB)
    sunbiz_enriched_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    sunbiz_status: Mapped[str] = mapped_column(String(20), nullable=False, default="pending")

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
        Index("idx_owner_phone_metadata", "phone_metadata", postgresql_using="gin"),
        Index("idx_owner_skip_trace_stale", "id", postgresql_where=text("skip_trace_stale")),
        Index("ix_owners_sunbiz_status", "sunbiz_status"),
        Index("ix_owners_managing_members", "managing_members", postgresql_using="gin"),
        CheckConstraint("owner_type IN ('Individual', 'LLC', 'Trust', 'Estate', 'Corporate')", name="check_owner_type"),
        CheckConstraint("absentee_status IN ('In-County', 'Out-of-County', 'Out-of-State')", name="check_absentee_status"),
        CheckConstraint(
            "sunbiz_status IN ('pending','matched','not_found','ambiguous',"
            "'parser_failed','not_an_llc')",
            name="check_sunbiz_status",
        ),
    )

    def __repr__(self):
        return f"<Owner(id={self.id}, property_id={self.property_id}, name='{self.owner_name}')>"


class SunbizSnapshot(Base):
    """
    Raw + parsed Sunbiz scrape audit, keyed by document number (fa031).
    Append-only. The scraper writes a row per scrape regardless of parse outcome
    so a future parser upgrade can reprocess historical scrapes without re-hitting
    the Sunbiz portal. Latest row per doc is retrieved via the
    (sunbiz_doc_number, scraped_at DESC) index.
    """
    __tablename__ = "sunbiz_snapshots"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    sunbiz_doc_number: Mapped[str] = mapped_column(Text, nullable=False)
    scraped_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
    raw_html: Mapped[Optional[str]] = mapped_column(Text)
    raw_jsonb: Mapped[dict] = mapped_column(JSONB, nullable=False)
    parser_version: Mapped[str] = mapped_column(String(40), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False)   # ok | partial | parser_failed

    __table_args__ = (
        CheckConstraint(
            "status IN ('ok','partial','parser_failed')",
            name="check_snapshot_status",
        ),
        Index("ix_sbsnap_doc_recent", "sunbiz_doc_number", "scraped_at"),
    )

    def __repr__(self):
        return (
            f"<SunbizSnapshot(doc={self.sunbiz_doc_number}, "
            f"scraped_at={self.scraped_at}, status={self.status})>"
        )


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

    # HCPA Enrichment — exemptions, SOH cap, tax status
    exemption_code: Mapped[Optional[str]] = mapped_column(String(10))
    soh_assessment_reduction: Mapped[Optional[float]] = mapped_column(Numeric(12, 2))
    taxable_value_county: Mapped[Optional[float]] = mapped_column(Numeric(12, 2))
    taxable_value_schools: Mapped[Optional[float]] = mapped_column(Numeric(12, 2))
    prior_year_market_value: Mapped[Optional[float]] = mapped_column(Numeric(12, 2))
    proposed_next_assessed: Mapped[Optional[float]] = mapped_column(Numeric(12, 2))
    tax_current_status: Mapped[Optional[str]] = mapped_column(String(20))
    tax_last_paid_amount: Mapped[Optional[float]] = mapped_column(Numeric(10, 2))
    tax_last_paid_date: Mapped[Optional[date]] = mapped_column(Date)
    hcpa_refreshed_at: Mapped[Optional[datetime]] = mapped_column(DateTime)

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

    # Match provenance
    match_confidence: Mapped[Optional[Decimal]] = mapped_column(Numeric(4, 3), nullable=True)  # 0.000–1.000
    match_method: Mapped[Optional[str]] = mapped_column(String(30), nullable=True)  # address | parcel_id

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

    # Court case identifier (extracted from PDF by OCR v2; 0% populated pre-OCR)
    case_number: Mapped[Optional[str]] = mapped_column(String(100))

    # Additional metadata
    document_type: Mapped[Optional[str]] = mapped_column(String(100))  # CCL, TCL, ML, TL, HL, Judgment
    legal_description: Mapped[Optional[str]] = mapped_column(Text)
    meta_data: Mapped[Optional[dict]] = mapped_column(JSONB)  # Additional type-specific fields

    # OCR v2 — PDF-extracted property identifiers
    parcel_id: Mapped[Optional[str]] = mapped_column(String(100))
    property_address: Mapped[Optional[str]] = mapped_column(Text)
    normalized_property_address: Mapped[Optional[str]] = mapped_column(Text)
    pdf_url: Mapped[Optional[str]] = mapped_column(Text)
    pdf_path: Mapped[Optional[str]] = mapped_column(Text)
    ocr_status: Mapped[Optional[str]] = mapped_column(String(30), default='pending')
    ocr_confidence: Mapped[Optional[float]] = mapped_column(Numeric(5, 4))
    ocr_extracted_at: Mapped[Optional[datetime]] = mapped_column(DateTime)

    # Match provenance — populated by the loader at insert time
    match_confidence: Mapped[Optional[Decimal]] = mapped_column(Numeric(4, 3), nullable=True)  # 0.000–1.000
    match_method: Mapped[Optional[str]] = mapped_column(String(30), nullable=True)  # parcel_id | normalized_address | legal_desc | owner_name | llm_verified | address | manual

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
        Index("idx_legal_case_number", "case_number"),
        Index("idx_legal_parcel_id", "parcel_id"),
        Index("idx_legal_ocr_status", "ocr_status"),
        Index("idx_legal_meta_data", "meta_data", postgresql_using="gin"),
        Index("idx_legal_match_method", "match_method"),
        CheckConstraint("record_type IN ('Lien', 'Judgment')", name="check_lien_record_type"),
        CheckConstraint(
            "match_method IS NULL OR match_method IN ("
            "'legal_desc', 'owner_name', 'llm_verified', 'address', "
            "'manual', 'parcel_id', 'normalized_address')",
            name="check_legal_match_method",
        ),
        CheckConstraint(
            "ocr_status IS NULL OR ocr_status IN ("
            "'pending', 'downloaded', 'extracted', 'low_confidence', "
            "'failed_download', 'failed_extraction')",
            name="check_legal_ocr_status",
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

    # HCPA Enrichment — sale qualification
    sale_qualified: Mapped[Optional[bool]] = mapped_column(Boolean)
    vacant_improved: Mapped[Optional[str]] = mapped_column(String(20))

    # Match provenance
    match_confidence: Mapped[Optional[Decimal]] = mapped_column(Numeric(4, 3), nullable=True)  # 0.000–1.000
    match_method: Mapped[Optional[str]] = mapped_column(String(30), nullable=True)  # legal_desc | owner_name | llm_verified | address | manual

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

    # Court-docket detail enrichment (Stage 2 — Pinellas Eviction/Probate/Divorce).
    # Populated by the per-engine detail extractor (courtrecords.mypinellasclerk.gov).
    mailing_address: Mapped[Optional[str]] = mapped_column(Text)  # promoted party addr: Defendant(evic)/Petitioner(div)/PR(probate)
    docket_detail: Mapped[Optional[dict]] = mapped_column(JSONB)  # full scrape_case() payload
    balance_due: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 2))  # Financial section Balance Due
    docket_status: Mapped[Optional[str]] = mapped_column(String(30))  # ok|case_number_missing|not_found|blocked|error
    # Promoted (queryable) projections of the docket payload — kept alongside docket_detail.
    court_docket_parties: Mapped[Optional[list]] = mapped_column(JSONB)  # scraped parties[]
    court_docket_events: Mapped[Optional[list]] = mapped_column(JSONB)   # scraped events[]
    court_docket_scraped_at: Mapped[Optional[datetime]] = mapped_column(DateTime)  # detail-scrape timestamp (UTC)

    # Match provenance
    match_confidence: Mapped[Optional[Decimal]] = mapped_column(Numeric(4, 3), nullable=True)  # 0.000–1.000
    match_method: Mapped[Optional[str]] = mapped_column(String(30), nullable=True)  # legal_desc | owner_name | llm_verified | address | manual

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
        Index("idx_proceeding_docket_status", "docket_status"),
        CheckConstraint("record_type IN ('Probate', 'Eviction', 'Bankruptcy', 'Divorce')", name="check_proceeding_record_type"),
        CheckConstraint(
            "docket_status IS NULL OR docket_status IN "
            "('ok','case_number_missing','not_found','blocked','error')",
            name="check_proceeding_docket_status",
        ),
    )

    def __repr__(self):
        return f"<LegalProceeding(id={self.id}, record_type='{self.record_type}', case_number='{self.case_number}')>"


class TaxDelinquency(Base):
    """
    Tax delinquency / tax certificate records for properties.

    Supports both:
    - Pinellas: Delinq Taxes-Certs Unpaid
    - Hillsborough: Public - Certificates (Unpaid)

    One property can have multiple tax delinquency/certificate rows across
    tax years and certificate numbers.
    """
    __tablename__ = "tax_delinquencies"

    # Primary Key
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    # Foreign Key
    property_id: Mapped[int] = mapped_column(ForeignKey("properties.id"), nullable=False, index=True)

    # County / source tracking
    source_report: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)

    # Tax Information
    tax_year: Mapped[Optional[int]] = mapped_column(Integer)
    years_delinquent: Mapped[Optional[int]] = mapped_column(Integer)

    # Raw source identifier from the upload CSV before any prefix stripping.
    # Hillsborough: "A12345", Pinellas: "R265727". Used for deduplication and
    # updates when the same cert is re-uploaded across runs.
    source_account_number: Mapped[Optional[str]] = mapped_column(String(50), nullable=True, index=True)

    # Normalized/source identifiers
    account_number: Mapped[Optional[str]] = mapped_column(String(50), nullable=True, index=True)
    alternate_key: Mapped[Optional[str]] = mapped_column(String(50), nullable=True, index=True)
    parcel_number: Mapped[Optional[str]] = mapped_column(String(100), nullable=True, index=True)

    # Owner / property snapshot from source file
    owner_name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True, index=True)
    owner_address: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    property_address: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)

    # Certificate details
    certificate_number: Mapped[Optional[str]] = mapped_column(String(50), nullable=True, index=True)
    certificate_status: Mapped[Optional[str]] = mapped_column(String(50), nullable=True, index=True)
    issued_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)

    bidder_number: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    certificate_buyer: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    certificate_buyer_address: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)

    # Financial details
    face_amount: Mapped[Optional[float]] = mapped_column(Numeric(12, 2), nullable=True)
    account_balance_amount: Mapped[Optional[float]] = mapped_column(Numeric(12, 2), nullable=True)
    total_amount_due: Mapped[Optional[float]] = mapped_column(Numeric(12, 2))
    interest_rate: Mapped[Optional[float]] = mapped_column(Numeric(8, 4), nullable=True)
    assessed_value: Mapped[Optional[float]] = mapped_column(Numeric(14, 2), nullable=True)

    # Status / lifecycle
    account_status: Mapped[Optional[str]] = mapped_column(String(50), nullable=True, index=True)
    deed_status: Mapped[Optional[str]] = mapped_column(String(50), nullable=True, index=True)
    deed_app_date: Mapped[Optional[datetime]] = mapped_column(Date)

    date_redeemed: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    purchased_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    county_held: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)

    # Classification / flags
    standard_flags: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    custom_flags: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    use_code: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)

    # Legacy/raw certificate data if needed
    certificate_data: Mapped[Optional[str]] = mapped_column(String(255))

    # Raw source safety/debugging
    raw_source_data: Mapped[Optional[dict[str, Any]]] = mapped_column(JSONB, nullable=True)

    # Timestamps
    created_at: Mapped[Optional[datetime]] = mapped_column(DateTime, default=datetime.utcnow, nullable=True)
    updated_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        nullable=True,
    )

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
        Index("idx_tax_delinquency_county_status", "county_id", "account_status"),
        Index("idx_tax_delinquency_cert_status", "certificate_status"),
        Index("idx_tax_delinquency_county_account", "county_id", "source_account_number"),
        Index("idx_tax_delinquency_parcel", "parcel_number"),
        UniqueConstraint("property_id", "tax_year", name="uq_tax_delinquency_property_year"),
    )

    def __repr__(self):
        return (
            f"<TaxDelinquency("
            f"id={self.id}, "
            f"county={self.county_id}, "
            f"account={self.source_account_number}, "
            f"tax_year={self.tax_year}, "
            f"cert={self.certificate_number}, "
            f"amount_due={self.total_amount_due}"
            f")>"
        )


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
    defendant: Mapped[Optional[str]] = mapped_column(Text)
    filing_date: Mapped[Optional[datetime]] = mapped_column(Date)
    lis_pendens_date: Mapped[Optional[datetime]] = mapped_column(Date)
    judgment_amount: Mapped[Optional[float]] = mapped_column(Numeric(12, 2))
    auction_date: Mapped[Optional[datetime]] = mapped_column(DateTime)
    case_status: Mapped[Optional[str]] = mapped_column(String(100))

    # Match provenance
    match_confidence: Mapped[Optional[Decimal]] = mapped_column(Numeric(4, 3), nullable=True)  # 0.000–1.000
    match_method: Mapped[Optional[str]] = mapped_column(String(30), nullable=True)  # parcel_id | address | legal_desc | owner_name

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
        Index("idx_foreclosure_defendant", "defendant"),
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
    description: Mapped[Optional[str]] = mapped_column(Text)

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

    # Scraper-specific metadata (structure varies by source)
    source_meta: Mapped[Optional[dict]] = mapped_column(JSONB)

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


class NWSAlert(Base):
    """
    Idempotent store for NWS CAP alerts processed by the platform.
    One row per unique NWS alert ID — prevents duplicate storm-pack triggers
    and Cora urgency messages across poll cycles.
    """
    __tablename__ = "nws_alerts"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    # NWS @id field — globally unique alert identifier (urn:oid:...)
    alert_id: Mapped[str] = mapped_column(String(200), unique=True, nullable=False, index=True)

    # Core alert fields from CAP properties
    event: Mapped[str] = mapped_column(String(100), nullable=False)
    severity: Mapped[Optional[str]] = mapped_column(String(30))
    urgency: Mapped[Optional[str]] = mapped_column(String(30))
    certainty: Mapped[Optional[str]] = mapped_column(String(30))
    headline: Mapped[Optional[str]] = mapped_column(Text)
    description: Mapped[Optional[str]] = mapped_column(Text)
    instruction: Mapped[Optional[str]] = mapped_column(Text)
    area_desc: Mapped[Optional[str]] = mapped_column(Text)

    # Geocode lists stored as JSONB (consistent with rest of codebase)
    same_codes: Mapped[Optional[dict]] = mapped_column(JSONB)
    ugc_codes: Mapped[Optional[dict]] = mapped_column(JSONB)
    affected_zips: Mapped[Optional[dict]] = mapped_column(JSONB)

    # Alert time window
    effective: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    onset: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    expires: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    ends: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))

    # Platform tracking
    county_id: Mapped[str] = mapped_column(String(50), default="hillsborough", index=True)
    storm_pack_triggered: Mapped[bool] = mapped_column(Boolean, default=False)
    cora_urgency_sent: Mapped[bool] = mapped_column(Boolean, default=False)
    subscriber_count: Mapped[int] = mapped_column(Integer, default=0)

    # Full raw properties payload for debugging/audit
    raw_payload: Mapped[Optional[dict]] = mapped_column(JSONB)

    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), index=True
    )

    __table_args__ = (
        Index("idx_nws_alerts_event_processed", "event", "processed_at"),
        Index("idx_nws_alerts_affected_zips", "affected_zips", postgresql_using="gin"),
    )

    def __repr__(self):
        return f"<NWSAlert(id={self.id}, event='{self.event}', alert_id='{self.alert_id[:40]}...')>"


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

class ConsentAcceptance(Base):
    """
    Immutable audit record of a user's T&C / privacy (and optional TCPA marketing)
    consent at a given flow step. One row per acceptance event. Backing table is
    created by migration fa070_consent_acceptances; this ORM model mirrors it.
    """
    __tablename__ = "consent_acceptances"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    subscriber_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("subscribers.id"), nullable=True)
    waitlist_entry_id: Mapped[Optional[int]] = mapped_column(
        BigInteger, ForeignKey("waitlist_entries.id"), nullable=True)
    phone: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    email: Mapped[str] = mapped_column(String(255), nullable=False)

    terms_version: Mapped[str] = mapped_column(String(20), nullable=False)
    privacy_version: Mapped[str] = mapped_column(String(20), nullable=False)
    accepted_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    source_flow: Mapped[str] = mapped_column(String(30), nullable=False, server_default="waitlist")
    ip_address: Mapped[Optional[str]] = mapped_column(String(45), nullable=True)
    user_agent: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    modal_opened_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    modal_scrolled_to_end_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    accepted_text_hash: Mapped[str] = mapped_column(String(64), nullable=False)

    tcpa_consent_text: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    tcpa_consent_version: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    tcpa_checked_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    consent_scope: Mapped[Optional[str]] = mapped_column(String(30), nullable=True)
    not_condition_of_purchase_ack: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)

    county_id: Mapped[Optional[str]] = mapped_column(String(50), nullable=True, server_default="hillsborough")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False,
        default=lambda: datetime.now(timezone.utc))

    __table_args__ = (
        CheckConstraint(
            "source_flow IN ('waitlist','signup','checkout','county_launch','free_signup')",
            name="ck_consent_source_flow",
        ),
        CheckConstraint(
            "consent_scope IS NULL OR consent_scope IN ('marketing','waitlist_notify','lead_alerts')",
            name="ck_consent_scope",
        ),
        Index("idx_consent_email", "email"),
        Index("idx_consent_accepted_at", "accepted_at"),
        Index("idx_consent_subscriber", "subscriber_id"),
        Index("idx_consent_waitlist", "waitlist_entry_id"),
    )


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

    # ── Stage 5+: dispute / fraud tracking (added 2026-05-04, fa004) ──
    disputed_count: Mapped[int] = mapped_column(Integer, default=0, server_default="0", nullable=False)
    disputed_at: Mapped[Optional[datetime]] = mapped_column(DateTime)

    # ── Phase A: Wallet-to-Lock candidate flags ──
    lock_candidate_zip: Mapped[Optional[str]] = mapped_column(String(10))
    lock_candidate_at: Mapped[Optional[datetime]] = mapped_column(DateTime)

    # ── Phase A: AP Lite candidate flag ──
    ap_lite_candidate_at: Mapped[Optional[datetime]] = mapped_column(DateTime)

    # ── Phase B: Pause flow ──
    paused_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    pause_resume_at: Mapped[Optional[datetime]] = mapped_column(DateTime)

    # ── Phase A: Human close routing ──
    escalation_routed_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    escalation_channel: Mapped[Optional[str]] = mapped_column(String(20))

    # ── Stage 6: Stripe payment-failure recovery ──
    payment_failed_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    recovery_day1_sent: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false", nullable=False)
    recovery_day3_sent: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false", nullable=False)
    recovery_day5_sent: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false", nullable=False)

    # ── Referral Core Loop ──
    bonus_zip_slots: Mapped[int] = mapped_column(Integer, default=0, server_default="0", nullable=False)

    # ── fa016: Accelerated Wallet Push ──
    phone: Mapped[Optional[str]] = mapped_column(String(20), unique=True, index=True)
    wallet_opt_out: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false", nullable=False)
    missed_lead_count: Mapped[int] = mapped_column(Integer, default=0, server_default="0", nullable=False)

    # ── fa017: Signup source attribution ──
    # CHECK constraint (`check_subscriber_signup_source`) enforces allow-list:
    # direct / landing_page / dbpr_email / cora_sms / missed_call / referral /
    # admin / unknown / affiliate (fa081).
    signup_source: Mapped[str] = mapped_column(
        String(30), default="direct", server_default="direct", nullable=False, index=True,
    )
    utm_source: Mapped[Optional[str]] = mapped_column(String(100))
    utm_medium: Mapped[Optional[str]] = mapped_column(String(100))
    utm_campaign: Mapped[Optional[str]] = mapped_column(String(100))
    campaign_id: Mapped[Optional[str]] = mapped_column(String(50))
    attribution_token: Mapped[Optional[str]] = mapped_column(String(200))
    # fa081: stamped at registration with the Affiliate's opaque ?ref= token.
    affiliate_ref: Mapped[Optional[str]] = mapped_column(String(40), index=True)

    bundle_purchases = relationship("BundlePurchase", back_populates="subscriber")

    # ── Stage 8: Revenue Signal Score (latest state; history in revenue_signal_score_events) ──
    revenue_signal_score: Mapped[int] = mapped_column(Integer, default=0, server_default="0", nullable=False)
    revenue_signal_band: Mapped[Optional[str]] = mapped_column(String(20))
    revenue_signal_breakdown: Mapped[Optional[dict]] = mapped_column(JSONB)
    revenue_signal_updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))

    # ── fa057: Synthflow inbound signup ──────────────────────────────────────
    # False when the Synthflow inbound event was missing ZIP or vertical.
    # First-login corrects vertical; capture_complete flips True after update.
    capture_complete: Mapped[bool] = mapped_column(
        Boolean, default=True, server_default="true", nullable=False
    )

    # ── Revenue / churn tracking (fa048) ─────────────────────────────────────
    plan_price: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2), nullable=True)
    churned_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    is_trial: Mapped[bool] = mapped_column(Boolean, server_default="false", nullable=False, default=False)
    trial_ends_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    # ── S0: Reactivation cooldown gate ───────────────────────────────────────
    last_reactivation_attempt_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    # ── Feed password login (fa061) ──────────────────────────────────────────
    # NULL until the subscriber has a password. event_feed_uuid stays the feed
    # identifier; these gate access behind a session JWT.
    password_hash: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    password_set_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    reset_token_hash: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    reset_token_expires_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    # ── ICP channel attribution (fa066) ──────────────────────────────────────
    # Explicit ICP attribution. Verticals can overlap between ICPs so scoping
    # by vertical alone is unsafe. Default 'contractor' for all existing rows.
    icp_channel_key: Mapped[str] = mapped_column(
        String(40), nullable=False, default="contractor", server_default="contractor"
    )

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
        Index("idx_subscriber_signal_score", "revenue_signal_score"),
        Index("idx_subscribers_icp_channel_key", "icp_channel_key"),
        Index("idx_subscriber_last_reactivation_at", "last_reactivation_attempt_at"),
        CheckConstraint(
            "tier IN ('free', 'starter', 'pro', 'dominator', 'data_only', 'autopilot_lite', 'autopilot_pro', 'partner', 'annual_lock')",
            name="check_subscriber_tier",
        ),
        CheckConstraint(
            "status IN ('active', 'grace', 'churned', 'cancelled', 'paused', 'disputed', 'past_due')",
            name="check_subscriber_status",
        ),
        CheckConstraint(
            "revenue_signal_band IS NULL OR revenue_signal_band IN ('low','medium','high','very_high')",
            name="check_subscriber_revenue_signal_band",
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


class LeadQualitySnapshot(Base):
    """
    30-day post-send snapshot for each Gold+ lead delivered to a subscriber.
    Used to compute false-positive rate: leads that looked distressed at send
    time but were already sold or had resolved signals.

    Populated by src/tasks/lead_quality_monitor.py, which runs daily after scoring.
    One row per (property_id, subscriber_id, sent_at) — unique constraint prevents
    double-snapshotting if the task re-runs.
    """
    __tablename__ = "lead_quality_snapshots"

    id            = Column(Integer, primary_key=True, autoincrement=True)
    property_id   = Column(Integer, ForeignKey("properties.id"), nullable=False, index=True)
    subscriber_id = Column(Integer, ForeignKey("subscribers.id"), nullable=False, index=True)
    county_id     = Column(String(50), nullable=False, default="hillsborough")

    sent_at     = Column(DateTime(timezone=True), nullable=False)
    snapshot_at = Column(DateTime(timezone=True), nullable=False)

    # Score state at send time (looked up from DistressScore history by score_date)
    score_at_send   = Column(Numeric(5, 2), nullable=True)
    tier_at_send    = Column(String(20), nullable=True)
    signals_at_send = Column(JSONB, nullable=True)  # distress_types list at send time

    # Score state at snapshot time (current)
    score_at_snapshot = Column(Numeric(5, 2), nullable=True)
    tier_at_snapshot  = Column(String(20), nullable=True)
    still_gold_plus   = Column(Boolean, nullable=False)

    # False-positive indicators (non-exclusive — both can be True)
    has_deed_transfer    = Column(Boolean, nullable=False, default=False)
    has_resolved_signals = Column(Boolean, nullable=False, default=False)

    # Single outcome classification (priority order: sold > resolved > decayed > active)
    # 'active'   — still Gold+, no deed, no resolved signals  (true positive)
    # 'decayed'  — no longer Gold+ but not sold/resolved      (borderline)
    # 'sold'     — deed transfer recorded within 30d of send  (false positive)
    # 'resolved' — primary code violations now closed/resolved (false positive)
    outcome = Column(String(20), nullable=False)

    __table_args__ = (
        UniqueConstraint("property_id", "subscriber_id", "sent_at",
                         name="uq_lead_quality_snapshot"),
        Index("idx_lqs_snapshot_at", "snapshot_at"),
        Index("idx_lqs_outcome", "outcome"),
    )

    def __repr__(self):
        return (
            f"<LeadQualitySnapshot(property_id={self.property_id}, "
            f"outcome='{self.outcome}', sent_at={self.sent_at.date()})>"
        )


class WebhookEvent(Base):
    """
    Unified audit log across all webhook + vendor-callback sources.

    One row per received/sent event. Best-effort write — failures here
    never block the webhook handler. Payloads are stored as SANITIZED
    summaries only (no raw PII, no message bodies, no payment details).
    Per-source sanitizers live in src/services/webhook_log.py.

    Stripe still uses stripe_webhook_events for its idempotency lock;
    this table is pure audit and additive to it.
    """
    __tablename__ = "webhook_events"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    source: Mapped[str] = mapped_column(String(30), nullable=False, index=True)
    event_type: Mapped[str] = mapped_column(String(80), nullable=False, index=True)
    direction: Mapped[str] = mapped_column(String(10), nullable=False, default="inbound")
    source_event_id: Mapped[Optional[str]] = mapped_column(String(120), index=True)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="received")
    status_detail: Mapped[Optional[str]] = mapped_column(Text)

    subscriber_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("subscribers.id"), nullable=True, index=True
    )
    property_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("properties.id"), nullable=True, index=True
    )

    payload_summary: Mapped[Optional[dict]] = mapped_column(JSONB)
    duration_ms: Mapped[Optional[int]] = mapped_column(Integer)

    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False, index=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    __table_args__ = (
        Index("idx_webhook_events_source_processed", "source", "processed_at"),
        CheckConstraint(
            "direction IN ('inbound', 'outbound')",
            name="check_webhook_event_direction",
        ),
        CheckConstraint(
            "status IN ('received', 'processed', 'failed', 'duplicate', 'skipped')",
            name="check_webhook_event_status",
        ),
    )

    def __repr__(self):
        return (
            f"<WebhookEvent(source={self.source!r}, type={self.event_type!r}, "
            f"status={self.status!r}, at={self.processed_at})>"
        )


class PhoneDeliverabilitySnapshot(Base):
    """
    Daily sample of phone-deliverability quality across Gold+ leads.

    Populated by src/tasks/phone_deliverability_sampler.py. Reads cached
    phone_metadata from Owner first; only calls Telnyx Number Lookup for
    phones missing metadata, then backfills the result onto Owner.phone_metadata.

    mobile_pct is the headline metric: % of sampled Gold+ contacts whose
    primary phone is a mobile line (SMS-deliverable). Alerts fire when
    mobile_pct drops below the configured threshold for two days in a row.
    """
    __tablename__ = "phone_deliverability_snapshots"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    snapshot_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    county_id: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    tier_filter: Mapped[str] = mapped_column(String(40), nullable=False, default="gold_plus")

    sample_size: Mapped[int] = mapped_column(Integer, nullable=False)
    lookups_cached: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    lookups_attempted: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    lookups_succeeded: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    mobile_count:   Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    voip_count:     Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    landline_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    unknown_count:  Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    no_phone_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    mobile_pct: Mapped[Optional[float]] = mapped_column(Numeric(5, 2))

    vendor: Mapped[str] = mapped_column(String(20), nullable=False, default="telnyx")
    cost_cents: Mapped[Optional[int]] = mapped_column(Integer, default=0)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    __table_args__ = (
        UniqueConstraint("snapshot_date", "county_id", "tier_filter",
                         name="uq_phone_deliv_snapshot_day"),
        Index("idx_phone_deliv_date_county", "snapshot_date", "county_id"),
    )

    def __repr__(self):
        return (
            f"<PhoneDeliverabilitySnapshot(date={self.snapshot_date}, "
            f"county={self.county_id}, mobile_pct={self.mobile_pct})>"
        )


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

    # Full raw API response — stored so callers can re-parse without another API call
    raw_response: Mapped[Optional[dict]] = mapped_column(JSONB)

    # Source tracking
    # batch_skip_tracing | idi | pdl | tracerfy | tax_collector
    # 'tax_collector' rows (fa077) carry only mailing_address — the county
    # tax-bill billing address when it differs from owners.mailing_address.
    # Per ADR 0013 they are never promoted into owner phone/email columns.
    source: Mapped[str] = mapped_column(String(50), nullable=False)
    match_success: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)

    # Which named individual was traced. NULL for legacy single-trace rows
    # (assessor owner or first heir). Populated when MULTI_HEIR_ENRICHMENT
    # produces one row per heir for a probate-derived lead — the name acts as
    # the discriminator that lets multiple rows share property_id without
    # collapsing into the same person.
    traced_name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    # Waterfall quality score (0.000–1.000) — set by the waterfall coordinator
    confidence: Mapped[Optional[float]] = mapped_column(Numeric(4, 3), nullable=True)

    # Contact verification + supersession chain (fa073) — unmapped until
    # ADR 0015. verification_status: e.g. 'valid' | 'invalid' (consumed by
    # contact_freshness). When a re-trace replaces this row's data, the old
    # row is stamped superseded_at/superseded_by_contact_id instead of being
    # mutated, preserving the audit trail.
    verification_status: Mapped[Optional[str]] = mapped_column(String(20))
    superseded_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    superseded_by_contact_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("enriched_contacts.id"), nullable=True
    )
    # Tracerfy distinguishes the API mode used: "normal" (name+address, 1 credit/hit)
    # vs "advanced" (address-only fallback, 2 credits/hit). NULL for non-Tracerfy rows.
    trace_type: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)

    # Audit
    enriched_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc))

    property: Mapped["Property"] = relationship("Property")

    __table_args__ = (
        Index("idx_enriched_match_success", "match_success"),
        Index("idx_enriched_source", "source"),
        Index("idx_ec_trace_type", "trace_type"),
        CheckConstraint(
            "source IN ('batch_skip_tracing', 'idi', 'pdl', 'tracerfy', 'tax_collector')",
            name="check_enriched_source",
        ),
    )

    def __repr__(self):
        return f"<EnrichedContact(id={self.id}, property_id={self.property_id}, source='{self.source}', match={self.match_success})>"


class Voter(Base):
    """
    Registered voters matched to a property by residential address (fa077).

    Source: county SOE bulk registry files (Hillsborough "All Eligible Voters"
    monthly report; FL DOS statewide extract as fallback). Multiple rows per
    property are intended — they form the household's alternative contact
    network (alt names, separate mailing addresses, phones, emails).

    Contact-enrichment only: voter rows never feed CDS scoring and their
    phones/emails are never auto-promoted into owners.* or any send path
    (ADR 0013 — they bypass the Tracerfy DNC scrub and often belong to
    non-owner co-residents).

    `phones` accumulates history across monthly loads (list of normalized
    numbers, newest last); `phone_1` is the current number.
    """
    __tablename__ = "voters"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    property_id: Mapped[int] = mapped_column(ForeignKey("properties.id"), nullable=False, index=True)
    county_id: Mapped[str] = mapped_column(String(50), nullable=False, index=True)

    # Source identity
    source_voter_id: Mapped[str] = mapped_column(String(20), nullable=False)

    # Names
    voter_name: Mapped[Optional[str]] = mapped_column(String(255), index=True)
    first_name: Mapped[Optional[str]] = mapped_column(String(100))
    middle_name: Mapped[Optional[str]] = mapped_column(String(100))
    last_name: Mapped[Optional[str]] = mapped_column(String(100))

    # Residential (match basis) + mailing (alt contact path; NULL = same as residence)
    residential_address: Mapped[Optional[str]] = mapped_column(String(500))
    residential_city: Mapped[Optional[str]] = mapped_column(String(100))
    residential_zip: Mapped[Optional[str]] = mapped_column(String(10))
    mailing_address: Mapped[Optional[str]] = mapped_column(String(500))

    # Registration
    registration_status: Mapped[Optional[str]] = mapped_column(String(10))  # ACT | INA
    registration_date: Mapped[Optional[date]] = mapped_column(Date)

    # Isolated contact data (ADR 0013)
    phones: Mapped[Optional[list]] = mapped_column(JSONB, default=list)
    phone_1: Mapped[Optional[str]] = mapped_column(String(20))
    email: Mapped[Optional[str]] = mapped_column(String(255))

    meta_data: Mapped[Optional[dict]] = mapped_column(JSONB)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), onupdate=func.now()
    )

    property: Mapped["Property"] = relationship("Property")

    __table_args__ = (
        UniqueConstraint("county_id", "source_voter_id", name="uq_voter_county_source_id"),
        Index("idx_voter_registration_status", "registration_status"),
        CheckConstraint(
            "registration_status IN ('ACT', 'INA') OR registration_status IS NULL",
            name="check_voter_registration_status",
        ),
    )

    def __repr__(self):
        return (
            f"<Voter(id={self.id}, property_id={self.property_id}, "
            f"name='{self.voter_name}', status='{self.registration_status}')>"
        )


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
    updated_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime, onupdate=lambda: datetime.now(timezone.utc), nullable=True
    )

    __table_args__ = (
        UniqueConstraint("run_date", "source_type", "county_id", name="uq_scraper_run_stats"),
        Index("idx_run_stats_date_source", "run_date", "source_type"),
        CheckConstraint(
            "source_type IN ("
            "'lien_tcl', 'lien_ccl', 'lien_hoa', 'lien_ml', 'lien_tl', 'lien_unknown', 'lis_pendens',"
            "'judgments', 'deeds', 'evictions', 'divorce_filings', 'probate', 'bankruptcy',"
            "'violations', 'foreclosures', 'permits', 'tax_delinquencies',"
            "'roofing_permits', 'storm_damage', 'flood_damage', 'insurance_claims', 'fire_incidents',"
            "'sunbiz', 'property_appraiser', 'dbpr_company'"
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

    # ── Cora self-healing baseline snapshots (fa034 + fa035) ─────────────────
    # Daily metric values written by kill_switch_metric_ingest after the
    # ks_metric_* Redis cache is updated. Read by cora_self_healing.compute_baseline
    # to derive a 7-day rolling mean per metric. NULL = "no data for this day"
    # (pre-deploy rows, or metric not computable for this county).
    # NUMERIC(7,4): admits 0-999.9999 — percent values are stored in 0-100
    # range so 100.0 fits (e.g. retention_30d=100.0 for a fresh cohort).
    sms_reply_rate: Mapped[Optional[float]] = mapped_column(Numeric(7, 4), nullable=True)
    offer_acceptance_rate: Mapped[Optional[float]] = mapped_column(Numeric(7, 4), nullable=True)
    first_payment_rate: Mapped[Optional[float]] = mapped_column(Numeric(7, 4), nullable=True)
    saved_card_rate: Mapped[Optional[float]] = mapped_column(Numeric(7, 4), nullable=True)
    wallet_adoption: Mapped[Optional[float]] = mapped_column(Numeric(7, 4), nullable=True)
    lock_conversion: Mapped[Optional[float]] = mapped_column(Numeric(7, 4), nullable=True)
    retention_30d: Mapped[Optional[float]] = mapped_column(Numeric(7, 4), nullable=True)
    cac_paid_channels: Mapped[Optional[float]] = mapped_column(Numeric(10, 2), nullable=True)

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


class CoraIncident(Base):
    """Cora self-healing incident ledger (fa034).

    One row per (metric, county, feature) breach. Opened by
    `src/tasks/cora_self_healing.py` when a metric crosses its yellow/red
    threshold; updated when duration passes action triggers; closed when
    the metric recovers.

    Runtime never instantiates this model directly — every read/write in
    cora_self_healing.py and the revenue_pulse extension uses raw SQL via
    `sa_text(...)` (per repo convention). This declaration exists only so
    Alembic autogenerate stays consistent with the live schema.
    """
    __tablename__ = "cora_incident"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    metric_name: Mapped[str] = mapped_column(String(64), nullable=False)
    county_id: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    feature_name: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)

    severity: Mapped[str] = mapped_column(String(16), nullable=False)
    observed_value: Mapped[float] = mapped_column(Numeric(10, 4), nullable=False)
    threshold_value: Mapped[float] = mapped_column(Numeric(10, 4), nullable=False)
    baseline_value: Mapped[Optional[float]] = mapped_column(Numeric(10, 4), nullable=True)

    breach_started: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    breach_resolved: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    duration_hours: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    action_taken: Mapped[str] = mapped_column(String(32), nullable=False, default="no_op")
    action_details: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    decision_id: Mapped[Optional[str]] = mapped_column(PG_UUID(as_uuid=True), nullable=True)

    # Human-readable root cause for the breach (fa050). Nullable; the daily
    # dashboard derives a fallback from metric_name/county_id when unset.
    root_cause: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    __table_args__ = (
        CheckConstraint("severity IN ('yellow','red')", name="check_cora_incident_severity"),
        CheckConstraint(
            "action_taken IN ('no_op','fallback_enabled','auto_paused',"
            "'human_escalated','feature_killed','resolved')",
            name="check_cora_incident_action",
        ),
        # Partial indexes (idx_cora_incident_metric_open, idx_cora_incident_unresolved)
        # are created via raw SQL in fa034 and not declared here, so autogenerate
        # doesn't try to recreate them.
        Index("idx_cora_incident_breach_started", "breach_started"),
    )

    def __repr__(self):
        return (
            f"<CoraIncident(id={self.id}, metric={self.metric_name}, "
            f"severity={self.severity}, action={self.action_taken}, "
            f"resolved={self.breach_resolved is not None})>"
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
    match_status          = mapped_column(String(20), nullable=False, default="unmatched", index=True)  # unmatched | matched | skipped | pending_review
    match_attempted_at    = mapped_column(DateTime(timezone=True), nullable=True)
    matched_property_id   = mapped_column(Integer, ForeignKey("properties.id"), nullable=True)
    match_confidence      = mapped_column(Numeric(4, 3), nullable=True)        # 0.000–1.000
    match_method          = mapped_column(String(30), nullable=True)            # address | owner_name | legal_desc | parcel_id
    candidate_property_id = mapped_column(Integer, ForeignKey("properties.id"), nullable=True)
    date_added            = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    matched_property   = relationship("Property", foreign_keys=[matched_property_id])
    candidate_property = relationship("Property", foreign_keys=[candidate_property_id])

    __table_args__ = (
        Index("ix_unmatched_source_status", "source_type", "match_status"),
        Index(
            "uq_unmatched_instrument_source_county",
            "instrument_number", "source_type", "county_id",
            unique=True,
            postgresql_where="instrument_number IS NOT NULL",
        ),
        Index("ix_unmatched_candidate_property", "candidate_property_id"),
        CheckConstraint(
            "match_status IN ('unmatched','matched','skipped','pending_review')",
            name="check_unmatched_match_status",
        ),
        CheckConstraint(
            "match_method IN ('address','owner_name','legal_desc','parcel_id') OR match_method IS NULL",
            name="check_unmatched_match_method",
        ),
    )

    def __repr__(self):
        return f"<UnmatchedRecord(id={self.id}, source='{self.source_type}', status='{self.match_status}')>"


# ============================================================================
# 8. LEAD PACK PURCHASES
# ============================================================================

class LeadExclusivity(Base):
    """
    Database-backed cross-trade exclusivity for leads.
    
    Replaces Redis lead_hold as the authoritative source for exclusivity.
    Each row represents a property locked for a specific subscriber/trade.
    """
    __tablename__ = "lead_exclusivity"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    
    property_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    zip_code: Mapped[str] = mapped_column(String(10), nullable=False)
    county_id: Mapped[str] = mapped_column(String(50), nullable=False)
    sold_to_trade: Mapped[str] = mapped_column(String(50), nullable=False)
    
    source: Mapped[str] = mapped_column(String(20), nullable=False)  # 'lead_pack' or 'bundle'
    source_id: Mapped[int] = mapped_column(Integer, nullable=False)
    
    exclusive_until: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )

    __table_args__ = (
        UniqueConstraint("property_id", "source", name="uq_property_source"),
        CheckConstraint("source IN ('lead_pack', 'bundle')", name="ck_lead_exclusivity_source"),
        Index("idx_exclusivity_zip_county", "zip_code", "county_id", "exclusive_until"),
        Index("idx_exclusivity_until", "exclusive_until"),
    )


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
    #   pending    — row created, leads not yet reserved (transient)
    #   enriching  — 5 leads reserved at payment; awaiting Hot-Enrichment (ADR 0018)
    #   delivered  — Quality Floor cleared, leads handed over
    #   expired    — non-recoverable selection error (e.g. unknown vertical)
    #   refunded   — short pack, unlaunched county, or Quality Floor miss
    status: Mapped[str] = mapped_column(
        String(20), default="pending", nullable=False
    )

    # Timestamps
    purchased_at: Mapped[datetime] = mapped_column(
        DateTime, default=lambda: datetime.now(timezone.utc), nullable=False
    )
    delivered_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    exclusive_until: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))  # purchased_at + 72h
    refunded_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))

    # Hot-Enrichment (ADR 0018) — post-payment Tracerfy re-trace of the reserved 5.
    enrichment_submitted_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    tracerfy_queue_id: Mapped[Optional[str]] = mapped_column(String(50))

    # Refund info
    refund_reason: Mapped[Optional[str]] = mapped_column(String(100))
    stripe_refund_id: Mapped[Optional[str]] = mapped_column(String(100))

    # The 5 selected property IDs (reserved at payment time)
    lead_ids: Mapped[Optional[list]] = mapped_column(ARRAY(Integer))

    # Relationship
    subscriber: Mapped["Subscriber"] = relationship("Subscriber")

    __table_args__ = (
        CheckConstraint(
            "status IN ('pending', 'enriching', 'delivered', 'expired', 'refunded')",
            name="check_lead_pack_status",
        ),
        Index("idx_lead_pack_zip_vertical", "zip_code", "vertical"),
        Index("idx_lead_pack_exclusive_until", "exclusive_until"),
        Index("idx_lead_pack_status", "status"),
    )

    def __repr__(self):
        return (
            f"<LeadPackPurchase(id={self.id}, subscriber_id={self.subscriber_id}, "
            f"zip={self.zip_code}, status={self.status})>"
        )


class DfyLiteOrder(Base):
    """
    DFY-Lite pitch generation order — one row per pitch request.

    Subscribers may generate up to `pitch_generation_limit` pitches per
    property (default 3). Status lifecycle:
    Order_Received → Signal_Compiled → Needs_Review → Delivered.
    """
    __tablename__ = "dfy_lite_orders"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    subscriber_id: Mapped[int] = mapped_column(ForeignKey("subscribers.id"), nullable=False)
    property_id: Mapped[int] = mapped_column(ForeignKey("properties.id"), nullable=False)

    # Nullable authorization references — whichever path granted access
    sent_lead_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    source_lead_purchase_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    status: Mapped[str] = mapped_column(String(30), nullable=False, default="Order_Received")

    # Subscriber-provided request options
    pitch_type: Mapped[str] = mapped_column(String(50), nullable=False)
    offer_angle: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    target_vertical: Mapped[str] = mapped_column(String(50), nullable=False)
    selected_output_formats: Mapped[list] = mapped_column(JSONB, nullable=False, server_default="[]")
    custom_instructions: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Snapshots captured at generation time
    distress_stack_json: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    property_snapshot_json: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    generated_outputs_json: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)

    # Generation tracking
    pitch_generation_number: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    pitch_generation_limit: Mapped[int] = mapped_column(Integer, nullable=False, default=3)
    generated_by: Mapped[str] = mapped_column(String(30), nullable=False, default="claude")

    # Review & delivery
    reviewed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    delivered_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    error_reason: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    __table_args__ = (
        CheckConstraint(
            "status IN ('Order_Received', 'Signal_Compiled', "
            "'Needs_Review', 'Delivered', 'Signal_Failed', 'Pitch_Failed', 'Cancelled')",
            name="ck_dfy_lite_status",
        ),
        Index("idx_dfy_lite_sub_id", "subscriber_id"),
        Index("idx_dfy_lite_prop_id", "property_id"),
        Index("idx_dfy_lite_status", "status"),
        Index("idx_dfy_lite_created_at", "created_at"),
        Index("idx_dfy_lite_sub_prop", "subscriber_id", "property_id"),
        Index("idx_dfy_lite_sent_lead", "sent_lead_id"),
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


class CoraEventQueue(Base):
    """
    Durable fallback queue for Cora bus events when Redis is unavailable
    (fa072). `publish_cora_event` writes here + emits NOTIFY cora_events; the
    Postgres listener drains pending rows on startup and every 60s.

    The table already exists in the DB; this ORM mapping was missing, which
    broke the Redis-down fallback path in src/agents/events/ingestion.py.
    """
    __tablename__ = "cora_event_queue"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    event_type: Mapped[str] = mapped_column(Text, nullable=False)
    subscriber_id: Mapped[Optional[int]] = mapped_column(Integer)
    payload: Mapped[Optional[dict]] = mapped_column(JSONB)
    idempotency_key: Mapped[Optional[str]] = mapped_column(Text)
    status: Mapped[str] = mapped_column(Text, nullable=False, default="pending")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )
    processed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    error: Mapped[Optional[str]] = mapped_column(Text)

    __table_args__ = (
        Index("idx_cora_event_queue_status", "status", "created_at"),
    )

    def __repr__(self):
        return f"<CoraEventQueue(id={self.id}, type={self.event_type}, status={self.status})>"


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
    # ZIP attribution for Wallet-to-Lock detection (Phase A)
    zip_code: Mapped[Optional[str]] = mapped_column(String(10))
    created_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc))

    wallet = relationship("WalletBalance", backref="transactions")

    __table_args__ = (
        CheckConstraint("txn_type IN ('credit', 'debit', 'reload', 'bonus', 'refund')", name="check_txn_type"),
        Index("idx_wallet_txn_sub_created", "subscriber_id", "created_at"),
        Index("idx_wallet_txn_sub_zip_created", "subscriber_id", "zip_code", "created_at"),
    )

    def __repr__(self):
        return f"<WalletTransaction(id={self.id}, type={self.txn_type}, amount={self.amount})>"


class WalletPushOffer(Base):
    """Accelerated Wallet Push offer funnel (fa016)."""
    __tablename__ = "wallet_push_offers"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    subscriber_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("subscribers.id", ondelete="CASCADE"), nullable=False, index=True
    )
    decision_id: Mapped[Optional[str]] = mapped_column(String(36), index=True)
    framing_variant: Mapped[str] = mapped_column(String(20), nullable=False)
    ab_variant: Mapped[Optional[str]] = mapped_column(String(1))
    tier: Mapped[str] = mapped_column(String(20), nullable=False, server_default="starter_wallet")
    status: Mapped[str] = mapped_column(String(20), nullable=False, server_default="offered")
    stripe_subscription_id: Mapped[Optional[str]] = mapped_column(String(100), index=True)

    offered_at: Mapped[datetime] = mapped_column(
        DateTime, default=lambda: datetime.now(timezone.utc), nullable=False
    )
    accepted_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    declined_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    activated_at: Mapped[Optional[datetime]] = mapped_column(DateTime)

    subscriber = relationship("Subscriber", backref="wallet_push_offers")

    __table_args__ = (
        CheckConstraint(
            "status IN ('offered','accepted','declined','activated','expired','failed')",
            name="check_wallet_push_offer_status",
        ),
        CheckConstraint(
            "framing_variant IN ('missing_leads','credits_ready')",
            name="check_wallet_push_framing_variant",
        ),
        Index("idx_wallet_push_offers_subscriber_offered", "subscriber_id", "offered_at"),
    )

    def __repr__(self):
        return f"<WalletPushOffer(id={self.id}, sub={self.subscriber_id}, status={self.status})>"


class UserSegment(Base):
    """Behavioral segment classification per subscriber (1:1)."""
    __tablename__ = "user_segments"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    subscriber_id: Mapped[int] = mapped_column(Integer, ForeignKey("subscribers.id"), nullable=False, unique=True, index=True)
    segment: Mapped[str] = mapped_column(String(30), nullable=False)  # 8 buckets
    revenue_signal_score: Mapped[Optional[int]] = mapped_column(Integer, default=0)  # 0–100

    # fa037 — Revenue Signal Score explainability + freshness columns.
    # Nullable to stay back-compat with rows written before fa037.
    revenue_signal_band: Mapped[Optional[str]] = mapped_column(String(20))  # low/medium/high/very_high
    revenue_signal_breakdown: Mapped[Optional[dict]] = mapped_column(JSONB)
    revenue_signal_updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    last_significant_action_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    revenue_signal_last_action: Mapped[Optional[str]] = mapped_column(String(80))

    # fa051 — Predictive Churn Risk. Nullable for back-compat with rows written before fa051.
    churn_risk_score: Mapped[Optional[int]] = mapped_column(Integer)
    churn_risk_band: Mapped[Optional[str]] = mapped_column(String(20))
    predicted_inactivity_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    churn_risk_reason: Mapped[Optional[str]] = mapped_column(String(255))
    churn_risk_updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))

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
        CheckConstraint(
            "revenue_signal_band IS NULL OR "
            "revenue_signal_band IN ('low', 'medium', 'high', 'very_high')",
            name="check_revenue_signal_band",
        ),
        CheckConstraint(
            "churn_risk_band IS NULL OR "
            "churn_risk_band IN ('low', 'medium', 'high', 'very_high')",
            name="check_churn_risk_band",
        ),
    )

    def __repr__(self):
        return f"<UserSegment(subscriber={self.subscriber_id}, segment={self.segment}, score={self.revenue_signal_score})>"


class RevenueSignalScoreEvent(Base):
    """fa037 — append-only audit row per Revenue Signal Score update.

    One row is written every time `update_revenue_signal_score()` runs,
    capturing the action that triggered the recompute, the old/new score,
    the delta, and the full breakdown at the time. Powers the admin
    subscriber detail "why did the score change?" view.

    Indexed for (subscriber_id, created_at DESC) so the admin endpoint
    can grab the last 20 history rows in one seek.
    """
    __tablename__ = "revenue_signal_score_events"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    subscriber_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("subscribers.id", ondelete="CASCADE"), nullable=False
    )
    action_type: Mapped[Optional[str]] = mapped_column(String(80))
    old_score: Mapped[Optional[int]] = mapped_column(Integer)
    new_score: Mapped[int] = mapped_column(Integer, nullable=False)
    delta: Mapped[int] = mapped_column(Integer, nullable=False)
    band: Mapped[Optional[str]] = mapped_column(String(20))
    breakdown: Mapped[Optional[dict]] = mapped_column(JSONB)
    meta_data: Mapped[Optional[dict]] = mapped_column("metadata", JSONB)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )

    def __repr__(self):
        return (
            f"<RevenueSignalScoreEvent(sub={self.subscriber_id}, "
            f"action={self.action_type}, delta={self.delta})>"
        )


class ConversionAttributionEvent(Base):
    """fa044 — one row per billable conversion event, capturing all 8 attribution
    dimensions at the moment the conversion was recorded.

    Unique on (source_table, source_event_id) — duplicate-safe at DB level.
    Score state is written to subscribers.revenue_signal_score; the audit trail
    lives in revenue_signal_score_events (fa037) with attribution_event_id in
    its metadata column linking back here.
    """
    __tablename__ = "conversion_attribution_events"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    conversion_type: Mapped[str] = mapped_column(String(60), nullable=False)
    source_table: Mapped[str] = mapped_column(String(80), nullable=False)
    source_event_id: Mapped[str] = mapped_column(String(120), nullable=False)
    subscriber_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("subscribers.id", ondelete="CASCADE"), nullable=False
    )
    lead_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("sent_leads.id", ondelete="SET NULL"), nullable=True
    )
    property_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("properties.id", ondelete="SET NULL"), nullable=True
    )
    zip_code: Mapped[Optional[str]] = mapped_column(String(10))
    trade: Mapped[Optional[str]] = mapped_column(String(50))
    wallet_tier: Mapped[Optional[str]] = mapped_column(String(30))
    lock_status: Mapped[Optional[str]] = mapped_column(String(20))
    lock_zip: Mapped[Optional[str]] = mapped_column(String(10))
    autopilot_tier: Mapped[Optional[str]] = mapped_column(String(30))
    bundle_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("bundle_purchases.id", ondelete="SET NULL"), nullable=True
    )
    bundle_type: Mapped[Optional[str]] = mapped_column(String(50))
    deal_size_bucket: Mapped[Optional[str]] = mapped_column(String(20))
    revenue_amount: Mapped[Optional[float]] = mapped_column(Numeric(12, 2))
    currency: Mapped[str] = mapped_column(String(3), default="usd", server_default="usd", nullable=False)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    attribution_status: Mapped[Optional[str]] = mapped_column(String(20))
    attribution_confidence: Mapped[Optional[str]] = mapped_column(String(20))
    attribution_metadata: Mapped[Optional[dict]] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    __table_args__ = (
        UniqueConstraint("source_table", "source_event_id", name="uq_attribution_source"),
        Index("idx_cae_subscriber_id", "subscriber_id"),
        Index("idx_cae_lead_id", "lead_id"),
        Index("idx_cae_zip_code", "zip_code"),
        Index("idx_cae_trade", "trade"),
        Index("idx_cae_wallet_tier", "wallet_tier"),
        Index("idx_cae_lock_status", "lock_status"),
        Index("idx_cae_autopilot_tier", "autopilot_tier"),
        Index("idx_cae_bundle_type", "bundle_type"),
        Index("idx_cae_deal_size_bucket", "deal_size_bucket"),
        Index("idx_cae_conversion_type", "conversion_type"),
        Index("idx_cae_occurred_at", "occurred_at"),
    )

    def __repr__(self):
        return (
            f"<ConversionAttributionEvent(id={self.id}, sub={self.subscriber_id}, "
            f"type={self.conversion_type}, status={self.attribution_status})>"
        )


class ChurnPrediction(Base):
    """fa051 — append-only row per nightly churn scoring run per subscriber.

    Written by churn_scoring job; consumed by proactive_save (save_offer_sent_at
    cooldown + in_holdout gate) and churn_validation_report (backfilled outcomes).
    Never mutated after write except for save_offer_sent_at, realized_inactive_at,
    was_correct (backfill pass).

    Index on (subscriber_id, predicted_at DESC) covers "latest prediction" and
    cooldown lookups efficiently.
    """
    __tablename__ = "churn_predictions"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    subscriber_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("subscribers.id", ondelete="CASCADE"), nullable=False
    )
    predicted_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    churn_risk_score: Mapped[int] = mapped_column(Integer, nullable=False)
    churn_risk_band: Mapped[Optional[str]] = mapped_column(String(20))
    predicted_inactivity_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    features: Mapped[Optional[dict]] = mapped_column(JSONB)
    in_holdout: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    save_offer_sent_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    realized_inactive_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    was_correct: Mapped[Optional[bool]] = mapped_column(Boolean)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )

    subscriber = relationship("Subscriber", backref="churn_predictions")

    __table_args__ = (
        Index("ix_churn_predictions_sub_predicted", "subscriber_id", "predicted_at"),
        Index("ix_churn_predictions_subscriber_id", "subscriber_id"),
        CheckConstraint(
            "churn_risk_band IS NULL OR churn_risk_band IN ('low', 'medium', 'high', 'very_high')",
            name="check_churn_prediction_band",
        ),
    )

    def __repr__(self):
        return (
            f"<ChurnPrediction(sub={self.subscriber_id}, score={self.churn_risk_score}, "
            f"band={self.churn_risk_band}, holdout={self.in_holdout})>"
        )


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
    # fa038 — personalization fields for variant performance attribution

    # hold / review / cancel flow
    send_status = Column(String(20), nullable=False, default="sent")
    requires_review = Column(Boolean, nullable=False, default=False)
    review_reason = Column(String(255), nullable=True)

    scheduled_send_at = Column(DateTime, nullable=True)
    approved_at = Column(DateTime, nullable=True)
    approved_by = Column(String(100), nullable=True)

    cancelled_at = Column(DateTime, nullable=True)
    cancelled_by = Column(String(100), nullable=True)
    cancel_reason = Column(String(255), nullable=True)

    # link back to Cora decision
    decision_id = Column(String(36), nullable=True, index=True)

    trade_vertical: Mapped[Optional[str]] = mapped_column(String(50))
    county_id: Mapped[Optional[str]] = mapped_column(String(50))
    behavioral_segment: Mapped[Optional[str]] = mapped_column(String(30))
    revenue_signal_score: Mapped[Optional[int]] = mapped_column(Integer)
    revenue_signal_score_band: Mapped[Optional[str]] = mapped_column(String(20))
    last_action_recency_band: Mapped[Optional[str]] = mapped_column(String(30))
    prompt_version: Mapped[Optional[str]] = mapped_column(String(20))
    context_snapshot: Mapped[Optional[dict]] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc))

    __table_args__ = (
        CheckConstraint("message_type IN ('sms', 'email', 'voice')", name="check_message_type"),
        Index("idx_msg_outcome_sub_sent", "subscriber_id", "sent_at"),
        Index("idx_msg_outcome_vertical_segment", "trade_vertical", "behavioral_segment"),
    )

    def __repr__(self):
        return f"<MessageOutcome(id={self.id}, type={self.message_type}, conversion={self.conversion_type})>"


class CoraSuppression(Base):
    """
    Active subscriber-level stop for Cora-led outbound touches.
    Compliance messages still flow through their own SMS gates.
    """
    __tablename__ = "cora_suppressions"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    subscriber_id: Mapped[int] = mapped_column(Integer, ForeignKey("subscribers.id"), nullable=False, index=True)
    reason: Mapped[str] = mapped_column(String(40), nullable=False)
    source: Mapped[str] = mapped_column(String(40), nullable=False)
    source_id: Mapped[Optional[str]] = mapped_column(String(100))
    paused_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    created_by: Mapped[Optional[str]] = mapped_column(String(100))
    notes: Mapped[Optional[str]] = mapped_column(String(255))

    __table_args__ = (
        Index("idx_cora_suppression_active_sub", "subscriber_id", "is_active"),
        Index("idx_cora_suppression_reason", "reason"),
    )

    def __repr__(self):
        return f"<CoraSuppression(sub={self.subscriber_id}, reason={self.reason}, active={self.is_active})>"


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
    pipeline_stage: Mapped[Optional[str]] = mapped_column(String(30))  # lead / contacted / qualified / proposal / negotiation / closed_won / closed_lost
    # fa056 — Stage 10 pricing cohort activation gate columns
    county_id: Mapped[Optional[str]] = mapped_column(String(50))
    trade_vertical: Mapped[Optional[str]] = mapped_column(String(50))
    created_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc))

    __table_args__ = (
        CheckConstraint(
            "deal_size_bucket IN ('5_10k', '10_25k', '25k_plus', 'skip')",
            name="check_deal_size_bucket",
        ),
        Index("idx_deal_outcome_sub_date", "subscriber_id", "deal_date"),
        CheckConstraint(
            "pipeline_stage IS NULL OR pipeline_stage IN ('lead','contacted','qualified','proposal','negotiation','closed_won','closed_lost')",
            name="check_deal_pipeline_stage",
        ),
        Index("idx_deal_outcome_pipeline_stage", "pipeline_stage"),
        Index("idx_deal_outcomes_county_vertical", "county_id", "trade_vertical"),
    )

    def __repr__(self):
        return f"<DealOutcome(id={self.id}, subscriber={self.subscriber_id}, bucket={self.deal_size_bucket})>"


class LossAutopsy(Base):
    """
    Structured failure retrospective written whenever a lead is marked closed_lost,
    declined, or ghosts past the 24-hour human-close SLA.  Claude parses
    multi-source context (transcripts, pricing, distress score) and classifies
    the loss into a standard taxonomy so A3 / A6 can retune scoring weights.
    """
    __tablename__ = "loss_autopsies"

    id: Mapped[object] = mapped_column(PG_UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()"))
    property_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("properties.id", ondelete="SET NULL"), nullable=True, index=True)
    prospect_id: Mapped[Optional[object]] = mapped_column(PG_UUID(as_uuid=True), ForeignKey("prospects.prospect_id", ondelete="SET NULL"), nullable=True)
    deal_outcome_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("deal_outcomes.id", ondelete="SET NULL"), nullable=True)
    trigger_reason: Mapped[str] = mapped_column(String(50), nullable=False)
    primary_rejection_reason: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    competitor_rate_delta: Mapped[Optional[float]] = mapped_column(Numeric(8, 4), nullable=True)
    underwriting_blocker: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    cora_behavior_adjustment: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    raw_context: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    model_response: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    claude_cost_usd: Mapped[Optional[float]] = mapped_column(Numeric(10, 6), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())

    __table_args__ = (
        CheckConstraint(
            "trigger_reason IN ('CLOSED_LOST','DECLINED','GHOSTED_SLA')",
            name="ck_loss_autopsy_trigger",
        ),
        Index("idx_loss_autopsies_deal_outcome_id", "deal_outcome_id"),
        Index("idx_loss_autopsies_trigger_reason", "trigger_reason"),
        Index("idx_loss_autopsies_created_at", "created_at"),
    )

    def __repr__(self) -> str:
        return f"<LossAutopsy(id={self.id}, trigger={self.trigger_reason}, reason={self.primary_rejection_reason})>"


class PreDecisionSnapshot(Base):
    """
    Pre-routing context snapshot captured at deal_outcome creation time.

    Stores all 6 CDS vertical scores (the roads not taken), the selected vertical,
    active pricing cohort, Cora graph, and pitch variant so the future A5b
    counterfactual engine can compare actual vs. alternative paths on resolution.

    Broker fields (broker_id, alternative_brokers) are nullable stubs — wirable
    when the broker routing layer is built without a schema migration.
    """
    __tablename__ = "pre_decision_snapshots"

    id: Mapped[object] = mapped_column(PG_UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()"))
    property_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("properties.id", ondelete="SET NULL"), nullable=True)
    prospect_id: Mapped[Optional[object]] = mapped_column(PG_UUID(as_uuid=True), ForeignKey("prospects.prospect_id", ondelete="SET NULL"), nullable=True)
    deal_outcome_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("deal_outcomes.id", ondelete="SET NULL"), nullable=True)

    snapshot_ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    selected_vertical: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    lead_tier: Mapped[Optional[str]] = mapped_column(String(30), nullable=True)
    final_cds_score: Mapped[Optional[float]] = mapped_column(Numeric(5, 2), nullable=True)
    distress_types: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)

    all_vertical_scores: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    runner_up_verticals: Mapped[Optional[list]] = mapped_column(JSONB, nullable=True)

    pricing_cohort_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    pricing_snapshot: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    cora_graph: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    pitch_variant: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)

    raw_context: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)

    resolved_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    outcome_status: Mapped[Optional[str]] = mapped_column(String(30), nullable=True)

    # Broker stub — populate when broker routing layer is built
    broker_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    alternative_brokers: Mapped[Optional[list]] = mapped_column(JSONB, nullable=True)

    counterfactual_run: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("FALSE"))
    counterfactual_run_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())

    __table_args__ = (
        Index("uq_pds_deal_outcome", "deal_outcome_id", unique=True,
              postgresql_where=text("deal_outcome_id IS NOT NULL")),
        Index("idx_pds_property_id", "property_id"),
        Index("idx_pds_snapshot_ts", "snapshot_ts"),
        Index("idx_pds_selected_vertical", "selected_vertical"),
        Index("idx_pds_outcome_status", "outcome_status"),
        Index("idx_pds_pending_cf", "id",
              postgresql_where=text("counterfactual_run = FALSE AND outcome_status IS NOT NULL")),
    )

    def __repr__(self) -> str:
        return f"<PreDecisionSnapshot(id={self.id}, vertical={self.selected_vertical}, outcome={self.outcome_status})>"


class SubscriberTag(Base):
    """Tags applied to subscribers for segmentation and filtering."""
    __tablename__ = "subscriber_tags"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    subscriber_id: Mapped[int] = mapped_column(Integer, ForeignKey("subscribers.id"), nullable=False, index=True)
    tag: Mapped[str] = mapped_column(String(50), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc))

    subscriber: Mapped["Subscriber"] = relationship("Subscriber", backref="tags")

    __table_args__ = (
        UniqueConstraint("subscriber_id", "tag", name="uq_subscriber_tag"),
        Index("idx_subscriber_tags_subscriber_id", "subscriber_id"),
        Index("idx_subscriber_tags_tag", "tag"),
    )

    def __repr__(self):
        return f"<SubscriberTag(id={self.id}, subscriber_id={self.subscriber_id}, tag='{self.tag}')>"
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
            "card_type IN ('message_perf', 'deal_pattern', 'ab_result', "
            "'churn_signal', 'pricing_test', 'general', "
            "'autonomy_summary')",      # fa036 — weekly Cora autonomy scorecard
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
            "status IN ('pending', 'confirmed', 'rewarded', 'expired', 'revoked')",
            name="check_referral_status",
        ),
        Index("idx_referral_referrer_status", "referrer_subscriber_id", "status"),
    )

    def __repr__(self):
        return f"<ReferralEvent(referrer={self.referrer_subscriber_id}, status={self.status})>"


class ReferralMilestoneAward(Base):
    """
    Idempotent record of a milestone grant for a referrer.
    UNIQUE(referrer_subscriber_id, milestone) prevents double-grants.
    milestone values: free_month_3 | lock_slot_5
    """
    __tablename__ = "referral_milestone_awards"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    referrer_subscriber_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("subscribers.id"), nullable=False, index=True
    )
    milestone: Mapped[str] = mapped_column(String(30), nullable=False)
    awarded_at: Mapped[datetime] = mapped_column(
        DateTime, default=lambda: datetime.now(timezone.utc), nullable=False
    )
    triggering_referral_event_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("referral_events.id")
    )
    grant_ref: Mapped[Optional[str]] = mapped_column(Text)  # Stripe coupon id or similar
    notified_at: Mapped[Optional[datetime]] = mapped_column(DateTime)

    __table_args__ = (
        UniqueConstraint("referrer_subscriber_id", "milestone", name="uq_referral_milestone_per_referrer"),
        CheckConstraint(
            "milestone IN ('free_month_3', 'lock_slot_5')",
            name="check_referral_milestone",
        ),
    )

    def __repr__(self):
        return f"<ReferralMilestoneAward(referrer={self.referrer_subscriber_id}, milestone={self.milestone})>"


class ReferralForwardCopy(Base):
    """
    Weekly Claude-generated share copy per buyer vertical.
    Cached to bound Claude spend and keep per-share latency low.
    """
    __tablename__ = "referral_forward_copy"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    vertical: Mapped[str] = mapped_column(String(50), nullable=False)
    week_start: Mapped[date] = mapped_column(Date, nullable=False)
    body: Mapped[str] = mapped_column(Text, nullable=False)
    generated_at: Mapped[datetime] = mapped_column(
        DateTime, default=lambda: datetime.now(timezone.utc), nullable=False
    )

    __table_args__ = (
        UniqueConstraint("vertical", "week_start", name="uq_referral_forward_copy_vertical_week"),
    )

    def __repr__(self):
        return f"<ReferralForwardCopy(vertical={self.vertical}, week_start={self.week_start})>"


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
    variant: Mapped[str] = mapped_column(String(10), nullable=False)  # 'a'/'b' for message-swap tests; 'variant'/'control' for rollout tests
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
    # Vendor neutral going forward. Historical rows keep "twilio_inbound";
    # new inbound STOP events tag as "inbound_sms" regardless of carrier.
    source: Mapped[str] = mapped_column(String(30), nullable=False, default="inbound_sms")  # inbound_sms/twilio_inbound (legacy)/manual/import
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
            "reason IN ('opt_out', 'delivery_failed', 'error', 'unresolvable', 'quiet_hours', 'no_opt_in', 'subscriber_sms_frequency_cap')",
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
    service: Mapped[str] = mapped_column(String(20), nullable=False)      # claude/telnyx/stripe
    model: Mapped[Optional[str]] = mapped_column(String(60))              # haiku/sonnet/opus (Claude only)
    input_tokens: Mapped[Optional[int]] = mapped_column(Integer)
    output_tokens: Mapped[Optional[int]] = mapped_column(Integer)
    cost_usd: Mapped[Optional[float]] = mapped_column(Numeric(10, 6))
    task_type: Mapped[Optional[str]] = mapped_column(String(60))          # sms_copy/classification/conversational_close/etc.
    graph_name: Mapped[Optional[str]] = mapped_column(String(60), index=True)
    pause_target: Mapped[Optional[str]] = mapped_column(String(80), index=True)
    blocked_by_pause: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    block_reason: Mapped[Optional[str]] = mapped_column(String(120))
    subscriber_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("subscribers.id"), index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc))

    __table_args__ = (
        CheckConstraint("service IN ('claude', 'telnyx', 'stripe', 'twilio')", name="check_api_service"),
        Index("idx_api_usage_service_created", "service", "created_at"),
        Index("idx_api_usage_task_created", "task_type", "created_at"),
        Index("idx_api_usage_pause_created", "pause_target", "created_at"),
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
    ab_variant: Mapped[Optional[str]] = mapped_column(String(8))   # Stage 5: 'a' / 'b' / NULL
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


class ReferralTeam(Base):
    """
    Stage 5 — referral team mechanic.

    When a referrer accumulates 3 confirmed referrals where every member is
    in the same county AND vertical, a ReferralTeam row is created and all
    three members get a Shared ZIP View (lead density across their union of
    locked ZIPs — density only, no PII shared between members).
    """
    __tablename__ = "referral_teams"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    lead_subscriber_id: Mapped[int] = mapped_column(Integer, ForeignKey("subscribers.id"), nullable=False, index=True)
    county_id: Mapped[str] = mapped_column(String(50), nullable=False)
    vertical: Mapped[str] = mapped_column(String(50), nullable=False)
    member_subscriber_ids: Mapped[list] = mapped_column(ARRAY(Integer), nullable=False)
    shared_zips: Mapped[Optional[list]] = mapped_column(ARRAY(String(10)))
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="active")  # active | broken
    unlocked_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)
    broken_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    broken_reason: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)  # dispute|refund|churn

    __table_args__ = (
        CheckConstraint("status IN ('active', 'broken')", name="check_referral_team_status"),
        Index("idx_referral_team_county_vertical", "county_id", "vertical"),
    )

    def __repr__(self):
        return f"<ReferralTeam(id={self.id}, lead={self.lead_subscriber_id}, members={self.member_subscriber_ids})>"


class PremiumPurchase(Base):
    """
    Stage 5: Premium credit SKU purchase (report / brief / transfer / byol).

    Either paid via wallet credits (`paid_via='credits'`, no Stripe row) or
    cash via Stripe Checkout / PaymentIntent (`paid_via='card'`,
    `stripe_payment_intent_id` set). Fulfillment artifact (PDF, skip-trace
    record id, etc.) is referenced via `output_ref`.
    """
    __tablename__ = "premium_purchases"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    subscriber_id: Mapped[int] = mapped_column(Integer, ForeignKey("subscribers.id"), nullable=False, index=True)
    sku: Mapped[str] = mapped_column(String(30), nullable=False)             # report|brief|transfer|byol
    paid_via: Mapped[str] = mapped_column(String(10), nullable=False)        # credits|card
    amount_cents: Mapped[Optional[int]] = mapped_column(Integer)             # null for credit purchases
    credits_spent: Mapped[Optional[int]] = mapped_column(Integer)            # null for cash purchases
    stripe_payment_intent_id: Mapped[Optional[str]] = mapped_column(String(100), unique=True, index=True)
    property_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("properties.id"), index=True)
    target_address: Mapped[Optional[str]] = mapped_column(String(255))       # for BYOL when no property_id
    output_ref: Mapped[Optional[str]] = mapped_column(String(255))           # path to PDF / FK to enriched_contacts.id
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="pending")  # pending|delivered|failed|refunded|disputed
    purchased_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc))
    delivered_at: Mapped[Optional[datetime]] = mapped_column(DateTime)

    # ── Refund / dispute audit (fa004, 2026-05-04) ──────────────────
    refunded_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    refund_reason: Mapped[Optional[str]] = mapped_column(String(100))
    refund_amount_cents: Mapped[Optional[int]] = mapped_column(Integer)
    disputed_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    dispute_reason: Mapped[Optional[str]] = mapped_column(String(100))
    stripe_charge_id: Mapped[Optional[str]] = mapped_column(String(100), index=True)

    __table_args__ = (
        CheckConstraint("sku IN ('report', 'brief', 'transfer', 'byol')", name="check_premium_sku"),
        CheckConstraint("paid_via IN ('credits', 'card')", name="check_premium_paid_via"),
        CheckConstraint(
            "status IN ('pending', 'delivered', 'failed', 'refunded', 'disputed')",
            name="check_premium_status",
        ),
        Index("idx_premium_purchase_sub_sku", "subscriber_id", "sku"),
    )

    def __repr__(self):
        return f"<PremiumPurchase(id={self.id}, sku={self.sku}, paid_via={self.paid_via}, status={self.status})>"


class EnrichmentUsageLog(Base):
    """
    Per-call cost log for third-party enrichment vendors (BatchData skip-trace,
    future Twilio Lookup, etc.).

    Drives the per-SKU margin dashboard and the daily Revenue Pulse line for
    each enrichment-backed product (lead unlock, BYOL, Transfer, batch run).
    Append-only — one row per vendor lookup, success or failure.

    Added 2026-05-04 (fa004).
    """
    __tablename__ = "enrichment_usage_logs"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    vendor: Mapped[str] = mapped_column(String(30), nullable=False)              # batchdata | twilio_lookup | ...
    purpose: Mapped[str] = mapped_column(String(40), nullable=False)             # premium_transfer | premium_byol | lead_unlock | batch_skip_trace
    subscriber_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("subscribers.id"), index=True)
    property_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("properties.id"), index=True)
    target_address: Mapped[Optional[str]] = mapped_column(String(255))
    cost_cents: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    success: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    error: Mapped[Optional[str]] = mapped_column(String(255))
    request_ref: Mapped[Optional[str]] = mapped_column(String(100))               # vendor request id, batch id, etc.
    created_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc), index=True)

    __table_args__ = (
        Index("idx_enrichment_purpose_created", "purpose", "created_at"),
        Index("idx_enrichment_vendor_created", "vendor", "created_at"),
    )

    def __repr__(self):
        return f"<EnrichmentUsageLog(vendor={self.vendor}, purpose={self.purpose}, cost_cents={self.cost_cents})>"


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
    # Per-identity consent scope (fa031). 'subscriber' = LLC/account-level opt-in,
    # default for all legacy rows. 'managing_member_direct' = personal consent from
    # a managing-member-derived phone — required before any member-targeted SMS.
    consent_scope: Mapped[str] = mapped_column(String(30), nullable=False, default="subscriber")

    __table_args__ = (
        CheckConstraint(
            "source IN ('double_opt_in', 'manual', 'import', 'widget', 'waitlist_form', 'synthflow_inbound', 'missed_call_inbound')",
            name="check_opt_in_source",
        ),
        CheckConstraint(
            "consent_scope IN ('subscriber','managing_member_direct','agent_direct','other')",
            name="check_opt_in_consent_scope",
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
    variant_id: Mapped[Optional[str]] = mapped_column(String(80), nullable=True, index=True)

    # ── fa036 — autonomy classification ────────────────────────────────────
    # autonomy_class: classification at decision time. Enum-CHECK enforces values.
    # was_autonomous: STICKY flag. Set TRUE on first 'autonomous' classification;
    #   never cleared. Metric 2 ("% overridden among autonomous") queries on this
    #   instead of the current autonomy_class so rows that flipped to 'overridden'
    #   still count in the denominator.
    # playbook_id: nullable link to the cora_playbook row that drove this decision.
    autonomy_class: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    was_autonomous: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    requires_approval: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    approved_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    approved_by: Mapped[Optional[str]] = mapped_column(String(80), nullable=True)
    overridden_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    overridden_by: Mapped[Optional[str]] = mapped_column(String(80), nullable=True)
    override_reason: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    override_reason_code: Mapped[Optional[str]] = mapped_column(String(40), nullable=True)
    playbook_id: Mapped[Optional[int]] = mapped_column(
        BigInteger, ForeignKey("cora_playbook.id", ondelete="SET NULL"), nullable=True,
    )

    __table_args__ = (
        CheckConstraint(
            "terminal_status IS NULL OR terminal_status IN ('completed', 'aborted', 'escalated', 'failed')",
            name="check_agent_terminal_status",
        ),
        CheckConstraint(
            "autonomy_class IS NULL OR autonomy_class IN ("
            "'autonomous','approval_required','approved',"
            "'rejected','overridden','recommendation_only')",
            name="check_agent_autonomy_class",
        ),
        Index("idx_agent_decisions_graph_started", "graph_name", "started_at"),
        Index("idx_agent_decisions_subscriber_started", "subscriber_id", "started_at"),
        Index("idx_agent_decisions_override_reason_code", "override_reason_code"),
    )

    def __repr__(self):
        return f"<AgentDecision(id={self.decision_id[:8]}, graph={self.graph_name}, status={self.terminal_status})>"


class QuoraQuestion(Base):
    """
    One row per Quora question that has been classified by Cora.
    Includes skip decisions — so the same question is never re-classified on
    future scrape runs. Drives the answer-generation and publishing pipeline.
    """
    __tablename__ = "quora_questions"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    # ── Quora identity ────────────────────────────────────────────────────────
    qid: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True, unique=True, index=True)
    slug: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    url: Mapped[str] = mapped_column(Text, nullable=False)
    title: Mapped[str] = mapped_column(Text, nullable=False)

    # ── Scraped signals ───────────────────────────────────────────────────────
    answer_count: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    follower_count: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    view_count: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    is_locked: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    is_sensitive: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    topics: Mapped[Optional[list]] = mapped_column(ARRAY(Text), nullable=True)
    created_time: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    # ── Deterministic scoring ─────────────────────────────────────────────────
    deterministic_score: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    deterministic_reasons: Mapped[Optional[list]] = mapped_column(ARRAY(Text), nullable=True)

    # ── Cora classification ───────────────────────────────────────────────────
    matched_keyword: Mapped[Optional[str]] = mapped_column(Text, nullable=True, index=True)
    cora_decision_id: Mapped[Optional[str]] = mapped_column(String(36), nullable=True)
    intent_lane: Mapped[Optional[str]] = mapped_column(String(60), nullable=True, index=True)
    recommended_action: Mapped[Optional[str]] = mapped_column(String(40), nullable=True, index=True)
    priority_score: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    risk_level: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    cora_classification: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)

    # ── Answer workflow ───────────────────────────────────────────────────────
    answer_draft: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    answer_status: Mapped[str] = mapped_column(
        String(20), nullable=False, default="pending", index=True
    )
    post_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    error_log: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    published_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    posted_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    quora_answer_id: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # ── Housekeeping ──────────────────────────────────────────────────────────
    first_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
    last_classified_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )

    __table_args__ = (
        CheckConstraint(
            "answer_status IN ('pending','drafted','skipped','published','failed')",
            name="check_quora_answer_status",
        ),
        Index("idx_quora_questions_action_priority", "recommended_action", "priority_score"),
    )

    def __repr__(self):
        return f"<QuoraQuestion(qid={self.qid}, action={self.recommended_action}, status={self.answer_status})>"


class QuoraTopic(Base):
    """
    Admin-managed pool of search keywords for the daily Quora organic-answer pipeline.
    The orchestrator picks one available topic per run using cooldown rotation.
    """
    __tablename__ = "quora_topics"

    id:          Mapped[int]               = mapped_column(Integer, primary_key=True, autoincrement=True)
    keyword:     Mapped[str]               = mapped_column(Text, nullable=False, unique=True)
    is_active:   Mapped[bool]              = mapped_column(Boolean, nullable=False, default=True)
    last_run_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at:  Mapped[datetime]          = mapped_column(
        DateTime(timezone=True), nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )

    def __repr__(self):
        return f"<QuoraTopic(id={self.id}, keyword={self.keyword!r}, active={self.is_active})>"


class QuoraSettings(Base):
    """
    Single-row configuration table for the Quora pipeline (id always = 1).
    cooldown_days: a topic that ran today cannot be picked again for this many days.
    Invariant: cooldown_days <= active_topic_count - 1 (enforced at write time).
    """
    __tablename__ = "quora_settings"

    id:            Mapped[int] = mapped_column(Integer, primary_key=True, default=1)
    cooldown_days: Mapped[int] = mapped_column(Integer, nullable=False, default=1)

    def __repr__(self):
        return f"<QuoraSettings(cooldown_days={self.cooldown_days})>"


class VendorCostPause(Base):
    """
    Durable cost-pause record for a pause_target.

    Lifecycle rows are retained for audit. Only one active row per
    (vendor, pause_target) should exist at a time.
    """
    __tablename__ = "vendor_cost_pauses"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    vendor: Mapped[str] = mapped_column(String(30), nullable=False, index=True)
    pause_target: Mapped[str] = mapped_column(String(80), nullable=False, index=True)
    source_table: Mapped[Optional[str]] = mapped_column(String(80))
    source_key: Mapped[Optional[str]] = mapped_column(String(120))
    reason: Mapped[str] = mapped_column(Text, nullable=False)
    anomaly_score: Mapped[Optional[float]] = mapped_column(Numeric(10, 4))
    today_cost_usd: Mapped[Optional[float]] = mapped_column(Numeric(10, 6))
    baseline_avg_usd: Mapped[Optional[float]] = mapped_column(Numeric(10, 6))
    baseline_stddev_usd: Mapped[Optional[float]] = mapped_column(Numeric(10, 6))
    threshold_usd: Mapped[Optional[float]] = mapped_column(Numeric(10, 6))
    sample_n: Mapped[Optional[int]] = mapped_column(Integer)
    window_days: Mapped[int] = mapped_column(Integer, nullable=False, default=14)
    paused_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False, index=True)
    auto_resume_at: Mapped[Optional[datetime]] = mapped_column(DateTime, index=True)
    resumed_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="active", index=True)
    created_by: Mapped[str] = mapped_column(String(40), nullable=False, default="cost_monitor")
    resumed_by: Mapped[Optional[str]] = mapped_column(String(80))
    metadata_json: Mapped[Optional[dict]] = mapped_column(JSONB, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    __table_args__ = (
        CheckConstraint(
            "status IN ('active', 'auto_resumed', 'manually_resumed', 'superseded')",
            name="check_vendor_cost_pause_status",
        ),
        Index("idx_vendor_cost_pause_vendor_target", "vendor", "pause_target"),
        Index("idx_vendor_cost_pause_status_resume", "status", "auto_resume_at"),
    )

    def __repr__(self):
        return f"<VendorCostPause(vendor={self.vendor}, pause_target={self.pause_target}, status={self.status})>"

      
class CoraPlaybook(Base):
    """Cora-recommended pattern lifecycle (fa036).

    One row per Cora-authored recommendation (A/B winner promotion, kill
    recommendation, future explicit recommendations). Lifecycle:
        recommended → adopted   (human approves via admin endpoint)
                    → rejected  (human declines)
                    → retired   (previously-adopted playbook is disabled)

    Runtime never instantiates this model — every read/write goes through
    raw SQL via `sa_text` (per repo convention) in `src/services/playbook_writer.py`,
    `src/api/admin_router.py`, and `src/tasks/cora_autonomy_report.py`. The
    declaration exists for Alembic autogenerate consistency.

    The `source_key` column + the partial-unique index on it prevent
    duplicate recommendations from the same A/B test or metric breach
    (see `idx_cora_playbook_source_key_unique` in fa036). NULL source_key
    is allowed and uncounted by the index.
    """
    __tablename__ = "cora_playbook"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(120), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    pattern_json: Mapped[dict] = mapped_column(JSONB, nullable=False)

    # authored_by: 'cora' for autonomous paths; <operator handle> for manual.
    # The ab_engine.complete_test source_actor kwarg carries this through.
    authored_by: Mapped[str] = mapped_column(String(80), nullable=False)
    authored_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc), nullable=False,
    )

    status: Mapped[str] = mapped_column(String(20), nullable=False, default="recommended")

    adopted_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    adopted_by: Mapped[Optional[str]] = mapped_column(String(80), nullable=True)
    rejected_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    rejected_by: Mapped[Optional[str]] = mapped_column(String(80), nullable=True)
    rejection_reason: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    retired_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    retired_by: Mapped[Optional[str]] = mapped_column(String(80), nullable=True)

    # decision_id is VARCHAR(36) to match agent_decisions.decision_id exactly
    # (which is String(36), not PG UUID type — see correction #8 in plan).
    decision_id: Mapped[Optional[str]] = mapped_column(
        String(36),
        ForeignKey("agent_decisions.decision_id", ondelete="SET NULL"),
        nullable=True,
    )

    # Source tracking — dedupes repeat recommendations for the same source.
    source_type: Mapped[Optional[str]] = mapped_column(String(40), nullable=True)
    source_id: Mapped[Optional[str]] = mapped_column(String(80), nullable=True)
    source_key: Mapped[Optional[str]] = mapped_column(String(160), nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc), nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    __table_args__ = (
        CheckConstraint(
            "status IN ('recommended','adopted','rejected','retired')",
            name="check_cora_playbook_status",
        ),
        # Non-unique indexes mirror fa036. The unique partial index on
        # source_key is created via raw SQL in the migration, not declared
        # here, so autogenerate doesn't try to re-create it.
        Index("idx_cora_playbook_status", "status"),
        Index("idx_cora_playbook_authored", "authored_by", "authored_at"),
    )

    def __repr__(self):
        return (
            f"<CoraPlaybook(id={self.id}, name={self.name}, "
            f"status={self.status}, authored_by={self.authored_by})>"
        )


class SmsSendLog(Base):
    """One row per sms_compliance.send_sms call — ops audit of every send attempt."""
    __tablename__ = "sms_send_logs"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    phone: Mapped[Optional[str]] = mapped_column(String(20))
    subscriber_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("subscribers.id"), nullable=True)
    message_outcome_id = Column(Integer, ForeignKey("message_outcomes.id"), nullable=True, index=True)
    task_type: Mapped[Optional[str]] = mapped_column(String(80))
    message_type: Mapped[str] = mapped_column(String(20), nullable=False)
    outcome: Mapped[str] = mapped_column(String(20), nullable=False)
    suppress_reason: Mapped[Optional[str]] = mapped_column(String(40))
    vendor_message_id: Mapped[Optional[str]] = mapped_column(String(80))
    vendor: Mapped[str] = mapped_column(String(20), nullable=False, default="telnyx")
    campaign: Mapped[Optional[str]] = mapped_column(String(100))
    variant_id: Mapped[Optional[str]] = mapped_column(String(100))
    decision_id: Mapped[Optional[str]] = mapped_column(String(36))
    body_preview: Mapped[Optional[str]] = mapped_column(String(160))
    prospect_id: Mapped[Optional[str]] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("prospects.prospect_id", ondelete="SET NULL"),
        nullable=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=lambda: datetime.now(timezone.utc), nullable=False
    )

    __table_args__ = (
        CheckConstraint(
            "outcome IN ('sent', 'suppressed', 'dry_run', 'failed')",
            name="check_ssl_outcome",
        ),
        CheckConstraint(
            "message_type IN ('marketing', 'transactional', 'opt_in_prompt')",
            name="check_ssl_message_type",
        ),
        Index("idx_ssl_phone", "phone"),
        Index("idx_ssl_sub_created", "subscriber_id", "created_at"),
        Index("idx_ssl_outcome_created", "outcome", "created_at"),
        Index("idx_ssl_vendor_msg_id", "vendor_message_id"),
        Index("idx_sms_send_logs_prospect_id", "prospect_id"),
    )

    def __repr__(self):
        return f"<SmsSendLog(id={self.id}, phone={self.phone}, outcome={self.outcome})>"


# ============================================================================
# COUNTY CONFIGURATION (Admin-managed, replaces counties.json)
# ============================================================================

class County(Base):
    """
    One row per county. Replaces the top-level keys in counties.json.
    Admin UI does CRUD on this table; scrapers and loaders read via get_county_config().
    """
    __tablename__ = "counties"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    county_id: Mapped[str] = mapped_column(String(50), unique=True, nullable=False)
    display_name: Mapped[str] = mapped_column(String(100), nullable=False)
    fips: Mapped[Optional[str]] = mapped_column(String(10))
    nws_zone: Mapped[Optional[str]] = mapped_column(String(20))
    parcel_id_format: Mapped[Optional[str]] = mapped_column(String(20), default="folio")
    bankruptcy_division: Mapped[Optional[str]] = mapped_column(String(10))
    city_filer_keywords: Mapped[Optional[dict]] = mapped_column(JSONB, default=list)
    code_lien_type_map: Mapped[Optional[dict]] = mapped_column(JSONB, default=dict)
    # Lowercase city/CDP tokens stripped from address suffixes during
    # normalization. Source of truth for per-county address city stripping —
    # replaces the hardcoded Hillsborough list previously in BaseLoader.
    address_city_tokens: Mapped[Optional[list]] = mapped_column(JSONB, default=list)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), onupdate=func.now()
    )

    sources: Mapped[List["CountySource"]] = relationship(
        "CountySource", back_populates="county", cascade="all, delete-orphan"
    )

    __table_args__ = (
        Index("idx_counties_county_id", "county_id"),
        Index("idx_counties_is_active", "is_active"),
    )

    def __repr__(self):
        return f"<County(county_id={self.county_id!r}, display_name={self.display_name!r})>"


class CountySource(Base):
    """
    One row per (county, signal_type) pair. Holds the portal URL, description,
    and navigation hints that browser-use scrapers consume at runtime.
    """
    __tablename__ = "county_sources"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    county_id: Mapped[str] = mapped_column(
        String(50), ForeignKey("counties.county_id"), nullable=False
    )
    signal_type: Mapped[str] = mapped_column(String(50), nullable=False)
    source_name: Mapped[Optional[str]] = mapped_column(String(100))
    url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    description: Mapped[Optional[str]] = mapped_column(Text)
    navigation_hint: Mapped[Optional[str]] = mapped_column(Text)
    output_format: Mapped[Optional[str]] = mapped_column(String(20))
    date_range_available: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    frequency: Mapped[Optional[str]] = mapped_column(String(20), default="daily")
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    special_flags: Mapped[Optional[dict]] = mapped_column(JSONB, default=dict)
    # Scrape-mode enum (DB-side CHECK constraint enforces values):
    #   ai_only            — browser-use Agent only
    #   playwright_only    — execute cached playwright_code only; no AI fallback
    #   playwright_then_ai — try cached code first, fall back to AI on failure
    scrape_mode: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default="ai_only",
        server_default="ai_only",
    )
    # Cached Playwright function (async def run_scrape(...)) — either LLM-
    # generated or admin-pasted. Engine refuses to run unapproved code unless
    # scrape_mode forces it; the in-engine warning logs unapproved usage.
    playwright_code: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    playwright_code_version: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    playwright_code_approved: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
        server_default=sa_false(),
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), onupdate=func.now()
    )

    county: Mapped["County"] = relationship("County", back_populates="sources")
    mappings: Mapped[List["CountyColumnMapping"]] = relationship(
        "CountyColumnMapping", back_populates="source", cascade="all, delete-orphan"
    )

    __table_args__ = (
        UniqueConstraint("county_id", "signal_type", name="uq_county_signal_source"),
        Index("idx_county_sources_county_id", "county_id"),
        Index("idx_county_sources_signal_type", "signal_type"),
        Index("idx_county_sources_is_active", "is_active"),
        CheckConstraint(
            "scrape_mode IN ('ai_only','playwright_only','playwright_then_ai','static_download','api')",
            name="ck_county_sources_scrape_mode",
        ),
    )

    def __repr__(self):
        return f"<CountySource(county_id={self.county_id!r}, signal_type={self.signal_type!r})>"


class CountyColumnMapping(Base):
    """
    Column mapping for a given source.  One row per (source, approval event).
    - mapped_by='llm'   — auto-proposed, is_approved=False until admin reviews
    - mapped_by='human' — saved directly from admin UI, is_approved=True immediately
    """
    __tablename__ = "county_column_mappings"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    source_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("county_sources.id"), nullable=False
    )
    source_columns: Mapped[dict] = mapped_column(JSONB, nullable=False)
    mapping: Mapped[dict] = mapped_column(JSONB, nullable=False)
    is_approved: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    mapped_by: Mapped[Optional[str]] = mapped_column(String(10), default="llm")
    approved_by: Mapped[Optional[str]] = mapped_column(String(100))
    approved_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    sample_rows: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    reject_feedback: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    # Ordered transformations applied to the renamed DataFrame, before value
    # normalization. Currently supports one op:
    #   {"op": "split_on_separator", "from": "BookPage", "sep": "/", "into": ["Book", "Page"]}
    post_processors: Mapped[Optional[list]] = mapped_column(JSONB, nullable=True)
    # Per-column value normalization, applied after rename + post_processors.
    # Shape: {"DocType": {"JUDGEMENT": "JUDGMENT", "LIEN (IRS)": "TAX LIEN", ...}}
    value_maps: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    # DocType -> downstream signal-bucket routing. Only set on multi-bucket
    # sources (the liens ORI export, which fans out into liens/deeds/judgments/
    # probate/divorce). Shape:
    #   {"column": "DocType",
    #    "default": "skip",
    #    "rules": [{"match_exact": ["DEED"], "bucket": "deeds"}, ...]}
    row_routing: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), onupdate=func.now()
    )

    source: Mapped["CountySource"] = relationship("CountySource", back_populates="mappings")

    __table_args__ = (
        Index("idx_county_col_mappings_source_id", "source_id"),
        Index("idx_county_col_mappings_is_approved", "is_approved"),
    )


class PlaywrightCodeHistory(Base):
    """
    Append-only history of every LLM-generated Playwright scrape function.

    One row per (re)generation or cache-clear event. Lets us answer:
      - When did this source's scraper change?
      - Was it a prompt-version change or a portal change?
      - Roll back to the previous code if a new generation regresses.
    """
    __tablename__ = "playwright_code_history"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    source_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("county_sources.id"), nullable=False, index=True
    )
    county_id: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    code: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    prompt_version: Mapped[Optional[str]] = mapped_column(String(20))
    reason: Mapped[str] = mapped_column(String(40), nullable=False)
    is_approved: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    generated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    __table_args__ = (
        Index("idx_pwc_history_source_generated", "source_id", "generated_at"),
    )

    def __repr__(self):
        return (
            f"<PlaywrightCodeHistory(source_id={self.source_id}, "
            f"reason={self.reason!r}, version={self.prompt_version})>"
        )


class CFBypassProfile(Base):
    """
    Per-county Cloudflare-bypass session metadata.

    Profile FILES live on the scraping host's local disk under
    data/cf_session/edge_profile_<profile_name>/ — this row tracks the
    metadata (status, last warmed / validated timestamps, failure reasons)
    plus an optional zipped backup blob so a fresh host can restore a
    known-good profile without re-warming from scratch.

    See: src/utils/cf_session_manager.py for the lifecycle logic and
    src/utils/cf_persistent_browser.py for the Playwright launch wrapper.
    """
    __tablename__ = "cf_bypass_profiles"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    profile_name: Mapped[str] = mapped_column(String(80), nullable=False, unique=True)
    county_id:    Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    portal_url:   Mapped[str] = mapped_column(Text, nullable=False)
    status:       Mapped[str] = mapped_column(
        String(20), nullable=False, default="unwarmed", server_default="unwarmed",
    )

    last_warmed_at:      Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    last_validated_at:   Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    last_failure_at:     Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    last_failure_reason: Mapped[Optional[str]]      = mapped_column(Text)

    profile_dir_path: Mapped[str] = mapped_column(Text, nullable=False)
    validation_ttl_minutes: Mapped[int] = mapped_column(
        Integer, nullable=False, default=540, server_default="540",
    )

    profile_blob:      Mapped[Optional[bytes]]    = mapped_column(sa_LargeBinary, nullable=True)  # type: ignore[name-defined]
    profile_blob_size: Mapped[Optional[int]]      = mapped_column(Integer, nullable=True)
    profile_blob_at:   Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False,
        onupdate=func.now(),
    )

    __table_args__ = (
        CheckConstraint(
            "status IN ('unwarmed', 'ready', 'warming', 'expired', 'failed')",
            name="check_cf_profile_status",
        ),
        Index("ix_cf_bypass_profiles_status_lookup", "status"),
    )

    def __repr__(self):
        return (
            f"<CFBypassProfile(name={self.profile_name!r}, "
            f"county={self.county_id}, status={self.status})>"
        )


# ============================================================================
# Phase A: Wallet-to-Lock / AP Lite / Human Close
# ============================================================================

class ManualActionLog(Base):
    """
    Tracks per-subscriber manual actions for AP Lite threshold detection.
    Populated by wallet_engine.debit() for any action in MANUAL_ACTION_TYPES.
    """
    __tablename__ = "manual_action_log"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    subscriber_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("subscribers.id"), nullable=False
    )
    action_type: Mapped[str] = mapped_column(String(40), nullable=False)
    week_start: Mapped[date] = mapped_column(Date, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=lambda: datetime.now(timezone.utc), nullable=False
    )

    __table_args__ = (
        Index("idx_mal_sub_week", "subscriber_id", "week_start"),
        Index("idx_mal_created", "created_at"),
    )

    def __repr__(self):
        return f"<ManualActionLog(sub={self.subscriber_id}, action={self.action_type}, week={self.week_start})>"


class HumanCloseEscalation(Base):
    """
    Audit trail for high-intent subscriber escalations routed to human closers.
    UNIQUE(subscriber_id, decision_id) prevents duplicate routing.
    """
    __tablename__ = "human_close_escalations"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    subscriber_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("subscribers.id"), nullable=False
    )
    decision_id: Mapped[str] = mapped_column(String(40), nullable=False)
    revenue_signal_score: Mapped[int] = mapped_column(Integer, nullable=False)
    interactions_count: Mapped[int] = mapped_column(Integer, nullable=False)
    target_tier: Mapped[str] = mapped_column(String(20), nullable=False)
    channel: Mapped[str] = mapped_column(String(20), nullable=False)
    routed_at: Mapped[datetime] = mapped_column(
        DateTime, default=lambda: datetime.now(timezone.utc), nullable=False
    )
    closer_assigned: Mapped[Optional[str]] = mapped_column(String(80))
    outcome: Mapped[Optional[str]] = mapped_column(String(20))
    outcome_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    context_json: Mapped[Optional[dict]] = mapped_column(JSONB)
    posted_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    post_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_post_error: Mapped[Optional[str]] = mapped_column(String(200), nullable=True)
    target_tier_price_cents: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    vertical: Mapped[Optional[str]] = mapped_column(String(40), nullable=True)

    __table_args__ = (
        UniqueConstraint("subscriber_id", "decision_id", name="uq_hce_sub_decision"),
        Index("idx_hce_routed", "routed_at"),
        Index("idx_hce_open", "outcome", "routed_at"),
        CheckConstraint(
            "channel IN ('slack', 'ghl', 'sms', 'email')",
            name="check_hce_channel",
        ),
        CheckConstraint(
            "outcome IN ('won', 'lost', 'no_response', 'rescheduled') OR outcome IS NULL",
            name="check_hce_outcome",
        ),
    )

    def __repr__(self):
        return f"<HumanCloseEscalation(sub={self.subscriber_id}, channel={self.channel}, outcome={self.outcome})>"


# ============================================================================
# Phase B: Partner Tier
# ============================================================================

DEFAULT_MAX_PARTNER_ZIPS = 5


class PartnerSubscription(Base):
    """
    One row per partner subscriber. Tracks max ZIP allotment and lifecycle.
    Multiple ZipTerritory rows (one per ZIP) point to the same subscriber_id.
    """
    __tablename__ = "partner_subscriptions"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    subscriber_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("subscribers.id"), nullable=False, unique=True
    )
    max_zips: Mapped[int] = mapped_column(Integer, nullable=False, default=DEFAULT_MAX_PARTNER_ZIPS)
    activated_at: Mapped[datetime] = mapped_column(
        DateTime, default=lambda: datetime.now(timezone.utc), nullable=False
    )
    deactivated_at: Mapped[Optional[datetime]] = mapped_column(DateTime)

    def __repr__(self):
        return f"<PartnerSubscription(sub={self.subscriber_id}, max_zips={self.max_zips})>"


# ============================================================================
# Affiliate Program (Stream D) — external promoters paid a cash Commission.
# Distinct from ReferralEvent (peer wallet-credit loop) and PartnerSubscription
# (a subscription tier). See docs/adr/0005 and CONTEXT.md "Affiliate Program".
# ============================================================================

class Affiliate(Base):
    """External promoter paid a cash Commission for referred paying subscribers.

    Admin-minted; ref_code is the opaque ?ref= token. commission_rate is
    per-affiliate, defaulting to 20%.
    """
    __tablename__ = "affiliates"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    ref_code: Mapped[str] = mapped_column(String(40), nullable=False, unique=True, index=True)
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    contact_email: Mapped[Optional[str]] = mapped_column(String(255))
    contact_phone: Mapped[Optional[str]] = mapped_column(String(20))
    commission_rate: Mapped[Decimal] = mapped_column(
        Numeric(5, 4), nullable=False, server_default=text("0.20")
    )
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="active")
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=lambda: datetime.now(timezone.utc), nullable=False
    )

    __table_args__ = (
        CheckConstraint("status IN ('active', 'disabled')", name="check_affiliate_status"),
    )

    def __repr__(self):
        return f"<Affiliate(ref_code={self.ref_code}, status={self.status})>"


class AffiliateReferral(Base):
    """Confirmed link between a paying Subscriber and the Affiliate who referred
    them — the unit a Commission is calculated against.

    One affiliate per subscriber (subscriber_id UNIQUE). Stamped pending at
    registration, flipped active at first paid upgrade.
    """
    __tablename__ = "affiliate_referrals"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    affiliate_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("affiliates.id"), nullable=False, index=True
    )
    subscriber_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("subscribers.id"), nullable=False, unique=True
    )
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="pending")
    attributed_at: Mapped[datetime] = mapped_column(
        DateTime, default=lambda: datetime.now(timezone.utc), nullable=False
    )
    confirmed_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    paid_tenure_start: Mapped[Optional[datetime]] = mapped_column(DateTime)
    window_end: Mapped[Optional[datetime]] = mapped_column(DateTime)

    __table_args__ = (
        CheckConstraint(
            "status IN ('pending', 'active', 'expired')",
            name="check_affiliate_referral_status",
        ),
    )

    def __repr__(self):
        return f"<AffiliateReferral(affiliate={self.affiliate_id}, sub={self.subscriber_id}, status={self.status})>"


class SubscriptionInvoice(Base):
    """A collected recurring-subscription invoice, captured from Stripe.

    Source of truth for Commission accrual — never nominal plan_price. Marked
    reversed (refund/dispute) so the monthly run can write a clawback.
    """
    __tablename__ = "subscription_invoices"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    subscriber_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("subscribers.id"), nullable=False, index=True
    )
    stripe_invoice_id: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    # fa081: cross-version refund linkage. Stripe API 2026-02-25 nulls
    # charge.invoice, so refunds are matched back to the invoice by this PI.
    stripe_payment_intent_id: Mapped[Optional[str]] = mapped_column(String(255), index=True)
    amount_collected_cents: Mapped[int] = mapped_column(Integer, nullable=False)
    period_month: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    paid_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    reversed_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    reversed_reason: Mapped[Optional[str]] = mapped_column(String(20))

    __table_args__ = (
        CheckConstraint(
            "reversed_reason IN ('refund', 'dispute')",
            name="check_subscription_invoice_reversed_reason",
        ),
    )

    def __repr__(self):
        return f"<SubscriptionInvoice(stripe_invoice_id={self.stripe_invoice_id}, cents={self.amount_collected_cents})>"


class AffiliatePayoutLedger(Base):
    """Append-only record of Commission owed: accrual lines plus negative
    clawback lines. Never mutated — corrections are new offsetting lines.

    UNIQUE(affiliate_referral_id, period_month, line_type) makes the monthly
    accrual run idempotent on rerun.
    """
    __tablename__ = "affiliate_payout_ledger"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    affiliate_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("affiliates.id"), nullable=False, index=True
    )
    affiliate_referral_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("affiliate_referrals.id"), nullable=False, index=True
    )
    period_month: Mapped[date] = mapped_column(Date, nullable=False)
    line_type: Mapped[str] = mapped_column(String(20), nullable=False)
    amount_cents: Mapped[int] = mapped_column(Integer, nullable=False)
    source_invoice_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("subscription_invoices.id")
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=lambda: datetime.now(timezone.utc), nullable=False
    )

    __table_args__ = (
        UniqueConstraint(
            "affiliate_referral_id", "period_month", "line_type",
            name="uq_affiliate_ledger_period_line",
        ),
        CheckConstraint(
            "line_type IN ('accrual', 'clawback')",
            name="check_affiliate_ledger_line_type",
        ),
    )

    def __repr__(self):
        return f"<AffiliatePayoutLedger(referral={self.affiliate_referral_id}, {self.line_type}, cents={self.amount_cents})>"


# ============================================================================
# DBPR — Contractor Contact Registry
# ============================================================================

class DBPRContact(Base):
    """
    Florida DBPR licensed contractor registry — one row per unique license number.

    Weekly file from myfloridalicense.com replaces all data. Loader upserts on
    license_number, preserving email/phone/enrichment_status across syncs so
    BatchData enrichment work is not lost on each weekly refresh.

    Flow: download -> parse -> dedup by license_number -> filter to target ZIPs
          -> upsert -> enrich via BatchData -> email campaign -> signup.
    """
    __tablename__ = "dbpr_contacts"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    license_number: Mapped[str] = mapped_column(String(30), unique=True, nullable=False, index=True)
    license_type_code: Mapped[str] = mapped_column(String(10), nullable=False)
    license_type_desc: Mapped[Optional[str]] = mapped_column(String(60))
    full_name: Mapped[str] = mapped_column(String(200), nullable=False)
    address: Mapped[Optional[str]] = mapped_column(String(255))
    city: Mapped[Optional[str]] = mapped_column(String(100))
    state: Mapped[Optional[str]] = mapped_column(String(5), default="FL")
    zip_code: Mapped[Optional[str]] = mapped_column(String(10), index=True)
    county_id: Mapped[Optional[str]] = mapped_column(String(50), index=True)
    license_expiry: Mapped[Optional[date]] = mapped_column(Date)
    data_source: Mapped[str] = mapped_column(String(20), nullable=False, default="certified")

    vertical: Mapped[Optional[str]] = mapped_column(String(50), index=True)

    # Company name (DBA) — scraped per-license from myfloridalicense.com (ADR 0003).
    # Not in the bulk CSV extract. status: pending (not scraped) / found / none
    # (license has no DBA) / failed (mismatch or error, retried next run).
    company_name: Mapped[Optional[str]] = mapped_column(String(255))
    company_name_status: Mapped[str] = mapped_column(String(20), nullable=False, default="pending")
    company_name_scraped_at: Mapped[Optional[datetime]] = mapped_column(DateTime)

    enrichment_status: Mapped[str] = mapped_column(String(20), nullable=False, default="pending")
    enrichment_attempted_at: Mapped[Optional[datetime]] = mapped_column(DateTime)

    # Clay enrichment provenance (fa062)
    email_source: Mapped[Optional[str]] = mapped_column(String(20))  # clay|batchdata|raw
    email_verified: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    email_verified_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    clay_enriched_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))

    # Clay enrichment outputs — DDL already applied to DB; model synced here.
    # Feed the email-campaign body merge variables (website/linkedIn/email).
    work_email: Mapped[Optional[str]] = mapped_column(String(200))
    domain: Mapped[Optional[str]] = mapped_column(String(255))           # {{website}}
    linkedin_url: Mapped[Optional[str]] = mapped_column(String(255))     # {{linkedIn}} (personal)
    company_linkedin_url: Mapped[Optional[str]] = mapped_column(String(255))
    clay_synced: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    clay_synced_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))

    # Global suppression flags — contact-level, survive all campaign membership (fa062)
    is_opted_out: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, index=True)
    is_hard_bounced: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, index=True)
    is_signed_up: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, index=True)

    email_status: Mapped[str] = mapped_column(String(20), nullable=False, default="not_sent")
    email_sent_at: Mapped[Optional[datetime]] = mapped_column(DateTime)

    subscriber_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("subscribers.id"), index=True)
    signed_up_at: Mapped[Optional[datetime]] = mapped_column(DateTime)

    last_synced_at: Mapped[Optional[datetime]] = mapped_column(DateTime)

    # Clay CRM sync
    clay_synced: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    clay_synced_at: Mapped[Optional[datetime]] = mapped_column(DateTime)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc), nullable=False)

    __table_args__ = (
        CheckConstraint(
            "enrichment_status IN ('pending', 'enriched', 'failed', 'skipped')",
            name="check_dbpr_enrichment_status",
        ),
        CheckConstraint(
            "email_status IN ('not_sent', 'sent', 'bounced', 'signed_up', 'opted_out')",
            name="check_dbpr_email_status",
        ),
        CheckConstraint(
            "data_source IN ('certified', 'registered')",
            name="check_dbpr_data_source",
        ),
        CheckConstraint(
            "company_name_status IN ('pending', 'found', 'none', 'failed')",
            name="check_dbpr_company_name_status",
        ),
        Index("ix_dbpr_company_name_status", "company_name_status"),
        Index("idx_dbpr_county_vertical", "county_id", "vertical"),
        Index("idx_dbpr_enrichment_status", "enrichment_status"),
        Index("idx_dbpr_email_status", "email_status"),
        Index("idx_dbpr_last_synced", "last_synced_at"),
        Index("idx_dbpr_clay_synced", "clay_synced"),
    )

    def __repr__(self):
        return f"<DBPRContact(license={self.license_number}, name={self.full_name}, status={self.email_status})>"


# ============================================================================
# 8. COUNTY LAUNCH — EXPANSION CANDIDATES + AUDIT
# ============================================================================

class ExpansionCandidate(Base):
    """Queue of counties awaiting launch approval."""
    __tablename__ = "expansion_candidates"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    county_id: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    priority: Mapped[int] = mapped_column(Integer, nullable=False, default=100)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="queued")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
    last_slack_posted_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    last_slack_message_ts: Mapped[Optional[str]] = mapped_column(String(32))
    approved_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    approved_by_slack_user: Mapped[Optional[str]] = mapped_column(String(32))
    launched_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    waitlist_notified_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    revenue_pulse_sent_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        CheckConstraint(
            "status IN ('queued','approved','launching','launched','aborted','skipped')",
            name="ck_expansion_candidates_status",
        ),
        Index("ix_expansion_candidates_status_priority", "status", "priority"),
    )

    def __repr__(self) -> str:
        return f"<ExpansionCandidate(id={self.id}, county={self.county_id}, status={self.status})>"


class CountyLaunchAudit(Base):
    """Immutable audit log for every event in the county launch lifecycle."""
    __tablename__ = "county_launch_audit"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    county_id: Mapped[str] = mapped_column(String(64), nullable=False)
    event_type: Mapped[str] = mapped_column(String(32), nullable=False)
    actor: Mapped[str] = mapped_column(String(64), nullable=False)
    gate_snapshot: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    detail: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )

    __table_args__ = (
        CheckConstraint(
            "event_type IN ('evaluated','posted','approved','rejected','launch_started',"
            "'launch_aborted_gate_red','launched','cooldown_skipped',"
            "'waitlist_notified','revenue_pulse_sent')",
            name="ck_county_launch_audit_event",
        ),
        Index("ix_county_launch_audit_county_time", "county_id", "created_at"),
    )

    def __repr__(self) -> str:
        return f"<CountyLaunchAudit(id={self.id}, county={self.county_id}, event={self.event_type})>"


class ExpansionIcpChannel(Base):
    """
    Config-only record for each Expansion ICP Channel candidate.

    Distinct from a Trade (a vertical inside the contractor lead product).
    Launch gated on all 7 Expansion Gates green for the Source County AND
    global Contractor MRR >= $50K.  Status stays 'gated' until the meta-gate
    is cleared; only then can it be flipped to 'approved' / 'live'.

    feed_scope: 'single_county' = Source County only (v1);
                'multi_county'  = aggregate across launched counties (future).
    """
    __tablename__ = "expansion_icp_channels"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    key: Mapped[str] = mapped_column(String(40), nullable=False, unique=True)
    display_name: Mapped[str] = mapped_column(String(80), nullable=False)
    price_monthly: Mapped[Decimal] = mapped_column(Numeric(10, 2), nullable=False)
    persona: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    data_source: Mapped[Optional[str]] = mapped_column(String(120), nullable=True)
    feed_scope: Mapped[str] = mapped_column(String(20), nullable=False, default="single_county")
    landing_slug: Mapped[str] = mapped_column(String(60), nullable=False, unique=True)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="gated")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    __table_args__ = (
        CheckConstraint(
            "status IN ('configured','gated','approved','live','retired')",
            name="ck_expansion_icp_channels_status",
        ),
        CheckConstraint(
            "feed_scope IN ('single_county','multi_county')",
            name="ck_expansion_icp_channels_feed_scope",
        ),
    )

    def __repr__(self) -> str:
        return f"<ExpansionIcpChannel(key={self.key!r}, status={self.status!r})>"


# ============================================================================
# WAITLIST
# ============================================================================

class WaitlistEntry(Base):
    """
    One person's interest in a (zip_code, vertical, county_id) tuple.
    Replaces ZipTerritory.waitlist_emails array.
    waitlist_type='coming_soon' fires on county launch;
    waitlist_type='sold_out' fires on ZIP available transition.
    """
    __tablename__ = "waitlist_entries"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    zip_code: Mapped[str] = mapped_column(String(10), nullable=False)
    vertical: Mapped[str] = mapped_column(String(50), nullable=False)
    county_id: Mapped[str] = mapped_column(String(50), nullable=False)
    name: Mapped[str] = mapped_column(String(120), nullable=False)
    email: Mapped[str] = mapped_column(String(255), nullable=False)
    phone_e164: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    sms_opt_in: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")
    waitlist_type: Mapped[str] = mapped_column(String(20), nullable=False, default="sold_out")
    signup_ip: Mapped[Optional[str]] = mapped_column(String(45), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    notified_email_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    notified_sms_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    reactivation_decision_id: Mapped[Optional[str]] = mapped_column(String(36), nullable=True)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="waiting", server_default="waiting")

    __table_args__ = (
        UniqueConstraint("zip_code", "vertical", "county_id", "email",
                         name="uq_waitlist_zip_vert_county_email"),
        Index("ix_waitlist_county_status", "county_id", "status"),
        Index("ix_waitlist_county_type_status", "county_id", "waitlist_type", "status"),
        Index("ix_waitlist_zip_vertical", "zip_code", "vertical"),
        CheckConstraint(
            "status IN ('waiting','notified','converted','expired','opted_out','lost')",
            name="ck_waitlist_entries_status",
        ),
        CheckConstraint(
            "waitlist_type IN ('coming_soon','sold_out')",
            name="ck_waitlist_entries_type",
        ),
        CheckConstraint(
            "vertical IN ('roofing','restoration','public_adjusters',"
            "'wholesalers','fix_flip','attorneys')",
            name="ck_waitlist_entries_vertical",
        ),
    )

    def __repr__(self) -> str:
        return (f"<WaitlistEntry(id={self.id}, zip={self.zip_code}, "
                f"vertical={self.vertical}, type={self.waitlist_type}, "
                f"status={self.status})>")


class GoldPlusZipSnapshot(Base):
    """
    Nightly aggregation of new Gold+ lead counts per ZIP, refreshed after CDS scoring.
    Consumed by sold-out reactivation eligibility as a fast supply gate.
    """
    __tablename__ = "gold_plus_zip_snapshots"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    zip_code: Mapped[str] = mapped_column(String(10), nullable=False)
    county_id: Mapped[str] = mapped_column(String(50), nullable=False)
    snapshot_date: Mapped[date] = mapped_column(Date, nullable=False)
    gold_plus_lead_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    computed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        UniqueConstraint("zip_code", "county_id", "snapshot_date", name="uq_gpzs_zip_county_date"),
        Index("idx_gpzs_zip_county_date", "zip_code", "county_id", "snapshot_date"),
    )

    def __repr__(self) -> str:
        return (
            f"<GoldPlusZipSnapshot(zip={self.zip_code}, county={self.county_id}, "
            f"date={self.snapshot_date}, count={self.gold_plus_lead_count})>"
        )


# ============================================================================
# HCPA ENRICHMENT — TAX PAYMENT HISTORY
# ============================================================================

class TaxPaymentHistory(Base):
    """
    Annual tax payment records scraped from Hillsborough County Tax Collector.
    One property can have multiple rows (one per tax year / bill type).
    UniqueConstraint prevents duplicate ingestion on re-runs.
    """
    __tablename__ = "tax_payment_history"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    property_id: Mapped[int] = mapped_column(ForeignKey("properties.id"), nullable=False, index=True)

    tax_year: Mapped[int] = mapped_column(Integer, nullable=False)
    bill_type: Mapped[Optional[str]] = mapped_column(String(50))   # Annual / Homestead Penalty / Tangible Personal Property
    amount_paid: Mapped[Optional[float]] = mapped_column(Numeric(10, 2))
    payment_date: Mapped[Optional[date]] = mapped_column(Date)
    receipt_number: Mapped[Optional[str]] = mapped_column(String(50))
    days_late: Mapped[Optional[int]] = mapped_column(Integer)      # negative = paid early; 0 = on time; positive = late

    # Multi-county
    county_id: Mapped[str] = mapped_column(String(50), default='hillsborough', nullable=False)
    date_added: Mapped[Optional[date]] = mapped_column(Date, default=date.today)

    # Relationship
    property: Mapped["Property"] = relationship("Property", back_populates="tax_payment_history")

    __table_args__ = (
        UniqueConstraint("property_id", "tax_year", "bill_type", name="uq_tax_payment_property_year_type"),
        Index("idx_tax_payment_property_year", "property_id", "tax_year"),
        Index("idx_tax_payment_date", "payment_date"),
        Index("idx_tax_payment_bill_type", "bill_type"),
    )

    def __repr__(self) -> str:
        return f"<TaxPaymentHistory(id={self.id}, property_id={self.property_id}, year={self.tax_year}, paid={self.amount_paid})>"


# ---------------------------------------------------------------------------
# Concierge Chat (M5a)
# ---------------------------------------------------------------------------

class ChatSession(Base):
    """One row per conversation window. Anonymous sessions are keyed by anonymous_id;
    post-signup sessions link to a subscriber via subscriber_id."""
    __tablename__ = "chat_sessions"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)  # UUID
    subscriber_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("subscribers.id"), nullable=True, index=True
    )
    anonymous_id: Mapped[Optional[str]] = mapped_column(String(36), nullable=True, index=True)
    source: Mapped[str] = mapped_column(String(20), nullable=False, default="landing")
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=lambda: datetime.now(timezone.utc), nullable=False
    )
    last_seen_at: Mapped[datetime] = mapped_column(
        DateTime, default=lambda: datetime.now(timezone.utc), nullable=False
    )
    linked_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    messages: Mapped[List["ChatMessage"]] = relationship(
        "ChatMessage", back_populates="session", cascade="all, delete-orphan"
    )

    __table_args__ = (
        CheckConstraint(
            "source IN ('landing', 'dashboard', 'lead_feed')",
            name="check_chat_session_source",
        ),
        Index("idx_chat_session_subscriber", "subscriber_id", "created_at"),
        Index("idx_chat_session_anon", "anonymous_id", "created_at"),
    )

    def __repr__(self) -> str:
        return f"<ChatSession(id={self.id}, source={self.source})>"


class ChatMessage(Base):
    """One row per message turn (user or assistant)."""
    __tablename__ = "chat_messages"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    session_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("chat_sessions.id"), nullable=False, index=True
    )
    role: Mapped[str] = mapped_column(String(10), nullable=False)
    content: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    intent_label: Mapped[Optional[str]] = mapped_column(String(40), nullable=True, index=True)
    intent_confidence: Mapped[Optional[Decimal]] = mapped_column(Numeric(4, 3), nullable=True)
    tool_calls_json: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    payment_trigger_json: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    claude_model: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    tokens_in: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    tokens_out: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    latency_ms: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    error: Mapped[Optional[str]] = mapped_column(String(200), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=lambda: datetime.now(timezone.utc), nullable=False
    )

    session: Mapped["ChatSession"] = relationship("ChatSession", back_populates="messages")

    __table_args__ = (
        CheckConstraint(
            "role IN ('user', 'assistant', 'system', 'tool')",
            name="check_chat_message_role",
        ),
        Index("idx_chat_message_session_created", "session_id", "created_at"),
        Index("idx_chat_message_intent", "intent_label", "created_at"),
    )

    def __repr__(self) -> str:
        return f"<ChatMessage(id={self.id}, session_id={self.session_id}, role={self.role})>"


# ============================================================================
# SYNTHFLOW CALLS (fa048)
# ============================================================================

class SynthflowCall(Base):
    """One row per inbound post-call webhook from Synthflow / Finetuner.ai."""
    __tablename__ = "synthflow_calls"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    prospect_phone: Mapped[str] = mapped_column(String(20), nullable=False)
    outcome: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    vertical: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    zip_code: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)
    contact_id: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    call_id: Mapped[Optional[str]] = mapped_column(String(100), nullable=True, unique=True)
    transcript_text: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    recording_url: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    duration_seconds: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    call_date: Mapped[date] = mapped_column(Date, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )

    __table_args__ = (
        Index("idx_synthflow_calls_call_date", "call_date"),
        Index("idx_synthflow_calls_outcome", "outcome"),
        Index("idx_synthflow_calls_prospect_phone", "prospect_phone"),
    )

    def __repr__(self) -> str:
        return f"<SynthflowCall(id={self.id}, phone={self.prospect_phone}, outcome={self.outcome})>"


# ══════════════════════════════════════════════════════════════════════════════
# Operator CRM — Notes & Deal Pipeline Audit (fa045)
# ══════════════════════════════════════════════════════════════════════════════


class SubscriberNote(Base):
    """Free-form operator note on a subscriber. Any admin can edit/delete."""
    __tablename__ = "subscriber_notes"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    subscriber_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("subscribers.id"), nullable=False, index=True
    )
    author_email: Mapped[str] = mapped_column(String(255), nullable=False)
    body: Mapped[str] = mapped_column(Text, nullable=False)
    pinned: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false", nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=lambda: datetime.now(timezone.utc), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc), nullable=False,
    )

    __table_args__ = (
        Index("idx_subscriber_notes_sub_pinned", "subscriber_id", "pinned", "created_at"),
    )

    def __repr__(self) -> str:
        return f"<SubscriberNote(id={self.id}, sub={self.subscriber_id}, pinned={self.pinned})>"


class DealPipelineEvent(Base):
    """Audit row written every time a deal's pipeline_stage changes."""
    __tablename__ = "deal_pipeline_events"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    deal_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("deal_outcomes.id"), nullable=False, index=True
    )
    from_stage: Mapped[Optional[str]] = mapped_column(String(30))
    to_stage: Mapped[str] = mapped_column(String(30), nullable=False)
    changed_by: Mapped[str] = mapped_column(String(255), nullable=False)
    note: Mapped[Optional[str]] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=lambda: datetime.now(timezone.utc), nullable=False
    )

    __table_args__ = (
        Index("idx_deal_pipeline_events_deal_created", "deal_id", "created_at"),
    )

    def __repr__(self) -> str:
        return f"<DealPipelineEvent(deal={self.deal_id}, {self.from_stage}->{self.to_stage})>"


# ══════════════════════════════════════════════════════════════════════════════
# Stage 10: 3-Variant A/B + Pricing Cohorts (fa055)
# ══════════════════════════════════════════════════════════════════════════════


class MessageVariantTest(Base):
    """3-slot (a/b/c) variant test per named message sequence (fa055).

    Tracks send counts, conversion counts, slot retirement state, and the
    proving-cycle pointer for the replacement variant. All runtime reads/
    writes go through raw SQL in variant_engine.py — this declaration keeps
    Alembic autogenerate consistent with the live schema.
    """
    __tablename__ = "message_variant_tests"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    sequence_name: Mapped[str] = mapped_column(String(100), nullable=False, unique=True)
    segment: Mapped[Optional[str]] = mapped_column(String(50))
    traffic_pct: Mapped[int] = mapped_column(Integer, nullable=False, default=10)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="active")

    slot_a_body: Mapped[str] = mapped_column(Text, nullable=False)
    slot_a_sends: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    slot_a_conversions: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    slot_a_replies: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    slot_a_status: Mapped[str] = mapped_column(String(20), nullable=False, default="active")
    slot_a_retired_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))

    slot_b_body: Mapped[str] = mapped_column(Text, nullable=False)
    slot_b_sends: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    slot_b_conversions: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    slot_b_replies: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    slot_b_status: Mapped[str] = mapped_column(String(20), nullable=False, default="active")
    slot_b_retired_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))

    slot_c_body: Mapped[str] = mapped_column(Text, nullable=False)
    slot_c_sends: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    slot_c_conversions: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    slot_c_replies: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    slot_c_status: Mapped[str] = mapped_column(String(20), nullable=False, default="active")
    slot_c_retired_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))

    proving_slot: Mapped[Optional[str]] = mapped_column(String(5))
    proving_baseline_conv_rate: Mapped[Optional[float]] = mapped_column(Numeric(8, 6))
    proving_started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )

    __table_args__ = (
        CheckConstraint("status IN ('active','paused','completed')", name="check_mvt_status"),
        CheckConstraint("traffic_pct BETWEEN 1 AND 10", name="check_mvt_traffic_cap"),
        CheckConstraint(
            "slot_a_status IN ('active','retired') AND "
            "slot_b_status IN ('active','retired') AND "
            "slot_c_status IN ('active','retired')",
            name="check_mvt_slot_statuses",
        ),
        CheckConstraint(
            "proving_slot IS NULL OR proving_slot IN ('a','b','c')",
            name="check_mvt_proving_slot",
        ),
        Index("idx_mvt_status", "status"),
        Index("idx_mvt_sequence_name", "sequence_name"),
    )

    def __repr__(self) -> str:
        return f"<MessageVariantTest(seq={self.sequence_name}, status={self.status})>"


class VariantRetirementLog(Base):
    """Idempotent audit record for every slot retirement, replacement, or reversion (fa055).

    idempotency_key UNIQUE ensures retries never produce duplicate rows.
    """
    __tablename__ = "variant_retirement_log"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    test_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("message_variant_tests.id", ondelete="CASCADE"), nullable=False
    )
    action: Mapped[str] = mapped_column(String(30), nullable=False)
    slot: Mapped[str] = mapped_column(String(5), nullable=False)
    old_body: Mapped[Optional[str]] = mapped_column(Text)
    new_body: Mapped[Optional[str]] = mapped_column(Text)
    old_conversion_rate: Mapped[Optional[float]] = mapped_column(Numeric(8, 6))
    new_conversion_rate: Mapped[Optional[float]] = mapped_column(Numeric(8, 6))
    reason: Mapped[Optional[str]] = mapped_column(Text)
    idempotency_key: Mapped[str] = mapped_column(String(120), nullable=False, unique=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )

    __table_args__ = (
        CheckConstraint(
            "action IN ('retired','replaced','reverted','promoted','rollback')",
            name="check_vrl_action",
        ),
        CheckConstraint("slot IN ('a','b','c')", name="check_vrl_slot"),
        Index("idx_vrl_test_id", "test_id"),
        Index("idx_vrl_idempotency_key", "idempotency_key"),
    )

    def __repr__(self) -> str:
        return f"<VariantRetirementLog(test={self.test_id}, slot={self.slot}, action={self.action})>"


class PricingCohort(Base):
    """Per-trade, per-county pricing override (fa055).

    Activates only after >= 6 weeks of deal data and within guardrail bounds.
    At most one active row per (county_id, trade_vertical, price_type) tuple,
    enforced by partial unique index idx_pc_active_unique.
    """
    __tablename__ = "pricing_cohorts"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    county_id: Mapped[str] = mapped_column(String(50), nullable=False)
    trade_vertical: Mapped[str] = mapped_column(String(50), nullable=False)
    price_type: Mapped[str] = mapped_column(String(30), nullable=False)
    base_price_cents: Mapped[int] = mapped_column(Integer, nullable=False)
    adjusted_price_cents: Mapped[int] = mapped_column(Integer, nullable=False)
    adjustment_pct: Mapped[float] = mapped_column(Numeric(6, 2), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="pending")
    activation_reason: Mapped[Optional[str]] = mapped_column(Text)
    rollback_reason: Mapped[Optional[str]] = mapped_column(Text)
    deal_weeks: Mapped[Optional[int]] = mapped_column(Integer)
    deal_count: Mapped[Optional[int]] = mapped_column(Integer)
    activated_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    rolled_back_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )

    __table_args__ = (
        CheckConstraint("status IN ('pending','active','rolled_back')", name="check_pc_status"),
        CheckConstraint("adjustment_pct BETWEEN -25 AND 25", name="check_pc_adjustment_bounds"),
        Index("idx_pc_county_vertical_type", "county_id", "trade_vertical", "price_type"),
        Index("idx_pc_status", "status"),
    )

    def __repr__(self) -> str:
        return (
            f"<PricingCohort(county={self.county_id}, vertical={self.trade_vertical}, "
            f"type={self.price_type}, adj={self.adjustment_pct}%, status={self.status})>"
        )


# ============================================================================
# STAGE 12 — WHITE-LABEL TIER (fa056)
# ============================================================================

class WhiteLabelClient(Base):
    """
    A B2B company account paying $2,500/mo (standard) or $5,000/mo (premium)
    for branded access to Forced Action distress property intelligence.
    Completely separate from the solo-operator Subscriber model.
    """
    __tablename__ = "white_label_clients"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    # Identity
    company_name: Mapped[str] = mapped_column(String(255), nullable=False)
    company_slug: Mapped[str] = mapped_column(String(100), unique=True, nullable=False, index=True)
    display_name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    admin_email: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    admin_name: Mapped[str] = mapped_column(String(255), nullable=False)

    # Lifecycle
    status: Mapped[str] = mapped_column(String(30), nullable=False, default="pending_verification", index=True)

    # Plan the client selected during signup (intent only — NOT a paid plan).
    # Used to pre-select the Subscribe option in Billing. plan_tier stays NULL
    # until a Stripe subscription is actually created.
    intended_plan_tier: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)

    # Stripe billing
    stripe_customer_id: Mapped[Optional[str]] = mapped_column(String(100), unique=True, nullable=True, index=True)
    stripe_subscription_id: Mapped[Optional[str]] = mapped_column(String(100), unique=True, nullable=True)
    plan_tier: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    plan_price_cents: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    trial_ends_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    # Branding
    logo_url: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    primary_color: Mapped[Optional[str]] = mapped_column(String(7), nullable=True)   # e.g. "#fbbf24"
    secondary_color: Mapped[Optional[str]] = mapped_column(String(7), nullable=True)

    # Data access scope
    counties_enabled: Mapped[Optional[list]] = mapped_column(JSONB, nullable=True)   # ["hillsborough","pinellas"]
    verticals_enabled: Mapped[Optional[list]] = mapped_column(JSONB, nullable=True)  # ["roofing","wholesalers"]
    api_enabled: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    api_requests_per_day: Mapped[int] = mapped_column(Integer, default=10000, nullable=False)

    # Timestamps
    verified_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    activated_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    churned_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )

    # Relationships
    users: Mapped[List["WhiteLabelUser"]] = relationship(
        "WhiteLabelUser", back_populates="client", cascade="all, delete-orphan"
    )
    api_keys: Mapped[List["WhiteLabelApiKey"]] = relationship(
        "WhiteLabelApiKey", back_populates="client", cascade="all, delete-orphan"
    )
    contractor_enrichments: Mapped[List["WhiteLabelContractorEnrichment"]] = relationship(
        "WhiteLabelContractorEnrichment", back_populates="client", cascade="all, delete-orphan"
    )

    __table_args__ = (
        CheckConstraint(
            "status IN ('pending_verification','active','suspended','churned')",
            name="check_wl_client_status",
        ),
        CheckConstraint(
            "plan_tier IS NULL OR plan_tier IN ('standard','premium')",
            name="check_wl_client_plan_tier",
        ),
    )

    def __repr__(self) -> str:
        return f"<WhiteLabelClient(id={self.id}, slug={self.company_slug}, status={self.status})>"


class WhiteLabelUser(Base):
    """
    A team member belonging to a WhiteLabelClient.
    First user (role=admin) is created at signup; others are invited by admins.
    """
    __tablename__ = "white_label_users"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    client_id: Mapped[int] = mapped_column(ForeignKey("white_label_clients.id"), nullable=False, index=True)

    # Auth
    email: Mapped[str] = mapped_column(String(255), unique=True, nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    role: Mapped[str] = mapped_column(String(20), nullable=False, default="member")
    password_hash: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)

    # State
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    email_verified_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    # Password reset
    reset_token: Mapped[Optional[str]] = mapped_column(String(64), nullable=True, index=True)
    reset_token_expires_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    # Invite provenance
    invited_by_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("white_label_users.id"), nullable=True
    )

    # Audit
    last_login_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )

    # Relationships
    client: Mapped["WhiteLabelClient"] = relationship("WhiteLabelClient", back_populates="users")
    api_keys_created: Mapped[List["WhiteLabelApiKey"]] = relationship(
        "WhiteLabelApiKey", back_populates="created_by_user", foreign_keys="WhiteLabelApiKey.created_by"
    )

    __table_args__ = (
        CheckConstraint("role IN ('admin','member')", name="check_wl_user_role"),
        Index("idx_wl_user_client_email", "client_id", "email"),
    )

    def __repr__(self) -> str:
        return f"<WhiteLabelUser(id={self.id}, email={self.email}, role={self.role})>"


class WhiteLabelApiKey(Base):
    """
    API key for programmatic access to /api/wl/data/* endpoints.
    Full key shown once at creation; only SHA-256 hash + 8-char prefix stored.
    Key format: fa_wl_{32 hex chars}
    """
    __tablename__ = "white_label_api_keys"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    client_id: Mapped[int] = mapped_column(ForeignKey("white_label_clients.id"), nullable=False, index=True)

    key_prefix: Mapped[str] = mapped_column(String(12), nullable=False, index=True)  # first 12 chars for display
    key_hash: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)   # SHA-256 hex of full key
    label: Mapped[str] = mapped_column(String(100), nullable=False, default="Default")

    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    created_by: Mapped[Optional[int]] = mapped_column(ForeignKey("white_label_users.id"), nullable=True)

    # Usage tracking
    requests_today: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    total_requests: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    last_used_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    # Lifecycle
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )
    revoked_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    # Relationships
    client: Mapped["WhiteLabelClient"] = relationship("WhiteLabelClient", back_populates="api_keys")
    created_by_user: Mapped[Optional["WhiteLabelUser"]] = relationship(
        "WhiteLabelUser", back_populates="api_keys_created", foreign_keys=[created_by]
    )

    __table_args__ = (
        Index("idx_wl_api_key_client_active", "client_id", "is_active"),
    )

    def __repr__(self) -> str:
        return f"<WhiteLabelApiKey(id={self.id}, prefix={self.key_prefix}, active={self.is_active})>"


class WhiteLabelContractorEnrichment(Base):
    """
    Clay-enriched contractor data cache per (client, county, vertical).
    Refreshed automatically when data is older than 7 days.
    """
    __tablename__ = "white_label_contractor_enrichments"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    client_id: Mapped[int] = mapped_column(ForeignKey("white_label_clients.id"), nullable=False, index=True)

    county_id: Mapped[str] = mapped_column(String(50), nullable=False)
    vertical: Mapped[str] = mapped_column(String(50), nullable=False)

    clay_run_id: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    data: Mapped[Optional[list]] = mapped_column(JSONB, nullable=True)  # list of contractor dicts
    enriched_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )

    # Relationship
    client: Mapped["WhiteLabelClient"] = relationship(
        "WhiteLabelClient", back_populates="contractor_enrichments"
    )

    __table_args__ = (
        UniqueConstraint("client_id", "county_id", "vertical", name="uq_wl_contractor_enrichment"),
        Index("idx_wl_enrichment_client_county", "client_id", "county_id"),
    )


# ══════════════════════════════════════════════════════════════════════════════
# Stage 12: Bankruptcy Filing Alert Product (fa059)
# ══════════════════════════════════════════════════════════════════════════════


class BankruptcyFiling(Base):
    """One row per unique CourtListener bankruptcy docket (fa059).

    case_number is the dedup key for ingestion. Stand-alone from the
    property hub-and-spoke — this is product data for the alert subscription,
    not a distress signal. All runtime I/O uses raw SQL via sa_text.
    """
    __tablename__ = "bankruptcy_filings"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    case_number: Mapped[str] = mapped_column(String(60), nullable=False, unique=True)
    chapter: Mapped[Optional[str]] = mapped_column(String(4))
    court: Mapped[str] = mapped_column(String(20), nullable=False)
    jurisdiction: Mapped[str] = mapped_column(String(40), nullable=False)
    filer: Mapped[Optional[str]] = mapped_column(String(255))
    trustee: Mapped[Optional[str]] = mapped_column(String(255))
    date_filed: Mapped[Optional[date]] = mapped_column(Date)
    docket_id: Mapped[Optional[str]] = mapped_column(String(40))
    nature_of_suit: Mapped[Optional[str]] = mapped_column(String(120))
    raw: Mapped[Optional[dict]] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )

    __table_args__ = (
        Index("idx_bkfiling_date_filed", "date_filed"),
        Index("idx_bkfiling_jurisdiction_chapter", "jurisdiction", "chapter"),
        Index("idx_bkfiling_created_at", "created_at"),
    )

    def __repr__(self) -> str:
        return f"<BankruptcyFiling(case={self.case_number}, ch={self.chapter}, juris={self.jurisdiction})>"


class BankruptcyAlertSubscription(Base):
    """Standalone $297/mo subscriber for the Bankruptcy Filing Alert product (fa059).

    Separate from the property `subscribers` table — these are attorneys,
    investors, and lenders with no ZIP territory or vertical. access_token
    (uuid) authenticates the subscriber-facing status endpoint.
    """
    __tablename__ = "bankruptcy_alert_subscriptions"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    email: Mapped[str] = mapped_column(String(255), nullable=False)
    phone: Mapped[Optional[str]] = mapped_column(String(20))
    name: Mapped[Optional[str]] = mapped_column(String(255))
    stripe_customer_id: Mapped[Optional[str]] = mapped_column(String(100), unique=True)
    stripe_subscription_id: Mapped[Optional[str]] = mapped_column(String(100), unique=True)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="trialing")
    jurisdictions: Mapped[Optional[list]] = mapped_column(JSONB)   # NULL = all
    chapters: Mapped[Optional[list]] = mapped_column(JSONB)        # NULL = all
    channel_email: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    channel_sms: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    trial_ends_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    access_token: Mapped[str] = mapped_column(String(36), nullable=False, unique=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )
    canceled_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))

    __table_args__ = (
        CheckConstraint(
            "status IN ('trialing','active','past_due','canceled')",
            name="check_bkalert_sub_status",
        ),
        Index("idx_bkalert_sub_status", "status"),
        Index("idx_bkalert_sub_email", "email"),
    )

    def __repr__(self) -> str:
        return f"<BankruptcyAlertSubscription(id={self.id}, email={self.email}, status={self.status})>"


class BankruptcyFilingAlert(Base):
    """Dedup + audit log for bankruptcy filing alerts (fa059).

    UNIQUE(subscription_id, filing_id, channel) guarantees a subscriber is
    never alerted twice for the same filing on the same channel.
    """
    __tablename__ = "bankruptcy_filing_alerts"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    subscription_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("bankruptcy_alert_subscriptions.id", ondelete="CASCADE"), nullable=False
    )
    filing_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("bankruptcy_filings.id", ondelete="CASCADE"), nullable=False
    )
    channel: Mapped[str] = mapped_column(String(10), nullable=False)
    status: Mapped[str] = mapped_column(String(12), nullable=False)
    error: Mapped[Optional[str]] = mapped_column(Text)
    sent_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )

    __table_args__ = (
        UniqueConstraint("subscription_id", "filing_id", "channel", name="uq_bkfiling_alert_dedup"),
        CheckConstraint("channel IN ('email','sms')", name="check_bkalert_channel"),
        CheckConstraint("status IN ('sent','failed','suppressed')", name="check_bkalert_status"),
        Index("idx_bkfiling_alert_sent_at", "sent_at"),
        Index("idx_bkfiling_alert_subscription", "subscription_id"),
    )

    def __repr__(self) -> str:
        return f"<BankruptcyFilingAlert(sub={self.subscription_id}, filing={self.filing_id}, {self.channel}={self.status})>"


# ══════════════════════════════════════════════════════════════════════════════
# Stage 12+ — ICP Channel Management (fa066)
# ══════════════════════════════════════════════════════════════════════════════


class IcpDailyStats(Base):
    """Per-ICP-channel daily raw metric counts (fa066).

    Percentages (first_payment_rate, saved_card_rate, etc.) are computed
    at read time from raw counts so kill-switch scores are fully auditable.
    Attribution is on icp_channel_key, NOT verticals (verticals overlap ICPs).
    """
    __tablename__ = "icp_daily_stats"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    run_date: Mapped[date] = mapped_column(Date, nullable=False)
    county_id: Mapped[str] = mapped_column(String(50), nullable=False)
    icp_channel_key: Mapped[str] = mapped_column(String(40), nullable=False)

    signup_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    payer_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    saved_card_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    sms_sent_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    sms_reply_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    active_subscriber_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    cancel_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    refund_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    mrr_cents: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )

    __table_args__ = (
        UniqueConstraint(
            "run_date", "county_id", "icp_channel_key",
            name="uq_icp_daily_stats_date_county_channel",
        ),
        Index("idx_icp_daily_stats_channel_date", "icp_channel_key", "run_date"),
    )

    def __repr__(self) -> str:
        return f"<IcpDailyStats(channel={self.icp_channel_key}, date={self.run_date}, mrr={self.mrr_cents})>"


class IcpChannelLaunchAudit(Base):
    """Immutable event log for every ICP channel status transition (fa066).

    Force activations require a reason and are flagged with is_force_activate=True.
    gate_snapshot stores the full gate evaluation at the time of the action.
    """
    __tablename__ = "icp_channel_launch_audit"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    channel_key: Mapped[str] = mapped_column(String(40), nullable=False)
    event_type: Mapped[str] = mapped_column(String(32), nullable=False)
    actor: Mapped[str] = mapped_column(String(100), nullable=False)
    is_force_activate: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    force_reason: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    gate_snapshot: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    prev_status: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    new_status: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    detail: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )

    __table_args__ = (
        CheckConstraint(
            "event_type IN ("
            "'activated','paused','killed','force_activated',"
            "'config_updated','gate_evaluated','created'"
            ")",
            name="ck_icp_audit_event_type",
        ),
        Index("idx_icp_audit_channel_created", "channel_key", "created_at"),
    )

    def __repr__(self) -> str:
        return f"<IcpChannelLaunchAudit(channel={self.channel_key}, event={self.event_type})>"


# ══════════════════════════════════════════════════════════════════════════════
# Supplier Intelligence Foundation (fa067)
# ══════════════════════════════════════════════════════════════════════════════


class SupplierAccount(Base):
    """One row per supplier company (fa067).

    Phase 1 foundation — admin-provisioned, no self-signup flow.
    access_token is the UUID passed to the supplier for dashboard access.
    """
    __tablename__ = "supplier_accounts"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    company_name: Mapped[str] = mapped_column(String(255), nullable=False)
    contact_name: Mapped[Optional[str]] = mapped_column(String(255))
    contact_email: Mapped[str] = mapped_column(String(255), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="active")
    counties: Mapped[Optional[list]] = mapped_column(JSONB)   # list of county_id strings
    verticals: Mapped[Optional[list]] = mapped_column(JSONB)  # list of vertical codes
    access_token: Mapped[str] = mapped_column(String(36), nullable=False, unique=True)
    stripe_customer_id: Mapped[Optional[str]] = mapped_column(String(100), unique=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )

    __table_args__ = (
        CheckConstraint("status IN ('active','suspended','canceled')", name="ck_supplier_accounts_status"),
        Index("idx_supplier_accounts_email", "contact_email"),
        Index("idx_supplier_accounts_status", "status"),
    )

    def __repr__(self) -> str:
        return f"<SupplierAccount(id={self.id}, company={self.company_name}, status={self.status})>"


class SupplierSubscription(Base):
    """Stripe subscription for a supplier account (fa067)."""
    __tablename__ = "supplier_subscriptions"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    account_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("supplier_accounts.id", ondelete="CASCADE"), nullable=False
    )
    plan_tier: Mapped[str] = mapped_column(String(20), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="trialing")
    stripe_subscription_id: Mapped[Optional[str]] = mapped_column(String(100), unique=True)
    stripe_price_id: Mapped[Optional[str]] = mapped_column(String(100))
    price_cents: Mapped[Optional[int]] = mapped_column(Integer)
    trial_ends_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    canceled_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )

    __table_args__ = (
        CheckConstraint(
            "status IN ('trialing','active','past_due','canceled')",
            name="ck_supplier_subscriptions_status",
        ),
        CheckConstraint(
            "plan_tier IN ('foundation','standard','premium')",
            name="ck_supplier_subscriptions_tier",
        ),
        Index("idx_supplier_subscriptions_account", "account_id"),
        Index("idx_supplier_subscriptions_status", "status"),
    )

    def __repr__(self) -> str:
        return f"<SupplierSubscription(account={self.account_id}, tier={self.plan_tier}, status={self.status})>"


# ============================================================================
# EMAIL CAMPAIGNS (fa062)
# ============================================================================

class EmailSequenceTemplate(Base):
    """
    FA-side reusable email sequence template.
    Authored once; campaigns reference it. Steps are expanded into an
    Instantly sequence via the API at campaign creation time.

    steps: [{step_number, delay_days, subject, body}]
    variables_used: ['{firstName}', '{company}', ...]  — validated whitelist
    """
    __tablename__ = "email_sequence_templates"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    steps: Mapped[list] = mapped_column(JSONB, nullable=False, default=list)
    variables_used: Mapped[list] = mapped_column(JSONB, nullable=False, default=list)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc)
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    def __repr__(self) -> str:
        return f"<EmailSequenceTemplate(id={self.id}, name='{self.name}', steps={len(self.steps or [])})>"


class EmailCampaign(Base):
    """
    One FA campaign = one Instantly campaign (1:1).

    geo_filter: {county_id: str, zips: [str]}
    send_schedule: Instantly campaign_schedule payload
        {schedules: [{name, timing:{from,to}, days:{}, timezone}],
         start_date, end_date}
    status: draft → active → paused / completed
    """
    __tablename__ = "email_campaigns"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    instantly_campaign_id: Mapped[Optional[str]] = mapped_column(String(100), unique=True)
    template_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("email_sequence_templates.id", ondelete="RESTRICT")
    )
    county_id: Mapped[Optional[str]] = mapped_column(String(50), index=True)
    geo_filter: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    vertical: Mapped[Optional[str]] = mapped_column(String(50), index=True)
    max_contacts: Mapped[Optional[int]] = mapped_column(Integer)
    start_date: Mapped[Optional[date]] = mapped_column(Date)
    end_date: Mapped[Optional[date]] = mapped_column(Date)
    send_schedule: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    # Instantly-only campaign knobs not modeled as columns (fa063): daily_limit,
    # daily_max_leads, email_list, stop_on_reply, open_tracking, link_tracking.
    # Persisted here AND PATCHed to Instantly on edit.
    instantly_settings: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    status: Mapped[str] = mapped_column(String(12), nullable=False, default="draft", index=True)
    last_synced_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc)
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    __table_args__ = (
        CheckConstraint(
            "status IN ('draft','active','paused','completed')",
            name="check_campaign_status",
        ),
        Index("idx_email_campaign_status", "status"),
        Index("idx_email_campaign_county", "county_id"),
        Index("idx_email_campaign_vertical", "vertical"),
    )

    def __repr__(self) -> str:
        return f"<EmailCampaign(id={self.id}, name='{self.name}', status='{self.status}')>"


class SupplierReport(Base):
    """Generated intelligence report for a supplier account (fa067).

    sections_json stores per-section data or {"status": "insufficient_data", ...}.
    data_readiness_snapshot records threshold vs actual counts at generation time.
    """
    __tablename__ = "supplier_reports"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    account_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("supplier_accounts.id", ondelete="CASCADE"), nullable=False
    )
    county_id: Mapped[str] = mapped_column(String(50), nullable=False)
    generated_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    report_period_start: Mapped[Optional[date]] = mapped_column(Date)
    report_period_end: Mapped[Optional[date]] = mapped_column(Date)
    sections_json: Mapped[Optional[dict]] = mapped_column(JSONB)
    data_readiness_snapshot: Mapped[Optional[dict]] = mapped_column(JSONB)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="pending")
    error_message: Mapped[Optional[str]] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )

    __table_args__ = (
        CheckConstraint(
            "status IN ('pending','generated','failed','exported')",
            name="ck_supplier_reports_status",
        ),
        Index("idx_supplier_reports_account_date", "account_id", "created_at"),
        Index("idx_supplier_reports_status", "status"),
    )

    def __repr__(self) -> str:
        return f"<SupplierReport(id={self.id}, account={self.account_id}, status={self.status})>"


class SupplierReportExport(Base):
    """PDF or CSV export of a supplier report (fa067)."""
    __tablename__ = "supplier_report_exports"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    report_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("supplier_reports.id", ondelete="CASCADE"), nullable=False
    )
    format: Mapped[str] = mapped_column(String(10), nullable=False)
    file_path: Mapped[Optional[str]] = mapped_column(Text)
    exported_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    emailed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )

    __table_args__ = (
        CheckConstraint("format IN ('pdf','csv')", name="ck_supplier_report_exports_format"),
        Index("idx_supplier_report_exports_report", "report_id"),
    )

    def __repr__(self) -> str:
        return f"<SupplierReportExport(report={self.report_id}, format={self.format})>"


class CampaignContact(Base):
    """
    M:N junction — one contractor in one campaign.
    Concurrent active memberships across DIFFERENT campaigns are allowed.
    UNIQUE(campaign_id, dbpr_contact_id) prevents duplicate within same campaign.

    engagement_status: Instantly lead status (per-campaign, not global).
    Global suppression (is_opted_out, is_hard_bounced, is_signed_up) lives on DBPRContact.
    """
    __tablename__ = "campaign_contacts"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    campaign_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("email_campaigns.id", ondelete="CASCADE"), nullable=False
    )
    dbpr_contact_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("dbpr_contacts.id", ondelete="CASCADE"), nullable=False, index=True
    )
    instantly_lead_id: Mapped[Optional[str]] = mapped_column(String(100))
    engagement_status: Mapped[str] = mapped_column(String(20), nullable=False, default="active")
    added_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc)
    )
    last_activity_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    converted_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))

    __table_args__ = (
        UniqueConstraint("campaign_id", "dbpr_contact_id", name="uq_campaign_contact"),
        CheckConstraint(
            "engagement_status IN "
            "('active','completed','bounced','unsubscribed','interested','not_interested')",
            name="check_engagement_status",
        ),
        Index("idx_cc_campaign_status", "campaign_id", "engagement_status"),
    )

    def __repr__(self) -> str:
        return (
            f"<CampaignContact(campaign={self.campaign_id}, "
            f"contact={self.dbpr_contact_id}, status='{self.engagement_status}')>"
        )


class CampaignDailyAnalytics(Base):
    """
    Daily analytics snapshot per campaign — one row per (campaign_id, date).
    Pulled from Instantly once/day; enables "last 30 days" without live API calls.
    """
    __tablename__ = "campaign_daily_analytics"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    campaign_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("email_campaigns.id", ondelete="CASCADE"), nullable=False
    )
    snapshot_date: Mapped[date] = mapped_column(Date, nullable=False)
    total_contacts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    emails_sent: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    opens: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    open_rate: Mapped[Decimal] = mapped_column(Numeric(6, 4), nullable=False, default=0)
    replies: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    reply_rate: Mapped[Decimal] = mapped_column(Numeric(6, 4), nullable=False, default=0)
    clicks: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    bounces: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    unsubscribes: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    interested: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc)
    )

    __table_args__ = (
        UniqueConstraint("campaign_id", "snapshot_date", name="uq_campaign_snapshot"),
        Index("idx_cda_campaign_date", "campaign_id", "snapshot_date"),
    )

    def __repr__(self) -> str:
        return (
            f"<CampaignDailyAnalytics(campaign={self.campaign_id}, "
            f"date={self.snapshot_date}, open_rate={self.open_rate})>"
        )


# ============================================================================
# FINANCING INTENT SCORING (Sprint S1)
# ============================================================================

class FinancingIntentScore(Base):
    """Per-property daily financing-intent score from the S1 scoring engine.

    One row per (property_id, score_date). UPSERT on conflict.
    """

    __tablename__ = "financing_intent_scores"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    property_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("properties.id"), nullable=False
    )
    county_id: Mapped[Optional[str]] = mapped_column(String(50))
    score_date: Mapped[date] = mapped_column(Date, nullable=False)
    financing_intent_score: Mapped[Decimal] = mapped_column(Numeric(5, 2), nullable=False)
    intent_tier: Mapped[str] = mapped_column(String(20), nullable=False)
    recommended_product: Mapped[Optional[str]] = mapped_column(String(50))
    signal_flags: Mapped[dict] = mapped_column(
        JSONB, nullable=False, server_default="{}"
    )
    signal_scores: Mapped[dict] = mapped_column(
        JSONB, nullable=False, server_default="{}"
    )
    signal_details: Mapped[dict] = mapped_column(
        JSONB, nullable=False, server_default="{}"
    )
    source_ids: Mapped[dict] = mapped_column(
        JSONB, nullable=False, server_default="{}"
    )
    excluded_reasons: Mapped[dict] = mapped_column(
        JSONB, nullable=False, server_default="{}"
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        UniqueConstraint("property_id", "score_date", name="uq_fis_property_date"),
        CheckConstraint(
            "intent_tier IN ('high', 'medium', 'low')",
            name="ck_fis_intent_tier",
        ),
        Index("idx_fis_property_id", "property_id"),
        Index("idx_fis_score_date", "score_date"),
        Index("idx_fis_intent_tier", "intent_tier"),
    )

    def __repr__(self) -> str:
        return (
            f"<FinancingIntentScore(property_id={self.property_id}, "
            f"date={self.score_date}, tier='{self.intent_tier}', "
            f"score={self.financing_intent_score})>"
        )


# ============================================================================
# Closer Cockpit (Sprint S1b) — Aircall call capture + tagging
# ============================================================================

class CloserCall(Base):
    """
    One Aircall call from a human closer to a subscriber (Sprint S1b).

    Holds the transcript, AI-derived tags (sentiment/topics from Aircall AI
    Assist; objections/outcome/resolution/follow-ups from Claude via
    claude_router.call_claude), and the closer's per-call one-tap feedback.

    Deliberately separate from `agent_decisions` (which is Cora-only): a closer
    call is a human action, not a Cora Touch. See ADR
    "closer-telemetry-separate-from-agent-decisions".
    """
    __tablename__ = "closer_calls"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    # Identity & correlation
    aircall_call_id: Mapped[str] = mapped_column(String(40), nullable=False, unique=True, index=True)
    subscriber_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("subscribers.id"), nullable=False, index=True
    )
    escalation_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("human_close_escalations.id"), nullable=True
    )
    closer_aircall_user_id: Mapped[Optional[str]] = mapped_column(String(40))
    closer_name: Mapped[Optional[str]] = mapped_column(String(120))

    # Call facts (from call.ended)
    direction: Mapped[Optional[str]] = mapped_column(String(12))
    dialed_e164: Mapped[Optional[str]] = mapped_column(String(20))
    duration_sec: Mapped[Optional[int]] = mapped_column(Integer)
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    ended_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))

    # Transcript (from transcription.created)
    transcript_text: Mapped[Optional[str]] = mapped_column(Text)
    transcript_fetched_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))

    # AI-derived tags (Aircall: sentiment/topics — Claude: the rest)
    sentiment: Mapped[Optional[str]] = mapped_column(String(12))
    topics: Mapped[Optional[list]] = mapped_column(JSONB)
    objections: Mapped[Optional[list]] = mapped_column(JSONB)
    objection_resolved: Mapped[Optional[str]] = mapped_column(String(12))
    call_outcome: Mapped[Optional[str]] = mapped_column(String(30))
    follow_ups: Mapped[Optional[list]] = mapped_column(JSONB)
    tagged_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))

    # Human one-tap feedback (per call)
    objection_type: Mapped[Optional[str]] = mapped_column(String(40))
    pitch_variant: Mapped[Optional[str]] = mapped_column(String(40))
    lead_quality_rating: Mapped[Optional[int]] = mapped_column(Integer)
    feedback_by: Mapped[Optional[str]] = mapped_column(String(120))
    feedback_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    __table_args__ = (
        Index("idx_closer_calls_subscriber", "subscriber_id"),
        Index("idx_closer_calls_closer_started", "closer_aircall_user_id", "started_at"),
        Index("idx_closer_calls_tagged_at", "tagged_at"),
        CheckConstraint(
            "lead_quality_rating IS NULL OR (lead_quality_rating BETWEEN 1 AND 5)",
            name="ck_closer_calls_lead_quality",
        ),
        CheckConstraint(
            "call_outcome IS NULL OR call_outcome IN "
            "('committed','callback_scheduled','undecided','declined','no_meaningful_conversation')",
            name="ck_closer_calls_outcome",
        ),
        CheckConstraint(
            "objection_resolved IS NULL OR objection_resolved IN ('resolved','unresolved','none')",
            name="ck_closer_calls_obj_resolved",
        ),
        CheckConstraint(
            "sentiment IS NULL OR sentiment IN ('positive','neutral','negative','mixed')",
            name="ck_closer_calls_sentiment",
        ),
    )

    def __repr__(self) -> str:
        return (
            f"<CloserCall(id={self.id}, aircall_call_id={self.aircall_call_id}, "
            f"subscriber_id={self.subscriber_id}, outcome={self.call_outcome})>"
        )


# ============================================================================
# Sprint S5 — Enhancement Workflows & Self-Growing Loops
# ============================================================================

class RevenueLeakLog(Base):
    """
    Nightly per-county aggregate of Gold+ leads that scored >48 hours ago
    but have received zero outreach (no SentLead row since the score).
    Written by src/tasks/revenue_leak.py. One row per (log_date, county_id).
    """
    __tablename__ = "revenue_leak_log"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    log_date: Mapped[date] = mapped_column(Date, nullable=False)
    county_id: Mapped[str] = mapped_column(String(50), nullable=False)
    total_leads_leaked: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    estimated_dollar_value: Mapped[Decimal] = mapped_column(
        Numeric(14, 2), nullable=False, default=0
    )
    vertical_breakdown: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    __table_args__ = (
        UniqueConstraint("log_date", "county_id", name="uq_revenue_leak_day_county"),
        Index("idx_revenue_leak_date", "log_date"),
        Index("idx_revenue_leak_county", "county_id"),
    )

    def __repr__(self) -> str:
        return (
            f"<RevenueLeakLog(date={self.log_date}, county={self.county_id}, "
            f"leads={self.total_leads_leaked}, value=${self.estimated_dollar_value})>"
        )


class WinStoryAsset(Base):
    """
    Sanitised proof statements auto-published when a lead pack is delivered
    or (in future) a loan is funded. No PII — county + deal type + amount range only.
    Written by src/services/win_story_publisher.py.
    """
    __tablename__ = "win_story_assets"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    event_type: Mapped[str] = mapped_column(String(40), nullable=False)
    county_id: Mapped[str] = mapped_column(String(50), nullable=False)
    proof_text: Mapped[str] = mapped_column(Text, nullable=False)
    amount_range: Mapped[Optional[str]] = mapped_column(String(60), nullable=True)
    is_public: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    approved_by: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    slack_message_ts: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    __table_args__ = (
        CheckConstraint(
            "event_type IN ('lead_pack', 'loan_funded')",
            name="ck_win_story_event_type",
        ),
        Index("idx_win_story_public_created", "is_public", "created_at"),
        Index("idx_win_story_county", "county_id"),
    )

    def __repr__(self) -> str:
        return (
            f"<WinStoryAsset(id={self.id}, event='{self.event_type}', "
            f"county='{self.county_id}')>"
        )


# ============================================================================
# 414 S1 — M1 SHARED BACKBONE (fa087)
# ============================================================================

class Prospect(Base):
    """
    Thin UUID bridge over the existing properties hub.
    Adds contactability state, channel consent, and contact tracking.
    All identity data (name, address, phone) stays in properties/owners.
    """
    __tablename__ = "prospects"

    prospect_id: Mapped[str] = mapped_column(
        PG_UUID(as_uuid=True), primary_key=True,
        server_default=text("generate_uuidv7()"),
    )
    property_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("properties.id"), nullable=False, unique=True,
    )
    contactability_state: Mapped[str] = mapped_column(
        String, nullable=False, server_default="unknown",
    )
    channel_consent: Mapped[dict] = mapped_column(
        JSONB, nullable=False, server_default=text("'{}'::jsonb"),
    )
    contact_attempts: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default=text("0"),
    )
    successful_contacts: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default=text("0"),
    )
    contactability_rate: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(5, 4),
        Computed(
            "CASE WHEN contact_attempts >= 5 "
            "THEN successful_contacts::numeric / contact_attempts "
            "ELSE NULL END",
            persisted=True,
        ),
    )
    cohort_key: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    last_touch_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True,
    )
    merged_into_id: Mapped[Optional[str]] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("prospects.prospect_id", ondelete="SET NULL"),
        nullable=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("NOW()"),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("NOW()"),
    )

    events: Mapped[List["ProspectEvent"]] = relationship(
        "ProspectEvent", back_populates="prospect",
    )

    __table_args__ = (
        CheckConstraint(
            "contactability_state IN "
            "('unknown','enriching','contactable','invalid','exhausted')",
            name="ck_prospects_contactability_state",
        ),
        Index("idx_prospects_property_id", "property_id"),
        Index(
            "idx_prospects_contactable", "prospect_id",
            postgresql_where=text("contactability_state = 'contactable'"),
        ),
        Index(
            "idx_prospects_cohort", "cohort_key",
            postgresql_where=text("cohort_key IS NOT NULL"),
        ),
    )

    def __repr__(self) -> str:
        return (
            f"<Prospect(prospect_id={self.prospect_id}, "
            f"property_id={self.property_id}, "
            f"state='{self.contactability_state}')>"
        )



class ProspectEvent(Base):
    """
    Universal event bus — transactional outbox pattern.
    Written in the same DB transaction as the state change that caused it.
    Consumers poll via processed_events for idempotent delivery.
    """
    __tablename__ = "events"

    event_id: Mapped[str] = mapped_column(
        PG_UUID(as_uuid=True), primary_key=True,
        server_default=text("generate_uuidv7()"),
    )
    prospect_id: Mapped[str] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("prospects.prospect_id"),
        nullable=False,
    )
    event_type: Mapped[str] = mapped_column(String, nullable=False)
    actor: Mapped[str] = mapped_column(String, nullable=False)
    payload: Mapped[dict] = mapped_column(
        JSONB, nullable=False, server_default=text("'{}'::jsonb"),
    )
    occurred_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("NOW()"),
    )
    source_component: Mapped[str] = mapped_column(String, nullable=False)

    prospect: Mapped["Prospect"] = relationship(
        "Prospect", back_populates="events",
    )
    processed_by: Mapped[List["ProcessedEvent"]] = relationship(
        "ProcessedEvent", back_populates="event",
        cascade="all, delete-orphan",
    )

    __table_args__ = (
        CheckConstraint(
            "event_type IN ("
            "'prospect.created','enrichment.completed','enrichment.failed',"
            "'cds.scored','truth.verdict',"
            "'lane.entry','lane.advance','lane.stall','lane.close',"
            "'broker.transition',"
            "'sms.sent','sms.reply',"
            "'commission.posted',"
            "'delivery.sent'"
            ")",
            name="ck_events_event_type",
        ),
        Index("idx_events_prospect_id", "prospect_id"),
        Index("idx_events_occurred_at", "occurred_at"),
        Index("idx_events_type", "event_type"),
    )

    def __repr__(self) -> str:
        return (
            f"<ProspectEvent(event_id={self.event_id}, "
            f"type='{self.event_type}', actor='{self.actor}')>"
        )


class ProcessedEvent(Base):
    """
    Idempotency guard — tracks which consumers have processed which events.
    ON CONFLICT DO NOTHING on (event_id, consumer) prevents double-processing.
    """
    __tablename__ = "processed_events"

    event_id: Mapped[str] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("events.event_id", ondelete="CASCADE"),
        primary_key=True,
    )
    consumer: Mapped[str] = mapped_column(String, primary_key=True)
    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("NOW()"),
    )

    event: Mapped["ProspectEvent"] = relationship(
        "ProspectEvent", back_populates="processed_by",
    )

    def __repr__(self) -> str:
        return (
            f"<ProcessedEvent(event_id={self.event_id}, "
            f"consumer='{self.consumer}')>"
        )


class MergeEvent(Base):
    """
    Audit log for prospect merges. Surviving prospect absorbs merged prospect.
    merged_into_id on the merged Prospect row points to the surviving prospect_id.
    """
    __tablename__ = "merge_events"

    merge_id: Mapped[str] = mapped_column(
        PG_UUID(as_uuid=True), primary_key=True,
        server_default=text("generate_uuidv7()"),
    )
    surviving_id: Mapped[str] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("prospects.prospect_id"),
        nullable=False,
    )
    merged_id: Mapped[str] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("prospects.prospect_id"),
        nullable=False,
    )
    field_decisions: Mapped[dict] = mapped_column(
        JSONB, nullable=False, server_default=text("'{}'::jsonb"),
    )
    merged_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("NOW()"),
    )

    def __repr__(self) -> str:
        return (
            f"<MergeEvent(surviving={self.surviving_id}, "
            f"merged={self.merged_id})>"
        )


# ============================================================================
# M6 — Lead Quality Truth Engine (verdicts, grade thresholds, cohort rates)
# ============================================================================

class GradeThreshold(Base):
    """
    Tunable grade cut-offs for the Truth Engine (spec §3.1a, config-over-code §190).

    cds_min/cds_max are on the 0–100 scale to match DistressScore.final_cds_score;
    the spec's original 0–1 values are recorded in `notes`. contactability_min is on
    the 0–1 scale and stays dormant until contactability rates exist.
    """
    __tablename__ = "grade_thresholds"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    grade: Mapped[str] = mapped_column(String, nullable=False, unique=True)
    cds_min: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    cds_max: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    contactability_min: Mapped[Optional[Decimal]] = mapped_column(Numeric(5, 4), nullable=True)
    requires_mobile_consent: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default=text("false"),
    )
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    is_active: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default=text("true"),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("NOW()"),
    )

    __table_args__ = (
        CheckConstraint(
            "grade IN ('Ultra','Platinum','Gold','Silver','Bronze','sub_grade')",
            name="ck_grade_thresholds_grade",
        ),
    )

    def __repr__(self) -> str:
        return f"<GradeThreshold(grade='{self.grade}', cds_min={self.cds_min}, cds_max={self.cds_max})>"


class Verdict(Base):
    """
    Truth Engine output — one explainable verdict per grading pass (spec §4.5).

    Append-only: a prospect may accrue several verdicts over time; the latest by
    created_at is the current one. Grade is the Truth Engine grade enum, distinct
    from DistressScore.lead_tier (M6 grades off the raw CDS score, not the tier).
    """
    __tablename__ = "verdicts"

    verdict_id: Mapped[str] = mapped_column(
        PG_UUID(as_uuid=True), primary_key=True,
        server_default=text("generate_uuidv7()"),
    )
    prospect_id: Mapped[str] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("prospects.prospect_id"),
        nullable=False,
    )
    grade: Mapped[str] = mapped_column(String, nullable=False)
    contributing_factors: Mapped[dict] = mapped_column(
        JSONB, nullable=False, server_default=text("'{}'::jsonb"),
    )
    contactability_flag: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default=text("false"),
    )
    routed_channel: Mapped[str] = mapped_column(String, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("NOW()"),
    )

    __table_args__ = (
        CheckConstraint(
            "grade IN ('Ultra','Platinum','Gold','Silver','Bronze','sub_grade')",
            name="ck_verdicts_grade",
        ),
        CheckConstraint(
            "routed_channel IN ('loan_lane','contractor_subscription','storm_retainer',"
            "'data_pack_bulk','free_hand_delivered','recycle_suppress')",
            name="ck_verdicts_routed_channel",
        ),
        Index("idx_verdicts_prospect_id", "prospect_id"),
        Index("idx_verdicts_created_at", "created_at"),
    )

    def __repr__(self) -> str:
        return (
            f"<Verdict(prospect_id={self.prospect_id}, grade='{self.grade}', "
            f"routed_channel='{self.routed_channel}')>"
        )


class CohortRate(Base):
    """
    Aggregated contactability per cohort (spec §12.1 cohort fallback).

    Refreshed nightly by src/tasks/cohort_rate_recompute.py. cohort_key is
    `{cds_lead_tier}|{county_id}|{enrichment_source}` (see config.grading).
    contactability_rate is NULL until the cohort has any contact attempts.
    """
    __tablename__ = "cohort_rates"

    cohort_key: Mapped[str] = mapped_column(String, primary_key=True)
    contact_attempts: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default=text("0"),
    )
    successful_contacts: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default=text("0"),
    )
    contactability_rate: Mapped[Optional[Decimal]] = mapped_column(Numeric(5, 4), nullable=True)
    sample_size: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default=text("0"),
    )
    computed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("NOW()"),
    )

    def __repr__(self) -> str:
        return (
            f"<CohortRate(cohort_key='{self.cohort_key}', "
            f"rate={self.contactability_rate}, n={self.sample_size})>"
        )


# ============================================================================
# B1 / M9 — Revenue Engine (S1 / 414 Stream A)
# ============================================================================


class Plan(Base):
    """Config-defined subscription tier (§4A.2). Add/edit a plan without code."""
    __tablename__ = "plans"

    plan_id: Mapped[str] = mapped_column(String(50), primary_key=True)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    tier: Mapped[str] = mapped_column(Text, nullable=False)
    price_cents: Mapped[int] = mapped_column(Integer, nullable=False)
    interval: Mapped[str] = mapped_column(Text, nullable=False)        # monthly|annual|one_time|trial
    entitlements: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    stripe_price_id: Mapped[Optional[str]] = mapped_column(Text)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("true"))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )

    __table_args__ = (
        CheckConstraint(
            "interval IN ('monthly','annual','one_time','trial')",
            name="ck_plans_interval",
        ),
    )

    def __repr__(self) -> str:
        return f"<Plan(plan_id={self.plan_id}, price_cents={self.price_cents}, interval={self.interval})>"


class CustomerAccount(Base):
    """S1 paying-contractor entity (§4A.1), bridged to legacy Subscriber.

    Recurring-revenue fields live here (no separate subscriptions table):
    stripe_subscription_id, current_period_end, mrr_cents. Collected cash stays
    in SubscriptionInvoice; change history in MrrMovement.
    """
    __tablename__ = "customer_accounts"

    account_id: Mapped[Any] = mapped_column(
        PG_UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    subscriber_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("subscribers.id"), nullable=True, index=True
    )
    company_name: Mapped[Optional[str]] = mapped_column(Text)
    contacts: Mapped[list] = mapped_column(JSONB, nullable=False, server_default=text("'[]'::jsonb"))
    status: Mapped[str] = mapped_column(Text, nullable=False, server_default=text("'free_trial'"))
    plan_tier: Mapped[Optional[str]] = mapped_column(ForeignKey("plans.plan_id"))
    service_area: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    trades: Mapped[list] = mapped_column(ARRAY(String), nullable=False, server_default=text("'{}'::text[]"))
    lead_entitlement: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    acquisition_source: Mapped[Optional[str]] = mapped_column(Text)

    # recurring-revenue fields
    stripe_customer_id: Mapped[Optional[str]] = mapped_column(Text, unique=True)
    stripe_subscription_id: Mapped[Optional[str]] = mapped_column(Text)
    current_period_end: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    mrr_cents: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    # M10/B2: per-grade lead credits earned from rejected deliveries (survives cycle reset)
    lead_credits: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    converted_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )

    __table_args__ = (
        CheckConstraint(
            "status IN ('prospect','free_trial','active','past_due','churned')",
            name="ck_ca_status",
        ),
        Index("idx_ca_status", "status"),
        Index("idx_ca_trades_gin", "trades", postgresql_using="gin"),
    )

    def __repr__(self) -> str:
        return f"<CustomerAccount(account_id={self.account_id}, status={self.status}, mrr_cents={self.mrr_cents})>"


class MrrMovement(Base):
    """Append-only MRR change ledger (§12.7). Idempotent on stripe_event_id."""
    __tablename__ = "mrr_movements"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    account_id: Mapped[Any] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("customer_accounts.account_id"), nullable=False, index=True
    )
    movement_type: Mapped[str] = mapped_column(Text, nullable=False)   # new|expansion|contraction|churn
    delta_cents: Mapped[int] = mapped_column(Integer, nullable=False)
    mrr_after_cents: Mapped[int] = mapped_column(Integer, nullable=False)
    is_involuntary: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"))
    effective_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    stripe_event_id: Mapped[Optional[str]] = mapped_column(Text, unique=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    __table_args__ = (
        CheckConstraint(
            "movement_type IN ('new','expansion','contraction','churn')",
            name="ck_mrr_type",
        ),
        Index("idx_mrr_effective", "effective_at"),
        Index("idx_mrr_type", "movement_type"),
    )

    def __repr__(self) -> str:
        return f"<MrrMovement(account_id={self.account_id}, type={self.movement_type}, delta={self.delta_cents})>"


class Delivery(Base):
    """M10/B2 — the authoritative record that one graded Lead (a scored property)
    was assigned to exactly one paying CustomerAccount. Distinct from SentLead
    (legacy email-send dedup, keyed on subscriber_id). Exclusivity is enforced in
    the claim transaction, not a hard unique on property_id (keeps shared-delivery
    a future per-plan option). The (property_id, account_id) unique is the dup-guard.
    """
    __tablename__ = "deliveries"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    property_id: Mapped[int] = mapped_column(ForeignKey("properties.id"), nullable=False, index=True)
    account_id: Mapped[Any] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("customer_accounts.account_id"), nullable=False
    )
    grade: Mapped[str] = mapped_column(Text, nullable=False)        # distress_scores.lead_tier snapshot
    vertical: Mapped[str] = mapped_column(Text, nullable=False)     # matched trade
    status: Mapped[str] = mapped_column(Text, nullable=False, server_default=text("'delivered'"))
    rejection_reason: Mapped[Optional[str]] = mapped_column(Text)
    rejected_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    billing_period_end: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    delivered_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    source: Mapped[str] = mapped_column(Text, nullable=False, server_default=text("'sweep'"))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    __table_args__ = (
        UniqueConstraint("property_id", "account_id", name="uq_delivery_property_account"),
        CheckConstraint("status IN ('delivered','rejected')", name="ck_delivery_status"),
        CheckConstraint(
            "rejection_reason IS NULL OR rejection_reason IN "
            "('disconnected','wrong_party','deceased','duplicate','other')",
            name="ck_delivery_reason",
        ),
        Index("idx_deliveries_account_grade_cycle", "account_id", "grade", "billing_period_end"),
        Index("idx_deliveries_status", "status"),
        Index("idx_deliveries_delivered_at", "delivered_at"),
    )

    def __repr__(self) -> str:
        return f"<Delivery(property_id={self.property_id}, account_id={self.account_id}, grade={self.grade}, status={self.status})>"


class FreeToPaidAttribution(Base):
    """M10/B2 — first-touch attribution (§12.8): the free Bronze lead that started
    a contractor's journey to their first paid subscription. One row per account
    (idempotent). last_free_delivery_id + free_leads_count are stored so a
    multi-touch model can be derived later without re-instrumenting (NOT built in S1).
    """
    __tablename__ = "free_to_paid_attribution"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    account_id: Mapped[Any] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("customer_accounts.account_id"), nullable=False
    )
    first_free_delivery_id: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("deliveries.id"))
    last_free_delivery_id: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("deliveries.id"))
    free_leads_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    converted_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    first_paid_plan: Mapped[Optional[str]] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    __table_args__ = (
        UniqueConstraint("account_id", name="uq_attribution_account"),
    )

    def __repr__(self) -> str:
        return f"<FreeToPaidAttribution(account_id={self.account_id}, free_leads={self.free_leads_count})>"


class ScoringWeightOverride(Base):
    """A3: Per-(vertical, signal_type) delta applied on top of VERTICAL_WEIGHTS at scoring time.

    Rows seeded from config/heuristics.json (source='seed') and updated nightly
    by the heuristic tuner job from loss/win autopsy outcomes (source='loss_feedback'/'win_feedback').
    """
    __tablename__ = "scoring_weight_overrides"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    vertical: Mapped[str] = mapped_column(String(50), nullable=False)
    signal_type: Mapped[str] = mapped_column(String(50), nullable=False)
    delta: Mapped[float] = mapped_column(Numeric(6, 2), nullable=False, server_default=text("0"))
    source: Mapped[str] = mapped_column(String(30), nullable=False, server_default=text("'seed'"))
    reason: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("TRUE"))
    loss_sample_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    win_sample_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())

    __table_args__ = (
        UniqueConstraint("vertical", "signal_type", name="uq_swo_vertical_signal"),
        Index("idx_swo_enabled", "enabled"),
        Index("idx_swo_updated_at", "updated_at"),
    )

    def __repr__(self) -> str:
        return f"<ScoringWeightOverride({self.vertical}/{self.signal_type} delta={self.delta} src={self.source})>"
