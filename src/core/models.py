"""
Database models for Distressed Property Intelligence Platform.
Implements the Hub-and-Spoke architecture with properties as the central hub.
"""

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from enum import Enum
from typing import Any, List, Optional

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Computed,
    Date,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Identity,
    Integer,
    LargeBinary as sa_LargeBinary,
    Numeric,
    Sequence,
    SmallInteger,
    String,
    Text,
    UniqueConstraint,
    CheckConstraint,
    Index,
    func,
    false as sa_false,
    true as sa_true,
    text,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, UUID as PG_UUID
from sqlalchemy.orm import DeclarativeBase, relationship, Mapped, mapped_column


class Base(DeclarativeBase):
    """Base class for all models."""
    pass


FA_MAX_TIMELINE_SEQUENCE = Sequence("fa_max_timeline_seq")


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
    hcpa_neighborhood_code: Mapped[Optional[str]] = mapped_column(String(255))
    building_details: Mapped[Optional[dict]] = mapped_column(JSONB)            # roof, walls, sub-areas, extra features
    hcpa_last_refreshed: Mapped[Optional[datetime]] = mapped_column(DateTime)  # NULL = never enriched
    # CDE-07: HCPA STRAP key, verbatim from the master bulk file — byte-identical
    # to the FL DOR SDF/NAL PARCEL_ID for Hillsborough, giving a deterministic
    # join to DOR statewide sales files. NULL for counties whose DOR key is
    # derivable from parcel_id instead (Pinellas: range/section swap transform).
    strap: Mapped[Optional[str]] = mapped_column(String(30), index=True)

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
    underwriting_feedback: Mapped[List["UnderwritingFeedback"]] = relationship("UnderwritingFeedback", back_populates="property", cascade="all, delete-orphan")

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
    last_sale_qualified: Mapped[Optional[bool]] = mapped_column(Boolean)
    last_sale_vacant_improved: Mapped[Optional[str]] = mapped_column(String(20))
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

    # Mortgage/deed-of-trust amount (Sprint 4.4) — populated from `Filing Amt`
    # column on mortgage-type docs. NULL for non-mortgage deeds.
    mortgage_amount: Mapped[Optional[float]] = mapped_column(Numeric(12, 2))

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
    winning_bid: Mapped[Optional[float]] = mapped_column(Numeric(12, 2))
    sold_to: Mapped[Optional[str]] = mapped_column(String(50))   # "Plaintiff" | "3rd Party Bidder" | None

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

    # Builder enrichment columns (Stage A — WP-T2-8) + detail-page fields (permit-detail enrichment)
    contractor_name: Mapped[Optional[str]] = mapped_column(Text)
    holder_name: Mapped[Optional[str]] = mapped_column(Text)       # permit applicant / owner-of-record
    job_value: Mapped[Optional[Decimal]] = mapped_column(Numeric(14, 2))  # declared construction value
    completion_status: Mapped[Optional[str]] = mapped_column(String(50))  # issued|active|expired|completed|pending
    contractor_license: Mapped[Optional[str]] = mapped_column(String(50))
    contractor_license_type: Mapped[Optional[str]] = mapped_column(String(100))
    contractor_phone: Mapped[Optional[str]] = mapped_column(String(20))
    contractor_email: Mapped[Optional[str]] = mapped_column(String(200))
    applicant_name: Mapped[Optional[str]] = mapped_column(String(200))
    owner_name: Mapped[Optional[str]] = mapped_column(String(200))

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
        Index("idx_permit_holder_name", "holder_name"),
        Index("idx_permit_contractor_name", "contractor_name"),
    )

    def __repr__(self):
        return f"<BuildingPermit(id={self.id}, permit_number='{self.permit_number}', type='{self.permit_type}')>"


class PermitStaging(Base):
    """
    Unmatched building permits — permits that could not be linked to an existing
    properties row (new construction / vacant lots / unplatted parcels).

    Builder Engine reads UNION(building_permits, permit_staging) so builder
    pattern detection runs on the full permit set, not just property-matched ones.
    The building_permits.property_id NOT NULL invariant is preserved.

    Stage A′ — WP-T2-8.
    """
    __tablename__ = "permit_staging"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    # Core permit identity
    permit_number: Mapped[str] = mapped_column(String(100), unique=True, nullable=False, index=True)
    permit_type: Mapped[Optional[str]] = mapped_column(String(100))
    county_id: Mapped[Optional[str]] = mapped_column(String(50), index=True)

    # Address as raw string — no property FK
    address: Mapped[Optional[str]] = mapped_column(Text)

    # Builder-relevant enrichment
    holder_name: Mapped[Optional[str]] = mapped_column(Text)
    contractor_name: Mapped[Optional[str]] = mapped_column(Text)
    job_value: Mapped[Optional[Decimal]] = mapped_column(Numeric(14, 2))
    completion_status: Mapped[Optional[str]] = mapped_column(String(50))
    status: Mapped[Optional[str]] = mapped_column(String(50))
    description: Mapped[Optional[str]] = mapped_column(Text)

    # Enforcement flag — mirrors BuildingPermit; computed before the property-match
    # branch so builder detectors can exclude enforcement permits from staging too.
    is_enforcement_permit: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False, index=True)

    # Dates
    issue_date: Mapped[Optional[date]] = mapped_column(Date)
    expire_date: Mapped[Optional[date]] = mapped_column(Date)
    date_added: Mapped[Optional[date]] = mapped_column(Date, default=date.today, index=True)

    # Resolution state — True once this staging row is linked to a properties row
    matched: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False, index=True)
    matched_property_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("properties.id"), nullable=True, index=True
    )

    __table_args__ = (
        Index("idx_permit_staging_holder", "holder_name"),
        Index("idx_permit_staging_county_issue", "county_id", "issue_date"),
    )

    def __repr__(self):
        return f"<PermitStaging(id={self.id}, permit_number='{self.permit_number}', matched={self.matched})>"


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
    and Lifecycle urgency messages across poll cycles.
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
    lifecycle_urgency_sent: Mapped[bool] = mapped_column(Boolean, default=False)
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

    # A2 — Lead Confidence gating. lead_confidence is 0.000–1.000 (NULL until A2
    # runs); is_guess_lead = lead_confidence < MIN_CONFIDENCE_THRESHOLD. Guess
    # leads are withheld from paid surfaces (feed / Lead Packs / Lifecycle recs).
    lead_confidence: Mapped[Optional[float]] = mapped_column(Numeric(4, 3))
    is_guess_lead: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default="false"
    )

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
        # Read paths filter `WHERE NOT is_guess_lead` on the hot lead-selection
        # path; partial index supports the sellable (FALSE) side cheaply.
        Index(
            "idx_score_sellable",
            "final_cds_score",
            postgresql_where=text("is_guess_lead = false"),
        ),
        CheckConstraint("urgency_level IN ('Immediate', 'High', 'Medium', 'Low')", name="check_urgency_level"),
        CheckConstraint("lead_tier IN ('Ultra Platinum', 'Platinum', 'Gold', 'Silver', 'Bronze')", name="check_lead_tier"),
    )

    def __repr__(self):
        return f"<DistressScore(id={self.id}, property_id={self.property_id}, score={self.final_cds_score}, tier='{self.lead_tier}')>"


class UnderwritingFeedback(Base):
    """
    Per-property broker underwriting decline record (Sprint 4.6).

    Each row captures one decline reason from a lending partner. The service
    layer maps reason_code → (vertical, signal_type, delta) nudges and upserts
    them into scoring_weight_overrides so the CDS engine de-values those signals
    globally for future properties carrying the same risk variables.
    """
    __tablename__ = "underwriting_feedback"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    property_id: Mapped[int] = mapped_column(ForeignKey("properties.id"), nullable=False, index=True)
    reason_code: Mapped[str] = mapped_column(String(60), nullable=False)
    reason_detail: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    lender_id: Mapped[Optional[str]] = mapped_column(String(80), nullable=True)
    loan_amount: Mapped[Optional[float]] = mapped_column(Numeric(12, 2), nullable=True)
    submitted_by: Mapped[str] = mapped_column(String(80), nullable=False)
    submitted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )

    property: Mapped["Property"] = relationship("Property", back_populates="underwriting_feedback")

    __table_args__ = (
        Index("idx_uw_feedback_property_id", "property_id"),
        Index("idx_uw_feedback_submitted_at", "submitted_at"),
        Index("idx_uw_feedback_reason_code", "reason_code"),
        CheckConstraint(
            "reason_code IN ("
            "'ltv_too_high','structural_damage','commercial_zoning','title_defect',"
            "'flood_zone','environmental_hazard','deferred_maintenance',"
            "'unpermitted_additions','tenant_occupied','market_saturation'"
            ")",
            name="ck_uw_feedback_reason_code",
        ),
    )

    def __repr__(self) -> str:
        return (
            f"<UnderwritingFeedback(id={self.id}, property_id={self.property_id},"
            f" reason_code='{self.reason_code}')>"
        )


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
    checkout_session_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
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

    # ── B0-06: voice-call PEWC consent (distinct from consent_scope='marketing') ──
    voice_consent_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    voice_consent_text: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    voice_consent_version: Mapped[Optional[str]] = mapped_column(String(30), nullable=True)

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
    tier: Mapped[str] = mapped_column(String(20), nullable=False)          # starter | pro | founder
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
        CheckConstraint("tier IN ('starter', 'pro', 'founder')", name="check_founding_tier"),
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
    tier: Mapped[str] = mapped_column(String(20), nullable=False)          # starter | pro | founder | annual_lock
    vertical: Mapped[str] = mapped_column(String(50), nullable=False)      # roofing | remediation | investor
    county_id: Mapped[str] = mapped_column(String(50), nullable=False)

    # Founding rate lock — set at checkout, never overwritten
    founding_member: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    founding_price_id: Mapped[Optional[str]] = mapped_column(String(100))  # Stripe price_id locked at checkout
    rate_locked_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    escalated_at: Mapped[Optional[datetime]] = mapped_column(DateTime)     # set when 6-month founding rate expires

    # Founder-tier (tier == 'founder') zip_held win-back benefit — a one-time,
    # +14-day territory grace extension in place of the standard 50%-off
    # coupon (founders don't get discounted). Live-state only: applies while
    # currently tier == 'founder'; not tied to founding_member/founding rate.
    founder_grace_extension_granted_at: Mapped[Optional[datetime]] = mapped_column(DateTime)

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
    # direct / landing_page / dbpr_email / lifecycle_sms / missed_call / referral /
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

    # ── Onboarding preference step ───────────────────────────────────────────
    # True for pre-existing rows (server_default) so the gate never disrupts
    # subscribers who signed up before this shipped. New signups set this
    # False explicitly (src/services/signup_engine.py) so first login shows
    # the one-screen preference step before the dashboard.
    onboarding_completed: Mapped[bool] = mapped_column(
        Boolean, default=True, server_default="true", nullable=False
    )
    preferred_property_type: Mapped[Optional[str]] = mapped_column(String(50))
    investment_budget_band: Mapped[Optional[str]] = mapped_column(String(30))

    # ── Revenue / churn tracking (fa048) ─────────────────────────────────────
    plan_price: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2), nullable=True)
    churned_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    is_trial: Mapped[bool] = mapped_column(Boolean, server_default="false", nullable=False, default=False)
    trial_ends_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    is_demo: Mapped[bool] = mapped_column(Boolean, server_default="false", nullable=False, default=False)
    is_test: Mapped[bool] = mapped_column(Boolean, server_default="false", nullable=False, default=False)

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

    # ── Magic-link (passwordless) login ──────────────────────────────────────
    # Single-use, short-lived login link — replaces emailing a plaintext password.
    magic_link_hash: Mapped[Optional[str]] = mapped_column(String(64), nullable=True, index=True)
    magic_link_expires_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    magic_link_used_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

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
            "tier IN ('free', 'starter', 'pro', 'data_only', 'autopilot_lite', 'autopilot_pro', 'partner', 'annual_lock', 'founder')",
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


class ActivationEvent(Base):
    """
    T-B12-05: 5-minute activation funnel timestamps, one row per subscriber.

    signup_time mirrors Subscriber.created_at (stamped at row creation so it
    survives even if Subscriber.created_at semantics ever change).
    onboarding_completed_time is stamped when the one-time preference form
    (PATCH /onboarding/{feed_uuid}) is submitted — the only step that
    currently sits between signup and first-leads-shown, so this is the one
    checkpoint that lets "where did they drop off" distinguish "never opened
    onboarding" from "opened it, never saw a lead" (Section 4.10 gap).
    first_leads_shown_time is stamped the first time the free-tier dashboard
    renders the 3-5 real scored leads (event_feed's no-locked-zip branch).
    first_unlock_time is stamped the first time the subscriber unlocks any
    lead's contact info (paid $4/hot-lead unlock or founder comp reveal) —
    this is the activation event per the locked decision. All three post-
    signup stamps are set-once (COALESCE-style in code, never overwritten) so
    "time to first value" and "time to activation" stay measurable against
    signup_time.
    """
    __tablename__ = "activation_events"

    subscriber_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("subscribers.id"), primary_key=True
    )
    signup_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    welcome_email_sent_time: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    magic_link_redeemed_time: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    onboarding_completed_time: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    first_leads_shown_time: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    first_unlock_time: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    def __repr__(self):
            return (
                f"<ActivationEvent(subscriber_id={self.subscriber_id}, "
                f"welcome_sent={self.welcome_email_sent_time}, "
                f"magic_redeemed={self.magic_link_redeemed_time}, "
                f"onboarded={self.onboarding_completed_time}, "
                f"shown={self.first_leads_shown_time}, unlocked={self.first_unlock_time})>"
            )


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
    status: Mapped[str] = mapped_column(String(20), default='available', nullable=False)  # available | locked | grace | held

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
        CheckConstraint("status IN ('available', 'locked', 'grace', 'held')", name="check_zip_status"),
    )

    def __repr__(self):
        return f"<ZipTerritory(zip='{self.zip_code}', vertical='{self.vertical}', status='{self.status}')>"


class DealRoom(Base):
    """
    One deal-room session per prospect hold-deposit interaction.
    Created when a prospect pays a refundable deposit to hold a ZIP while
    evaluating a deal room. Audit-only — the properties_snapshot is never
    mutated after creation.
    """
    __tablename__ = "deal_rooms"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    token: Mapped[str] = mapped_column(PG_UUID(as_uuid=False), nullable=False, unique=True)

    # Prospect identity
    prospect_name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    prospect_email: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    zip_code: Mapped[Optional[str]] = mapped_column(String(10), nullable=True, index=True)
    # Territory scope — a hold is on one (zip_code, vertical, county_id) row of
    # zip_territories, never the whole ZIP (one ZIP has many vertical/county rows).
    vertical: Mapped[Optional[str]] = mapped_column(String(50), nullable=True, index=True)
    county_id: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    tier: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)

    # Economics shown in the room (point-in-time snapshot)
    job_value: Mapped[Optional[Decimal]] = mapped_column(Numeric, nullable=True)
    close_rate: Mapped[Optional[Decimal]] = mapped_column(Numeric, nullable=True)

    # Audit snapshot of leads shown — never mutated post-creation
    properties_snapshot: Mapped[dict] = mapped_column(JSONB, nullable=False)

    # Hold lifecycle
    held_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    expires_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    converted_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    # Stripe hold payment intent — populated by the webhook after payment
    stripe_payment_intent_id: Mapped[Optional[str]] = mapped_column(String(100), nullable=True, index=True)

    # Refund state: null = not yet refunded, 'pending' = conversion recorded but
    # refund not yet confirmed (durable — swept by pending_refund_sweep),
    # 'refunded' = successful, 'refund_failed' = failed (retried by the sweep).
    refund_status: Mapped[Optional[str]] = mapped_column(String(20), nullable=True, default=None)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc)
    )

    __table_args__ = (
        CheckConstraint(
            "refund_status IS NULL OR refund_status IN ('pending', 'refunded', 'refund_failed')",
            name="check_deal_room_refund_status",
        ),
    )

    def __repr__(self) -> str:
        return f"<DealRoom(id={self.id}, zip='{self.zip_code}', token='{self.token}', refund_status={self.refund_status!r})>"


class DemoUser(Base):
    """
    Login for the standalone deal-room demo generator (/demo/deal-room).
    Replaces the shared X-Demo-Passcode with email + bcrypt-hashed password,
    stored in the DB. The plaintext password is never stored.
    """
    __tablename__ = "demo_users"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    email: Mapped[str] = mapped_column(String(255), nullable=False, unique=True, index=True)
    password_hash: Mapped[str] = mapped_column(String(255), nullable=False)
    is_active: Mapped[bool] = mapped_column(default=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc)
    )

    def __repr__(self) -> str:
        return f"<DemoUser(id={self.id}, email='{self.email}', active={self.is_active})>"


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

    # fa109: amount actually captured for this delivery, when paid via a
    # one-time charge (lead_unlock/lead_pack). NULL for daily_email rows and
    # for historical rows predating this column.
    amount_cents: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    __table_args__ = (
        UniqueConstraint("subscriber_id", "property_id", name="uq_sent_lead"),
        Index("idx_sent_lead_subscriber_sent_at", "subscriber_id", "sent_at"),
        Index("idx_sent_leads_source", "source"),
    )

    def __repr__(self):
        return f"<SentLead(subscriber_id={self.subscriber_id}, property_id={self.property_id})>"


class DemoSession(Base):
    """One per demo call. Records the ZIP searched, the featured lead, and when it was revealed."""
    __tablename__ = "demo_sessions"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    subscriber_id: Mapped[int] = mapped_column(ForeignKey("subscribers.id"), nullable=False)
    zip_code: Mapped[str] = mapped_column(String(10), nullable=False)
    vertical: Mapped[str] = mapped_column(String(50), nullable=False)
    county_id: Mapped[str] = mapped_column(String(50), nullable=False, default="hillsborough")
    property_id: Mapped[Optional[int]] = mapped_column(ForeignKey("properties.id"), nullable=True)
    masked_address: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    lead_tier: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    distress_types: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    revealed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )

    __table_args__ = (
        Index("idx_demo_sessions_sub", "subscriber_id", "created_at"),
    )


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

    # B1-03 — SLA auto-remediation. delivery_id is set only for entitlement-model
    # (Block 1 storefront) snapshots; NULL for legacy SentLead-sourced snapshots.
    delivery_id = Column(BigInteger, ForeignKey("deliveries.id"), nullable=True, index=True)
    # 'credit_issued'  — reject_delivery() granted a same-grade replacement credit
    # 'refund_issued'  — a Stripe refund was issued for the paid one-time purchase
    # 'refund_failed'  — a refund was attempted but Stripe errored
    # 'not_applicable' — outcome wasn't sold/resolved, or nothing to remediate (e.g. free lead)
    remediation_action = Column(String(30), nullable=True)
    remediated_at = Column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        UniqueConstraint("property_id", "subscriber_id", "sent_at",
                         name="uq_lead_quality_snapshot"),
        Index("idx_lqs_snapshot_at", "snapshot_at"),
        Index("idx_lqs_outcome", "outcome"),
        CheckConstraint(
            "remediation_action IS NULL OR remediation_action IN "
            "('credit_issued','refund_issued','refund_failed','not_applicable')",
            name="ck_lqs_remediation_action",
        ),
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
        Index("idx_webhook_events_type_processed", "event_type", "processed_at"),
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


class SubscriberSessionMetrics(Base):
    """
    Per-subscriber portal-engagement snapshot (Task 6.3, Phase 6).

    One row per subscriber, upserted each weekly worker run
    (src/tasks/churn_defense_engagement_decay.py). Rolling counts are recomputed
    from webhook_events each run (self-correcting) rather than incremented in place.
    engagement_decay_scalar is the latest computed decay score, clamped to the
    numeric(3,2) ceiling. auth_intervals_seconds is stored per spec but is not
    used by the decay formula.
    """
    __tablename__ = "subscriber_session_metrics"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    subscriber_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("subscribers.id", ondelete="CASCADE"),
        nullable=False, unique=True, index=True,
    )
    last_login_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    dashboard_views_7_day: Mapped[int] = mapped_column(Integer, default=0, server_default="0", nullable=False)
    lead_downloads_7_day: Mapped[int] = mapped_column(Integer, default=0, server_default="0", nullable=False)
    auth_intervals_seconds: Mapped[int] = mapped_column(Integer, default=0, server_default="0", nullable=False)
    engagement_decay_scalar: Mapped[Decimal] = mapped_column(
        Numeric(3, 2), default=Decimal("1.00"), server_default="1.00", nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    def __repr__(self):
        return (
            f"<SubscriberSessionMetrics(subscriber_id={self.subscriber_id}, "
            f"decay={self.engagement_decay_scalar})>"
        )


class ChurnDefenseLead(Base):
    """
    Staged retention-outreach record (Task 6.3, Phase 6).

    One row per firing event: created when a subscriber's engagement_decay drops
    below the threshold. risk_score = 1 - engagement_decay (churn probability).
    outreach_status lifecycle: STAGED -> SEQUENCE_TRIGGERED -> ENGAGED -> CONVERTED
    (the worker writes STAGED then SEQUENCE_TRIGGERED; later states are advanced
    by downstream GHL callbacks). FAILED is a worker-only terminal state for a
    row whose pitch/GHL push did not succeed — it is excluded from the open-lead
    cooldown check (src/tasks/churn_defense_engagement_decay.py) so a transient
    failure does not block a retry on the next weekly run.
    """
    __tablename__ = "churn_defense_leads"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    subscriber_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("subscribers.id", ondelete="CASCADE"),
        nullable=False, index=True,
    )
    risk_score: Mapped[Decimal] = mapped_column(Numeric(4, 3), nullable=False)
    outreach_status: Mapped[str] = mapped_column(
        String(50), default="STAGED", server_default="STAGED", nullable=False
    )
    triggered_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False, index=True
    )

    def __repr__(self):
        return (
            f"<ChurnDefenseLead(subscriber_id={self.subscriber_id}, "
            f"risk={self.risk_score}, status={self.outreach_status!r})>"
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

    # Outcome classification (replaces free-text error_type going forward —
    # see config/scraper_outcomes.py). NULL = clean success with real data,
    # same role error_type=NULL/'none' already played. Additive: old callers
    # that never pass outcome= keep writing error_type exactly as before;
    # nothing here is populated until a call site opts in.
    outcome_category: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    # Heartbeat pair distinguishing "genuinely never ran" from "ran and
    # crashed before ever reaching the completion write" — only populated by
    # call sites using src.utils.scraper_run_tracking.scraper_run().
    attempt_started_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

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
            "'sunbiz', 'property_appraiser', 'dbpr_company',"
            "'tax_deed_auction', 'vacant_land',"
            "'tax_deed_outcomes', 'appraiser_sale_outcomes', 'foreclosure_outcomes',"
            "'outcome_label_layer', 'dor_sales', 'dor_sale_outcomes',"
            "'deed_flip_outcomes', 'probate_lien_outcomes', 'lis_pendens_outcomes',"
            "'partner_mining'"
            ")",
            name="check_run_stats_source_type",
        ),
        CheckConstraint(
            "outcome_category IS NULL OR outcome_category IN "
            "('NO_DATA','TIMEOUT','SOURCE_ERROR','INTERNAL_ERROR','UNKNOWN')",
            name="check_run_stats_outcome_category",
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

    # ── Lifecycle self-healing baseline snapshots (fa034 + fa035) ─────────────────
    # Daily metric values written by kill_switch_metric_ingest after the
    # ks_metric_* Redis cache is updated. Read by lifecycle_self_healing.compute_baseline
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

    # Stream self-diagnosis metrics (fa102) — fractions 0–1, NULL = not computed
    enrichment_rate: Mapped[Optional[float]] = mapped_column(Numeric(7, 4), nullable=True)
    dialable_rate: Mapped[Optional[float]] = mapped_column(Numeric(7, 4), nullable=True)
    sms_delivery_rate: Mapped[Optional[float]] = mapped_column(Numeric(7, 4), nullable=True)
    closer_conv_rate: Mapped[Optional[float]] = mapped_column(Numeric(7, 4), nullable=True)

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


class LifecycleIncident(Base):
    """Lifecycle self-healing incident ledger (fa034).

    One row per (metric, county, feature) breach. Opened by
    `src/tasks/lifecycle_self_healing.py` when a metric crosses its yellow/red
    threshold; updated when duration passes action triggers; closed when
    the metric recovers.

    Runtime never instantiates this model directly — every read/write in
    lifecycle_self_healing.py and the revenue_pulse extension uses raw SQL via
    `sa_text(...)` (per repo convention). This declaration exists only so
    Alembic autogenerate stays consistent with the live schema.
    """
    __tablename__ = "lifecycle_incident"

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
        CheckConstraint("severity IN ('yellow','red')", name="check_lifecycle_incident_severity"),
        CheckConstraint(
            "action_taken IN ('no_op','fallback_enabled','auto_paused',"
            "'human_escalated','feature_killed','resolved')",
            name="check_lifecycle_incident_action",
        ),
        # Partial indexes (idx_lifecycle_incident_metric_open, idx_lifecycle_incident_unresolved)
        # are created via raw SQL in fa034 and not declared here, so autogenerate
        # doesn't try to recreate them.
        Index("idx_lifecycle_incident_breach_started", "breach_started"),
    )

    def __repr__(self):
        return (
            f"<LifecycleIncident(id={self.id}, metric={self.metric_name}, "
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
    match_method          = mapped_column(String(30), nullable=True)            # parcel_id | normalized_address | owner_name_zip | owner_name_city | owner_name | legal_desc | llm_verified
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
            # 'address' is a legacy value from before the cascade's stage-2 constant was
            # renamed to 'normalized_address' — kept for backward compatibility with
            # existing rows, not written by current code.
            "match_method IN ('parcel_id','address','normalized_address','owner_name_zip',"
            "'owner_name_city','owner_name','legal_desc','llm_verified') OR match_method IS NULL",
            name="check_unmatched_match_method",
        ),
    )

    def __repr__(self):
        return f"<UnmatchedRecord(id={self.id}, source='{self.source_type}', status='{self.match_status}')>"


class OutcomeCandidate(Base):
    """
    Canonical staging shape for outcomes mined from already-ingested public
    records (foreclosure auction results, tax-deed auction results, appraiser
    sales, etc.) by the Lifecycle Data Engine connectors (src/connectors/).

    Deliberately has no FK to deal_outcomes — the label layer (CDE-10,
    src/connectors/label_layer.py) promotes unconsumed rows into DealOutcome
    (subscriber_id NULL, confidence_tier public_record_inferred) keyed by a
    deterministic source_ref, stamping consumed_at here.
    """
    __tablename__ = "outcome_candidates"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    property_id: Mapped[int] = mapped_column(Integer, ForeignKey("properties.id"), nullable=False, index=True)
    county_id: Mapped[str] = mapped_column(String(50), nullable=False)
    source_type: Mapped[str] = mapped_column(String(50), nullable=False, index=True)   # matches connector registry source_type
    source_table: Mapped[str] = mapped_column(String(50), nullable=False)              # e.g. 'foreclosures', 'tax_deed_auctions'
    source_id: Mapped[int] = mapped_column(Integer, nullable=False)                    # PK of the row in source_table
    event_type: Mapped[str] = mapped_column(String(40), nullable=False)
    event_date: Mapped[date] = mapped_column(Date, nullable=False)
    amount: Mapped[Optional[Decimal]] = mapped_column(Numeric(14, 2))
    counterparty: Mapped[Optional[str]] = mapped_column(String(255))
    raw_status: Mapped[Optional[str]] = mapped_column(String(100))                     # untranslated source string, for audit
    raw_payload: Mapped[Optional[dict]] = mapped_column(JSONB)                         # connector-specific extras (e.g. deed_flip margin/hold/instruments)
    match_confidence: Mapped[Optional[Decimal]] = mapped_column(Numeric(4, 3))         # only set when resolve_or_quarantine() was used
    match_method: Mapped[Optional[str]] = mapped_column(String(30))
    consumed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))   # set by the label layer (src/connectors/label_layer.py)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), onupdate=func.now())

    property = relationship("Property", foreign_keys=[property_id])

    __table_args__ = (
        # event_date is part of the key (not just source_type/table/id) so a stable
        # per-property source row (e.g. Financial, overwritten on every appraiser
        # refresh) can still stage a SEPARATE outcome for each distinct sale date —
        # otherwise a second qualified sale on the same property would silently
        # overwrite the first sale's staged outcome instead of creating a new one.
        UniqueConstraint("source_type", "source_table", "source_id", "event_date", name="uq_outcome_candidate"),
        Index("ix_outcome_candidates_property", "property_id"),
        Index(
            "ix_outcome_candidates_unconsumed",
            "consumed_at",
            postgresql_where=text("consumed_at IS NULL"),
        ),
        CheckConstraint(
            "event_type IN ('auction_sold_third_party','auction_reverted_to_lender',"
            "'auction_cancelled','tax_deed_sold','tax_deed_cancelled','tax_deed_redeemed',"
            "'qualified_sale','unqualified_sale','deed_flip','probate_sale','lien_sale',"
            "'lp_sold_pre_auction')",
            name="check_outcome_candidate_event_type",
        ),
    )

    def __repr__(self):
        return f"<OutcomeCandidate(id={self.id}, source='{self.source_type}', event='{self.event_type}')>"


class DorSale(Base):
    """
    Raw FL DOR SDF (Sale Data File) rows — the statewide standardized sales
    feed (CDE-07). One row per (county, parcel, recorded sale event) from the
    per-county CSVs on the DOR data portal; a re-posted roll updates rows in
    place (this is how a pending QUAL_CD 98/99 gets its final code).

    property_id is resolved at ingestion, set-based: Hillsborough joins
    properties.strap = parcel_id_dor (verbatim STRAP); Pinellas derives
    parcel_id_norm via the range/section swap transform and joins
    properties.parcel_id. Unresolved rows keep property_id NULL here — the SDF
    carries no address/owner, so the UnmatchedRecord review-queue cascade has
    nothing extra to work with (a future NAL ingest can re-resolve them).
    The dor_sale_outcomes connector (CDE-07) reads matched rows and stages
    OutcomeCandidates.
    """
    __tablename__ = "dor_sales"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    county_id: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    co_no: Mapped[int] = mapped_column(Integer, nullable=False)                     # DOR county number (Hillsborough 39, Pinellas 62)
    parcel_id_dor: Mapped[str] = mapped_column(String(30), nullable=False)          # verbatim SDF PARCEL_ID
    parcel_id_norm: Mapped[Optional[str]] = mapped_column(String(30))               # derived properties.parcel_id form (Pinellas transform); NULL when not derivable
    property_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("properties.id"), index=True)
    match_method: Mapped[Optional[str]] = mapped_column(String(30))                 # 'strap' | 'parcel_transform'
    state_parcel_id: Mapped[Optional[str]] = mapped_column(String(30))
    assessment_year: Mapped[Optional[int]] = mapped_column(Integer)
    dor_uc: Mapped[Optional[str]] = mapped_column(String(10))
    vi_cd: Mapped[Optional[str]] = mapped_column(String(2))
    # Natural-key components are '' (never NULL) so the UNIQUE constraint
    # actually dedupes — Postgres treats NULLs as distinct.
    or_book: Mapped[str] = mapped_column(String(10), nullable=False, server_default=text("''"))
    or_page: Mapped[str] = mapped_column(String(10), nullable=False, server_default=text("''"))
    clerk_no: Mapped[str] = mapped_column(String(30), nullable=False, server_default=text("''"))
    qual_cd: Mapped[str] = mapped_column(String(5), nullable=False)
    sal_chg_cd: Mapped[Optional[str]] = mapped_column(String(5))
    sale_yr: Mapped[int] = mapped_column(Integer, nullable=False)
    sale_mo: Mapped[int] = mapped_column(Integer, nullable=False)
    sale_price: Mapped[Optional[Decimal]] = mapped_column(Numeric(14, 2))
    multi_par_sal: Mapped[Optional[str]] = mapped_column(String(2))
    roll_tag: Mapped[Optional[str]] = mapped_column(String(20))                     # portal folder, e.g. '2025F'
    source_file: Mapped[Optional[str]] = mapped_column(String(120))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )
    updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), onupdate=func.now())

    property: Mapped[Optional["Property"]] = relationship("Property", foreign_keys=[property_id])

    __table_args__ = (
        UniqueConstraint(
            "co_no", "parcel_id_dor", "sale_yr", "sale_mo", "clerk_no", "or_book", "or_page",
            name="uq_dor_sales_natural",
        ),
        Index("ix_dor_sales_county_qual", "county_id", "qual_cd"),
        Index(
            "ix_dor_sales_unresolved",
            "county_id",
            postgresql_where=text("property_id IS NULL"),
        ),
    )

    def __repr__(self) -> str:
        return (
            f"<DorSale(county={self.county_id!r}, parcel={self.parcel_id_dor!r}, "
            f"{self.sale_yr}-{self.sale_mo:02d}, qual={self.qual_cd!r})>"
        )


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

    # fa109: amount actually captured at payment (cents). NULL for historical
    # rows predating this column.
    amount_cents: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

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


class LifecycleEventQueue(Base):
    """
    Durable fallback queue for Lifecycle bus events when Redis is unavailable
    (fa072). `publish_lifecycle_event` writes here + emits NOTIFY lifecycle_events; the
    Postgres listener drains pending rows on startup and every 60s.

    The table already exists in the DB; this ORM mapping was missing, which
    broke the Redis-down fallback path in src/agents/events/ingestion.py.
    """
    __tablename__ = "lifecycle_event_queue"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    event_type: Mapped[str] = mapped_column(Text, nullable=False)
    subscriber_id: Mapped[Optional[int]] = mapped_column(Integer)
    payload: Mapped[Optional[dict]] = mapped_column(JSONB)
    idempotency_key: Mapped[Optional[str]] = mapped_column(Text)
    decision_id: Mapped[Optional[str]] = mapped_column(String(36))  # preserved across the fallback path so downstream joins survive Redis-down
    status: Mapped[str] = mapped_column(Text, nullable=False, default="pending")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )
    processed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    error: Mapped[Optional[str]] = mapped_column(Text)

    __table_args__ = (
        Index("idx_lifecycle_event_queue_status", "status", "created_at"),
    )

    def __repr__(self):
        return f"<LifecycleEventQueue(id={self.id}, type={self.event_type}, status={self.status})>"


class FaMaxExceptionsAlertQueue(Base):
    """
    Durable pending-alert queue for the FA Max EXCEPTIONS Slack lane
    (WP-T2-1 go-live review, 2026-09).

    Before this table existed, post_exceptions_alert() was called directly
    and fire-and-forget by both fa_max_send_health_monitor.py and
    relay/sweep.py's suppression-sync-failure path: a Slack outage OR a
    process crash between deciding to alert and the Slack call completing
    meant the alert was silently lost -- there was nothing durable to retry.
    Mirrors LifecycleEventQueue's pending/committed-first pattern: a row is
    inserted and committed BEFORE the Slack call is attempted, so a crash at
    any point after that leaves a recoverable 'pending' row rather than
    nothing at all.

    Ambiguous-result note (documented, not solved): if the process crashes
    or times out AFTER Slack has accepted chat_postMessage but BEFORE this
    row is marked 'sent', the next drain tick re-posts the same content --
    a duplicate Slack message, not a lost one. Unlike relay's approval-card
    retry (slack_post.post_for_approval), which reconciles via
    conversations_history search because a duplicate APPROVAL CARD risks a
    duplicate SEND, an EXCEPTIONS alert carries no send risk -- worst case
    is Josh sees the same warning twice. That asymmetry is why this queue
    does not implement history-based reconciliation: the cost of building
    it isn't justified by what a duplicate here actually costs.

    Status values: 'pending' (not yet delivered) -> 'sent' (terminal).
    Retry is intentionally unbounded (a stale-source-style alert must not
    silently give up — see testing-verification's failure/retry guidance);
    `attempts` is tracked for observability, not as a cutoff.

    `claimed_until` (code-review finding, 2026-09): enqueue_and_attempt()'s
    immediate delivery attempt and drain_pending()'s retry sweep both
    operate on 'pending' rows with a real network call (Slack) in between
    the row becoming visible and it being finalized. Without an atomic
    claim, the immediate attempt and an overlapping drain tick — or two
    overlapping drain ticks, if one run takes longer than the cron cadence
    — can both select and post the SAME row concurrently: a genuine
    duplicate-post case beyond the documented crash-after-Slack one above.
    Mirrors relay_approval_queue.slack_post_lease_until's exact pattern
    (queue.py's claim_slack_post/release_slack_post) — a short-lived lease,
    not a hard lock, so a crashed claimant's row naturally becomes
    claimable again after the lease expires rather than staying stuck.
    """
    __tablename__ = "fa_max_exceptions_alert_queue"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    venture_key: Mapped[str] = mapped_column(Text, nullable=False)
    rule: Mapped[str] = mapped_column(Text, nullable=False)
    message: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False, default="pending")
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )
    last_attempt_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    sent_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    error: Mapped[Optional[str]] = mapped_column(Text)
    claimed_until: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))

    __table_args__ = (
        Index("idx_fa_max_exceptions_alert_queue_status", "status", "created_at"),
        # Dedup lookup: "is there already a pending-or-recently-sent row for
        # this (venture, rule)" — sweep.py's suppression-sync-failure path
        # can otherwise fire once per sweep tick (every 30 min) for a
        # multi-hour Instantly outage, flooding this table with duplicate
        # pending rows for the same underlying condition.
        Index("idx_fa_max_exceptions_alert_queue_dedup", "venture_key", "rule", "created_at"),
        Index("idx_fa_max_exceptions_alert_queue_claim", "status", "claimed_until"),
        # Code-review finding (third round, 2026-09): the dedup lookup above
        # is a plain SELECT, and SELECT-then-INSERT is a check-then-act race
        # -- two concurrent producers can both see "nothing pending" and
        # both insert. Reproduced directly. This partial unique index is
        # the actual fix: the database itself refuses a second 'pending' row
        # for the same (venture_key, rule), so exceptions_alert_queue.py's
        # enqueue only needs to catch the resulting IntegrityError, not
        # prevent the race in application code (which a SELECT can't do
        # alone).
        Index(
            "ux_fa_max_exceptions_alert_queue_pending_dedup", "venture_key", "rule",
            unique=True, postgresql_where=text("status = 'pending'"),
        ),
    )

    def __repr__(self):
        return (
            f"<FaMaxExceptionsAlertQueue(id={self.id}, rule={self.rule!r}, "
            f"status={self.status!r})>"
        )


class UnifiedSubscriberMemory(Base):
    """
    Single audit-spine table aggregating all external-interaction events
    across every system module (fa096). Every row maps back to a subscriber
    and carries a standardized JSONB payload with a creation timestamp.

    Stream sources:
      STRIPE       - checkout, payment, subscription lifecycle events
      GHL          - outbound FA -> GHL CRM pushes (stage changes, upserts)
      SMS          - outbound SMS delivery confirmations (Telnyx callbacks)
      SYNTHFLOW    - Synthflow call logging + outcome tags
      UNDERWRITING - underwriting milestone changes / financing intent scoring
    """
    __tablename__ = "unified_subscriber_memory"

    id: Mapped[str] = mapped_column(
        PG_UUID, primary_key=True,
        server_default=text("gen_random_uuid()"),
    )
    subscriber_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("subscribers.id"), nullable=False, index=True,
    )
    property_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("properties.id"), nullable=True, index=True,
    )
    stream_source: Mapped[str] = mapped_column(String(50), nullable=False)
    event_type: Mapped[str] = mapped_column(String(100), nullable=False)
    event_payload: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )

    __table_args__ = (
        CheckConstraint(
            "stream_source IN ('STRIPE', 'GHL', 'SMS', 'SYNTHFLOW', 'UNDERWRITING')",
            name="ck_usm_stream_source",
        ),
        Index("idx_usm_subscriber_created", "subscriber_id", "created_at"),
        Index(
            "idx_usm_property",
            "property_id",
            postgresql_where=text("property_id IS NOT NULL"),
        ),
        Index("idx_usm_stream_source", "stream_source", "created_at"),
        Index("idx_usm_event_type", "event_type"),
    )

    def __repr__(self):
        return (
            f"<UnifiedSubscriberMemory(id={self.id}, sub={self.subscriber_id}, "
            f"source={self.stream_source}, type={self.event_type})>"
        )


# ══════════════════════════════════════════════════════════════════════════════
# Phase 2B Models
# ══════════════════════════════════════════════════════════════════════════════


class SubscriberMemorySummary(Base):
    """Derived current-state snapshot built from unified subscriber memory."""
    __tablename__ = "subscriber_memory_summary"

    subscriber_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("subscribers.id"), primary_key=True
    )
    last_event_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    last_event_type: Mapped[Optional[str]] = mapped_column(String(100))
    last_stripe_event_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    last_stripe_event_type: Mapped[Optional[str]] = mapped_column(String(100))
    latest_payment_state: Mapped[Optional[str]] = mapped_column(String(100))
    latest_checkout_state: Mapped[Optional[str]] = mapped_column(String(100))
    latest_crm_status: Mapped[Optional[str]] = mapped_column(String(100))
    latest_crm_stage: Mapped[Optional[str]] = mapped_column(String(100))
    last_sms_event_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    last_sms_event_type: Mapped[Optional[str]] = mapped_column(String(100))
    latest_sms_state: Mapped[Optional[str]] = mapped_column(String(100))
    last_sms_reply_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    sms_opted_out: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default="false"
    )
    last_voice_event_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    last_voice_event_type: Mapped[Optional[str]] = mapped_column(String(100))
    last_underwriting_event_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    last_underwriting_event_type: Mapped[Optional[str]] = mapped_column(String(100))
    latest_underwriting_state: Mapped[Optional[str]] = mapped_column(String(100))
    latest_underwriting_milestone: Mapped[Optional[str]] = mapped_column(String(100))
    last_lead_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("properties.id"))
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )

    __table_args__ = (
        Index("idx_sms_last_event", "last_sms_event_at"),
        Index("idx_usm_summary_last_event", "last_event_at"),
    )

    def __repr__(self):
        return (
            f"<SubscriberMemorySummary(subscriber_id={self.subscriber_id}, "
            f"last_event_type={self.last_event_type})>"
        )


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
    Ground truth for all Lifecycle learning — must log from Day 1.
    """
    __tablename__ = "message_outcomes"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    subscriber_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("subscribers.id"), index=True)
    message_type: Mapped[str] = mapped_column(String(20), nullable=False)  # sms/email/voice
    template_id: Mapped[Optional[str]] = mapped_column(String(100))
    variant_id: Mapped[Optional[str]] = mapped_column(String(100), index=True)  # A/B test variant
    # Discrete calendar-day bucket backing uq_message_outcomes_dedup — a real
    # unique index can't express the old rolling-24h dup_q window directly.
    # See Follow-on 4 of system_decisions/lifecycle-notify-sweep-double-processing.md.
    send_date: Mapped[Optional[date]] = mapped_column(Date)
    channel: Mapped[Optional[str]] = mapped_column(String(50))  # twilio/ses/synthflow
    recipient_email: Mapped[Optional[str]] = mapped_column(String(255), index=True)
    provider_message_id: Mapped[Optional[str]] = mapped_column(String(100), index=True)
    failure_reason: Mapped[Optional[str]] = mapped_column(String(255))
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

    # link back to Lifecycle decision
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
        # Atomic send-dedup (Follow-on 4, Direction 1a). COALESCE(variant_id, '')
        # because Postgres never treats NULL = NULL as a match in a plain unique
        # index — a non-A/B-tested send (variant_id IS NULL, the common case)
        # would otherwise get zero protection. message_type is included because
        # the old check-then-insert dup_q never filtered on it, letting an SMS
        # and an email sharing a template_id/variant_id false-positive on each
        # other. See system_decisions/lifecycle-notify-sweep-double-processing.md.
        Index(
            "uq_message_outcomes_dedup",
            "subscriber_id", "template_id", text("COALESCE(variant_id, '')"),
            "message_type", "send_date",
            unique=True,
        ),
    )

    def __repr__(self):
        return f"<MessageOutcome(id={self.id}, type={self.message_type}, conversion={self.conversion_type})>"


class LifecycleSuppression(Base):
    """
    Active subscriber-level stop for Lifecycle-led outbound touches.
    Compliance messages still flow through their own SMS gates.
    """
    __tablename__ = "lifecycle_suppressions"

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
        Index("idx_lifecycle_suppression_active_sub", "subscriber_id", "is_active"),
        Index("idx_lifecycle_suppression_reason", "reason"),
    )

    def __repr__(self):
        return f"<LifecycleSuppression(sub={self.subscriber_id}, reason={self.reason}, active={self.is_active})>"


class DealOutcome(Base):
    """
    Tracks confirmed deals reported by subscribers. Feeds revenue signal score,
    annual push triggers, and attribution.
    """
    __tablename__ = "deal_outcomes"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    # Nullable: founder-import and public-record-inferred outcomes have no
    # subscriber (CDE-11, ADR 0025). Subscriber-only side-effects on the
    # deal-capture path fire only when this is set.
    subscriber_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("subscribers.id"), nullable=True, index=True)
    property_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("properties.id"), index=True)
    # CDE-11 — trust in the outcome LABEL (distinct from A2 Lead Confidence).
    # Default is the LOWEST tier so an insert that forgets to set it is safe
    # (never silently high-trust); every real writer sets it explicitly.
    confidence_tier: Mapped[str] = mapped_column(String(30), nullable=False, server_default=text("'public_record_inferred'"))
    # Finer provenance: subscriber_tap / founder_import / <connector>. Free text.
    outcome_source: Mapped[Optional[str]] = mapped_column(String(50))
    # B0-01 idempotency key for bulk imports (hash of parcel/address|date|amount).
    # NULL for subscriber-tap rows. Partial unique index (see __table_args__).
    source_ref: Mapped[Optional[str]] = mapped_column(String(64))
    deal_size_bucket: Mapped[Optional[str]] = mapped_column(String(20))  # 5_10k/10_25k/25k_plus/skip
    deal_amount: Mapped[Optional[float]] = mapped_column(Numeric(12, 2))
    deal_date: Mapped[Optional[date]] = mapped_column(Date)
    lead_source: Mapped[Optional[str]] = mapped_column(String(50))  # which signal drove the lead
    days_to_close: Mapped[Optional[int]] = mapped_column(Integer)
    pipeline_stage: Mapped[Optional[str]] = mapped_column(String(30))  # lead / contacted / qualified / proposal / negotiation / closed_won / closed_lost
    # T-B13-01 — one-tap buyer outcome on the delivered-lead card.
    # outcome_state is the buyer-facing tap (closed/dead/pending); pipeline_stage
    # is the derived stage kept for existing consumers. dead_reason is REQUIRED
    # when outcome_state='dead' (enforced at the API, mirrored by a check
    # constraint). reason_fault_class splits dead reasons into lead_fault (feeds
    # the CDS retune) vs buyer_neutral (score-protected, buyer-side log only).
    outcome_state: Mapped[Optional[str]] = mapped_column(String(10))  # closed / dead / pending
    dead_reason: Mapped[Optional[str]] = mapped_column(String(30))
    reason_fault_class: Mapped[Optional[str]] = mapped_column(String(15))  # lead_fault / buyer_neutral
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
        CheckConstraint(
            "confidence_tier IN ('founder_verified','subscriber_reported','public_record_inferred')",
            name="ck_deal_outcomes_confidence_tier",
        ),
        Index("idx_deal_outcome_pipeline_stage", "pipeline_stage"),
        # T-B13-01 — buyer outcome tap constraints + retune-routing index.
        CheckConstraint(
            "outcome_state IS NULL OR outcome_state IN ('closed','dead','pending')",
            name="ck_deal_outcomes_outcome_state",
        ),
        CheckConstraint(
            "reason_fault_class IS NULL OR reason_fault_class IN ('lead_fault','buyer_neutral')",
            name="ck_deal_outcomes_reason_fault_class",
        ),
        CheckConstraint(
            "outcome_state <> 'dead' OR dead_reason IS NOT NULL",
            name="ck_deal_outcomes_dead_requires_reason",
        ),
        Index("idx_deal_outcomes_fault_class", "reason_fault_class"),
        Index("idx_deal_outcomes_county_vertical", "county_id", "trade_vertical"),
        Index("idx_deal_outcomes_confidence_tier", "confidence_tier"),
        Index(
            "uq_deal_outcomes_source_ref",
            "source_ref",
            unique=True,
            postgresql_where=text("source_ref IS NOT NULL"),
        ),
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
    lifecycle_behavior_adjustment: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
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
    active pricing cohort, Lifecycle graph, and pitch variant so the future A5b
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
    lifecycle_graph: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
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
    Weekly Lifecycle learning summary. Sunday midnight LangGraph job writes one card
    per type. Lifecycle reads the most recent cards at the start of every decision tree.
    """
    __tablename__ = "learning_cards"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    card_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    card_type: Mapped[str] = mapped_column(String(30), nullable=False)
    summary_text: Mapped[str] = mapped_column(Text, nullable=False)
    data_json: Mapped[Optional[dict]] = mapped_column(JSONB)        # raw metrics
    action_taken: Mapped[Optional[str]] = mapped_column(String(255))  # what Lifecycle did
    created_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc))

    __table_args__ = (
        CheckConstraint(
            # kill_switch_scorecard/win_autopsy/conversion_tier_report were
            # already live in the DB constraint (pre-existing drift from
            # another feature) — included here so this string matches
            # reality; see migrations/apply_learning_card_holdout_result.py.
            "card_type IN ('message_perf', 'deal_pattern', 'ab_result', "
            "'churn_signal', 'pricing_test', 'general', "
            "'autonomy_summary', "        # fa036 — weekly Lifecycle autonomy scorecard
            "'kill_switch_scorecard', 'win_autopsy', 'conversion_tier_report', "
            "'holdout_result')",          # Task 4.1 — frozen control holdout surfacing
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
    # Set when the signup arrived via a proactive referral prompt link carrying
    # a signed attribution token — lets mark_confirmed() credit the exact
    # referral_prompt_funnel row that drove the conversion. Plain int (the funnel
    # table is raw-SQL, not an ORM model), nullable for organic/reactive signups.
    prompt_funnel_id: Mapped[Optional[int]] = mapped_column(Integer)
    # T-B12-06: attribution marker distinguishing where the referral ask
    # originated. 'generic' = the standard referral link; 'investor_to_investor'
    # = the Tier-3-gated "invite a fellow investor" ask. No reward-ladder impact
    # (rewards are unchanged) — this exists purely for attribution/reporting.
    referral_source: Mapped[str] = mapped_column(String(30), nullable=False, default="generic", index=True)
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


class ReferralPromptFunnel(Base):
    """
    Proactive referral-prompt funnel: prompt shown -> link shared -> referral confirmed.
    Schema-only (provisions the table for Base.metadata.create_all() in tests) — all
    runtime reads/writes go through sqlalchemy.text() raw SQL, not this ORM class.
    """
    __tablename__ = "referral_prompt_funnel"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    subscriber_id: Mapped[int] = mapped_column(Integer, ForeignKey("subscribers.id"), nullable=False, index=True)
    trigger_type: Mapped[str] = mapped_column(String(30), nullable=False)
    trigger_source_table: Mapped[str] = mapped_column(String(30), nullable=False)
    trigger_source_id: Mapped[int] = mapped_column(Integer, nullable=False)
    referral_code: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    state: Mapped[str] = mapped_column(String(20), nullable=False, default="shown")
    prompt_shown_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)
    sms_sent: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    email_sent: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    shared_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    confirmed_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    confirmed_referral_event_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("referral_events.id"))
    created_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)

    __table_args__ = (
        CheckConstraint(
            "trigger_type IN ('deal_win', 'lead_pack_delivery')",
            name="check_rpf_trigger_type",
        ),
        CheckConstraint(
            "state IN ('shown', 'shared', 'confirmed', 'expired')",
            name="check_rpf_state",
        ),
        UniqueConstraint("trigger_source_table", "trigger_source_id", name="uq_rpf_source"),
        Index("idx_rpf_subscriber_shown", "subscriber_id", "prompt_shown_at"),
        Index("idx_rpf_state", "state"),
    )

    def __repr__(self):
        return f"<ReferralPromptFunnel(subscriber={self.subscriber_id}, trigger={self.trigger_type}, state={self.state})>"


class AbTest(Base):
    """A/B test definition. Lifecycle creates and manages tests within guardrail bounds."""
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
    # When record_outcome set `outcome` — lets a time-windowed holdout verdict
    # (e.g. "any paid action within 7 days") check outcome_at - created_at
    # rather than treating any eventual outcome as an unbounded conversion.
    outcome_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc))

    test = relationship("AbTest", backref="assignments")

    __table_args__ = (
        UniqueConstraint("test_id", "subscriber_id", name="uq_ab_assignment"),
    )


# ══════════════════════════════════════════════════════════════════════════════
# Agent Lane Experiment Models
# ══════════════════════════════════════════════════════════════════════════════
# Agent Lane's own experiment registry — pre-customer/cold-outbound tests
# (Cora, REVINT price-band tests, Hunter's vertical autopilot, LEARN).
# Deliberately separate from AbTest/AbAssignment above: those are Lifecycle's
# (post-customer/subscriber) tables. Agent Lane and Lifecycle are two
# different engines (pre- vs post-customer outreach) — sharing one
# experiment table would couple their schemas and blast radius (e.g.
# ab_rollback_check walks every active AbTest with no name filter, so any
# row inserted there is already subject to Lifecycle's own rollback math).
# See docs/agent-lane-data-access-matrix.md.

class AgentLaneExperiment(Base):
    """Agent Lane's experiment definition — the registry Cora/REVINT/Hunter/LEARN
    register tests against. Same field shape REVINT-v2.2 originally added to
    AbTest, ported to its own table rather than grafted onto Lifecycle's."""
    __tablename__ = "agent_lane_experiments"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    test_name: Mapped[str] = mapped_column(String(100), nullable=False, unique=True)
    hypothesis: Mapped[Optional[str]] = mapped_column(Text)
    audience: Mapped[Optional[str]] = mapped_column(String(100))
    offer: Mapped[Optional[str]] = mapped_column(String(60))
    variant_a: Mapped[dict] = mapped_column(JSONB, nullable=False)
    variant_b: Mapped[dict] = mapped_column(JSONB, nullable=False)
    control_price_cents: Mapped[Optional[int]] = mapped_column(Integer)
    test_price_cents: Mapped[Optional[int]] = mapped_column(Integer)
    traffic_pct: Mapped[int] = mapped_column(Integer, nullable=False, default=10)
    min_sample: Mapped[Optional[int]] = mapped_column(Integer)
    success_metric: Mapped[Optional[str]] = mapped_column(String(60))
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="active")
    verdict: Mapped[Optional[str]] = mapped_column(String(20))  # control_wins | test_wins | inconclusive
    started_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc))
    ended_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    winner: Mapped[Optional[str]] = mapped_column(String(10))  # 'a' / 'b'

    __table_args__ = (
        CheckConstraint("status IN ('active', 'completed', 'rolled_back')", name="check_agent_lane_experiment_status"),
        CheckConstraint("traffic_pct BETWEEN 1 AND 100", name="check_agent_lane_experiment_traffic_pct"),
        CheckConstraint(
            "verdict IS NULL OR verdict IN ('control_wins', 'test_wins', 'inconclusive')",
            name="check_agent_lane_experiment_verdict",
        ),
    )

    def __repr__(self):
        return f"<AgentLaneExperiment(name={self.test_name}, status={self.status})>"


class AgentLaneExperimentAssignment(Base):
    """An opportunity's assignment to an Agent Lane experiment arm.

    Keyed ONLY on opportunity_thread_id — never subscriber_id. Agent Lane is
    pre-customer by definition; an assignment for someone who's already a
    subscriber belongs on Lifecycle's AbAssignment instead. This removes the
    need for an XOR constraint entirely (unlike AbAssignment previously on
    this branch, which needed one only because a single table was being
    asked to serve two domains)."""
    __tablename__ = "agent_lane_experiment_assignments"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    test_id: Mapped[int] = mapped_column(Integer, ForeignKey("agent_lane_experiments.id"), nullable=False, index=True)
    opportunity_thread_id: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    variant: Mapped[str] = mapped_column(String(10), nullable=False)
    outcome: Mapped[Optional[str]] = mapped_column(String(30))
    outcome_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc))

    test = relationship("AgentLaneExperiment", backref="assignments")

    __table_args__ = (
        UniqueConstraint("test_id", "opportunity_thread_id", name="uq_agent_lane_experiment_assignment"),
    )


class ExperimentDecisionSnapshot(Base):
    """Immutable record of what was known and chosen at the moment an
    opportunity was assigned to an Agent Lane experiment arm (LEARN-v2.2
    Layer 1, Step 3).

    Captured once, at assignment time — not updated afterward. Layer 2's
    attribution join (Step 6) and Layer 4's Golden CLOSE chains (Step 12)
    both walk backward from this row. leading_alternative is the
    counterfactual the spec calls for: the offer/angle NOT chosen (e.g.
    "offered subscription over pack"), recorded here because it's only
    knowable at decision time, before the outcome exists.

    buyer_type / target_characteristics are nullable and expected to be
    NULL until Hunter's buyer-type classification (feat/hunter-03-04-05-
    buyer-profiling) merges — degrade gracefully rather than block on it.
    """
    __tablename__ = "experiment_decision_snapshots"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    test_id: Mapped[int] = mapped_column(Integer, ForeignKey("agent_lane_experiments.id"), nullable=False, index=True)
    opportunity_thread_id: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    assigned_variant: Mapped[str] = mapped_column(String(10), nullable=False)
    message_angle: Mapped[Optional[str]] = mapped_column(String(100))
    offer: Mapped[Optional[str]] = mapped_column(String(60))
    buyer_type: Mapped[Optional[str]] = mapped_column(String(30))
    target_characteristics: Mapped[Optional[dict]] = mapped_column(JSONB)
    chosen_action: Mapped[Optional[str]] = mapped_column(String(60))
    leading_alternative: Mapped[Optional[str]] = mapped_column(String(60))
    created_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc))

    test = relationship("AgentLaneExperiment", backref="decision_snapshots")

    __table_args__ = (
        UniqueConstraint("test_id", "opportunity_thread_id", name="uq_experiment_decision_snapshot"),
    )

    def __repr__(self):
        return f"<ExperimentDecisionSnapshot(test_id={self.test_id}, thread={self.opportunity_thread_id}, variant={self.assigned_variant})>"


class PriceAssignment(Base):
    """Source of truth for an assigned price through the entire offer chain.

    A new row is created whenever a price is (re-)assigned for a given
    opportunity_thread_id + offer combination.  The previous row is flipped to
    status='superseded'.  Only one 'active' row should exist per thread+offer
    pair at any time (enforced by assign_price service logic, not a DB
    constraint, to keep supersede writes cheap).
    """
    __tablename__ = "price_assignments"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    opportunity_thread_id: Mapped[str] = mapped_column(String(30), nullable=False, index=True)
    offer: Mapped[str] = mapped_column(String(60), nullable=False)
    assigned_price_cents: Mapped[int] = mapped_column(Integer, nullable=False)
    currency: Mapped[str] = mapped_column(String(3), nullable=False, default="usd")
    experiment_assignment_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("agent_lane_experiment_assignments.id")
    )
    price_band_floor_cents: Mapped[int] = mapped_column(Integer, nullable=False)
    price_band_ceiling_cents: Mapped[int] = mapped_column(Integer, nullable=False)
    band_validated: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    assigned_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc)
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc)
    )
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="active")

    __table_args__ = (
        CheckConstraint(
            "status IN ('active', 'superseded', 'expired')",
            name="check_price_assignment_status",
        ),
        Index("ix_price_assignments_thread_offer_status", "opportunity_thread_id", "offer", "status"),
    )

    def __repr__(self) -> str:
        return (
            f"<PriceAssignment(thread={self.opportunity_thread_id}, offer={self.offer}, "
            f"price={self.assigned_price_cents}, status={self.status})>"
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


class EmailOptOut(Base):
    """
    Cross-channel suppression list, email side. Sibling of SmsOptOut — any
    address in this table must never receive outbound email, including
    transactional (receipts, payment-failed, login links). Cascades to/from
    sms_opt_outs via src.services.email_suppression.suppress_contact().
    """
    __tablename__ = "email_opt_outs"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    email: Mapped[str] = mapped_column(String(255), nullable=False, unique=True, index=True)
    source: Mapped[str] = mapped_column(String(30), nullable=False, default="manual")  # unsubscribe_link/hard_bounce/instantly_sync/manual/cascaded_from_sms
    # Durable per-row watermark: True once pushed to Instantly's block list.
    # Unpushed rows are retried every sync run (survives failed/partial pushes).
    pushed_to_instantly: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")
    opted_out_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)

    def __repr__(self):
        return f"<EmailOptOut(email={self.email}, source={self.source})>"


class DncPhoneCheck(Base):
    """Latest Tracerfy DNC result per normalized phone.

    This is the universal freshness source for outbound compliance. Positive
    DNC/litigator results are enforced through sms_opt_outs; this table tracks
    when a phone was last proven clean or blocked.
    """
    __tablename__ = "dnc_phone_checks"

    phone: Mapped[str] = mapped_column(String(20), primary_key=True)
    national_dnc: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")
    litigator: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")
    checked_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))
    source: Mapped[str] = mapped_column(String(40), nullable=False, default="tracerfy_dnc_refresh", server_default="tracerfy_dnc_refresh")
    raw_result: Mapped[Optional[dict]] = mapped_column(JSONB)

    __table_args__ = (
        Index("idx_dnc_phone_checks_checked_at", "checked_at"),
        Index("idx_dnc_phone_checks_clean_fresh", "checked_at", postgresql_where=text("national_dnc = false AND litigator = false")),
    )

    def __repr__(self):
        return (
            f"<DncPhoneCheck(phone={self.phone}, national_dnc={self.national_dnc}, "
            f"litigator={self.litigator})>"
        )


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


class CheckoutProvisioningFailure(Base):
    """
    Durable recovery queue for a checkout that Stripe completed (charge and
    subscription both real) but whose ZIP-territory provisioning failed and
    was rolled back — see stripe_webhooks._on_checkout_completed. Written via
    its own committed session, deliberately independent of the request's main
    db session, so it survives that session's rollback. This table is the
    monitored ops queue: ops must actually cancel/refund the Stripe
    subscription or re-provision, then mark the row resolved via
    /api/admin/checkout-provisioning-failures — this table only records the
    fact and the detail, it does not decide or automate the recovery action.
    """
    __tablename__ = "checkout_provisioning_failures"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    stripe_customer_id: Mapped[Optional[str]] = mapped_column(String(100), index=True)
    stripe_subscription_id: Mapped[Optional[str]] = mapped_column(String(100), index=True)
    email: Mapped[Optional[str]] = mapped_column(String(255))
    tier: Mapped[Optional[str]] = mapped_column(String(20))
    vertical: Mapped[Optional[str]] = mapped_column(String(50))
    county_id: Mapped[Optional[str]] = mapped_column(String(50))
    requested_zips: Mapped[Optional[list]] = mapped_column(JSONB)
    unclaimed_zips: Mapped[Optional[list]] = mapped_column(JSONB)
    reason: Mapped[str] = mapped_column(String(50), nullable=False, default="zip_territory_unavailable")
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="open")  # open | resolved
    created_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc))
    resolved_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    resolved_by: Mapped[Optional[str]] = mapped_column(String(100))
    notes: Mapped[Optional[str]] = mapped_column(Text)

    __table_args__ = (
        CheckConstraint("status IN ('open', 'resolved')", name="check_checkout_provisioning_status"),
        Index("idx_checkout_provisioning_status", "status"),
    )

    def __repr__(self):
        return f"<CheckoutProvisioningFailure(id={self.id}, status={self.status}, reason={self.reason})>"


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
    output_ref_expires_at: Mapped[Optional[datetime]] = mapped_column(DateTime)

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
    # A4: set once this hit's owner has had its quality rating discounted due to
    # a degraded-provider event, so a multi-day degradation never re-discounts
    # the same rows (idempotency guard for apply_degraded_discount).
    quality_discounted: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false", nullable=False)
    caller: Mapped[Optional[str]] = mapped_column(String(40))   # QUALITY-v2.2 Q2: seat name for P&L attribution

    __table_args__ = (
        Index("idx_enrichment_purpose_created", "purpose", "created_at"),
        Index("idx_enrichment_vendor_created", "vendor", "created_at"),
        Index("idx_enrichment_caller", "caller", postgresql_where=text("caller IS NOT NULL")),
    )

    def __repr__(self):
        return f"<EnrichmentUsageLog(vendor={self.vendor}, purpose={self.purpose}, cost_cents={self.cost_cents})>"


class EnrichmentAnomalyLog(Base):
    """
    A4 — one row per detected degraded-provider event.

    A provider is "degraded" when its hit rate over a recent time window falls
    below its configured floor (with a minimum sample guard). Distinct from
    abnormal spend (Vendor Cost Monitor): a pay-per-hit provider that degrades
    spends *less*, so the cost monitor cannot see it. Append-only; also acts as
    the re-alert cooldown source (no separate state file).
    """
    __tablename__ = "enrichment_anomaly_log"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    provider: Mapped[str] = mapped_column(String(32), nullable=False, index=True)   # tracerfy | batchdata
    detected_at: Mapped[datetime] = mapped_column(
        DateTime, default=lambda: datetime.now(timezone.utc), index=True
    )
    observed_hit_rate: Mapped[float] = mapped_column(Numeric(5, 4), nullable=False)
    floor_hit_rate: Mapped[float] = mapped_column(Numeric(5, 4), nullable=False)
    sample_size: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    records_affected: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    alert_sent: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    def __repr__(self):
        return (f"<EnrichmentAnomalyLog(provider={self.provider}, "
                f"observed={self.observed_hit_rate}, floor={self.floor_hit_rate})>")


class PlatformRevenueLedger(Base):
    """
    Centralized revenue ledger — one row per confirmed payment, regardless of
    product. Written exclusively via src/services/revenue_ledger.py:record_revenue(),
    never by hand-rolled SQL at each call site. SentLead/LeadPackPurchase/
    PremiumPurchase/SubscriptionInvoice remain each product's own operational
    source of truth (idempotency keys, exclusivity windows, refund tracking
    specific to that product) — this table is a pure additive reporting layer
    so margin/gating code never needs to know which table a given product's
    revenue actually lives in.
    """
    __tablename__ = "platform_revenue_ledger"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    subscriber_id: Mapped[int] = mapped_column(Integer, ForeignKey("subscribers.id"), nullable=False, index=True)
    product_type: Mapped[str] = mapped_column(String(40), nullable=False, index=True)
    amount_cents: Mapped[int] = mapped_column(Integer, nullable=False)
    # Nullable: subscription revenue isn't tied to one property. Per-lead
    # products (lead_unlock, lead_pack, premium) always set this so cost can
    # be joined directly via property_id, with no per-product knowledge.
    property_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("properties.id"), nullable=True, index=True)
    source_table: Mapped[str] = mapped_column(String(60), nullable=False)
    source_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    refunded_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    # Actual amount refunded, distinct from amount_cents — a partial refund
    # must not zero out the whole row. NULL for legacy/not-yet-updated
    # callers; mark_ledger_refunded() defaults it to the full amount_cents
    # when the caller doesn't know the actual refunded amount.
    refunded_amount_cents: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    opportunity_thread_id: Mapped[Optional[str]] = mapped_column(String(20))   # QUALITY-v2.2 Q2: links revenue to an agent-driven thread

    __table_args__ = (
        UniqueConstraint("source_table", "source_id", name="uq_revenue_ledger_source"),
        Index("idx_revenue_ledger_thread", "opportunity_thread_id", postgresql_where=text("opportunity_thread_id IS NOT NULL")),
    )

    def __repr__(self):
        return f"<PlatformRevenueLedger(subscriber_id={self.subscriber_id}, product_type={self.product_type}, amount_cents={self.amount_cents})>"


class RevenueHeartbeatAlertLog(Base):
    """Cooldown log for src/tasks/revenue_fulfillment_heartbeat.py's alert
    email — a distinct alert_key re-alerts at most once per cooldown window,
    so an unresolved issue doesn't nag daily. The CSV report always lists
    every exception regardless of cooldown; this only suppresses the email.
    """
    __tablename__ = "revenue_heartbeat_alert_log"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    alert_key: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    alerted_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())


class PlatformCostAttribution(Base):
    """
    Generalizes "which subscriber does this enrichment cost belong to" the
    same way for every attribution method — direct purchase (lead_unlock,
    lead_pack, premium) or the zip-territory multi-vertical collision
    result (refreshed daily by a scheduled job). computed_for_date is NULL
    for point-in-time purchase attributions; set (and versioned, never
    overwritten) for the daily zip-territory refresh, so a report run today
    and re-run later against the same date give the same answer even after
    territory ownership shifts.
    """
    __tablename__ = "platform_cost_attribution"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    enrichment_usage_log_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("enrichment_usage_logs.id"), nullable=False, index=True
    )
    subscriber_id: Mapped[int] = mapped_column(Integer, ForeignKey("subscribers.id"), nullable=False, index=True)
    property_id: Mapped[int] = mapped_column(Integer, ForeignKey("properties.id"), nullable=False, index=True)
    attribution_method: Mapped[str] = mapped_column(String(40), nullable=False)
    attributed_cost_cents: Mapped[int] = mapped_column(Integer, nullable=False)
    computed_for_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    __table_args__ = (
        Index("idx_cost_attribution_method_date", "attribution_method", "computed_for_date"),
        # Partial unique indexes (not a single UniqueConstraint including the
        # nullable computed_for_date) — Postgres treats NULL != NULL, so a
        # composite constraint would silently allow duplicate direct_purchase
        # rows (where computed_for_date is always NULL) through.
        Index(
            "uq_cost_attribution_direct_purchase", "enrichment_usage_log_id", "subscriber_id",
            unique=True, postgresql_where=text("attribution_method = 'direct_purchase'"),
        ),
        Index(
            "uq_cost_attribution_zip_territory_daily", "enrichment_usage_log_id", "subscriber_id", "computed_for_date",
            unique=True, postgresql_where=text("attribution_method = 'zip_territory_highest_vertical'"),
        ),
    )

    def __repr__(self):
        return f"<PlatformCostAttribution(subscriber_id={self.subscriber_id}, method={self.attribution_method}, cost_cents={self.attributed_cost_cents})>"


class GridCellPnl(Base):
    """
    CLONE-v2.2 CL2 — per-grid-cell P&L rollup, generalizing
    PlatformRevenueLedger/PlatformCostAttribution's additive-rollup pattern
    down from product/subscriber level to the cell level. A "cell" is
    county_id x distress_type x buyer_vertical x offer_step — distress_type
    is a signal key from config/scoring.py:VERTICAL_WEIGHTS[buyer_vertical]
    (e.g. 'foreclosures', 'tax_delinquencies'), buyer_vertical is one of the
    6 keys of VERTICAL_WEIGHTS itself, and offer_step is a `name` from
    config/revenue_ladder.py:REVENUE_LADDER.

    One row per (cell, period). Written exclusively via
    src/services/grid_cell_pnl.py:upsert_cell_pnl(), which sums
    platform_revenue_ledger and platform_cost_attribution for the period —
    this table never accepts a hand-written revenue/cost figure, matching
    the existing ledger's "one writer" convention. Re-running the rollup for
    an already-computed period overwrites that row (period P&L is a
    point-in-time recomputation, not an append-only event), unlike the
    underlying ledgers themselves.
    """
    __tablename__ = "grid_cell_pnl"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    county_id: Mapped[str] = mapped_column(String(50), nullable=False)
    distress_type: Mapped[str] = mapped_column(String(50), nullable=False)
    buyer_vertical: Mapped[str] = mapped_column(String(50), nullable=False)
    offer_step: Mapped[str] = mapped_column(String(50), nullable=False)
    period_start: Mapped[date] = mapped_column(Date, nullable=False)
    period_end: Mapped[date] = mapped_column(Date, nullable=False)

    revenue_cents: Mapped[int] = mapped_column(BigInteger, nullable=False, server_default=text("0"))
    cost_cents: Mapped[int] = mapped_column(BigInteger, nullable=False, server_default=text("0"))
    # Additive-rollup convention: stored, not computed on read, so a report
    # run today and re-run later against the same period give the same
    # answer even if revenue_cents/cost_cents' underlying source rows later
    # gain refunds (PlatformRevenueLedger keeps refunded rows in place).
    contribution_margin_cents: Mapped[int] = mapped_column(BigInteger, nullable=False, server_default=text("0"))
    deal_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))

    computed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    __table_args__ = (
        UniqueConstraint(
            "county_id", "distress_type", "buyer_vertical", "offer_step",
            "period_start", "period_end",
            name="uq_grid_cell_pnl_cell_period",
        ),
        Index("idx_grid_cell_pnl_cell", "county_id", "distress_type", "buyer_vertical", "offer_step"),
        Index("idx_grid_cell_pnl_period", "period_start", "period_end"),
    )

    def __repr__(self):
        return (
            f"<GridCellPnl(cell={self.county_id}/{self.distress_type}/{self.buyer_vertical}/{self.offer_step}, "
            f"period={self.period_start}..{self.period_end}, margin_cents={self.contribution_margin_cents})>"
        )


class AlgorithmicVarianceLog(Base):
    """
    Task 6.2 — one row per paid/free enrichment routing decision made by
    EnrichmentRouter.fetch_contact_profile(). Append-only audit trail for
    the cost-control gate in front of Tracerfy/BatchData/IDI/PDL.
    """
    __tablename__ = "algorithmic_variance_log"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    lead_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True, index=True)
    property_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("properties.id"), nullable=True, index=True)
    subscriber_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("subscribers.id"), nullable=True, index=True)
    county: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    vertical: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    lead_tier: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)

    spend_ratio: Mapped[Optional[float]] = mapped_column(Numeric(14, 6), nullable=True)  # NULL when revenue unavailable
    threshold: Mapped[float] = mapped_column(Numeric(14, 6), nullable=False)

    selected_path: Mapped[str] = mapped_column(String(20), nullable=False)
    provider: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)  # tracerfy|batchdata|idi|pdl|voters, null if blocked pre-lookup

    paid_lookup_allowed: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    routing_reason: Mapped[str] = mapped_column(String(32), nullable=False)

    lookup_success: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    cost_cents: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False, index=True
    )

    __table_args__ = (
        CheckConstraint(
            "selected_path IN ('paid_trace','free_cross_match','blocked','override_paid')",
            name="check_avl_selected_path",
        ),
        CheckConstraint(
            "routing_reason IN ('spend_ratio_safe','spend_ratio_exceeded',"
            "'zero_revenue_guard','missing_telemetry_guard','manual_override')",
            name="check_avl_routing_reason",
        ),
        Index("idx_avl_subscriber_created", "subscriber_id", "created_at"),
        Index("idx_avl_property_created", "property_id", "created_at"),
    )

    def __repr__(self):
        return f"<AlgorithmicVarianceLog(path={self.selected_path}, reason={self.routing_reason}, ratio={self.spend_ratio})>"


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
# Agents — Lifecycle LangGraph Audit Log
# ══════════════════════════════════════════════════════════════════════════════


class AgentDecision(Base):
    """
    One row per Lifecycle graph decision. Separate from message_outcomes (which is
    outcome-focused). This is the "why did Lifecycle do X for user Y" audit table —
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
    # playbook_id: nullable link to the lifecycle_playbook row that drove this decision.
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
        BigInteger, ForeignKey("lifecycle_playbook.id", ondelete="SET NULL"), nullable=True,
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


class InboundResponse(Base):
    """
    Block 11 / B11-04 — one row per hot inbound call, tracking time-to-callback.

    t0 = webhook_log.created_at (inbound call arrived), copied at score time.
    t1 = callback resolution time, backfilled from agent_decisions.completed_at
    (decision_id == this row's decision_id) once the shared new_lead_voice_call
    graph run finishes — report-only optimization; no closed loop.
    """
    __tablename__ = "inbound_response"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    subscriber_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("subscribers.id"), nullable=True, index=True
    )
    decision_id: Mapped[Optional[str]] = mapped_column(String(36), index=True)  # == call_id; join key to agent_decisions
    t0: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    t1: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    score: Mapped[int] = mapped_column(Integer, nullable=False)
    matched_signals: Mapped[Optional[list]] = mapped_column(JSONB)
    outcome: Mapped[str] = mapped_column(String(20), nullable=False, default="pending")  # pending|called|consent_blocked|dnc_blocked|failed
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )

    __table_args__ = (
        CheckConstraint(
            "outcome IN ('pending', 'called', 'consent_blocked', 'dnc_blocked', 'failed')",
            name="check_inbound_response_outcome",
        ),
        Index("idx_inbound_response_created_at", "created_at"),
    )

    def __repr__(self):
        return f"<InboundResponse(id={self.id}, decision_id={self.decision_id}, outcome={self.outcome})>"


class QuoraQuestion(Base):
    """
    One row per Quora question that has been classified by Lifecycle.
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

    # ── Lifecycle classification ───────────────────────────────────────────────────
    matched_keyword: Mapped[Optional[str]] = mapped_column(Text, nullable=True, index=True)
    lifecycle_decision_id: Mapped[Optional[str]] = mapped_column(String(36), nullable=True)
    intent_lane: Mapped[Optional[str]] = mapped_column(String(60), nullable=True, index=True)
    recommended_action: Mapped[Optional[str]] = mapped_column(String(40), nullable=True, index=True)
    priority_score: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    risk_level: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    lifecycle_classification: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)

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

    # ── Task 5.1 Autonomous Inbound Content Loop (fa-5.1, see docs/adr/0021) ──
    # Metric columns fold onto quora_topics rather than a separate
    # scraper_keyword_metrics table; cluster is an enum string, not thread_clusters.
    cluster:             Mapped[Optional[str]] = mapped_column(String(30), nullable=True)
    signup_count:        Mapped[int]           = mapped_column(Integer, nullable=False, server_default="0", default=0)
    cumulative_spend:    Mapped[Decimal]       = mapped_column(Numeric(10, 4), nullable=False, server_default="0", default=Decimal("0"))
    performance_score:   Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 4), nullable=True)
    impression_count:    Mapped[Optional[int]] = mapped_column(Integer, nullable=True)   # no organic source in v1
    click_through_count: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)   # no organic source in v1

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

      
class LifecyclePlaybook(Base):
    """Fleet-wide playbook / anti-playbook table (fa036, widened CLONE-v2.2).

    One row per authored recommendation — originally Lifecycle-only (A/B
    winner promotion, kill recommendation), now open to any agent/domain via
    `agent_domain` ('lifecycle' | 'vera' | 'cora' | 'hunter' | 'fleet') and to
    either polarity via `entry_kind` ('playbook' | 'anti_playbook'), per the
    fleet constitutions' "playbooks at 3+ proofs, anti-playbooks at 3+
    failures, inherited at birth" rule (docs/constitutions/*.md). The table
    name and existing columns are unchanged — this is a widening, not a
    replacement; every pre-existing row defaults to agent_domain='lifecycle',
    entry_kind='playbook'. Status lifecycle unchanged:
        recommended → adopted   (human approves via admin endpoint)
                    → rejected  (human declines)
                    → retired   (previously-adopted playbook is disabled)

    Runtime never instantiates this model — every read/write goes through
    raw SQL via `sa_text` (per repo convention) in `src/services/playbook_writer.py`,
    `src/api/admin_router.py`, and `src/tasks/lifecycle_autonomy_report.py`. The
    declaration exists for Alembic autogenerate consistency.

    The `source_key` column + the partial-unique index on it prevent
    duplicate recommendations from the same source within the same
    agent_domain (see `idx_lifecycle_playbook_source_key_unique` in fa036,
    widened by migrations/apply_lifecycle_playbook_fleet_widen.py to key on
    agent_domain too). NULL source_key is allowed and uncounted by the index.
    """
    __tablename__ = "lifecycle_playbook"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(120), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    pattern_json: Mapped[dict] = mapped_column(JSONB, nullable=False)

    # authored_by: 'lifecycle' for autonomous paths; <operator handle> for manual.
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

    # CLONE-v2.2 widening — this table now serves the whole fleet, not just
    # Lifecycle. agent_domain identifies which agent/domain authored the
    # entry ('lifecycle' | 'vera' | 'cora' | 'hunter' | 'fleet' for
    # cross-agent entries); default 'lifecycle' preserves every existing row
    # and every pre-widening caller's behavior unchanged. entry_kind splits
    # playbook (proven pattern, 3+ proofs per the fleet constitutions) from
    # anti_playbook (documented failure, 3+ instances) — same table, same
    # dedupe machinery, per docs/constitutions/*.md's "Playbooks at 3+
    # proofs; anti-playbooks at 3+ failures; inherited at birth."
    agent_domain: Mapped[str] = mapped_column(String(40), nullable=False, server_default=text("'lifecycle'"))
    entry_kind: Mapped[str] = mapped_column(String(20), nullable=False, server_default=text("'playbook'"))

    # LEARN-v2.2 Layer 4 (Step 11) — lesson versioning/confidence/portability
    # on top of CLONE-v2.2's fleet-wide widening above, rather than a
    # parallel table: CL1 already made this the fleet's one shared
    # playbook/anti-playbook library (docs/constitutions/*.md), so LEARN
    # extends it further instead of re-fragmenting fleet knowledge into a
    # second store. All nullable/defaulted — every pre-existing row and
    # caller is unaffected.
    confidence: Mapped[Optional[int]] = mapped_column(Integer)  # 0-100
    version: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("1"))
    # scope: portability dimensions this lesson applies to, e.g.
    # {"buyer_type": "buy_and_hold", "offer": "founder_tier"} — the spec's
    # "prospect / vertical / county / offer / fleet" portability score,
    # kept as a flexible bag rather than fixed columns since the dimension
    # set is expected to grow.
    scope: Mapped[Optional[dict]] = mapped_column(JSONB)
    # Self-referential supersession chain: when a newer, validated version
    # replaces this one, this row's status flips to 'superseded' and
    # superseded_by_id points at the replacement — preserving audit history
    # rather than overwriting in place.
    superseded_by_id: Mapped[Optional[int]] = mapped_column(
        BigInteger, ForeignKey("lifecycle_playbook.id", ondelete="SET NULL")
    )

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
            "status IN ('recommended','adopted','rejected','retired','superseded','contradicted')",
            name="check_lifecycle_playbook_status",
        ),
        CheckConstraint(
            "entry_kind IN ('playbook','anti_playbook')",
            name="check_lifecycle_playbook_entry_kind",
        ),
        CheckConstraint(
            "confidence IS NULL OR confidence BETWEEN 0 AND 100",
            name="check_lifecycle_playbook_confidence",
        ),
        # Non-unique indexes mirror fa036. The unique partial index on
        # source_key is created via raw SQL in the migration, not declared
        # here, so autogenerate doesn't try to re-create it.
        Index("idx_lifecycle_playbook_status", "status"),
        Index("idx_lifecycle_playbook_authored", "authored_by", "authored_at"),
        Index("idx_lifecycle_playbook_agent_domain_kind", "agent_domain", "entry_kind", "status"),
    )

    def __repr__(self):
        return (
            f"<LifecyclePlaybook(id={self.id}, name={self.name}, "
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


class OwnerAlertDispatch(Base):
    """
    One row per notify_owner() call — claims an idempotency key so a Stripe/
    Synthflow webhook retry can't fire the same founder alert twice, and
    tracks SMS delivery state so a Telnyx "queued" response (accepted, not
    delivered) can still fall back to email once the delivery-status webhook
    or the sweep confirms it never landed.
    """
    __tablename__ = "owner_alert_dispatch"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    alert_key: Mapped[str] = mapped_column(String(120), unique=True, nullable=False)
    subject: Mapped[str] = mapped_column(String(200), nullable=False)
    body: Mapped[str] = mapped_column(Text, nullable=False)
    telnyx_message_id: Mapped[Optional[str]] = mapped_column(String(80))
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="pending")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), onupdate=func.now()
    )

    __table_args__ = (
        CheckConstraint(
            "status IN ('pending','sms_sent','sms_delivered','sms_failed','email_sent')",
            name="check_oad_status",
        ),
        Index("idx_oad_telnyx_message_id", "telnyx_message_id"),
        Index("idx_oad_status_created", "status", "created_at"),
    )

    def __repr__(self):
        return f"<OwnerAlertDispatch(alert_key={self.alert_key!r}, status={self.status})>"


# ============================================================================
# VENTURE CONFIGURATION (CLONE-v2.2 / CL3)
# ============================================================================

class Venture(Base):
    """
    One row per business running on this agent fleet.

    A venture owns a Relay sending identity (Slack approval channel,
    Instantly campaign, sender address, send window, daily ceiling, kill
    switch) and a geography (state, bankruptcy court, and the set of
    `counties` rows pointing back here via counties.venture_key). Before
    CL3 every one of these was a single-valued env global in
    config/settings.py, which is what made a second venture impossible
    without code changes.

    Venture #1 is 'hillsborough_distress'. Every venture_key column added
    by CL3 defaults to it and the CL3 migration seeds this row from the
    current env values, so a deployment that never creates a second
    venture behaves exactly as it did before.

    Read at runtime through src/utils/venture_config.py:get_venture_config()
    (5-minute cache, falls back to config/settings.py when no row exists),
    never by querying this table directly. config/venture_template.py is
    the copy-and-fill template; src/services/venture_provisioning.py turns
    a filled-in copy into rows.
    """
    __tablename__ = "ventures"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    venture_key: Mapped[str] = mapped_column(String(60), unique=True, nullable=False)
    display_name: Mapped[str] = mapped_column(String(120), nullable=False)
    # Name rendered in the CAN-SPAM footer of every Relay email this
    # venture sends — replaces the hardcoded "Forced Action" literal that
    # used to live in src/services/relay/channels_email.py.
    brand_name: Mapped[str] = mapped_column(String(120), nullable=False)
    postal_address: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    # Phone line and compliance disclaimer rendered in the same CAN-SPAM
    # footer as brand_name/postal_address (WP-T2-1 go-live review, 2026-09,
    # client Q9 "Email Branding, Signature, and Compliance Footer"). Both
    # optional -- a venture with neither set gets the pre-existing
    # brand+address+unsubscribe footer unchanged (channels_email.py).
    outbound_contact_phone: Mapped[Optional[str]] = mapped_column(String(30), nullable=True)
    outbound_disclaimer: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Geography. `state` is read by the flood/insurance/storm scrapers for
    # NWS + FEMA lookups; the court fields by bankruptcy_engine. Both were
    # hardcoded to Florida in county_config.py before CL3.
    state: Mapped[str] = mapped_column(String(2), nullable=False, server_default="FL")
    bankruptcy_court_code: Mapped[str] = mapped_column(
        String(10), nullable=False, server_default="flmb"
    )
    default_bankruptcy_division: Mapped[str] = mapped_column(
        String(10), nullable=False, server_default="8:"
    )
    # County whose county_sources rows new counties in this venture clone
    # from (see src/services/venture_provisioning.clone_county_sources).
    template_county_id: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)

    # Relay approval surface.
    relay_slack_channel: Mapped[Optional[str]] = mapped_column(String(120), nullable=True)
    relay_approvers: Mapped[list] = mapped_column(
        JSONB, nullable=False, server_default=text("'[]'::jsonb")
    )

    # Relay email channel. Each venture needs its own Instantly passthrough
    # campaign — sharing one would cross-contaminate Instantly's
    # duplicate-contact guard (docs/adr/0011).
    relay_instantly_campaign_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    relay_instantly_sender_email: Mapped[Optional[str]] = mapped_column(String(200), nullable=True)

    # Relay execution guards.
    relay_send_window_start: Mapped[int] = mapped_column(
        SmallInteger, nullable=False, server_default=text("11")
    )
    relay_send_window_end: Mapped[int] = mapped_column(
        SmallInteger, nullable=False, server_default=text("18")
    )
    relay_send_window_timezone: Mapped[str] = mapped_column(
        String(60), nullable=False, server_default="America/New_York"
    )
    relay_daily_ceiling: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default=text("20")
    )
    # Kill-switch feature key checked before every batch and every item.
    # 'relay_global' shares the fleet-wide Relay stop; a venture-specific
    # key stops just that venture. The fleet-wide 'global' override takes
    # precedence over both.
    kill_switch_feature: Mapped[str] = mapped_column(
        String(60), nullable=False, server_default="relay_global"
    )

    # Autonomous venture ladder (CLONE-v2.2 / CL4). Which rung of
    # radar -> probe -> pilot -> unit_economics -> cell -> spin_up -> portfolio
    # this venture currently occupies. Advanced only by
    # src/services/venture_ladder.py:advance(), which refuses on any red gate
    # and writes a venture_ladder_events audit row for every decision.
    #
    # A radar-stage candidate is a real row here with is_active=false: the
    # CL3 resolver falls back to env settings for an inactive venture, so an
    # unproven candidate structurally cannot govern sends. That gives one
    # identity and one join key from radar all the way to portfolio, with no
    # separate candidate table and no promotion step.
    ladder_stage: Mapped[str] = mapped_column(
        String(30), nullable=False, server_default="radar"
    )
    ladder_entered_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    is_active: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=True, server_default=sa_true()
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), onupdate=func.now()
    )

    __table_args__ = (
        CheckConstraint(
            "relay_send_window_start >= 0 AND relay_send_window_start <= 24",
            name="ck_ventures_send_window_start",
        ),
        CheckConstraint(
            "relay_send_window_end >= 0 AND relay_send_window_end <= 24",
            name="ck_ventures_send_window_end",
        ),
        CheckConstraint(
            "relay_send_window_start < relay_send_window_end",
            name="ck_ventures_send_window_order",
        ),
        CheckConstraint("relay_daily_ceiling > 0", name="ck_ventures_daily_ceiling"),
        CheckConstraint(
            "ladder_stage IN ('radar', 'probe', 'pilot', 'unit_economics', "
            "'cell', 'spin_up', 'portfolio')",
            name="ck_ventures_ladder_stage",
        ),
        Index("idx_ventures_is_active", "is_active"),
        Index("idx_ventures_ladder_stage", "ladder_stage"),
    )

    def __repr__(self):
        return f"<Venture(venture_key={self.venture_key!r}, display_name={self.display_name!r})>"


class VentureLadderEvidence(Base):
    """One recorded fact backing a venture's advance up the ladder (CL4).

    Deliberately one table typed by `evidence_type` rather than a table per
    kind: every rung needs to record something (a market score, a reachable
    scrape sample, a presell commitment), the gates only ever count rows and
    sum a JSONB field, and a new evidence kind must not need a migration.
    Same idiom as src/connectors/outcomes.py's OutcomeCandidate payload.

    Presell commitments are `evidence_type='presell_commitment'` with a
    payload of {kind, amount_cents, stripe_payment_intent_id, contact_ref}.
    `kind` distinguishes deposit/first_month/saved_card and the accepted set
    lives in config/venture_ladder.py:PRESELL_ACCEPTED_KINDS, so changing
    what counts as demand evidence is a config edit, not a schema change.

    `verified` is what separates a claim from evidence. Only a row set true
    by a machine check — a Stripe webhook confirming the deposit actually
    settled — counts toward a gate; a hand-entered row stays false and is
    ignored. That is what makes the presell gate autonomous.
    """
    __tablename__ = "venture_ladder_evidence"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    venture_key: Mapped[str] = mapped_column(
        String(60), ForeignKey("ventures.venture_key"), nullable=False
    )
    # The rung this evidence was gathered for — kept so a later replay can
    # tell "probe-stage scrape sample" from a re-sample taken at spin_up.
    stage: Mapped[str] = mapped_column(String(30), nullable=False)
    evidence_type: Mapped[str] = mapped_column(String(50), nullable=False)
    payload: Mapped[dict] = mapped_column(
        JSONB, nullable=False, server_default=text("'{}'::jsonb")
    )
    # Idempotency handle for machine-recorded evidence: the Stripe
    # PaymentIntent id for a deposit, the source URL for a scrape sample.
    # UNIQUE per venture so a webhook retry cannot inflate a presell count.
    source_ref: Mapped[Optional[str]] = mapped_column(String(200), nullable=True)
    verified: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default=sa_false()
    )
    recorded_by: Mapped[str] = mapped_column(String(120), nullable=False)
    recorded_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False,
        default=lambda: datetime.now(timezone.utc), server_default=func.now(),
    )

    __table_args__ = (
        Index("ix_venture_ladder_evidence_key_type", "venture_key", "evidence_type"),
        UniqueConstraint(
            "venture_key", "evidence_type", "source_ref",
            name="uq_venture_ladder_evidence_source_ref",
        ),
    )

    def __repr__(self) -> str:
        return (
            f"<VentureLadderEvidence(venture_key={self.venture_key!r}, "
            f"type={self.evidence_type!r}, verified={self.verified})>"
        )


class VentureLadderEvent(Base):
    """Append-only audit of every ladder decision (CL4).

    Written on advance, on a refused advance, and on an auto-double. Never
    updated, never deleted.

    `gate_results` stores the computed value, threshold and colour of every
    gate at decision time, so a doubling or a promotion is reconstructable
    months later without re-running the queries against data that has since
    moved. It is also the idempotency source for auto-double: "has this
    venture already doubled today / within the cooldown" is answered by
    selecting the last `auto_double` row, not by a Redis flag that expires
    independently of the ceiling it guards.
    """
    __tablename__ = "venture_ladder_events"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    venture_key: Mapped[str] = mapped_column(
        String(60), ForeignKey("ventures.venture_key"), nullable=False
    )
    from_stage: Mapped[str] = mapped_column(String(30), nullable=False)
    # Equal to from_stage on a 'blocked' decision and on 'auto_double' —
    # neither moves the venture.
    to_stage: Mapped[str] = mapped_column(String(30), nullable=False)
    decision: Mapped[str] = mapped_column(String(20), nullable=False)
    gate_results: Mapped[dict] = mapped_column(
        JSONB, nullable=False, server_default=text("'{}'::jsonb")
    )
    blocked_reasons: Mapped[list] = mapped_column(
        JSONB, nullable=False, server_default=text("'[]'::jsonb")
    )
    actor: Mapped[str] = mapped_column(String(120), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False,
        default=lambda: datetime.now(timezone.utc), server_default=func.now(),
    )

    __table_args__ = (
        Index("ix_venture_ladder_events_key_created", "venture_key", "created_at"),
        Index("ix_venture_ladder_events_key_decision", "venture_key", "decision"),
        CheckConstraint(
            "decision IN ('advanced', 'blocked', 'auto_double', 'demoted')",
            name="ck_venture_ladder_events_decision",
        ),
    )

    def __repr__(self) -> str:
        return (
            f"<VentureLadderEvent(venture_key={self.venture_key!r}, "
            f"{self.from_stage!r}->{self.to_stage!r}, decision={self.decision!r})>"
        )


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
    # Which venture this county belongs to (CLONE-v2.2 / CL3). Pre-CL3 rows
    # are backfilled to venture #1 by the column default.
    venture_key: Mapped[str] = mapped_column(
        String(60),
        ForeignKey("ventures.venture_key"),
        nullable=False,
        server_default="hillsborough_distress",
    )
    fips: Mapped[Optional[str]] = mapped_column(String(10))
    nws_zone: Mapped[Optional[str]] = mapped_column(String(20))
    parcel_id_format: Mapped[Optional[str]] = mapped_column(String(20), default="folio")
    bankruptcy_division: Mapped[Optional[str]] = mapped_column(String(10))
    # 3-digit ZIP prefixes belonging to this county, consumed by
    # county_config.is_zip_in_county(). Empty means "not configured" — the
    # one caller (src/api/main.py) then falls back to the properties table.
    zip_prefixes: Mapped[list] = mapped_column(
        JSONB, nullable=False, server_default=text("'[]'::jsonb")
    )
    city_filer_keywords: Mapped[Optional[dict]] = mapped_column(JSONB, default=list)
    code_lien_type_map: Mapped[Optional[dict]] = mapped_column(JSONB, default=dict)
    # Lowercase city/CDP tokens stripped from address suffixes during
    # normalization. Source of truth for per-county address city stripping —
    # replaces the hardcoded Hillsborough list previously in BaseLoader.
    address_city_tokens: Mapped[Optional[list]] = mapped_column(JSONB, default=list)
    # Task 8 landing conversion features (ADR 0029). Ordered list, rendered as
    # a carousel — reversed from the original single-slot decision (CONTEXT.md).
    landing_featured_testimonials: Mapped[Optional[list]] = mapped_column(JSONB, nullable=True)
    founding_price_deadline_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
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
        Index("idx_counties_venture_key", "venture_key"),
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
    #   playwright_only    — execute cached playwright_code (Playwright driver) only; no AI fallback
    #   playwright_then_ai — try cached code (Playwright driver) first, fall back to AI on failure
    #   nodriver_only      — execute cached playwright_code (nodriver driver) only; no AI fallback
    #   nodriver_then_ai   — try cached code (nodriver driver) first, fall back to AI on failure
    #   nodriver_* is for CF-protected portals where Playwright's CDP fingerprint
    #   re-triggers Cloudflare Turnstile even on a warmed profile (see
    #   docs/MULTI_COUNTY_SCRAPING_ARCHITECTURE.md Section 4). The stored
    #   playwright_code contract is identical either way — execute_playwright_code()
    #   is driver-agnostic, it just hands the code whatever page-like object the
    #   engine launched (Playwright Page or nodriver Tab).
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
    # QUALITY-v2.2 Q4 — named-alternate source failover plumbing (decision
    # A2-revised / E3-revised). Both alternate_* columns start NULL and stay
    # NULL until a real backup source is researched and named for this
    # (county, signal_type) — out of scope for this build. active_source
    # flips to 'alternate' only when heartbeat_monitor.py's SLA-breach hook
    # (src/services/source_failover.py:maybe_failover) finds a non-NULL
    # alternate_url at the moment of a genuinely new stale alert.
    alternate_source_name: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    alternate_url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    active_source: Mapped[str] = mapped_column(
        String(10), nullable=False, default="primary", server_default="primary",
    )
    failover_confidence_penalty: Mapped[int] = mapped_column(
        Integer, nullable=False, default=20, server_default="20",
    )
    switched_to_alternate_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True,
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
            "scrape_mode IN ('ai_only','playwright_only','playwright_then_ai',"
            "'nodriver_only','nodriver_then_ai','static_download','api')",
            name="ck_county_sources_scrape_mode",
        ),
        CheckConstraint(
            "active_source IN ('primary','alternate')",
            name="ck_county_sources_active_source",
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

    # Raw email extracted at load time (pre-Clay-enrichment). Populated by the
    # loader; distinct from work_email (Clay-sourced) below.
    email: Mapped[Optional[str]] = mapped_column(String(200))
    phone: Mapped[Optional[str]] = mapped_column(String(20))
    mobile_phone: Mapped[Optional[str]] = mapped_column(String(20))
    landline_phone: Mapped[Optional[str]] = mapped_column(String(20))

    enrichment_status: Mapped[str] = mapped_column(String(30), nullable=False, default="pending")
    enrichment_attempted_at: Mapped[Optional[datetime]] = mapped_column(DateTime)

    # Tracerfy submission tracking — set when a batch trace is submitted and
    # billed, cleared once the queue result is polled and persisted. Lets a
    # crashed/interrupted run resume polling an already-paid-for submission
    # instead of resubmitting it (enrichment_status alone can't distinguish
    # "never submitted" from "submitted, awaiting poll").
    tracerfy_queue_id: Mapped[Optional[str]] = mapped_column(String(50))

    # Which trace_type the in-flight tracerfy_queue_id was submitted as
    # ('normal' or 'advanced') — persisted alongside the queue_id so a
    # resumed/crashed run knows how to interpret a miss on resolution
    # (normal miss -> retry address-only; advanced miss -> terminal failed),
    # without re-deriving it from enrichment_status alone.
    tracerfy_mode: Mapped[Optional[str]] = mapped_column(String(10))

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
            "enrichment_status IN ('pending', 'enriched', 'failed', 'skipped', "
            "'tracerfy_submitted', 'awaiting_address_only')",
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


class ReferralProspect(Base):
    """
    Section 7.3 — the one-question referral ask inside onboarding: "who is
    one good contractor you know in a county we haven't opened yet?"

    Deliberately NOT a WaitlistEntry: the referring subscriber gives a name,
    company, and target county for someone else — they don't have that
    person's email or phone, which WaitlistEntry requires (nullable=False).
    This is a lightweight lead list, not a notify-on-launch subscription —
    "so when a county launches, its first outreach list already exists"
    means ops pulls these rows for that county, not an automated SMS/email.

    One row per (referring_subscriber_id, target_county_id): a subscriber
    referring the same county twice (retry, resubmit) updates the existing
    row rather than stacking duplicates — see submit_onboarding's upsert.
    """
    __tablename__ = "referral_prospects"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    referring_subscriber_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("subscribers.id"), nullable=False, index=True
    )
    prospect_name: Mapped[str] = mapped_column(String(120), nullable=False)
    prospect_company: Mapped[Optional[str]] = mapped_column(String(120))
    target_county_id: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        UniqueConstraint(
            "referring_subscriber_id", "target_county_id",
            name="uq_referral_prospects_subscriber_county",
        ),
    )

    def __repr__(self) -> str:
        return (f"<ReferralProspect(id={self.id}, "
                f"referring_subscriber_id={self.referring_subscriber_id}, "
                f"target_county_id={self.target_county_id!r})>")


class NonBuyerNurtureSequence(Base):
    """
    One row per email — the per-email suppression/state list for the non-buyer
    nurture sequence (free-signup / checkout-abandon / waitlist leads who
    haven't converted). Terminal states block re-enrollment forever (v1: once
    per email, ever).
    """
    __tablename__ = "non_buyer_nurture_sequences"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    email: Mapped[str] = mapped_column(String(255), nullable=False, unique=True, index=True)
    subscriber_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("subscribers.id", ondelete="SET NULL"), nullable=True, index=True
    )
    source: Mapped[str] = mapped_column(String(20), nullable=False)  # free_signup | checkout_abandon | waitlist
    captured_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    instantly_campaign_id: Mapped[Optional[str]] = mapped_column(String(100))
    instantly_lead_id: Mapped[Optional[str]] = mapped_column(String(100))
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="eligible", server_default="eligible", index=True)
    eligible_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), index=True)
    enrolled_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    removed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    removal_reason: Mapped[Optional[str]] = mapped_column(String(40))
    converted_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )

    __table_args__ = (
        CheckConstraint(
            # 'in_recovery' — held out of nurture while an active checkout-recovery
            # sequence (Task 7) owns the contact; released back to 'eligible' when
            # recovery fails. Non-'eligible' → excluded by find_candidates.
            "status IN ('eligible','in_recovery','enrolled','converted','unsubscribed','bounced','removed')",
            name="ck_non_buyer_nurture_status",
        ),
        CheckConstraint(
            "removal_reason IS NULL OR removal_reason IN "
            "('paid_conversion','unsubscribe','bounce','manual','campaign_removed')",
            name="ck_non_buyer_nurture_removal_reason",
        ),
        CheckConstraint(
            "source IN ('free_signup','checkout_abandon','waitlist')",
            name="ck_non_buyer_nurture_source",
        ),
    )

    def __repr__(self) -> str:
        return f"<NonBuyerNurtureSequence(id={self.id}, email={self.email}, status={self.status})>"


class CheckoutRecovery(Base):
    """
    Abandoned-checkout recovery sequence — one row per email (Task 7), reused
    across episodes: a closed (recovered/failed) row is reopened rather than
    blocking the next abandonment for that email.

    Covers three drop-off sources: a Stripe checkout session that expired
    without payment (`session_expired`), a buyer who provisioned a
    pre-checkout intent but never paid (`pre_payment`), and an abandoned lead
    pack PaymentIntent (`lead_pack`). A fast, high-intent "finish your
    purchase" sequence — distinct from the slower non-buyer nurture drip. While
    a row is `active`, the sibling non_buyer_nurture row is held at
    `in_recovery` so the two flows never double-contact the same person; on
    `failed` the nurture row is released to `eligible`.
    """
    __tablename__ = "checkout_recovery"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    email: Mapped[str] = mapped_column(String(255), nullable=False, unique=True, index=True)
    subscriber_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("subscribers.id", ondelete="SET NULL"), nullable=True, index=True
    )
    phone: Mapped[Optional[str]] = mapped_column(String(20))
    source: Mapped[str] = mapped_column(String(20), nullable=False)  # session_expired | pre_payment | lead_pack
    status: Mapped[str] = mapped_column(
        String(20), nullable=False, default="active", server_default="active", index=True
    )  # active | recovered | failed
    touches_sent: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    # Context needed to mint a FRESH resume-checkout link — the expired Stripe
    # session can't be reused, so recovery rebuilds checkout from these.
    resume_context: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    first_touch_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    last_touch_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), index=True)
    closed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    # B1-04: stamped once the founder SMS fires for this episode (at the first
    # confirmed-abandonment sweep touch, not at row creation) so retried
    # sweeps/reopened episodes don't re-alert.
    founder_alerted_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )

    __table_args__ = (
        CheckConstraint(
            "status IN ('active','recovered','failed')",
            name="ck_checkout_recovery_status",
        ),
        CheckConstraint(
            "source IN ('session_expired','pre_payment','lead_pack')",
            name="ck_checkout_recovery_source",
        ),
    )

    def __repr__(self) -> str:
        return f"<CheckoutRecovery(id={self.id}, email={self.email}, status={self.status}, touches={self.touches_sent})>"


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


class DealOfTheDay(Base):
    """
    T-B12-07 — Daily exclusive-unlock deal. One row per calendar date, picking
    the top-CDS qualified lead not yet delivered (no sent_leads row anywhere,
    never previously featured). 24h exclusive unlock window at STANDARD price
    (scarcity mechanic, not a discount).
    """
    __tablename__ = "deal_of_the_day"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    date: Mapped[date] = mapped_column(Date, nullable=False, unique=True, index=True)
    lead_id: Mapped[int] = mapped_column(ForeignKey("properties.id"), nullable=False, index=True)
    window_start: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    window_end: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )

    __table_args__ = (
        UniqueConstraint("date", name="uq_deal_of_the_day_date"),
        Index("idx_deal_of_the_day_window", "window_start", "window_end"),
    )

    def __repr__(self) -> str:
        return f"<DealOfTheDay(date={self.date}, lead_id={self.lead_id})>"


class WinbackOffer(Base):
    """
    T-B12-07 — Tier3 win-back redemption token.

    Created when a tier3_winback reactivation message is SENT (not when it's
    redeemed) so the outbound link can carry a token that, when it comes back
    through checkout, proves this specific offer — not just "a message went
    out" — is what triggers the promised benefit:
      zip_held     — 50% off the return month (Stripe coupon applied at
                      checkout session creation, gated on a valid token).
      zip_released — 5 free credits, granted only when the checkout webhook
                      redeems the token (i.e. the subscriber actually paid),
                      never at send time.
    One-time use: `redeemed_at` is set exactly once; a second redemption
    attempt on the same token is a no-op.

    `redeemed_at` and `credits_granted_at` are deliberately separate columns
    (PR #172 review fix): the webhook's credit grant is a best-effort side
    effect that can itself fail (wallet write error, transient DB issue).
    If `redeemed_at` alone marked completion, a failed grant would still
    look "done" — the token is spent and a webhook retry finds nothing left
    to redeem, so the customer paid but never got their credits, with no
    path to recover. Keeping the two separate lets a periodic reconciliation
    sweep (`winback_offers.reconcile_pending_credit_grants`) find and retry
    exactly the rows that redeemed successfully but never got credited.
    """
    __tablename__ = "winback_offers"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    subscriber_id: Mapped[int] = mapped_column(ForeignKey("subscribers.id"), nullable=False, index=True)
    branch: Mapped[str] = mapped_column(String(20), nullable=False)  # zip_held | zip_released
    token: Mapped[str] = mapped_column(String(43), nullable=False, unique=True, index=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    redeemed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    credits_granted_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        Index("idx_winback_offers_subscriber_branch", "subscriber_id", "branch"),
    )

    def __repr__(self) -> str:
        return f"<WinbackOffer(subscriber_id={self.subscriber_id}, branch={self.branch}, redeemed={self.redeemed_at is not None})>"


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
    dbpr_contact_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("dbpr_contacts.id"), nullable=True, index=True)
    call_id: Mapped[Optional[str]] = mapped_column(String(100), nullable=True, unique=True)
    transcript_text: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    recording_url: Mapped[Optional[str]] = mapped_column(String(2000), nullable=True)
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


class GoldenCloseChain(Base):
    """
    CLONE-v2.2 CL2 — one row per closed deal, holding the full winning
    chain (first signal -> enrichment -> first outreach -> objections
    handled -> call -> proposal -> payment -> account expansion) as a
    portable, queryable record so a second venture spun up off this same
    agent fleet inherits proven patterns instead of starting from a blank
    slate.

    Deliberately denormalized (chain_stages JSONB) rather than requiring a
    consumer to re-join deal_outcomes/deal_pipeline_events/closer_calls/
    message_outcomes/platform_revenue_ledger itself — those remain each
    stage's own source of truth; this table is a point-in-time assembled
    snapshot, same relationship LifecyclePlaybook has to the tables it
    summarizes.

    schema_version + venture exist for THIS table's own future revisions
    (a second venture, or a later CL2 schema change) — not for merging
    with LEARN-v2.2 / L4's golden-close data model. Checked directly:
    that reconciliation doesn't actually work. This table tracks a
    subscriber's own real-estate deal, reported after they're already a
    paying customer (deal_id is a required FK to deal_outcomes). LEARN's
    L4 golden-close chains track the opposite: how a cold, not-yet-a-
    customer prospect became one through Cora's outreach — there is no
    deal_outcomes row for that yet, because the person isn't a customer
    yet. Two different real-world events that happen to share a name.
    L4 owns its own separate table, keyed on opportunity_thread_id, not
    deal_id — do not add a nullable opportunity_thread_id + XOR
    constraint here to force a merge; that recreates the exact coupling
    problem AbTest/AbAssignment had before the Agent Lane / Lifecycle
    schema split (see AgentLaneExperiment's docstring). If a Clone-Pack
    needs both kinds of proven pattern, the packaging step reads from
    both tables — the tables themselves stay separate.
    """
    __tablename__ = "golden_close_chains"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    deal_id: Mapped[int] = mapped_column(Integer, ForeignKey("deal_outcomes.id"), nullable=False, index=True)
    venture: Mapped[str] = mapped_column(String(60), nullable=False, server_default=text("'hillsborough_distress'"))
    schema_version: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("1"))

    subscriber_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("subscribers.id"), nullable=True, index=True)
    property_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("properties.id"), nullable=True, index=True)
    county_id: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    distress_type: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    buyer_vertical: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    offer_step: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)

    deal_amount: Mapped[Optional[float]] = mapped_column(Numeric(12, 2), nullable=True)
    days_to_close: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    # Ordered list of {stage, occurred_at, source_table, source_id, summary}
    # dicts — 'first_signal','enrichment','first_outreach','objection_handled',
    # 'call','proposal','payment','account_expansion'. Not every deal has
    # every stage (e.g. no objections raised); consumers should not assume a
    # fixed length.
    chain_stages: Mapped[list] = mapped_column(JSONB, nullable=False, server_default=text("'[]'::jsonb"))

    status: Mapped[str] = mapped_column(String(20), nullable=False, server_default=text("'draft'"))
    authored_by: Mapped[str] = mapped_column(String(120), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False,
    )

    __table_args__ = (
        CheckConstraint(
            "status IN ('draft','verified','promoted_to_playbook','retired')",
            name="check_golden_close_chain_status",
        ),
        Index(
            "uq_golden_close_chains_deal_venture", "deal_id", "venture",
            unique=True,
        ),
        Index("idx_golden_close_chains_cell", "county_id", "distress_type", "buyer_vertical", "offer_step"),
        Index("idx_golden_close_chains_venture_status", "venture", "status"),
    )

    def __repr__(self) -> str:
        return f"<GoldenCloseChain(deal_id={self.deal_id}, venture={self.venture}, status={self.status})>"


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
    proving_baseline_conv_rate: Mapped[Optional[float]] = mapped_column(Numeric(14, 6))
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
    old_conversion_rate: Mapped[Optional[float]] = mapped_column(Numeric(14, 6))
    new_conversion_rate: Mapped[Optional[float]] = mapped_column(Numeric(14, 6))
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
        # Case-insensitive lookups — invite _already_subscribed() and the
        # invite-conversion join both match on LOWER(email); the plain btree
        # above can't serve those, this functional index can.
        Index("idx_bkalert_sub_email_lower", text("lower(email)")),
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

    Deliberately separate from `agent_decisions` (which is Lifecycle-only): a closer
    call is a human action, not a Lifecycle Touch. See ADR
    "closer-telemetry-separate-from-agent-decisions".
    """
    __tablename__ = "closer_calls"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    # Identity & correlation. Exactly one of subscriber_id/buyer_entity_id is
    # set (ck_closer_calls_one_identity) — a call is either with an existing
    # customer or a cold whale prospect sourced from Hunter's ranked queue
    # (item 49), never both, never neither.
    aircall_call_id: Mapped[str] = mapped_column(String(40), nullable=False, unique=True, index=True)
    subscriber_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("subscribers.id"), nullable=True, index=True
    )
    buyer_entity_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("buyer_entities.id"), nullable=True, index=True
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
        Index("idx_closer_calls_buyer_entity", "buyer_entity_id"),
        Index("idx_closer_calls_closer_started", "closer_aircall_user_id", "started_at"),
        Index("idx_closer_calls_tagged_at", "tagged_at"),
        CheckConstraint(
            "(subscriber_id IS NOT NULL) != (buyer_entity_id IS NOT NULL)",
            name="ck_closer_calls_one_identity",
        ),
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
            "'delivery.sent',"
            "'outcome.recorded'"
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


class MarketingSpend(Base):
    """Manually-entered ad spend, for channels with no stored cost (Block 4).

    Quora (`QuoraTopic.cumulative_spend`) and affiliate commissions
    (`affiliate_payout_ledger`) already track real cost and are read directly
    by the CAC/payback compiler — they are NOT entered here. `channel` MUST
    use the same vocabulary as the compiler's channel key
    (COALESCE(subscribers.utm_source, subscribers.signup_source)), enforced
    by the admin route's allow-list, or the spend↔revenue join silently
    misses and CAC breaks.
    """
    __tablename__ = "marketing_spend"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    channel: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    campaign_key: Mapped[Optional[str]] = mapped_column(Text)
    period_start: Mapped[date] = mapped_column(Date, nullable=False)
    period_end: Mapped[date] = mapped_column(Date, nullable=False)
    amount_cents: Mapped[int] = mapped_column(Integer, nullable=False)
    currency: Mapped[str] = mapped_column(Text, nullable=False, server_default=text("'usd'"))
    notes: Mapped[Optional[str]] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )

    __table_args__ = (
        UniqueConstraint(
            "channel", "campaign_key", "period_start", "period_end",
            name="uq_marketing_spend_period",
        ),
        CheckConstraint("amount_cents >= 0", name="ck_marketing_spend_amount_nonneg"),
        CheckConstraint("period_end >= period_start", name="ck_marketing_spend_period_order"),
        Index("idx_marketing_spend_period", "period_start", "period_end"),
    )

    def __repr__(self) -> str:
        return f"<MarketingSpend(channel={self.channel}, period={self.period_start}..{self.period_end}, cents={self.amount_cents})>"


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
    notified_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), index=True)

    __table_args__ = (
        UniqueConstraint("property_id", "account_id", name="uq_delivery_property_account"),
        CheckConstraint("status IN ('delivered','rejected')", name="ck_delivery_status"),
        CheckConstraint(
            "rejection_reason IS NULL OR rejection_reason IN "
            "('disconnected','wrong_party','deceased','duplicate','other',"
            "'sold_before_delivery','signals_resolved')",
            name="ck_delivery_reason",
        ),
        Index("idx_deliveries_account_grade_cycle", "account_id", "grade", "billing_period_end"),
        Index("idx_deliveries_status", "status"),
        Index("idx_deliveries_delivered_at", "delivered_at"),
    )

    def __repr__(self) -> str:
        return f"<Delivery(property_id={self.property_id}, account_id={self.account_id}, grade={self.grade}, status={self.status})>"


class GuaranteeCredit(Base):
    """Tiered volume guarantee (config/guarantees.py): one row per subscriber
    per evaluated ~30-day cycle, written by
    src/tasks/guarantee_shortfall_sweep.py. The (subscriber_id, period_end)
    unique constraint lets a cycle be claimed with a 'pending' row (INSERT ..
    ON CONFLICT) before Stripe is called — 'pending'/'failed' rows are not
    terminal and are retried in place rather than skipped.
    """
    __tablename__ = "guarantee_credits"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    subscriber_id: Mapped[int] = mapped_column(Integer, ForeignKey("subscribers.id"), nullable=False, index=True)
    period_start: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    period_end: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    tier: Mapped[str] = mapped_column(String(20), nullable=False)
    quota: Mapped[int] = mapped_column(Integer, nullable=False)
    delivered: Mapped[int] = mapped_column(Integer, nullable=False)
    shortfall: Mapped[int] = mapped_column(Integer, nullable=False)
    credit_cents: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    stripe_balance_txn_id: Mapped[Optional[str]] = mapped_column(String(100))
    # pending (claimed, Stripe call in flight/retryable) | met (no shortfall)
    # | issued | failed (retryable) | skipped_no_charge_basis
    status: Mapped[str] = mapped_column(String(30), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    __table_args__ = (
        UniqueConstraint("subscriber_id", "period_end", name="uq_guarantee_credit_subscriber_period"),
        CheckConstraint(
            "status IN ('pending', 'met', 'issued', 'failed', 'skipped_no_charge_basis')",
            name="ck_guarantee_credit_status",
        ),
        Index("idx_guarantee_credits_subscriber_period", "subscriber_id", "period_end"),
    )

    def __repr__(self) -> str:
        return f"<GuaranteeCredit(subscriber={self.subscriber_id}, period_end={self.period_end}, status={self.status})>"


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


# ============================================================================
# A6 — Closer-to-Lifecycle Teaching Interface (Sprint A6)
# ============================================================================

class LifecycleTrainingOverride(Base):
    """
    Human corrections from the Closer Cockpit Teach action (A6) and future
    feedback rituals (4.3).  Serves two purposes simultaneously:
      1. Score Dampener — dampener_active=True suppresses the property's CDS score
         via type-keyed gate logic in cds_engine.score_property().
      2. Fine-tuning label queue — queue_status tracks the row from collection
         through future export to a training pipeline.

    See ADR 0006 (dampener = gate-reuse, not multiplier) and ADR 0007 (shared
    polymorphic schema, row-as-queue).
    """
    __tablename__ = "lifecycle_training_overrides"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    # Discriminator — which workflow produced this correction.
    source: Mapped[str] = mapped_column(String(30), nullable=False)

    # Polymorphic subject — stored as an opaque string ref so A6 property ids and
    # 4.3 agent decision ids can share one queue table.
    subject_type: Mapped[str] = mapped_column(String(30), nullable=False)
    subject_ref: Mapped[str] = mapped_column(String(80), nullable=False)

    # Optional provenance: which closer call triggered this correction.
    closer_call_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("closer_calls.id", ondelete="SET NULL"), nullable=True
    )

    # Correction details. A6 writes these at creation time; 4.3 fills them after
    # human review, so they may start null for queued feedback ritual rows.
    correction_reason: Mapped[Optional[str]] = mapped_column(String(40), nullable=True)
    signal_type: Mapped[Optional[str]] = mapped_column(String(40), nullable=True)
    note: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    corrected_output: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Dampener state — False for bad_contact/other (label-only corrections).
    dampener_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)

    # Fine-tuning queue state.
    queue_status: Mapped[str] = mapped_column(String(20), nullable=False, default="pending")
    review_outcome: Mapped[Optional[str]] = mapped_column(String(30), nullable=True)
    reviewed_by: Mapped[Optional[str]] = mapped_column(String(120), nullable=True)
    reviewed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    snapshot_payload: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    source_metadata: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)

    created_by: Mapped[str] = mapped_column(String(120), nullable=False)
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
        # Hot-path read: engine fetches active corrections per property at score time.
        Index(
            "idx_lifecycle_overrides_subject_active",
            "subject_type", "subject_ref", "dampener_active",
        ),
        # Queue consumer reads pending rows.
        Index("idx_lifecycle_overrides_queue_status", "queue_status"),
        # A6 duplicate protection: one active property correction per reason/signal.
        Index(
            "uq_lifecycle_override_active",
            "subject_ref",
            "correction_reason",
            text("COALESCE(signal_type, '')"),
            unique=True,
            postgresql_where=text(
                "dampener_active AND subject_type = 'property' AND correction_reason IS NOT NULL"
            ),
        ),
        # 4.3 duplicate protection: one queue row per reviewed Lifecycle Touch.
        Index(
            "uq_lifecycle_feedback_ritual_subject",
            "subject_type",
            "subject_ref",
            unique=True,
            postgresql_where=text("source = 'feedback_ritual'"),
        ),
        CheckConstraint(
            "source IN ('closer_teach', 'feedback_ritual')",
            name="ck_lifecycle_overrides_source",
        ),
        CheckConstraint(
            "queue_status IN ('pending', 'exported', 'discarded')",
            name="ck_lifecycle_overrides_queue_status",
        ),
        CheckConstraint(
            "review_outcome IS NULL OR review_outcome IN ('approved', 'needs_correction', 'discarded')",
            name="ck_lifecycle_overrides_review_outcome",
        ),
    )

    def __repr__(self) -> str:
        return (
            f"<LifecycleTrainingOverride(id={self.id}, subject={self.subject_type}:{self.subject_ref}, "
            f"reason={self.correction_reason}, active={self.dampener_active}, "
            f"queue={self.queue_status})>"
        )

    @property
    def subject_id(self):
        """Backward-compat shim for A6 property corrections."""
        if self.subject_ref is None:
            return None
        if self.subject_type == "property":
            try:
                return int(self.subject_ref)
            except (TypeError, ValueError):
                return self.subject_ref
        return self.subject_ref

    @subject_id.setter
    def subject_id(self, value):
        self.subject_ref = None if value is None else str(value)


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


# ============================================================================
# C2 / M5 — CDS Score Feedback (S1 / 414 Stream A)
# ============================================================================


class ScoreFeedback(Base):
    """
    CDS scoring feedback loop — one row per prospect outcome (spec §4.4).

    Records what the model predicted (predicted_tier from the Truth Engine verdict)
    vs what actually happened on a homeowner call (realized_outcome). Delta is the
    gap between predicted rate and actual rate — fed to scoring_fit.py for retraining.

    closer_call_id is nullable: homeowner outbound calling is not built yet.
    It will be populated and wired when that system is implemented.

    Grade names follow GRADE_ORDER from config/grading.py:
        sub_grade < Bronze < Silver < Gold < Platinum < Ultra
    Always use 'Ultra' — distress_scores.lead_tier uses the legacy 'Ultra Platinum'.
    """
    __tablename__ = "score_feedback"

    score_id: Mapped[Any] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text("generate_uuidv7()"),
    )
    prospect_id: Mapped[Any] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("prospects.prospect_id", ondelete="RESTRICT"),
        nullable=False,
        unique=True,
    )
    closer_call_id: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    predicted_tier: Mapped[str] = mapped_column(String, nullable=False)
    predicted_rate: Mapped[Optional[Decimal]] = mapped_column(Numeric(6, 4), nullable=True)
    realized_outcome: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    # B0-02 Outcome Sanity Filter: raw buyer-capacity reason on a shielded death
    # (low_fico / no_capital). NULL for every normal row. The buyer_could_not_act
    # tag is derived (this column IS NOT NULL); realized_outcome is left NULL so
    # the row is excluded from all rate/training consumers.
    buyer_could_not_act_reason: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    delta: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    scored_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("NOW()"),
    )
    resolved_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True,
    )

    __table_args__ = (
        CheckConstraint(
            "predicted_tier IN ('Bronze','Silver','Gold','Platinum','Ultra','sub_grade')",
            name="ck_score_feedback_predicted_tier",
        ),
        CheckConstraint(
            "realized_outcome IS NULL OR "
            "realized_outcome IN ('contacted','converted','funded','dead')",
            name="ck_score_feedback_realized_outcome",
        ),
        Index("idx_score_feedback_prospect_id", "prospect_id"),
        Index("idx_score_feedback_predicted_tier", "predicted_tier"),
    )

    def __repr__(self) -> str:
        return (
            f"<ScoreFeedback(prospect_id={self.prospect_id}, "
            f"predicted_tier='{self.predicted_tier}', outcome='{self.realized_outcome}')>"
        )


# ============================================================================
# A7 — Macro Signal
# ============================================================================

class MacroSignal(Base):
    """External macroeconomic signal (FRED, FHFA, BLS, Census ACS5).

    One row per (source, signal_key, source_series_id, observed_at,
    geography_scope, geography_id) observation. Upserted idempotently.
    """
    __tablename__ = "macro_signals"

    id: Mapped[str] = mapped_column(
        PG_UUID(as_uuid=False),
        primary_key=True,
        server_default=text("generate_uuidv7()"),
    )
    source:            Mapped[str] = mapped_column(String(30), nullable=False)
    signal_key:        Mapped[str] = mapped_column(String(80), nullable=False)
    source_series_id:  Mapped[str] = mapped_column(String(120), nullable=False)
    value:             Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False)
    unit:              Mapped[str] = mapped_column(String(30), nullable=False)
    observed_at:       Mapped[date] = mapped_column(Date, nullable=False)
    frequency:         Mapped[str] = mapped_column(String(20), nullable=False)
    geography_scope:   Mapped[str] = mapped_column(String(50), nullable=False)
    geography_id:      Mapped[str] = mapped_column(String(30), nullable=False)
    raw_payload:       Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    created_at:        Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at:        Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), onupdate=func.now()
    )

    __table_args__ = (
        UniqueConstraint(
            "source", "signal_key", "source_series_id",
            "observed_at", "geography_scope", "geography_id",
            name="uq_macro_signal_observation",
        ),
        Index("ix_macro_signals_source", "source"),
        Index("ix_macro_signals_signal_key", "signal_key"),
        Index("ix_macro_signals_observed_at", "observed_at"),
        Index("ix_macro_signals_source_key_date", "source", "signal_key", "observed_at"),
        Index("ix_macro_signals_geo", "geography_scope", "geography_id"),
        Index("ix_macro_signals_source_geo", "source", "geography_scope", "geography_id"),
    )

    def __repr__(self) -> str:
        return (
            f"<MacroSignal(source={self.source!r}, key={self.signal_key!r}, "
            f"geo={self.geography_id!r}, obs={self.observed_at})>"
        )


class CompetitorRateSheet(Base):
    """Task 4.8 — a scraped competitor lender rate sheet (one row per scrape).

    Stores published DSCR/private loan terms from public FL lender pages.
    `high_margin_target` is NOT stored here — it is computed live at report time
    against forced_action_lender_terms (see src/services/competitor_benchmark.py).
    """
    __tablename__ = "competitor_rate_sheets"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    lender_name: Mapped[str] = mapped_column(String(128), nullable=False)
    product: Mapped[str] = mapped_column(String(24), nullable=False)
    region: Mapped[Optional[str]] = mapped_column(String(64))  # metro/city tag, NULL = FL-statewide
    rate_low: Mapped[Optional[float]] = mapped_column(Numeric(6, 3))
    rate_high: Mapped[Optional[float]] = mapped_column(Numeric(6, 3))
    max_ltv: Mapped[Optional[float]] = mapped_column(Numeric(5, 2))
    min_fico: Mapped[Optional[int]] = mapped_column(Integer)
    min_dscr: Mapped[Optional[float]] = mapped_column(Numeric(4, 2))
    points: Mapped[Optional[float]] = mapped_column(Numeric(4, 2))
    prepay: Mapped[Optional[str]] = mapped_column(String(64))
    term_months: Mapped[Optional[int]] = mapped_column(Integer)
    hq_location: Mapped[Optional[str]] = mapped_column(String(96))
    source_url: Mapped[str] = mapped_column(Text, nullable=False)
    source_adapter: Mapped[str] = mapped_column(String(48), nullable=False)
    confidence: Mapped[str] = mapped_column(String(16), nullable=False, default="high")
    raw_text: Mapped[Optional[str]] = mapped_column(Text)
    captured_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc))

    __table_args__ = (
        CheckConstraint("product IN ('dscr','private')", name="check_crs_product"),
        CheckConstraint("confidence IN ('high','low')", name="check_crs_confidence"),
        UniqueConstraint("lender_name", "product", "region", "captured_at", name="uq_competitor_rate_sheet"),
        Index("idx_competitor_rate_sheets_product_region", "product", "region"),
    )


class ForcedActionLenderTerms(Base):
    """Task 4.8 — Forced Action's own rate card (one row per loan product).

    The comparison baseline for high-margin-target classification. Admin-editable.
    """
    __tablename__ = "forced_action_lender_terms"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    product: Mapped[str] = mapped_column(String(24), nullable=False, unique=True)
    rate: Mapped[float] = mapped_column(Numeric(6, 3), nullable=False)
    max_ltv: Mapped[float] = mapped_column(Numeric(5, 2), nullable=False)
    points: Mapped[Optional[float]] = mapped_column(Numeric(4, 2))
    prepay: Mapped[Optional[str]] = mapped_column(String(64))
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc))

    __table_args__ = (
        CheckConstraint("product IN ('dscr','private')", name="check_falt_product"),
    )


# ============================================================================
# LOAN LANE CORE & BROKER STATE MACHINE (Sprint S1)
# ============================================================================

class LaneStageConfig(Base):
    """Config-as-data lane stage definitions. One row per (lane_type, stage_key).

    Editable with no deploy. `allowed_next` is the legal next-stage set;
    `sms_allowed` gates SMS automation on that stage.
    """

    __tablename__ = "lane_stage_config"

    lane_type: Mapped[str] = mapped_column(String(50), primary_key=True)
    stage_key: Mapped[str] = mapped_column(String(50), primary_key=True)
    display_name: Mapped[str] = mapped_column(String(100), nullable=False)
    order_index: Mapped[int] = mapped_column(Integer, nullable=False)
    allowed_next: Mapped[list] = mapped_column(JSONB, nullable=False, server_default=text("'[]'::jsonb"))
    sms_allowed: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"))
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("true"))


class Broker(Base):
    """A logged-in platform user (role='broker') who works assigned Loan Lanes.

    Modeled on white_label_users. Self-records work-state transitions; sees only
    own lanes (RBAC). Distinct from the config funding_broker_terms rate config.
    """

    __tablename__ = "brokers"

    broker_id: Mapped[str] = mapped_column(
        PG_UUID(as_uuid=True), primary_key=True, server_default=text("generate_uuidv7()")
    )
    email: Mapped[str] = mapped_column(String(255), nullable=False, unique=True, index=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    password_hash: Mapped[Optional[str]] = mapped_column(String(255))
    role: Mapped[str] = mapped_column(String(20), nullable=False, server_default=text("'broker'"))
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("true"))
    reset_token: Mapped[Optional[str]] = mapped_column(String(64), index=True)
    reset_token_expires_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    last_login_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )

    __table_args__ = (
        CheckConstraint("role = 'broker'", name="ck_brokers_role"),
    )

    def __repr__(self) -> str:
        return f"<Broker(broker_id={self.broker_id!r}, email={self.email!r}, active={self.is_active})>"


class Lender(Base):
    """Admin-curated lender reference used by Loan Lane deal tracking."""

    __tablename__ = "lenders"

    lender_id: Mapped[str] = mapped_column(
        PG_UUID(as_uuid=True), primary_key=True, server_default=text("generate_uuidv7()")
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False, unique=True, index=True)
    is_cleared: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"))
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("true"))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class Lane(Base):
    """Loan Lane funnel record — one per property/prospect routed to loan_lane channel."""

    __tablename__ = "lanes"

    lane_id: Mapped[str] = mapped_column(
        PG_UUID(as_uuid=True), primary_key=True, server_default=text("generate_uuidv7()")
    )
    property_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("properties.id", name="fk_lanes_property_id"), nullable=False
    )
    lane_type: Mapped[str] = mapped_column(String(50), nullable=False)
    loan_program: Mapped[Optional[str]] = mapped_column(String(50))
    current_stage: Mapped[str] = mapped_column(String(50), nullable=False)
    outcome: Mapped[str] = mapped_column(
        String(50), nullable=False, server_default=text("'open'")
    )
    assigned_broker_id: Mapped[Optional[str]] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("brokers.broker_id")
    )
    lender_id: Mapped[Optional[str]] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("lenders.lender_id")
    )
    claimed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    last_activity_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    fee_config_flag: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"))
    entered_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        ForeignKeyConstraint(
            ["lane_type", "current_stage"],
            ["lane_stage_config.lane_type", "lane_stage_config.stage_key"],
            name="fk_lanes_stage_config",
        ),
        CheckConstraint(
            "outcome IN ('open','funded','dead','recycled')",
            name="ck_lanes_outcome",
        ),
        UniqueConstraint("property_id", "lane_type", name="uq_lanes_property_lane_type"),
        Index("idx_lanes_open", "outcome", postgresql_where=text("outcome = 'open'")),
        Index("idx_lanes_broker", "assigned_broker_id"),
    )


class BrokerTransition(Base):
    """Append-only broker work-state audit log. Latest row's to_state is current state."""

    __tablename__ = "broker_transitions"

    transition_id: Mapped[str] = mapped_column(
        PG_UUID(as_uuid=True), primary_key=True, server_default=text("generate_uuidv7()")
    )
    lane_id: Mapped[str] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("lanes.lane_id"), nullable=False
    )
    broker_id: Mapped[str] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("brokers.broker_id"), nullable=False
    )
    from_state: Mapped[str] = mapped_column(String(50), nullable=False)
    to_state: Mapped[str] = mapped_column(String(50), nullable=False)
    reason_code: Mapped[str] = mapped_column(String(100), nullable=False)
    actor: Mapped[str] = mapped_column(String(255), nullable=False)
    occurred_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        CheckConstraint(
            "to_state IN ('unassigned','assigned','working','quoted','committed','closed_won','closed_lost')",
            name="ck_bt_to_state",
        ),
        Index("idx_bt_lane_id", "lane_id"),
        Index("idx_bt_broker_id", "broker_id"),
        Index("idx_bt_lane_occurred", "lane_id", "occurred_at"),
    )


class LaneFeeConfigAudit(Base):
    """Append-only audit log for every fee_config_flag flip (RESPA gate) on a lane."""

    __tablename__ = "lane_fee_config_audit"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    lane_id: Mapped[str] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("lanes.lane_id"), nullable=False
    )
    previous_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False)
    new_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False)
    actor: Mapped[str] = mapped_column(String(255), nullable=False)
    occurred_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        Index("idx_lfca_lane_occurred", "lane_id", "occurred_at"),
    )


class CommissionSplit(Base):
    """Config-as-data commission allocation. `parties` is a list of {party, pct}.

    Deal-size fee tiers (Task 3.2 / ADR 0031) are rows here, not code: an active
    split matches a deal when `min_gross_cents <= gross < max_gross_cents`
    (`max_gross_cents` NULL = unbounded). `resolve_split_config()` picks the
    highest-floor matching tier. A new tier is a new row — no schema change.
    """

    __tablename__ = "commission_splits"

    split_config_id: Mapped[str] = mapped_column(String(100), primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    parties: Mapped[list] = mapped_column(JSONB, nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("true"))
    min_gross_cents: Mapped[int] = mapped_column(BigInteger, nullable=False, server_default=text("0"))
    max_gross_cents: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)


class CommissionLedgerEntry(Base):
    """Append-only broker-earnings ledger. One row per closed_won transition.

    `gross_amount_cents` is manually entered at close; `net_lines` is the gross
    allocated per the CommissionSplit. Never UPDATEd except status; disputes post
    a new offsetting entry. Tracks what brokers EARN — never Stripe billing.
    """

    __tablename__ = "commission_ledger"

    entry_id: Mapped[str] = mapped_column(
        PG_UUID(as_uuid=True), primary_key=True, server_default=text("generate_uuidv7()")
    )
    lane_id: Mapped[str] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("lanes.lane_id"), nullable=False
    )
    broker_id: Mapped[str] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("brokers.broker_id"), nullable=False
    )
    trigger_transition_id: Mapped[Optional[str]] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("broker_transitions.transition_id"),
        nullable=True, unique=True,
    )
    gross_amount_cents: Mapped[int] = mapped_column(BigInteger, nullable=False)
    split_config_id: Mapped[str] = mapped_column(
        String(100), ForeignKey("commission_splits.split_config_id"), nullable=False
    )
    net_lines: Mapped[list] = mapped_column(JSONB, nullable=False)
    status: Mapped[str] = mapped_column(
        String(20), nullable=False, server_default=text("'posted'")
    )
    posted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        CheckConstraint("gross_amount_cents >= 0", name="ck_cl_gross_nonneg"),
        CheckConstraint(
            "status IN ('posted','disputed','reconciled')", name="ck_cl_status"
        ),
        Index("idx_cl_lane_id", "lane_id"),
        Index("idx_cl_broker_id", "broker_id"),
    )
# TAX DEED AUCTIONS  (fa103)
# ============================================================================

class TaxDeedAuction(Base):
    """
    Tax deed sale auction listing scraped from realtaxdeed.com.

    One row per (county_id, auction_date, case_number). property_id is NULL
    when the parcel could not be matched in the properties table.
    Raw fields from the portal are preserved in raw_fields JSONB.
    """
    __tablename__ = "tax_deed_auctions"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    property_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("properties.id", ondelete="SET NULL"), nullable=True, index=True
    )
    county_id: Mapped[str] = mapped_column(String(50), nullable=False)
    parcel_id: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    auction_date: Mapped[date] = mapped_column(Date, nullable=False)
    case_number: Mapped[str] = mapped_column(String(100), nullable=False)
    certificate_number: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    certificate_year: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    status: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    auction_type: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    opening_bid: Mapped[Optional[Decimal]] = mapped_column(Numeric(14, 2), nullable=True)
    sold_amount: Mapped[Optional[Decimal]] = mapped_column(Numeric(14, 2), nullable=True)
    sold_to: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    raw_fields: Mapped[Optional[Any]] = mapped_column(JSONB, nullable=True)
    match_method: Mapped[Optional[str]] = mapped_column(String(30), nullable=True)
    match_confidence: Mapped[Optional[Decimal]] = mapped_column(Numeric(4, 3), nullable=True)
    scraped_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )

    # HUNTER-05 (H5) — per-auction winner-resolution outcome, distinct from
    # match_method above (which is deed-loader property-matching provenance,
    # not buyer-identity resolution). NULL = not yet processed by
    # src/agents/hunter/auction_resolution.py. 'provisional' satisfies the
    # <24h processing SLA without asserting a verified identity — see that
    # module's docstring for why processing and verification are tracked
    # separately.
    buyer_resolution_status: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)

    property: Mapped[Optional["Property"]] = relationship("Property", foreign_keys=[property_id])

    __table_args__ = (
        UniqueConstraint("county_id", "auction_date", "case_number", name="uq_tax_deed_auction"),
        Index("ix_tax_deed_auctions_county_date", "county_id", "auction_date"),
        Index("ix_tax_deed_auctions_parcel_id", "parcel_id"),
        CheckConstraint(
            "buyer_resolution_status IS NULL OR buyer_resolution_status IN ('verified', 'provisional', 'ambiguous')",
            name="check_tax_deed_buyer_resolution_status",
        ),
    )

    def __repr__(self) -> str:
        return (
            f"<TaxDeedAuction(county={self.county_id!r}, date={self.auction_date}, "
            f"case={self.case_number!r}, status={self.status!r})>"
        )


# ============================================================================
# VACANT PARCELS  (fa103)
# ============================================================================

class VacantParcel(Base):
    """
    Current-state vacancy record for a parcel — one row per (county_id, parcel_id).

    Updated in-place on each scrape run. source_name is 'pcpao' (Pinellas) or
    'hcpa' (Hillsborough). Only vacancy classification fields are stored here;
    assessed value, acreage, and owner live in the financials / owners tables.
    """
    __tablename__ = "vacant_parcels"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    property_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("properties.id", ondelete="SET NULL"), nullable=True, index=True
    )
    county_id: Mapped[str] = mapped_column(String(50), nullable=False)
    parcel_id: Mapped[str] = mapped_column(String(100), nullable=False)
    use_code: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    property_use: Mapped[Optional[str]] = mapped_column(String(200), nullable=True)
    dor_code: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    source_name: Mapped[str] = mapped_column(String(20), nullable=False)
    last_verified: Mapped[date] = mapped_column(Date, nullable=False)
    scraped_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )

    property: Mapped[Optional["Property"]] = relationship("Property", foreign_keys=[property_id])

    __table_args__ = (
        UniqueConstraint("county_id", "parcel_id", name="uq_vacant_parcel"),
        Index("ix_vacant_parcels_county_id", "county_id"),
    )

    def __repr__(self) -> str:
        return (
            f"<VacantParcel(county={self.county_id!r}, parcel={self.parcel_id!r}, "
            f"use={self.use_code!r}, source={self.source_name!r})>"
        )


# ============================================================================
# Task 5.2 — Programmatic SEO Engine
# ============================================================================

class SeoPage(Base):
    """Per-page state for the programmatic SEO engine.

    One row per (city_slug, topic_slug) cell. Persists content hash and
    retirement hysteresis between weekly compile_all() runs.
    """
    __tablename__ = "seo_pages"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    city_slug: Mapped[str] = mapped_column(Text, nullable=False)
    topic_slug: Mapped[str] = mapped_column(Text, nullable=False)
    city_raw: Mapped[str] = mapped_column(Text, nullable=False)
    vertical: Mapped[str] = mapped_column(Text, nullable=False)
    url_path: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    content_hash: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    lastmod: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    status: Mapped[str] = mapped_column(Text, nullable=False, default="live")
    below_threshold_runs: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    qualified_count: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    first_published_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_built_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        UniqueConstraint("city_slug", "topic_slug", name="uq_seo_pages_cell"),
        CheckConstraint("status IN ('live', 'noindex', 'retired')", name="ck_seo_pages_status"),
        Index("idx_seo_pages_status", "status"),
    )

    def __repr__(self) -> str:
        return f"<SeoPage(url_path={self.url_path!r}, status={self.status})>"


class ScoringCutoverLog(Base):
    """Stage F audit trail + active-weights pointer for the CDS retune loop.

    One row per cutover attempt. The most recent row with ``applied = true`` is
    the fit artifact the live (non-shadow) scoring engine loads at startup and
    overlays onto config/scoring.py. Rows with ``applied = false`` record a
    blocked attempt (Stage E did not PASS, or the artifact carried thin-data
    coverage warnings) so the decision trail is auditable.
    """
    __tablename__ = "scoring_cutover_log"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False,
        default=lambda: datetime.now(timezone.utc), server_default=func.now(),
    )
    fit_artifact_path: Mapped[str] = mapped_column(Text, nullable=False)
    validation_status: Mapped[str] = mapped_column(String(16), nullable=False)  # PASS / WARN / FAIL / UNKNOWN
    applied: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default="false",
    )
    weights_snapshot: Mapped[Optional[dict]] = mapped_column(JSONB)
    detail: Mapped[Optional[str]] = mapped_column(Text)
    run_id: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)

    __table_args__ = (
        Index("ix_scoring_cutover_log_active", "applied", text("created_at DESC")),
    )

    def __repr__(self) -> str:
        return (
            f"<ScoringCutoverLog(id={self.id}, status={self.validation_status}, "
            f"applied={self.applied})>"
        )


class RelayApprovalQueueItem(Base):
    """One proposed outreach action awaiting (or past) Josh's approval
    (RELAY-v2.2 sub-task R1).

    This table IS "Josh's queue": Cora (Phase 2) will write ``pending`` rows
    here; R1 seeds rows directly (``python -m src.services.relay --seed``)
    to build/prove the engine now — same schema, zero change when Cora
    lands. A pending row is posted to Slack as an interactive
    approve/reject message; the button press (signature-verified webhook)
    flips status to ``approved``/``rejected``. The Relay cron sweep then
    reads ``approved`` rows as a batch and executes them.

    ``idempotency_key`` is UNIQUE — the no-double-send guarantee: a retry,
    a crash-resume, or the sweep re-selecting an already-dispatched row is
    a no-op, and the row is marked ``skipped``, never re-sent. R1 writes
    this row throughout its lifecycle; R4 reuses the same row as the
    Action Completion Receipt (dispatched_at/channel/thread_id/status).
    Written via the normal app DB role; vera_readonly holds SELECT only
    (audits approved-vs-sent), granted conditionally by the migration in
    case that role does not exist yet in this environment.
    """
    __tablename__ = "relay_approval_queue"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    idempotency_key: Mapped[str] = mapped_column(String(120), nullable=False, unique=True)
    # Which venture proposed this action (CLONE-v2.2 / CL3). Scopes the
    # sweep's batch, the Slack channel it is posted to, the Instantly
    # campaign it sends through, and the daily-ceiling counter — without it
    # two ventures would share one send cap and one approval channel.
    venture_key: Mapped[str] = mapped_column(
        String(60),
        ForeignKey("ventures.venture_key"),
        nullable=False,
        server_default="hillsborough_distress",
    )
    batch_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    thread_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)  # OPP-YYYY-#####
    channel: Mapped[str] = mapped_column(String(30), nullable=False)  # noop (R1); email/sms (R2)
    recipient: Mapped[str] = mapped_column(Text, nullable=False)  # phone via phone_utils.normalize
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)  # subject/body/etc — exactly what's proposed/approved
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="pending")
    # pending | approved | rejected | sent | failed | skipped | uncertain
    slack_message_ts: Mapped[Optional[str]] = mapped_column(String(30), nullable=True)
    slack_post_attempted_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    slack_post_lease_until: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    decided_by: Mapped[Optional[str]] = mapped_column(String(120), nullable=True)
    decided_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    dispatched_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    # FA Max WP-2: operating lane, acting agent, and autonomy tier at send time.
    # All three are NULL for non-FA-Max items so existing rows are unchanged.
    lane: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    # MONEY | EXCEPTIONS | RELATIONSHIPS
    agent_name: Mapped[Optional[str]] = mapped_column(String(120), nullable=True)
    autonomy_tier_at_send: Mapped[Optional[str]] = mapped_column(String(1), nullable=True)
    # A | B | C — stamped at dispatch time, never after
    person_id: Mapped[Optional[Any]] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("fa_max_persons.person_id"), nullable=True
    )
    autonomy_gate_reason: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    decision_interaction_id: Mapped[Optional[Any]] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("fa_max_interactions.interaction_id"), nullable=True
    )
    send_interaction_id: Mapped[Optional[Any]] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("fa_max_interactions.interaction_id"), nullable=True
    )
    # WP-T2-2: Snooze / Revise support. eligible_at is distinct from
    # fa_max_work_queue.available_at (a different table's deferred-execution
    # column) — this governs whether THIS row is currently postable /
    # dispatchable. NULL = always eligible.
    eligible_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    original_draft: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    final_content: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    revision_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    last_revised_by: Mapped[Optional[str]] = mapped_column(String(120), nullable=True)
    last_revised_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    material_edit: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    # WP-T2-3: which fa_max_opportunity this queue item targets. Nullable —
    # non-opportunity-linked items (bulk partner touches, EXCEPTIONS lane) skip
    # attribution. When set, mark_sent() writes backflip_attribution_owner on
    # that opportunity row under a WHERE IS NULL guard so concurrent sends are safe.
    opportunity_id: Mapped[Optional[Any]] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("fa_max_opportunities.opportunity_id", ondelete="SET NULL"),
        nullable=True,
    )
    channel_split_source: Mapped[Optional[str]] = mapped_column(String(60), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False,
        default=lambda: datetime.now(timezone.utc), server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False,
        default=lambda: datetime.now(timezone.utc), server_default=func.now(),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    __table_args__ = (
        Index("ix_relay_approval_queue_status", "status"),
        Index("ix_relay_approval_queue_batch_status", "batch_id", "status"),
        Index("ix_relay_approval_queue_venture_status", "venture_key", "status"),
        Index(
            "ix_relay_approval_queue_eligible_at", "eligible_at",
            postgresql_where=text("eligible_at IS NOT NULL"),
        ),
        # CL4: venture_ladder.cell_reply_rates() joins outbound_drafts to this
        # table on (thread_id, venture_key) to count only items that were
        # really dispatched, so the reply rate the auto-double rule scales on
        # is never inflated by approved-but-unsent drafts.
        Index("ix_relay_approval_queue_thread_venture", "thread_id", "venture_key"),
        CheckConstraint(
            "status IN ('pending', 'approved', 'rejected', 'sent', 'failed', 'skipped', 'uncertain')",
            name="ck_relay_approval_queue_status",
        ),
        CheckConstraint(
            "lane IS NULL OR lane IN ('MONEY', 'EXCEPTIONS', 'RELATIONSHIPS')",
            name="ck_relay_approval_queue_lane",
        ),
        CheckConstraint(
            "autonomy_tier_at_send IS NULL OR autonomy_tier_at_send IN ('A', 'B', 'C')",
            name="ck_relay_approval_queue_tier",
        ),
        CheckConstraint(
            "venture_key <> 'fa_max_lending' OR "
            "(lane IS NOT NULL AND agent_name IS NOT NULL AND "
            "autonomy_tier_at_send IS NOT NULL AND person_id IS NOT NULL)",
            name="ck_relay_fa_max_governance_fields",
        ),
        CheckConstraint(
            "venture_key <> 'fa_max_lending' OR "
            "payload::text !~* '(ssn|social.security|credit.score|fico|income|"
            "bank.statement|tax.return|debt.to.income|interest.rate|loan.rate|"
            "loan.term|commitment)'",
            name="ck_relay_fa_max_no_financial_payload",
        ),
        Index("ix_relay_approval_queue_person_id", "person_id"),
    )

    def __repr__(self) -> str:
        return (
            f"<RelayApprovalQueueItem(id={self.id}, status={self.status}, "
            f"channel={self.channel})>"
        )


# ============================================================================
# Hunter — Buyer Entity Resolution (HUNTER-01)
# ============================================================================

class BuyerEntity(Base):
    """
    Canonical buyer identity — one row per real person or LLC, collapsed from
    potentially many `owners` rows (one per property) and `deeds.grantee`
    mentions via src/services/buyer_entity_resolution.py.

    confidence_score is 0-100 (not the 0.000-1.000 scale used by
    Deed.match_confidence) — matches Hunter's constitution wording verbatim
    ("confidence-scored 0-100", "<70 confidence = UNVERIFIED"). Entities below
    the UNVERIFIED threshold must never surface in a Lifecycle draft.

    IDs are stable across nightly re-runs by design — the resolver matches new
    deed/owner activity against existing rows here first and only creates a
    new entity when nothing matches, so downstream references (whale flags,
    Cell #1's ranked list) never silently break.
    """
    __tablename__ = "buyer_entities"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    canonical_name: Mapped[str] = mapped_column(Text, nullable=False)
    entity_type: Mapped[str] = mapped_column(String(20), nullable=False)   # Individual | LLC | Trust | Corporate
    primary_mailing_address: Mapped[Optional[str]] = mapped_column(String(255))
    # Modal (most common) normalized email/phone across the cluster's member
    # records -- denormalized the same way canonical_name/primary_mailing_address
    # already are, so run_incremental's existing-entity anchors (see
    # load_existing_entity_candidates in buyer_entity_resolution.py) can be
    # contact-matched against a new owners/deeds row without a fresh query.
    # phone is always via src/services/phone_utils.normalize (E.164).
    primary_email: Mapped[Optional[str]] = mapped_column(Text)
    primary_phone: Mapped[Optional[str]] = mapped_column(String(20))
    # The controlling PERSON's name for an entity resolved via Sunbiz LLC
    # piercing (e.g. an entity whose canonical_name is an LLC because no
    # Individual/Trust candidate was in the cluster still has principal_name
    # set to the pierced managing member). NULL when the entity was never
    # pierced -- an LLC entity with no known principal. See
    # buyer_entity_resolution.canonical_name()/find_structural_edges().
    principal_name: Mapped[Optional[str]] = mapped_column(Text)
    confidence_score: Mapped[int] = mapped_column(Integer, nullable=False)
    verification_status: Mapped[str] = mapped_column(String(20), nullable=False, default="unverified")
    total_purchase_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    total_cash_volume: Mapped[Decimal] = mapped_column(Numeric(14, 2), nullable=False, default=0)

    # HUNTER-02 (W1) — 3+ purchases in trailing 18 months OR >$500K total cash.
    # Persisted rather than recomputed on every read since Cell #1's ranked
    # whale list (W3) needs to query this cheaply and often; refreshed by the
    # nightly sweep (H3), not on every write to this row.
    is_whale: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")
    whale_flagged_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))

    # Minted once, the moment an entity first qualifies as a whale (per the
    # dev-split plan §6b — Hunter is first in Phase 1 to need one). Format
    # OPP-YYYY-##### per docs/plans/agent_lane_phase1_week1_dev_split.md.
    # Never reassigned even if the entity later drops out of whale status —
    # it identifies the opportunity, not the current flag state.
    opportunity_thread_id: Mapped[Optional[str]] = mapped_column(String(20), unique=True)

    # HUNTER-03 (H3) — behavioral investor-type classification, distinct from
    # entity_type above (legal structure). buyer_type_evidence/rule_version
    # persist the raw counts and rule generation a label was produced under,
    # for audit -- a label + confidence number alone isn't reviewable.
    # Populated by src/agents/hunter/buyer_type_classification.py, which
    # reads portfolio_evidence below rather than re-deriving it.
    buyer_type: Mapped[Optional[str]] = mapped_column(String(20))
    buyer_type_confidence: Mapped[Optional[int]] = mapped_column(Integer)
    buyer_type_classified_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    buyer_type_evidence: Mapped[Optional[Any]] = mapped_column(JSONB)
    buyer_type_rule_version: Mapped[Optional[int]] = mapped_column(SmallInteger)

    # HUNTER-04 (H4) — rolling purchase cadence, estimated acquisition
    # capacity, financing pattern, and average hold-time, populated by
    # src/agents/hunter/portfolio_profiling.py. financing_signal is a 3-state
    # signal ('cash_inferred' | 'financed' | 'unknown') computed per
    # acquisition then majority-voted onto the entity -- 'unknown' (no
    # correlated mortgage deed found, or too little history to judge) never
    # boosts estimated_annual_acquisition_capacity's multiplier the way a
    # positive 'cash_inferred' signal does. portfolio_evidence carries the
    # full bucketed evidence (acquisition/exit/still-held counts by window)
    # that H3's classifier reads directly.
    cadence_purchases_per_year: Mapped[Optional[Decimal]] = mapped_column(Numeric(6, 2))
    estimated_annual_acquisition_capacity: Mapped[Optional[int]] = mapped_column(Integer)
    financing_signal: Mapped[Optional[str]] = mapped_column(String(20))
    avg_hold_days: Mapped[Optional[int]] = mapped_column(Integer)
    portfolio_profiled_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    portfolio_evidence: Mapped[Optional[Any]] = mapped_column(JSONB)

    county_id: Mapped[Optional[str]] = mapped_column(String(50), index=True)
    first_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False,
        default=lambda: datetime.now(timezone.utc), server_default=func.now(),
    )
    last_updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False,
        default=lambda: datetime.now(timezone.utc), server_default=func.now(),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    links: Mapped[List["BuyerEntityLink"]] = relationship(
        "BuyerEntityLink", back_populates="buyer_entity", cascade="all, delete-orphan",
    )

    __table_args__ = (
        CheckConstraint(
            "entity_type IN ('Individual', 'LLC', 'Trust', 'Corporate', 'Estate')",
            name="check_buyer_entity_type",
        ),
        CheckConstraint(
            "verification_status IN ('verified', 'unverified')",
            name="check_buyer_entity_verification_status",
        ),
        CheckConstraint(
            "confidence_score >= 0 AND confidence_score <= 100",
            name="check_buyer_entity_confidence_range",
        ),
        CheckConstraint(
            "buyer_type IS NULL OR buyer_type IN ('flipper', 'buy-and-hold', 'wholesaler', 'institutional')",
            name="check_buyer_entity_buyer_type",
        ),
        CheckConstraint(
            "buyer_type_confidence IS NULL OR (buyer_type_confidence >= 0 AND buyer_type_confidence <= 100)",
            name="check_buyer_entity_buyer_type_confidence",
        ),
        CheckConstraint(
            "financing_signal IS NULL OR financing_signal IN ('cash_inferred', 'financed', 'unknown')",
            name="check_buyer_entity_financing_signal",
        ),
        Index("idx_buyer_entities_confidence", "confidence_score"),
        Index("idx_buyer_entities_is_whale", "is_whale", postgresql_where=text("is_whale")),
        Index("idx_buyer_entities_buyer_type", "buyer_type", postgresql_where=text("buyer_type IS NOT NULL")),
    )

    def __repr__(self) -> str:
        return (
            f"<BuyerEntity(id={self.id}, name={self.canonical_name!r}, "
            f"confidence={self.confidence_score})>"
        )


class BuyerEntityLink(Base):
    """
    One row per raw source record (an `owners` row, a `deeds` row via its
    grantee mention, or a `sunbiz_snapshots` row) resolved onto a
    `BuyerEntity`. This is the traceability layer Hunter's constitution
    requires ("every record traceable to source") — a BuyerEntity's identity
    is never asserted without a path back to the raw rows that produced it.

    `(source_table, source_id)` is unique — each raw record resolves to
    exactly one buyer entity; a buyer with many properties gets many links
    pointing at the same buyer_entity_id, not the reverse.

    match_method distinguishes a sourced structural fact
    (`sunbiz_llc_piercing` — a person named in Owner.managing_members) from an
    inferred match (`fuzzy_name`, `llm_adjudicated`) so a future audit can tell
    which links are facts vs. resolver judgment calls.
    """
    __tablename__ = "buyer_entity_links"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    buyer_entity_id: Mapped[int] = mapped_column(
        ForeignKey("buyer_entities.id", ondelete="CASCADE"), nullable=False, index=True,
    )
    source_table: Mapped[str] = mapped_column(String(30), nullable=False)   # owners | deeds | sunbiz_snapshots
    source_id: Mapped[int] = mapped_column(Integer, nullable=False)
    match_confidence: Mapped[int] = mapped_column(Integer, nullable=False)
    match_method: Mapped[str] = mapped_column(String(30), nullable=False)
    linked_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False,
        default=lambda: datetime.now(timezone.utc), server_default=func.now(),
    )

    match_explanation: Mapped[Optional[str]] = mapped_column(Text)

    buyer_entity: Mapped["BuyerEntity"] = relationship("BuyerEntity", back_populates="links")

    __table_args__ = (
        UniqueConstraint("source_table", "source_id", name="uq_buyer_entity_link_source"),
        CheckConstraint(
            "source_table IN ('owners', 'deeds', 'sunbiz_snapshots', 'tax_deed_auctions', "
            "'building_permits', 'permit_staging', 'deed_lender', 'deed_wholesaler')",
            name="check_buyer_entity_link_source_table",
        ),
        CheckConstraint(
            "match_method IN ('sunbiz_llc_piercing', 'exact_name_address', 'fuzzy_name', 'llm_adjudicated', 'manual', "
            "'exact_name_only', 'auction_name_only_unverified', 'singleton_no_edge')",
            name="check_buyer_entity_link_match_method",
        ),
        CheckConstraint(
            "match_confidence >= 0 AND match_confidence <= 100",
            name="check_buyer_entity_link_confidence_range",
        ),
    )

    def __repr__(self) -> str:
        return (
            f"<BuyerEntityLink(entity_id={self.buyer_entity_id}, "
            f"source={self.source_table}:{self.source_id}, method={self.match_method!r})>"
        )


class BuyerEntityMergeLog(Base):
    """
    Append-only audit log for manual buyer entity merges and their reversals.

    A merge collapses two buyer_entities rows into one by reassigning all
    buyer_entity_links from the absorbed entity to the surviving entity, then
    deleting the absorbed row. The absorbed row's full state is snapshotted
    into absorbed_snapshot before deletion so an unmerge can restore it.

    absorbed_id carries no FK because the row it referenced has been deleted.
    restored_id is populated by unmerge_entity() with the new PK assigned to
    the restored entity (old PK cannot be reused safely).

    moved_link_ids is the exact set of buyer_entity_links.id values reassigned
    to surviving_id at merge time. unmerge_entity() restores precisely these
    IDs -- never a timestamp heuristic, which cannot distinguish links moved
    by this merge from the survivor's own pre-existing links (both predate
    merged_at). A log row with moved_link_ids IS NULL (logged before this
    column existed) cannot be safely unmerged; unmerge_entity() raises rather
    than guess.
    """
    __tablename__ = "buyer_entity_merge_log"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    surviving_id: Mapped[int] = mapped_column(
        ForeignKey("buyer_entities.id"), nullable=False, index=True,
    )
    absorbed_id: Mapped[int] = mapped_column(Integer, nullable=False)
    absorbed_snapshot: Mapped[dict] = mapped_column(JSONB, nullable=False)
    links_moved: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    moved_link_ids: Mapped[Optional[list]] = mapped_column(JSONB)
    moved_ledger_event_ids: Mapped[Optional[list]] = mapped_column(JSONB)
    moved_monitor_log_ids: Mapped[Optional[list]] = mapped_column(JSONB)
    moved_closer_call_ids: Mapped[Optional[list]] = mapped_column(JSONB)
    moved_selfserve_session_ids: Mapped[Optional[list]] = mapped_column(JSONB)
    merged_by: Mapped[str] = mapped_column(String(100), nullable=False)
    merge_reason: Mapped[Optional[str]] = mapped_column(Text)
    merged_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(),
    )
    reversed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    reversed_by: Mapped[Optional[str]] = mapped_column(String(100))
    restored_id: Mapped[Optional[int]] = mapped_column(Integer)

    surviving_entity: Mapped["BuyerEntity"] = relationship(
        "BuyerEntity", foreign_keys="[BuyerEntityMergeLog.surviving_id]",
    )

    __table_args__ = (
        Index("idx_merge_log_surviving", "surviving_id"),
        Index("idx_merge_log_absorbed", "absorbed_id"),
        Index("idx_merge_log_active", "id", postgresql_where=text("reversed_at IS NULL")),
    )


class BuyerEntityMatchException(Base):
    """
    Durable record of a resolver decision that correctly refused to
    auto-merge -- an ambiguous pair, a cluster touching 2+ existing
    buyer_entities anchors, or an LLM tie-break that came back DIFFERENT on
    a high-name-score pair. Previously these were logger.warning only and
    forgotten. Client spec: "Identity resolution is uncertain. Records stay
    separate and a possible-match flag routes to EXCEPTIONS."

    Also the missing input to merge_entities() (buyer_entity_merge.py),
    which had no caller before this table existed -- an admin resolving one
    of these rows to 'merged' is expected to call merge_entities() and stamp
    merge_log_id here in the same transaction.

    Upserted on (kind, left_ref, right_ref): the nightly sweep re-seeing the
    same ambiguous pair every run bumps last_seen_at rather than creating a
    new row per run.
    """
    __tablename__ = "buyer_entity_match_exception"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    kind: Mapped[str] = mapped_column(Text, nullable=False)   # ambiguous_pair | multi_anchor_conflict | llm_different
    left_ref: Mapped[str] = mapped_column(Text, nullable=False)    # e.g. 'owners#4412'
    right_ref: Mapped[str] = mapped_column(Text, nullable=False)   # e.g. 'buyer_entities#88'
    entity_ids: Mapped[Optional[list]] = mapped_column(JSONB)      # anchors, for multi_anchor_conflict
    name_score: Mapped[Optional[int]] = mapped_column(Integer)
    address_score: Mapped[Optional[int]] = mapped_column(Integer)
    explanation: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="open")  # open|merged|rejected|stale
    resolved_by: Mapped[Optional[str]] = mapped_column(String(100))
    resolved_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    merge_log_id: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("buyer_entity_merge_log.id"))
    first_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(),
    )
    last_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(),
    )

    __table_args__ = (
        UniqueConstraint("kind", "left_ref", "right_ref", name="uq_match_exception_pair"),
        Index("idx_match_exception_open", "first_seen_at", postgresql_where=text("status = 'open'")),
        CheckConstraint(
            "kind IN ('ambiguous_pair', 'multi_anchor_conflict', 'llm_different')",
            name="buyer_entity_match_exception_kind_check",
        ),
        CheckConstraint(
            "status IN ('open', 'merged', 'rejected', 'stale')",
            name="buyer_entity_match_exception_status_check",
        ),
    )


class BorrowerLedgerEvent(Base):
    """
    Append-only longitudinal event timeline for a canonical buyer/borrower.

    One row per meaningful event in a borrower's history — deed acquisitions,
    foreclosures, permits, liens, legal proceedings, tax delinquencies, and
    opportunities. Each row traces back to the raw source record via
    (source_table, source_id), making the backfill idempotent and every
    event auditable.

    buyer_entity_id is the identity anchor (buyer_entities.id). property_id
    is nullable because some events are person-level rather than
    property-specific (e.g. opportunity_opened).

    Never update rows — append only. Source data corrections produce a new
    event, not a mutation of existing history.
    """
    __tablename__ = "borrower_ledger_events"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    buyer_entity_id: Mapped[int] = mapped_column(
        ForeignKey("buyer_entities.id", ondelete="CASCADE"), nullable=False, index=True,
    )
    event_type: Mapped[str] = mapped_column(String(40), nullable=False)
    event_date: Mapped[date] = mapped_column(Date, nullable=False)
    property_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("properties.id", ondelete="SET NULL"), nullable=True,
    )
    source_table: Mapped[str] = mapped_column(String(40), nullable=False)
    source_id: Mapped[int] = mapped_column(Integer, nullable=False)
    summary: Mapped[Optional[str]] = mapped_column(Text)
    amount: Mapped[Optional[Decimal]] = mapped_column(Numeric(14, 2))
    meta: Mapped[Optional[dict]] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(),
    )

    buyer_entity: Mapped["BuyerEntity"] = relationship("BuyerEntity")
    property: Mapped[Optional["Property"]] = relationship("Property")

    __table_args__ = (
        UniqueConstraint(
            "source_table", "source_id", "event_type", "buyer_entity_id",
            name="uq_ble_source_event_entity",
        ),
        CheckConstraint(
            "event_type IN ("
            "'deed_acquisition','deed_sale',"
            "'foreclosure_filed','foreclosure_resolved',"
            "'permit_filed','permit_closed',"
            "'lien_filed','lien_released',"
            "'legal_proceeding_filed',"
            "'tax_delinquency',"
            "'opportunity_opened','opportunity_closed'"
            ")",
            name="ck_ble_event_type",
        ),
        Index("idx_ble_entity_date", "buyer_entity_id", "event_date"),
        Index("idx_ble_property", "property_id", postgresql_where=text("property_id IS NOT NULL")),
        Index("idx_ble_event_type", "event_type"),
        Index("idx_ble_event_date", "event_date"),
    )

    def __repr__(self) -> str:
        return (
            f"<BorrowerLedgerEvent(id={self.id}, entity={self.buyer_entity_id}, "
            f"type={self.event_type!r}, date={self.event_date})>"
        )


class VeraFact(Base):
    """Vera's facts store — the fleet's single source of verified truth.

    Append-only: a fact is never updated in place, only re-verified with a
    new row (observed_at DESC gives the current value; older rows are
    history). Freshness is computed at read time from freshness_class +
    observed_at rather than expired by a background job — an expired fact
    reads as "unknown because stale" per Vera's constitution, it isn't
    deleted.

    Vera is the only writer, and only to this table (Agent Lane v2.2 Part 2 —
    her immutable core is permanently read-only on every business table; the
    facts directory is her one designated write target). Written through the
    normal app DB role, never through vera_readonly (which holds no write
    grants anywhere, including this table) — see
    docs/agent-lane-data-access-matrix.md.
    """
    __tablename__ = "vera_facts"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    fact_key: Mapped[str] = mapped_column(String(120), nullable=False)
    fact_value: Mapped[str] = mapped_column(Text, nullable=False)
    value_numeric: Mapped[Optional[Decimal]] = mapped_column(Numeric, nullable=True)
    county_id: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    source: Mapped[str] = mapped_column(String(60), nullable=False)
    method: Mapped[str] = mapped_column(Text, nullable=False)
    freshness_class: Mapped[str] = mapped_column(String(20), nullable=False)
    confidence: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    observed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False,
        default=lambda: datetime.now(timezone.utc), server_default=func.now(),
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False,
        default=lambda: datetime.now(timezone.utc), server_default=func.now(),
    )

    __table_args__ = (
        Index("ix_vera_facts_key_observed", "fact_key", text("observed_at DESC")),
        Index("ix_vera_facts_county", "county_id", postgresql_where=text("county_id IS NOT NULL")),
    )

    def __repr__(self) -> str:
        return f"<VeraFact(key={self.fact_key!r}, source={self.source!r}, observed_at={self.observed_at})>"


class VeraPromise(Base):
    """Open commitments Vera tracks (Constitution standing job #3, VERA-v2.2 V4).

    Unlike VeraFact (append-only), a promise is MUTABLE: status flips
    open -> closed/cancelled and closed_at is stamped when it resolves. Vera
    writes this via the normal app DB role (like vera_facts) — vera_readonly
    holds no write grants anywhere. The single writer is
    src/agents/vera/promises.py:record_promise(); Phase 2's reply-forwarding
    parser will call that same function unchanged. Nothing else writes here.
    """
    __tablename__ = "vera_promises"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    thread_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    description: Mapped[str] = mapped_column(Text, nullable=False)
    owner: Mapped[str] = mapped_column(String(120), nullable=False)
    source: Mapped[str] = mapped_column(String(60), nullable=False)
    mrr_at_risk_cents: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="open")
    due_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    observed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False,
        default=lambda: datetime.now(timezone.utc), server_default=func.now(),
    )
    closed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False,
        default=lambda: datetime.now(timezone.utc), server_default=func.now(),
    )

    __table_args__ = (
        Index("ix_vera_promises_status_due", "status", "due_at"),
    )

    def __repr__(self) -> str:
        return f"<VeraPromise(id={self.id}, owner={self.owner!r}, status={self.status!r})>"


class OutboundDraft(Base):
    """One Cora cold-outreach draft — send-free, always pending human review.

    Was an interim append-only JSON-Lines file (src/agents/cora/store.py) for
    as long as this build ran alongside the unmerged Cora->Lifecycle rename
    branch (risk of a models.py merge conflict). That rename is now merged
    and its DB migration run — this table replaces that interim store.

    Unlike the old file store's "append a new line per transition" pattern,
    status changes here are plain UPDATEs — draft_id is a real primary key,
    not a de-duplication key applied at read time. Nothing in the app layer
    ever needed the full transition history, only the current state per
    draft_id, so this is the simpler, idiomatic shape for a real table.
    """
    __tablename__ = "outbound_drafts"

    draft_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    opportunity_thread_id: Mapped[str] = mapped_column(String(64), nullable=False)
    buyer_entity_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    # Which venture produced this draft (CL4). relay_approval_queue already
    # carries venture_key, but a draft is created before a queue row exists
    # and the per-cell reply rate must be attributable without depending on
    # a downstream join that may never happen (rejected drafts never queue).
    venture_key: Mapped[str] = mapped_column(
        String(60),
        ForeignKey("ventures.venture_key"),
        nullable=False,
        server_default="hillsborough_distress",
    )
    cell_id: Mapped[str] = mapped_column(String(50), nullable=False)
    offer: Mapped[str] = mapped_column(String(50), nullable=False)
    avenue: Mapped[str] = mapped_column(String(50), nullable=False)
    angle: Mapped[str] = mapped_column(String(50), nullable=False)
    subject: Mapped[str] = mapped_column(Text, nullable=False)
    body: Mapped[str] = mapped_column(Text, nullable=False)
    facts_used: Mapped[list] = mapped_column(JSONB, nullable=False, default=list)
    source_refs: Mapped[list] = mapped_column(JSONB, nullable=False, default=list)
    recommended_channel: Mapped[str] = mapped_column(String(20), nullable=False)
    confidence_score: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(String(30), nullable=False, default="draft")
    booking_link: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    payment_link: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    reject_reason: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False,
        default=lambda: datetime.now(timezone.utc), server_default=func.now(),
    )
    schema_version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    published: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    is_followup: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    followup_sequence: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    contact_email: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    contact_phone: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    # Reply timestamp (CL4). Cora's canonical opportunity state lives in the
    # append-only file store (src/agents/cora/store.py); this is a dual-write
    # from opportunity_state.mark_replied() so reply rate is answerable in
    # SQL, per (venture_key, cell_id), from one indexed table.
    #
    # The file store cannot serve that query: it is gitignored, guarded by a
    # single-process threading.Lock, and read by de-duplicating transitions
    # at read time. Scaling send volume off a number derived that way is a
    # correctness bug, so the auto-double rule reads this column instead.
    replied_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    __table_args__ = (
        Index("ix_outbound_drafts_thread_cell_status", "opportunity_thread_id", "cell_id", "status"),
        Index("ix_outbound_drafts_contact_email", "contact_email"),
        Index("ix_outbound_drafts_venture_cell_created", "venture_key", "cell_id", "created_at"),
        CheckConstraint(
            "status IN ('draft', 'rejected', 'expired', 'superseded', 'approved_pending_send')",
            name="ck_outbound_drafts_status",
        ),
    )

    def __repr__(self) -> str:
        return f"<OutboundDraft(draft_id={self.draft_id!r}, thread={self.opportunity_thread_id!r}, status={self.status!r})>"


# ── QUALITY-v2.2 Q2 — Agent P&L Ledger ────────────────────────────────────────

class AgentPnl(Base):
    """Per-seat, per-month P&L ledger (QUALITY-v2.2 Q2).

    One row per (seat, period_month), written by src/tasks/agent_pnl_monthly.py.
    Never updated after close — a later refund posts in the month it occurs
    (decision C4). Primary key is the natural key; no autoincrement id.
    """
    __tablename__ = "agent_pnl"

    seat: Mapped[str] = mapped_column(String(20), primary_key=True)
    period_month: Mapped[date] = mapped_column(Date, primary_key=True)
    attributed_gp_cents: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    compute_cost_cents: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    data_cost_cents: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    founder_minutes_cost_cents: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    net_contribution_cents: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    binding_constraint: Mapped[Optional[str]] = mapped_column(String(40))
    approval_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    queue_dwell_median_minutes: Mapped[Optional[float]] = mapped_column(Numeric(8, 2))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=text("NOW()"))

    __table_args__ = (
        CheckConstraint(
            "seat IN ('vera','cora','hunter','relay','dev_shop','lifecycle')",
            name="ck_agent_pnl_seat",
        ),
    )

    def __repr__(self) -> str:
        return f"<AgentPnl(seat={self.seat!r}, month={self.period_month!r}, net={self.net_contribution_cents})>"


class AgentManualCostEntry(Base):
    """Manually-entered costs with no automated source (QUALITY-v2.2 Q2).

    Used for Dev Shop contractor invoices, Instantly flat-plan cost, Synthflow.
    Distinct from marketing_spend — that table's channel must map to utm_source
    for the CAC compiler; adding non-CAC entries there silently breaks it.
    """
    __tablename__ = "agent_manual_cost_entries"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    seat: Mapped[str] = mapped_column(String(20), nullable=False)
    period_month: Mapped[date] = mapped_column(Date, nullable=False)
    vendor: Mapped[str] = mapped_column(String(40), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text)
    amount_cents: Mapped[int] = mapped_column(Integer, nullable=False)
    entered_by: Mapped[str] = mapped_column(String(100), nullable=False, server_default=text("'admin'"))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=text("NOW()"))

    __table_args__ = (
        CheckConstraint(
            "seat IN ('vera','cora','hunter','relay','dev_shop','lifecycle')",
            name="ck_agent_manual_seat",
        ),
        CheckConstraint("amount_cents >= 0", name="ck_agent_manual_amount_nonneg"),
        Index("idx_agent_manual_seat_month", "seat", "period_month"),
    )

    def __repr__(self) -> str:
        return f"<AgentManualCostEntry(seat={self.seat!r}, vendor={self.vendor!r}, cents={self.amount_cents})>"
# ============================================================================
# REVINT-v2.2 — VERTICAL AUTOPILOT
# ============================================================================

class VerticalCandidatePacket(Base):
    """
    Stores the 6-dimension fit evaluation for a candidate vertical.

    Created by vertical_autopilot.score_vertical(). A packet with
    total_score >= VERTICAL_FIT_THRESHOLD and legal_status="approved"
    is eligible for a probe run.
    """
    __tablename__ = "vertical_candidate_packets"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    vertical_name: Mapped[str] = mapped_column(String(100), nullable=False, index=True)

    # Per-dimension binary scores (0 or 1)
    dim1_score: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    dim2_score: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    dim3_score: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    dim4_score: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    dim5_score: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    dim6_score: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    total_score: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    # Legal gate
    legal_status: Mapped[str] = mapped_column(String(30), nullable=False)     # "approved" | "blocked" | "pending_review"
    eligible_for_probe: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    # Dimension-level evidence (dim → detail dict)
    evidence: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)

    # Lifecycle
    status: Mapped[str] = mapped_column(
        String(30), nullable=False, default="candidate"
    )  # "candidate" | "probing" | "won" | "killed" | "pending_legal"

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    probes: Mapped[List["VerticalProbe"]] = relationship(
        "VerticalProbe", back_populates="packet", cascade="all, delete-orphan"
    )

    __table_args__ = (
        CheckConstraint(
            "legal_status IN ('approved','blocked','pending_review')",
            name="ck_vcp_legal_status",
        ),
        CheckConstraint(
            "status IN ('candidate','probing','won','killed','pending_legal','awaiting_ruling')",
            name="ck_vcp_status",
        ),
        Index("idx_vcp_status", "status"),
        Index("idx_vcp_vertical_name", "vertical_name"),
    )

    def __repr__(self) -> str:
        return (
            f"<VerticalCandidatePacket(id={self.id}, vertical={self.vertical_name!r}, "
            f"score={self.total_score}/6, status={self.status!r})>"
        )


class VerticalProbe(Base):
    """
    Tracks a single probe run for a candidate vertical.

    Compliance pre-flight fields are set before sends begin; reply_rate is
    updated as replies come in; verdict is recorded in VerticalVerdict.
    """
    __tablename__ = "vertical_probes"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    vertical_candidate_packet_id: Mapped[int] = mapped_column(
        ForeignKey("vertical_candidate_packets.id"), nullable=False, index=True
    )
    vertical_name: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    idempotency_key: Mapped[str] = mapped_column(String(100), nullable=False, unique=True)

    # Volume counters
    sends_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    reply_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    reply_rate: Mapped[float] = mapped_column(Numeric(6, 4), nullable=False, default=0.0)

    # Compliance pre-flight checks — NULL means not yet checked (stub); True/False = checked result.
    # Stubs must write NULL, not True, so persisted rows don't claim a check that never ran.
    tcpa_preflight_passed: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    suppression_checked: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    touch_collision_checked: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    frequency_cap_checked: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    quiet_hours_checked: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    channel_limits_checked: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    kill_switch_active: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    completion_receipt: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    # Timestamps
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    instantly_campaign_id: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    probe_emails: Mapped[Optional[list]] = mapped_column(JSONB, nullable=True)

    status: Mapped[str] = mapped_column(
        String(20), nullable=False, default="running"
    )  # "running" | "completed" | "killed" | "aborted"

    packet: Mapped["VerticalCandidatePacket"] = relationship(
        "VerticalCandidatePacket", back_populates="probes"
    )
    verdicts: Mapped[List["VerticalVerdict"]] = relationship(
        "VerticalVerdict", back_populates="probe", cascade="all, delete-orphan"
    )

    __table_args__ = (
        CheckConstraint(
            "status IN ('running','completed','killed','aborted')",
            name="ck_vprobe_status",
        ),
        Index("idx_vprobe_status", "status"),
    )

    def __repr__(self) -> str:
        return (
            f"<VerticalProbe(id={self.id}, vertical={self.vertical_name!r}, "
            f"sends={self.sends_count}, reply_rate={self.reply_rate}, status={self.status!r})>"
        )


class VerticalVerdict(Base):
    """
    Final ruling on a vertical probe — won, killed, or running (pending Josh).

    presell_confirmed gates entry into the dev queue.
    package_generated is auto-True on won verdicts.
    handoff_payload carries the sell+clone deferred payload when clone is
    deferred until county_2.
    """
    __tablename__ = "vertical_verdicts"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    vertical_probe_id: Mapped[int] = mapped_column(
        ForeignKey("vertical_probes.id"), nullable=False, index=True
    )
    vertical_candidate_packet_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    vertical_name: Mapped[str] = mapped_column(String(100), nullable=False, index=True)

    verdict: Mapped[str] = mapped_column(String(20), nullable=False)  # "won" | "killed" | "running" | "awaiting_ruling"
    verdict_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
    rule_fired: Mapped[str] = mapped_column(String(60), nullable=False)
    # e.g. "reply_rate_gt_8pct" | "reply_rate_lt_3pct" | "min_sample_josh_ruling"
    reply_rate_at_verdict: Mapped[float] = mapped_column(Numeric(6, 4), nullable=False, default=0.0)

    # Downstream gates
    presell_confirmed: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    package_generated: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    package_id: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    clone_status: Mapped[Optional[str]] = mapped_column(String(60), nullable=True)
    # e.g. "deferred_until_county_2" on won
    source_county: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    handoff_payload: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)

    probe: Mapped["VerticalProbe"] = relationship("VerticalProbe", back_populates="verdicts")

    __table_args__ = (
        CheckConstraint(
            "verdict IN ('won','killed','running','awaiting_ruling')",
            name="ck_vverdict_verdict",
        ),
        Index("idx_vverdict_verdict", "verdict"),
        Index("idx_vverdict_vertical_name", "vertical_name"),
    )

    def __repr__(self) -> str:
        return (
            f"<VerticalVerdict(id={self.id}, vertical={self.vertical_name!r}, "
            f"verdict={self.verdict!r}, rule={self.rule_fired!r})>"
        )


# ============================================================================
# REVINT-I1: Revenue Intelligence — Opportunity Scoring
# ============================================================================


class RevenueType(str, Enum):
    SUBSCRIPTION = "subscription"
    ONE_TIME = "one_time"
    USAGE_BASED = "usage_based"
    PILOT = "pilot"
    # NOTE: referral_fee calculation is DISABLED until RESPA clearance is confirmed.
    # lender_intro actions should NOT trigger financial projections until legal sign-off.
    REFERRAL_FEE = "referral_fee"
    LICENSING = "licensing"


class OpportunityScore(Base):
    """
    NBRA (Net Business Return per Action) scoring record for one opportunity.

    `nbra_score` = expected_retained_gross_profit_cents / josh_minutes_required.
    Automated actions (is_automated=True) carry josh_minutes_required=0 and
    nbra_score=None — they bypass the NBRA queue and go to Relay directly.

    segment values: "whale" | "auction_winner" | "lapsed_subscriber" | "default"
    billing_interval values: "monthly" | "annual" | None (for non-subscription types)
    """
    __tablename__ = "opportunity_scores"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    # OPP-YYYY-##### format; matches BuyerEntity.opportunity_thread_id
    opportunity_thread_id: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    buyer_entity_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    segment: Mapped[str] = mapped_column(String(30), nullable=False)
    revenue_type: Mapped[str] = mapped_column(String(30), nullable=False)
    billing_interval: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)
    expected_revenue_cents: Mapped[int] = mapped_column(Integer, nullable=False)
    expected_mrr_cents: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    expected_retained_gross_profit_cents: Mapped[int] = mapped_column(Integer, nullable=False)
    p_reply: Mapped[float] = mapped_column(Numeric(6, 4), nullable=False)
    p_close: Mapped[float] = mapped_column(Numeric(6, 4), nullable=False)
    time_to_cash_days: Mapped[int] = mapped_column(Integer, nullable=False)
    josh_minutes_required: Mapped[float] = mapped_column(Numeric(8, 2), nullable=False)
    # None when is_automated=True (josh_minutes_required == 0, never in NBRA denominator)
    nbra_score: Mapped[Optional[float]] = mapped_column(Numeric(12, 4), nullable=True)
    source_action_type: Mapped[Optional[str]] = mapped_column(String(60), nullable=True)
    # Automated actions bypass NBRA queue and route directly to Relay
    is_automated: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False,
        default=lambda: datetime.now(timezone.utc), server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        server_default=func.now(),
    )

    __table_args__ = (
        UniqueConstraint(
            "opportunity_thread_id", "source_action_type", "revenue_type",
            name="uq_opportunity_score_action",
        ),
        Index("ix_opp_scores_thread_id", "opportunity_thread_id"),
        Index("ix_opp_scores_buyer_entity", "buyer_entity_id"),
        Index("ix_opp_scores_segment_nbra", "segment", "nbra_score"),
        CheckConstraint(
            "segment IN ('whale','auction_winner','lapsed_subscriber','default')",
            name="ck_opp_scores_segment",
        ),
        CheckConstraint(
            "billing_interval IN ('monthly','annual') OR billing_interval IS NULL",
            name="ck_opp_scores_billing_interval",
        ),
    )

    def __repr__(self) -> str:
        return (
            f"<OpportunityScore(id={self.id!r}, thread={self.opportunity_thread_id!r}, "
            f"segment={self.segment!r}, nbra={self.nbra_score!r})>"
        )


class OpportunityScoreHistory(Base):
    """
    Immutable audit trail — one row per recalculation of an OpportunityScore.
    Never updated after insert; written by calibration_service and scoring service.
    """
    __tablename__ = "opportunity_score_history"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    opportunity_score_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("opportunity_scores.id", ondelete="CASCADE"), nullable=False, index=True,
    )
    opportunity_thread_id: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    snapshot_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False,
        default=lambda: datetime.now(timezone.utc), server_default=func.now(),
    )
    p_reply: Mapped[float] = mapped_column(Numeric(6, 4), nullable=False)
    p_close: Mapped[float] = mapped_column(Numeric(6, 4), nullable=False)
    time_to_cash_days: Mapped[int] = mapped_column(Integer, nullable=False)
    nbra_score: Mapped[float] = mapped_column(Numeric(12, 4), nullable=False)
    reason: Mapped[Optional[str]] = mapped_column(String(200), nullable=True)

    __table_args__ = (
        Index("ix_opp_score_history_score_id", "opportunity_score_id"),
        Index("ix_opp_score_history_thread_id", "opportunity_thread_id"),
    )

    def __repr__(self) -> str:
        return (
            f"<OpportunityScoreHistory(id={self.id!r}, score_id={self.opportunity_score_id!r}, "
            f"snapshot_at={self.snapshot_at!r})>"
        )


# ============================================================================
# THROUGH-v2.2 — CORA BATCH APPROVAL
# ============================================================================

class CoraDraftBatch(Base):
    """One THROUGH-v2.2 batch shown to Josh in Slack for one-tap approval —
    the founder-facing layer between Cora's drafts and Relay's execution
    queue. Separate from RelayApprovalQueueItem.batch_id, which groups rows
    claimed together by one execution run, a different concept entirely."""
    __tablename__ = "cora_draft_batches"

    batch_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False,
        default=lambda: datetime.now(timezone.utc), server_default=func.now(),
    )
    slack_message_ts: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    slack_channel: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="pending")
    decided_by: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    decided_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        CheckConstraint(
            "status IN ('pending', 'approved', 'rejected', 'partial', 'expired')",
            name="ck_cora_draft_batches_status",
        ),
    )

    def __repr__(self) -> str:
        return f"<CoraDraftBatch(batch_id={self.batch_id!r}, status={self.status!r})>"


class CoraBatchItem(Base):
    """One draft's membership + individual decision within a CoraDraftBatch —
    what THROUGH-v2.2's standing-order compiler (T4) mines for approval
    history, grouped by outbound_drafts.cell_id."""
    __tablename__ = "cora_batch_items"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    batch_id: Mapped[str] = mapped_column(String(36), ForeignKey("cora_draft_batches.batch_id"), nullable=False)
    draft_id: Mapped[str] = mapped_column(String(36), ForeignKey("outbound_drafts.draft_id"), nullable=False)
    decision: Mapped[str] = mapped_column(String(20), nullable=False, default="included")
    decided_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False,
        default=lambda: datetime.now(timezone.utc), server_default=func.now(),
    )

    __table_args__ = (
        Index("ix_cora_batch_items_batch_id", "batch_id"),
        Index("ix_cora_batch_items_draft_id", "draft_id"),
        UniqueConstraint("batch_id", "draft_id", name="uq_cora_batch_items_batch_draft"),
        CheckConstraint(
            "decision IN ('included', 'exception_rejected')",
            name="ck_cora_batch_items_decision",
        ),
    )

    def __repr__(self) -> str:
        return f"<CoraBatchItem(batch_id={self.batch_id!r}, draft_id={self.draft_id!r}, decision={self.decision!r})>"


class CoraStandingOrder(Base):
    """A founder-ratified rule (THROUGH-v2.2 T4) letting future drafts of a
    given cell_id auto-approve without a Slack tap, once the same action has
    been approved cleanly (no exception-rejects) enough times in a row.
    No 'existing amendment-diff mechanism' was found anywhere in this repo
    to build on top of — this is genuinely new, not a reuse."""
    __tablename__ = "cora_standing_orders"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    cell_id: Mapped[str] = mapped_column(String(50), nullable=False)
    rule_text: Mapped[str] = mapped_column(Text, nullable=False)
    created_by: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False,
        default=lambda: datetime.now(timezone.utc), server_default=func.now(),
    )
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    slack_message_ts: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    approval_count_at_proposal: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )

    __table_args__ = (
        Index("ix_cora_standing_orders_cell_id_active", "cell_id", "active"),
    )

    def __repr__(self) -> str:
        return f"<CoraStandingOrder(cell_id={self.cell_id!r}, active={self.active!r})>"

# ---------------------------------------------------------------------------
# QUALITY-v2.2 Q1 — Fleet event-trigger dispatcher
# Deliberately separate from ProspectEvent/ProcessedEvent/EventFailure:
# those tables require a NOT NULL prospect_id FK and enumerate a closed set
# of prospect-lifecycle event types — neither fits a fleet-wide event (a
# Stripe cancellation or a Dev-Shop finding has no prospect_id). These tables
# also add a priority column for deadline-aware preemption (spec §9.5).
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# QUALITY-v2.2 Q1 — Fleet event-trigger dispatcher
# Deliberately separate from ProspectEvent/ProcessedEvent/EventFailure:
# those tables require a NOT NULL prospect_id FK and enumerate a closed set
# of prospect-lifecycle event types — neither fits a fleet-wide event (a
# Stripe cancellation or a Dev-Shop finding has no prospect_id). These tables
# also add a priority column for deadline-aware preemption (spec §9.5).
# ---------------------------------------------------------------------------

class FleetEvent(Base):
    __tablename__ = "fleet_events"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    event_type: Mapped[str] = mapped_column(String(40), nullable=False)
    priority: Mapped[int] = mapped_column(SmallInteger, nullable=False, default=100)
    source_component: Mapped[str] = mapped_column(String(60), nullable=False)
    subscriber_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("subscribers.id"))
    opportunity_thread_id: Mapped[Optional[str]] = mapped_column(String(20))
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    occurred_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("NOW()"),
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("NOW()"),
    )

    __table_args__ = (
        CheckConstraint(
            "event_type IN ('filing.new','payment.received','reply.received',"
            "'booking.created','subscription.cancelled','source.failure')",
            name="ck_fleet_events_event_type",
        ),
        CheckConstraint("priority >= 0", name="ck_fleet_events_priority"),
        Index("idx_fleet_events_type", "event_type"),
        Index("idx_fleet_events_priority_occurred", "priority", "occurred_at"),
        Index("idx_fleet_events_subscriber", "subscriber_id"),
    )


class FleetProcessedEvent(Base):
    __tablename__ = "fleet_processed_events"

    event_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("fleet_events.id", ondelete="CASCADE"), primary_key=True,
    )
    consumer: Mapped[str] = mapped_column(String(100), primary_key=True)
    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("NOW()"),
    )


class FleetEventFailure(Base):
    __tablename__ = "fleet_event_failures"

    event_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("fleet_events.id", ondelete="CASCADE"), primary_key=True,
    )
    consumer: Mapped[str] = mapped_column(String(100), primary_key=True)
    retry_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_error: Mapped[Optional[str]] = mapped_column(Text)
    failed_permanently: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    last_attempt_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))

    __table_args__ = (
        Index("idx_fleet_event_failures_consumer_permanent", "consumer", "failed_permanently"),
    )


class RevenueCanaryAlertLog(Base):
    """QUALITY-v2.2 Q4 — dedup log for the revenue canary sweep's alert
    email. A distinct check_name re-alerts at most once per cooldown window
    (src/tasks/revenue_canary_sweep.py's _ALERT_COOLDOWN_HOURS), same
    pattern as RevenueHeartbeatAlertLog / ScraperAlertLog.
    """
    __tablename__ = "revenue_canary_alert_log"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    check_name: Mapped[str] = mapped_column(String(20), nullable=False)
    alerted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(),
    )


class RevenueCanaryProbeLog(Base):
    """QUALITY-v2.2 Q4 — dedicated round-trip table for the entitlement/
    delivery canary checks. One row per check_name ('entitlement',
    'delivery'), upserted every 5 minutes. Deliberately NOT
    platform_revenue_ledger or sent_leads — see
    src/services/revenue_canary.py's module docstring for why.
    """
    __tablename__ = "revenue_canary_probe_log"

    check_name: Mapped[str] = mapped_column(String(20), primary_key=True)
    probe_value: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(),
    )


class SourceFailoverLog(Base):
    """QUALITY-v2.2 Q4 — event log for named-alternate source failover
    (decision A2-revised). Every SLA-breach-driven switch attempt is
    recorded here, whether it actually switched to an alternate or logged
    'no alternate configured'.
    """
    __tablename__ = "source_failover_log"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    source_type: Mapped[str] = mapped_column(String(50), nullable=False)
    county_id: Mapped[str] = mapped_column(String(50), nullable=False)
    event_type: Mapped[str] = mapped_column(String(30), nullable=False)
    detail: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    occurred_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(),
    )

    __table_args__ = (
        CheckConstraint(
            "event_type IN ('switched_to_alternate','no_alternate_configured','switched_back_to_primary')",
            name="ck_source_failover_log_event_type",
        ),
        Index("idx_source_failover_log_lookup", "source_type", "county_id", "occurred_at"),
    )


class ExperimentAttribution(Base):
    """LEARN-v2.2 Layer 2 — one attribution row per fleet event credited to
    an experiment arm.

    Written by the nightly experiment_attribution_sweep. Idempotent on
    (fleet_event_id, assignment_id) — re-running the sweep never double-counts.

    attribution_method: 'draft_match' when a draft on the same thread was
    found within window_days; 'last_touch' when the assignment itself is
    within window_days but no matching draft exists.

    cell_id / venture_key are populated ONLY on draft_match — the cell and
    venture of the specific draft that earned the reply. They are NULL on
    last_touch (no producing draft found). This is what makes the two methods
    genuinely distinct rather than a label, and is the join key T-LEARN-06
    (feature -> revenue by cell) reads.
    """
    __tablename__ = "experiment_attributions"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    fleet_event_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("fleet_events.id"), nullable=False
    )
    assignment_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("agent_lane_experiment_assignments.id"), nullable=False
    )
    test_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("agent_lane_experiments.id"), nullable=False, index=True
    )
    opportunity_thread_id: Mapped[str] = mapped_column(String(20), nullable=False)
    variant: Mapped[str] = mapped_column(String(10), nullable=False)
    event_type: Mapped[str] = mapped_column(String(40), nullable=False)
    attribution_method: Mapped[str] = mapped_column(String(20), nullable=False)
    cell_id: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    venture_key: Mapped[Optional[str]] = mapped_column(String(60), nullable=True)
    window_days: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    attributed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        UniqueConstraint("fleet_event_id", "assignment_id", name="uq_experiment_attribution"),
        Index("idx_experiment_attribution_test", "test_id", "event_type", "attributed_at"),
        Index("idx_experiment_attribution_thread", "opportunity_thread_id"),
        Index(
            "idx_experiment_attribution_cell", "cell_id",
            postgresql_where=text("cell_id IS NOT NULL"),
        ),
        CheckConstraint(
            "attribution_method IN ('draft_match','last_touch')",
            name="ck_experiment_attribution_method",
        ),
    )


class AgentLaneOpportunityOutcome(Base):
    """LEARN-v2.2 T-LEARN-03 — the terminal outcome of one Agent Lane opportunity.

    No pre-existing row owned "this opportunity is over, and here is why" —
    opportunity_thread_id was a bare string across BuyerEntity/OpportunityScore/
    OutboundDraft/PriceAssignment. This table is that missing home. One row per
    thread (UNIQUE). outcome='won' needs no reason; outcome='lost' carries one of
    the spec's eight loss codes. Win is auto-coded from payment.received; loss is
    supplied by a human tap (Josh) except no_response, which a 30-day timeout
    sweep auto-codes.
    """
    __tablename__ = "agent_lane_opportunity_outcomes"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    opportunity_thread_id: Mapped[str] = mapped_column(String(20), nullable=False)
    outcome: Mapped[str] = mapped_column(String(10), nullable=False)  # 'won' | 'lost'
    reason_code: Mapped[Optional[str]] = mapped_column(String(20))    # one of 8 loss codes, NULL on won
    coded_by: Mapped[str] = mapped_column(String(60), nullable=False) # actor: 'payment_fleet_event','opportunity_timeout_sweep','admin:<who>'
    source_ref: Mapped[Optional[str]] = mapped_column(String(120))    # e.g. fleet_event id, or note
    coded_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())

    __table_args__ = (
        UniqueConstraint("opportunity_thread_id", name="uq_agent_lane_opportunity_outcome"),
        Index("idx_alo_outcome_reason", "outcome", "reason_code"),
        CheckConstraint("outcome IN ('won','lost')", name="ck_alo_outcome"),
        CheckConstraint(
            "(outcome = 'won' AND reason_code IS NULL) OR "
            "(outcome = 'lost' AND reason_code IN "
            "('timing','price','trust','fit','no_urgency','wrong_contact','competitor','no_response'))",
            name="ck_alo_reason_code",
        ),
    )


# ============================================================================
# WP-T2-8 — BUILDER ENGINE: dial queue + operator decisions
# ============================================================================


class BuilderDialQueue(Base):
    """Operator decisions on RELATIONSHIPS-lane builder cards.

    A row per buyer_entity_id tracks whether the operator queued the builder
    for the dial list, snoozed them, or dismissed them as not a fit.
    Upserted by the Slack action handlers; read by the Stage-E dial-wiring.
    """
    __tablename__ = "builder_dial_queue"

    buyer_entity_id: Mapped[int] = mapped_column(
        ForeignKey("buyer_entities.id", ondelete="CASCADE"),
        primary_key=True,
    )
    queued_by: Mapped[str] = mapped_column(String(64), nullable=False)
    queued_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(),
    )
    snoozed_until: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    dismissed: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")


# ============================================================================
# FA MAX — DURABLE STATE ENGINE (WP-1)
# ============================================================================


class FaMaxEntityRegistry(Base):
    """Single canonical UUID for every FA Max-tracked object.

    Resolves the polymorphic-FK problem: state_transition_events.entity_uuid
    is a real FK into this table, not a bare text reference with no DB
    enforcement. One row per tracked entity, created once at first FA Max
    contact.

    entity_type values:
        person       — canonical borrower identity (persons.person_id)
        property     — existing properties.id (stored as text)
        opportunity  — fa_max_opportunities.opportunity_id
        partner      — future partner/referral entity
        interaction  — future interaction record

    native_id is text to accommodate both int PKs (properties.id) and UUID
    PKs; the type+native_id pair uniquely identifies the backing row.
    """

    __tablename__ = "fa_max_entity_registry"

    entity_uuid: Mapped[str] = mapped_column(
        PG_UUID(as_uuid=True), primary_key=True, server_default=text("generate_uuidv7()")
    )
    entity_type: Mapped[str] = mapped_column(String(30), nullable=False)
    native_id: Mapped[str] = mapped_column(String(255), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        CheckConstraint(
            "entity_type IN ('person','property','opportunity','partner','interaction')",
            name="ck_fa_max_entity_registry_type",
        ),
        UniqueConstraint("entity_type", "native_id", name="uq_fa_max_entity_registry_type_native"),
        Index("ix_fa_max_entity_registry_type", "entity_type"),
    )

    def __repr__(self) -> str:
        return (
            f"<FaMaxEntityRegistry(uuid={self.entity_uuid!r}, "
            f"type={self.entity_type!r}, native={self.native_id!r})>"
        )


class FaMaxPersonLifecycleStageConfig(Base):
    """Config-as-data stage definitions for FA Max person lifecycle.

    Mirrors LaneStageConfig's pattern but is its own table — LaneStageConfig
    carries sms_allowed and lane_type semantics that belong to the Agent Lane
    broker marketplace, not FA Max's borrower lifecycle.

    Seeded by migration with the 12-stage SOT progression. Editable with no
    deploy.
    """

    __tablename__ = "fa_max_person_lifecycle_stage_config"

    stage_key: Mapped[str] = mapped_column(String(50), primary_key=True)
    display_name: Mapped[str] = mapped_column(String(100), nullable=False)
    order_index: Mapped[int] = mapped_column(Integer, nullable=False)
    allowed_next: Mapped[list] = mapped_column(
        JSONB, nullable=False, server_default=text("'[]'::jsonb")
    )
    is_terminal: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default=text("false")
    )
    is_active: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default=text("true")
    )

    def __repr__(self) -> str:
        return f"<FaMaxPersonLifecycleStageConfig(stage={self.stage_key!r}, order={self.order_index})>"


class FaMaxPerson(Base):
    """Canonical FA Max borrower/person identity record.

    One row per unique real-world person Josh is working with as a potential
    borrower. Cross-property, cross-session, permanent. merged_into_id
    supports WP-4 identity resolution — when two rows are proven to be the
    same person the surviving row's person_id is canonical and this field
    carries the link on the merged row.

    Compliance: NO financial data columns. No credit score, income, bank
    statement, tax return, SSN, or any field that holds borrower financial
    information. This is an absolute prohibition from SOT.md.
    """

    __tablename__ = "fa_max_persons"

    person_id: Mapped[str] = mapped_column(
        PG_UUID(as_uuid=True), primary_key=True, server_default=text("generate_uuidv7()")
    )
    lifecycle_state: Mapped[str] = mapped_column(
        String(50), nullable=False, server_default=text("'identified'")
    )
    merged_into_id: Mapped[Optional[str]] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("fa_max_persons.person_id", name="fk_fa_max_persons_merged_into"),
        nullable=True,
    )
    source: Mapped[str] = mapped_column(String(60), nullable=False)
    source_reference: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    full_name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    email: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    phone: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    # CAS optimistic-concurrency guard — incremented on every successful transition().
    # Callers must supply the current value when calling transition(); a mismatched
    # version (stale read) produces already_advanced without a state mutation.
    state_version: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default=text("0")
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        ForeignKeyConstraint(
            ["lifecycle_state"],
            ["fa_max_person_lifecycle_stage_config.stage_key"],
            name="fk_fa_max_persons_lifecycle_state",
            deferrable=True,
            initially="DEFERRED",
        ),
        CheckConstraint(
            "merged_into_id IS NULL OR merged_into_id <> person_id",
            name="ck_fa_max_persons_no_self_merge",
        ),
        Index("ix_fa_max_persons_lifecycle_state", "lifecycle_state"),
        Index(
            "ix_fa_max_persons_not_merged",
            "person_id",
            postgresql_where=text("merged_into_id IS NULL"),
        ),
        Index(
            "ix_fa_max_persons_full_name_trgm", "full_name",
            postgresql_using="gin", postgresql_ops={"full_name": "gin_trgm_ops"},
        ),
        Index(
            "ix_fa_max_persons_email", "email",
            postgresql_where=text("email IS NOT NULL"),
        ),
    )

    def __repr__(self) -> str:
        return (
            f"<FaMaxPerson(person_id={self.person_id!r}, "
            f"state={self.lifecycle_state!r})>"
        )


class FaMaxOpportunityStageConfig(Base):
    """Config-as-data stage definitions for FA Max opportunities.

    Separate from LaneStageConfig and from FaMaxPersonLifecycleStageConfig.
    Opportunity stages track the loan/deal pipeline; person lifecycle stages
    track the borrower relationship arc. They move at different cadences.
    """

    __tablename__ = "fa_max_opportunity_stage_config"

    stage_key: Mapped[str] = mapped_column(String(50), primary_key=True)
    display_name: Mapped[str] = mapped_column(String(100), nullable=False)
    order_index: Mapped[int] = mapped_column(Integer, nullable=False)
    allowed_next: Mapped[list] = mapped_column(
        JSONB, nullable=False, server_default=text("'[]'::jsonb")
    )
    is_terminal: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default=text("false")
    )
    is_active: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default=text("true")
    )

    def __repr__(self) -> str:
        return f"<FaMaxOpportunityStageConfig(stage={self.stage_key!r}, order={self.order_index})>"


class FaMaxOpportunity(Base):
    """FA Max canonical opportunity/loan record.

    One row per distinct loan/deal occurrence for a person. Deliberately NOT
    unique on (person_id, property_id) — the same borrower can have an
    acquisition opportunity and a later rehab loan on the same property, and
    a repeat borrower gets a new row per project.

    Dedup against duplicate ingest events is handled by idempotency_key
    (partial unique index — only when non-NULL), not by a compound constraint
    on business keys.

    opportunity_type values mirror SOT.md's loan product taxonomy:
        acquisition, rehab, construction, extension, refinance,
        dscr_takeout, repeat

    Compliance: no pricing fields (rate, term, LTV commitment) to borrower.
    loan_amount_cents and maturity_months are internal working fields only —
    never surfaced in any outbound communication.

    backflip_ref: opaque reference to the Backflip portal/application. NULL
    until Josh submits. Nothing populates this without Josh's explicit action.
    """

    __tablename__ = "fa_max_opportunities"

    opportunity_id: Mapped[str] = mapped_column(
        PG_UUID(as_uuid=True), primary_key=True, server_default=text("generate_uuidv7()")
    )
    person_id: Mapped[str] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("fa_max_persons.person_id", name="fk_fa_max_opp_person"),
        nullable=False,
    )
    opportunity_type: Mapped[str] = mapped_column(String(30), nullable=False)
    current_stage: Mapped[str] = mapped_column(
        String(50), nullable=False, server_default=text("'new'")
    )
    outcome: Mapped[str] = mapped_column(
        String(20), nullable=False, server_default=text("'open'")
    )
    source: Mapped[str] = mapped_column(String(60), nullable=False)
    source_reference: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    idempotency_key: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    expected_need_date: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    actual_funded_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    loan_amount_cents: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    maturity_months: Mapped[Optional[int]] = mapped_column(SmallInteger, nullable=True)
    backflip_ref: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    # Free-text address as typed by Josh in the log-submission modal for a
    # new borrower -- NOT a FK into properties(id) (see FaMaxOpportunityProperty
    # for the matched-property link table). This just preserves what he
    # entered at submission time; it is never fuzzy-matched or validated.
    property_address: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    assigned_to: Mapped[Optional[str]] = mapped_column(String(120), nullable=True)
    # WP-T2-2: write-once attribution to the interaction that triggered this
    # opportunity's creation. NULL = unattributed = counts as zero for the
    # Tier C funded-loan causal-join evidence (fa_max_autonomy.get_funded_
    # loan_count). Set exactly once at creation time by the caller that
    # inserts the opportunity row; application-layer enforced (no current
    # production call site creates FaMaxOpportunity rows yet -- see WP-T2-2
    # deviation notes), never overwritten thereafter.
    origin_interaction_id: Mapped[Optional[Any]] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("fa_max_interactions.interaction_id", name="fk_fa_max_opp_origin_interaction"),
        nullable=True,
    )
    # CAS optimistic-concurrency guard — same pattern as FaMaxPerson.state_version.
    state_version: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default=text("0")
    )
    # WP-T2-3: write-once channel attribution. NULL = no real send has landed yet.
    # 'forced_action' = FA Max originated the relationship (off-market trigger,
    # partner layer). 'backflip' = this contact was already in an active Backflip
    # campaign at the time of first send (should not occur — suppression blocks
    # those; present as a guard for unexpected state). Written by mark_sent()
    # under WHERE backflip_attribution_owner IS NULL — concurrent workers are safe.
    backflip_attribution_owner: Mapped[Optional[str]] = mapped_column(
        String(30), nullable=True
    )
    backflip_attribution_set_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    # WP-T2-11: GYR routing columns
    gyr_color: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)
    expected_revenue_cents: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    gyr_reason: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    gyr_ranked_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    gyr_stale_alerted_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        ForeignKeyConstraint(
            ["current_stage"],
            ["fa_max_opportunity_stage_config.stage_key"],
            name="fk_fa_max_opp_stage_config",
            deferrable=True,
            initially="DEFERRED",
        ),
        CheckConstraint(
            "opportunity_type IN ('acquisition','rehab','construction','extension',"
            "'refinance','dscr_takeout','repeat')",
            name="ck_fa_max_opp_type",
        ),
        CheckConstraint(
            "outcome IN ('open','funded','dead','recycled','referred')",
            name="ck_fa_max_opp_outcome",
        ),
        CheckConstraint(
            "backflip_attribution_owner IS NULL "
            "OR backflip_attribution_owner IN ('forced_action','backflip')",
            name="ck_fa_max_opp_attribution_owner",
        ),
        CheckConstraint(
            "gyr_color IN ('green','yellow','red') OR gyr_color IS NULL",
            name="ck_fa_max_opp_gyr_color",
        ),
        Index(
            "uq_fa_max_opp_idempotency_key",
            "idempotency_key",
            unique=True,
            postgresql_where=text("idempotency_key IS NOT NULL"),
        ),
        Index("ix_fa_max_opp_person_id", "person_id"),
        Index("ix_fa_max_opp_stage", "current_stage"),
        Index(
            "ix_fa_max_opp_open",
            "outcome",
            postgresql_where=text("outcome = 'open'"),
        ),
        Index(
            "ix_fa_max_opp_origin_interaction", "origin_interaction_id",
            postgresql_where=text("origin_interaction_id IS NOT NULL"),
        ),
        Index(
            "ix_fa_max_opp_gyr_money",
            "gyr_color",
            "expected_revenue_cents",
            postgresql_where=text("outcome = 'open'"),
        ),
    )

    def __repr__(self) -> str:
        return (
            f"<FaMaxOpportunity(opportunity_id={self.opportunity_id!r}, "
            f"person_id={self.person_id!r}, type={self.opportunity_type!r}, "
            f"stage={self.current_stage!r})>"
        )


class FaMaxOpportunityProperty(Base):
    """N:N link between FA Max opportunities and properties.

    Kept as a link table (not a FK on fa_max_opportunities) because:
    - Pre-property borrower conversations are valid opportunities with no
      property yet (simply no rows here).
    - Portfolio/multi-property transactions link multiple properties.
    - role distinguishes subject property from collateral, exit asset, etc.
    """

    __tablename__ = "fa_max_opportunity_properties"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    opportunity_id: Mapped[str] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("fa_max_opportunities.opportunity_id", name="fk_fa_max_opp_prop_opp"),
        nullable=False,
    )
    property_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("properties.id", name="fk_fa_max_opp_prop_property"),
        nullable=False,
    )
    role: Mapped[str] = mapped_column(
        String(30), nullable=False, server_default=text("'subject'")
    )
    source: Mapped[Optional[str]] = mapped_column(String(60), nullable=True)
    linked_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        CheckConstraint(
            "role IN ('subject','collateral','current_project','exit_property')",
            name="ck_fa_max_opp_prop_role",
        ),
        UniqueConstraint(
            "opportunity_id", "property_id", "role",
            name="uq_fa_max_opp_prop_role",
        ),
        Index("ix_fa_max_opp_prop_opp_id", "opportunity_id"),
        Index("ix_fa_max_opp_prop_property_id", "property_id"),
    )

    def __repr__(self) -> str:
        return (
            f"<FaMaxOpportunityProperty(opp={self.opportunity_id!r}, "
            f"prop={self.property_id!r}, role={self.role!r})>"
        )


class FaMaxStateTransitionEvent(Base):
    """Append-only audit of every FA Max state change — the event spine for WP-1.

    Written in the same transaction as the state-column update on the owning
    entity. Never updated, never deleted. The full ordered history of any
    entity's state changes is reconstructable by querying this table filtered
    on entity_uuid + occurred_at.

    entity_uuid is a real FK into fa_max_entity_registry — not a polymorphic
    text reference. This preserves referential integrity regardless of entity
    type. person_id is denormalized here as the borrower-history partition key
    (NULL when the entity has no person association, e.g. a property-only
    enrichment event).

    idempotency_key = ON CONFLICT DO NOTHING guard. Callers must supply a
    deterministic key (e.g. sha256 of entity_uuid+from_state+to_state+actor+
    epoch-minute) so retried transitions are safe no-ops.

    actor: who/what caused this transition. Format: 'agent:<name>' for
    autonomous agents, 'user:josh' for Josh, 'system:<component>' for
    scheduled jobs.

    source_component: the specific module that wrote the row. For tracing.

    decision_id: FK into agent_decisions when the transition was driven by an
    agent decision. NULL for system/human-initiated transitions.
    """

    __tablename__ = "fa_max_state_transition_events"

    event_id: Mapped[str] = mapped_column(
        PG_UUID(as_uuid=True), primary_key=True, server_default=text("generate_uuidv7()")
    )
    entity_uuid: Mapped[str] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey(
            "fa_max_entity_registry.entity_uuid",
            name="fk_fa_max_ste_entity_uuid",
        ),
        nullable=False,
    )
    person_id: Mapped[Optional[str]] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("fa_max_persons.person_id", name="fk_fa_max_ste_person_id"),
        nullable=True,
    )
    entity_type: Mapped[str] = mapped_column(String(30), nullable=False)
    from_state: Mapped[str] = mapped_column(String(50), nullable=False)
    to_state: Mapped[str] = mapped_column(String(50), nullable=False)
    actor: Mapped[str] = mapped_column(String(120), nullable=False)
    source_component: Mapped[str] = mapped_column(String(120), nullable=False)
    decision_id: Mapped[Optional[str]] = mapped_column(
        String(36),
        ForeignKey("agent_decisions.decision_id", name="fk_fa_max_ste_decision_id"),
        nullable=True,
    )
    context: Mapped[dict] = mapped_column(
        JSONB, nullable=False, server_default=text("'{}'::jsonb")
    )
    idempotency_key: Mapped[str] = mapped_column(String(255), nullable=False)
    occurred_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    # Monotonic ordering column — occurred_at uses NOW() (frozen at
    # transaction start), so a transaction that waits at the advisory lock
    # and commits later can carry an earlier occurred_at than one that
    # started later. seq is allocated at actual INSERT execution time and
    # is authoritative for reconstructing true event order; occurred_at
    # remains for human-readable attribution. IDENTITY here (not the
    # migration's BIGSERIAL) is the modern SQLAlchemy/Postgres equivalent —
    # functionally the same (auto-incrementing, unique, non-null); the
    # migration's ADD COLUMN IF NOT EXISTS is a no-op against a
    # create_all-built table where this column already exists.
    seq: Mapped[int] = mapped_column(
        BigInteger, Identity(always=False), nullable=False, unique=True
    )
    timeline_seq: Mapped[int] = mapped_column(
        BigInteger,
        FA_MAX_TIMELINE_SEQUENCE,
        server_default=FA_MAX_TIMELINE_SEQUENCE.next_value(),
        nullable=False,
    )

    __table_args__ = (
        CheckConstraint(
            "entity_type IN ('person','property','opportunity','partner','interaction')",
            name="ck_fa_max_ste_entity_type",
        ),
        UniqueConstraint("idempotency_key", name="uq_fa_max_ste_idempotency_key"),
        Index("ix_fa_max_ste_entity_uuid_occurred", "entity_uuid", "occurred_at"),
        Index("ix_fa_max_ste_person_id_occurred", "person_id", "occurred_at"),
        Index("ix_fa_max_ste_decision_id", "decision_id",
              postgresql_where=text("decision_id IS NOT NULL")),
        Index("ix_fa_max_ste_person_id_seq", "person_id", "seq"),
        Index("ix_fa_max_ste_entity_uuid_seq", "entity_uuid", "seq"),
        Index("ix_fa_max_ste_person_timeline_seq", "person_id", "timeline_seq"),
    )

    def __repr__(self) -> str:
        return (
            f"<FaMaxStateTransitionEvent(entity={self.entity_uuid!r}, "
            f"{self.from_state!r}->{self.to_state!r}, actor={self.actor!r})>"
        )


# ============================================================================
# FA Max WP-2 — Consent per contact per channel
# ============================================================================

class FaMaxPersonConsent(Base):
    """Opt-in consent record for one FA Max person on one channel.

    One row per (person_id, channel). Upserted when consent changes —
    source and consented_at always reflect the most recent event.
    No financial data; no rate/term/commitment fields.
    """
    __tablename__ = "fa_max_person_consent"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    person_id: Mapped[Any] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("fa_max_persons.person_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    channel: Mapped[str] = mapped_column(String(20), nullable=False)
    # email | sms | voice
    consented: Mapped[bool] = mapped_column(Boolean, nullable=False)
    source: Mapped[str] = mapped_column(String(120), nullable=False)
    # e.g. "opt_in_form", "import", "backflip_campaign_csv"
    consented_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(),
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(),
    )

    __table_args__ = (
        CheckConstraint(
            "channel IN ('email', 'sms', 'voice')",
            name="ck_fa_max_person_consent_channel",
        ),
        UniqueConstraint("person_id", "channel", name="uq_fa_max_person_consent_person_channel"),
    )

    def __repr__(self) -> str:
        return (
            f"<FaMaxPersonConsent(person={self.person_id!r}, "
            f"channel={self.channel!r}, consented={self.consented!r})>"
        )


class FaMaxBooking(Base):
    """One meeting the calendar tool scheduled on the client's calendar.

    Keyed by `booking_ref` rather than the provider's event id. A reschedule
    or cancellation arrives referring to a meeting that may since have been
    recreated provider-side under a new id, and the opportunity it belongs to
    needs a handle that survives that. The provider id is recorded alongside
    so the event can still be found, not as identity.

    No rate, term, or commitment fields — a booking records that a
    conversation was scheduled, never anything about the deal.
    """
    __tablename__ = "fa_max_bookings"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    booking_ref: Mapped[str] = mapped_column(String(40), nullable=False, unique=True)
    # Derived from the calendar, slot and attendee, so a replayed booking
    # resolves to the row it already created instead of a second meeting.
    idempotency_key: Mapped[Optional[str]] = mapped_column(
        String(64), nullable=True, unique=True,
    )
    tracked_link_id: Mapped[Optional[int]] = mapped_column(
        BigInteger,
        ForeignKey("tracked_links.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    calendar_id: Mapped[str] = mapped_column(String(320), nullable=False)
    provider_event_id: Mapped[Optional[str]] = mapped_column(
        String(200), nullable=True, index=True,
    )
    person_id: Mapped[Optional[Any]] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("fa_max_persons.person_id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    attendee_email: Mapped[str] = mapped_column(String(320), nullable=False, index=True)
    topic: Mapped[str] = mapped_column(String(200), nullable=False)
    starts_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, index=True,
    )
    ends_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    status: Mapped[str] = mapped_column(
        String(30), nullable=False, server_default=text("'confirmed'"),
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(),
    )
    updated_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True,
    )

    __table_args__ = (
        CheckConstraint(
            "status IN ('pending', 'confirmed', 'cancelled', 'reschedule_requested')",
            name="ck_fa_max_bookings_status",
        ),
        CheckConstraint("ends_at > starts_at", name="ck_fa_max_bookings_span"),
        # The partial unique index and the live-overlap exclusion constraint
        # are created in migrations/apply_fa_max_bookings_integrity.py — both
        # are filtered, and the exclusion needs the btree_gist extension.
    )

    def __repr__(self) -> str:
        return (
            f"<FaMaxBooking(ref={self.booking_ref!r}, "
            f"starts_at={self.starts_at!r}, status={self.status!r})>"
        )


class FaMaxFileState(Base):
    """WP-T2-6: Backflip-side stage detail for one submitted file.

    backflip_stage is deliberately NOT the same enum as
    fa_max_opportunities.current_stage — it tracks Backflip's finer-grained
    internal stage detail the coarse borrower-journey FSM has no room for.
    Runtime reads/writes go through src/services/fa_max_file_state.py via
    sqlalchemy.text(); this class exists as the schema source of truth for
    tests' create_all (CLAUDE.md, ADR 0024).

    Mirrors migrations/apply_fa_max_wp_t2_6_stage_monitoring.py.
    """
    __tablename__ = "fa_max_file_state"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    opportunity_id: Mapped[Any] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("fa_max_opportunities.opportunity_id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
    )
    person_id: Mapped[Any] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("fa_max_persons.person_id", ondelete="CASCADE"),
        nullable=False,
    )
    backflip_stage: Mapped[str] = mapped_column(
        Text, nullable=False, server_default=text("'submitted'")
    )
    contact_email: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    last_stage_change_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(),
    )
    last_borrower_touch_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    expected_next_stage: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    stall_flagged_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(),
    )

    __table_args__ = (
        CheckConstraint(
            "backflip_stage IN ('submitted', 'under_review', 'conditional_approval', "
            "'docs_requested', 'cleared_to_close', 'funded', 'declined')",
            name="ck_fa_max_file_state_stage",
        ),
        Index(
            "idx_fa_max_file_state_stall",
            "last_stage_change_at",
            postgresql_where=text("backflip_stage NOT IN ('funded', 'declined')"),
        ),
        Index(
            "idx_fa_max_file_state_touch",
            "last_borrower_touch_at",
            postgresql_where=text("backflip_stage NOT IN ('funded', 'declined')"),
        ),
    )

    def __repr__(self) -> str:
        return (
            f"<FaMaxFileState(opportunity={self.opportunity_id!r}, "
            f"stage={self.backflip_stage!r})>"
        )


class FaMaxDocumentRequests(Base):
    """WP-T2-6: one row per outstanding document ask, with its own chase timers.

    Per-document rather than per-file because one file can have several
    documents outstanding at once with different request dates and therefore
    independent follow-up/escalation clocks.

    Mirrors migrations/apply_fa_max_wp_t2_6_stage_monitoring.py.
    """
    __tablename__ = "fa_max_document_requests"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    opportunity_id: Mapped[Any] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("fa_max_opportunities.opportunity_id", ondelete="CASCADE"),
        nullable=False,
    )
    person_id: Mapped[Any] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("fa_max_persons.person_id", ondelete="CASCADE"),
        nullable=False,
    )
    document_name: Mapped[str] = mapped_column(Text, nullable=False)
    requested_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(),
    )
    received_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    first_chase_sent_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    followup_chase_sent_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    escalated_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    source: Mapped[str] = mapped_column(Text, nullable=False)
    idempotency_key: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(),
    )

    __table_args__ = (
        CheckConstraint(
            "source IN ('email_parsed', 'manual')",
            name="ck_fa_max_doc_request_source",
        ),
        UniqueConstraint("idempotency_key", name="uq_fa_max_doc_request_idempotency"),
        Index(
            "idx_fa_max_doc_requests_outstanding",
            "opportunity_id",
            postgresql_where=text("received_at IS NULL"),
        ),
        Index(
            "idx_fa_max_doc_requests_chase_due",
            "first_chase_sent_at",
            postgresql_where=text(
                "received_at IS NULL AND followup_chase_sent_at IS NULL"
            ),
        ),
    )

    def __repr__(self) -> str:
        return (
            f"<FaMaxDocumentRequests(opportunity={self.opportunity_id!r}, "
            f"document={self.document_name!r}, received_at={self.received_at!r})>"
        )

class FaMaxPersonFirstTouch(Base):
    """Permanent first clean FA reach, before an opportunity may exist."""
    __tablename__ = "fa_max_person_first_touch"

    person_id: Mapped[Any] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("fa_max_persons.person_id"), primary_key=True,
    )
    relay_item_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("relay_approval_queue.id"), nullable=False,
    )
    channel_split_source: Mapped[str] = mapped_column(String(60), nullable=False)
    claimed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())


class FaMaxPersonContactIdentifier(Base):
    """Operator-verified identifiers used for person-wide suppression."""
    __tablename__ = "fa_max_person_contact_identifiers"

    identifier_kind: Mapped[str] = mapped_column(String(10), primary_key=True)
    identifier_value: Mapped[str] = mapped_column(Text, primary_key=True)
    person_id: Mapped[Any] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("fa_max_persons.person_id"), nullable=False,
    )
    source: Mapped[str] = mapped_column(String(60), nullable=False)
    verified_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    __table_args__ = (
        CheckConstraint("identifier_kind IN ('email','phone')", name="ck_fa_max_person_identifier_kind"),
        Index("ix_fa_max_person_contact_identifiers_person", "person_id"),
    )


class FaMaxBackflipSuppressionDecision(Base):
    """Durable draft/send boundary decision; recipient digest avoids raw PII."""
    __tablename__ = "fa_max_backflip_suppression_decisions"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    gate: Mapped[str] = mapped_column(String(10), nullable=False)
    recipient_masked: Mapped[str] = mapped_column(String(20), nullable=False)
    recipient_sha256: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    opportunity_id: Mapped[Optional[Any]] = mapped_column(PG_UUID(as_uuid=True), nullable=True)
    suppressed: Mapped[bool] = mapped_column(Boolean, nullable=False)
    reason: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())

    __table_args__ = (
        CheckConstraint("gate IN ('draft','send')", name="ck_fa_max_bsd_gate"),
        Index("ix_fa_max_bsd_opportunity_id", "opportunity_id", postgresql_where=text("opportunity_id IS NOT NULL")),
        Index("ix_fa_max_bsd_created_at", created_at.desc()),
    )


class FaMaxBackflipCampaignContact(Base):
    """Current Backflip campaign membership; separate from permanent opt-outs."""
    __tablename__ = "fa_max_backflip_campaign_contacts"

    identifier_kind: Mapped[str] = mapped_column(String(10), primary_key=True)
    identifier_value: Mapped[str] = mapped_column(Text, primary_key=True)
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("true"))
    imported_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())

    __table_args__ = (
        CheckConstraint("identifier_kind IN ('email', 'phone')", name="ck_fa_max_backflip_identifier_kind"),
        Index("ix_fa_max_backflip_active_contact", "identifier_kind", "identifier_value", postgresql_where=text("active")),
    )


class FaMaxBackflipCampaignFeed(Base):
    """Last complete campaign snapshot, used to fail closed on stale data."""
    __tablename__ = "fa_max_backflip_campaign_feed"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    last_success_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (CheckConstraint("id = 1", name="ck_fa_max_backflip_feed_singleton"),)


# ============================================================================
# FA Max WP-1 remaining — Partners, Interactions, Property Associations,
# and the Durable Work Queue
# ============================================================================


class FaMaxPartner(Base):
    """Canonical partner/referral source record for FA Max.

    One row per unique referral relationship. status ∈ {identified, active,
    inactive} is a lightweight 3-state machine; transitions go through
    transition() with entity_type='partner' and CAS on state_version.

    DNC / suppression lives on fa_max_persons (person/consent boundary) and
    is never duplicated here. No financial data fields of any kind.
    """

    __tablename__ = "fa_max_partners"

    partner_id: Mapped[Any] = mapped_column(
        PG_UUID(as_uuid=True), primary_key=True, server_default=text("generate_uuidv7()")
    )
    person_id: Mapped[Any] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("fa_max_persons.person_id", name="fk_fa_max_partner_person"),
        nullable=False,
    )
    partner_class: Mapped[str] = mapped_column(String(60), nullable=False)
    status: Mapped[str] = mapped_column(
        String(20), nullable=False, server_default=text("'identified'")
    )
    rank: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    source: Mapped[str] = mapped_column(String(60), nullable=False)
    state_version: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default=text("0")
    )
    # WP-T2-9 ranking snapshot fields (apply_partner_ranking_snapshot.py)
    observed_transaction_count: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default=text("0")
    )
    first_observed_at: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    last_observed_at: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    county_id: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    buyer_entity_id: Mapped[Optional[int]] = mapped_column(
        Integer,
        ForeignKey("buyer_entities.id", name="fk_fa_max_partner_buyer_entity", ondelete="SET NULL"),
        nullable=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        CheckConstraint(
            "status IN ('identified', 'active', 'inactive')",
            name="ck_fa_max_partner_status",
        ),
        UniqueConstraint("person_id", "partner_class", name="uq_fa_max_partner_person_class"),
        Index("ix_fa_max_partner_person_id", "person_id"),
        Index("ix_fa_max_partner_status", "status"),
        Index("ix_fa_max_partner_class_txn", "partner_class", "observed_transaction_count"),
        Index("ix_fa_max_partner_buyer_entity", "buyer_entity_id"),
        Index("ix_fa_max_partner_county", "county_id"),
    )

    def __repr__(self) -> str:
        return (
            f"<FaMaxPartner(partner_id={self.partner_id!r}, "
            f"person={self.person_id!r}, class={self.partner_class!r}, "
            f"status={self.status!r})>"
        )


class FaMaxInteraction(Base):
    """Write-once record of a single communication interaction.

    Append-only — never updated after creation (enforced by a DB trigger
    installed by apply_fa_max_wp1_remaining.py). Contributes to the unified
    borrower timeline via get_borrower_timeline().

    approved_bool: True when the outbound draft was approved as-written,
    False when materially edited before send (edit rate tracking for
    autonomy-tier graduation evidence). NULL for inbound interactions.

    autonomy_tier_at_time: A/B/C at the moment of the send, for graduation
    evidence. NULL for inbound interactions where no tier applies.

    No body/content column — no PII storage obligation. Use body_redacted
    for a content-free summary (e.g., "initial outreach email") if needed.
    """

    __tablename__ = "fa_max_interactions"

    interaction_id: Mapped[Any] = mapped_column(
        PG_UUID(as_uuid=True), primary_key=True, server_default=text("generate_uuidv7()")
    )
    person_id: Mapped[Any] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("fa_max_persons.person_id", name="fk_fa_max_interaction_person"),
        nullable=False,
    )
    channel: Mapped[str] = mapped_column(String(20), nullable=False)
    direction: Mapped[str] = mapped_column(String(10), nullable=False)
    actor: Mapped[str] = mapped_column(String(120), nullable=False)
    approved_bool: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    autonomy_tier_at_time: Mapped[Optional[str]] = mapped_column(String(5), nullable=True)
    # WP-T2-2: which agent authored/drove this interaction. Nullable —
    # populated going forward by write_interaction()/mark_sent() callers.
    # Traffic direction continues to be carried by autonomy_tier_at_time's
    # existing (agent_name, autonomy_tier_at_time) pairing semantics; no
    # separate direction column is added.
    agent_name: Mapped[Optional[str]] = mapped_column(String(120), nullable=True)
    body_redacted: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    occurred_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    # Monotonic ordering for unified timeline — allocated at INSERT time.
    seq: Mapped[int] = mapped_column(
        BigInteger, Identity(always=False), nullable=False, unique=True
    )
    timeline_seq: Mapped[int] = mapped_column(
        BigInteger,
        FA_MAX_TIMELINE_SEQUENCE,
        server_default=FA_MAX_TIMELINE_SEQUENCE.next_value(),
        nullable=False,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        CheckConstraint(
            "channel IN ('email', 'sms', 'voice', 'slack', 'linkedin')",
            name="ck_fa_max_interaction_channel",
        ),
        CheckConstraint(
            "direction IN ('inbound', 'outbound')",
            name="ck_fa_max_interaction_direction",
        ),
        CheckConstraint(
            "autonomy_tier_at_time IS NULL OR autonomy_tier_at_time IN ('A', 'B', 'C')",
            name="ck_fa_max_interaction_tier",
        ),
        Index("ix_fa_max_interaction_person_id", "person_id"),
        Index("ix_fa_max_interaction_person_seq", "person_id", "seq"),
        Index("ix_fa_max_interaction_person_timeline_seq", "person_id", "timeline_seq"),
        Index(
            "ix_fa_max_interaction_agent_name", "agent_name",
            postgresql_where=text("agent_name IS NOT NULL"),
        ),
    )

    def __repr__(self) -> str:
        return (
            f"<FaMaxInteraction(interaction_id={self.interaction_id!r}, "
            f"person={self.person_id!r}, channel={self.channel!r}, "
            f"direction={self.direction!r})>"
        )


class FaMaxToolCallLog(Base):
    """Per-tool-call audit trail for the FA Max agent runtime (WP-T2-2).

    Separate from agent_decisions — agent_decisions records a DECISION
    (autonomy-tier-gated outcome, logged via write_tools.log_decision());
    this table records every individual tool INVOCATION inside the agent's
    bounded tool-call loop, whether or not it produced a decision. input/
    output are redacted (PII/financial-shaped values stripped) before
    storage — never raw payload content.
    """

    __tablename__ = "fa_max_tool_call_log"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    work_item_id: Mapped[Optional[Any]] = mapped_column(PG_UUID(as_uuid=True), nullable=True)
    agent_name: Mapped[str] = mapped_column(String(120), nullable=False)
    tool_name: Mapped[str] = mapped_column(String(120), nullable=False)
    input: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    output: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    duration_ms: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        CheckConstraint(
            "status IN ('in_progress', 'claimed', 'success', 'error', 'blocked')",
            name="ck_fa_max_tool_call_log_status",
        ),
        Index("ix_fa_max_tool_call_log_work_item", "work_item_id"),
        Index("ix_fa_max_tool_call_log_agent_tool", "agent_name", "tool_name", text("created_at DESC")),
    )

    def __repr__(self) -> str:
        return (
            f"<FaMaxToolCallLog(id={self.id}, agent={self.agent_name!r}, "
            f"tool={self.tool_name!r}, status={self.status!r})>"
        )


class FaMaxPropertyAssociation(Base):
    """Temporal association between a person and a property.

    Not a state machine — valid_from/valid_to is a temporal link pattern.
    valid_to=NULL means the association is current. Closing an association
    sets valid_to=NOW() via close_property_association(); it is never deleted.

    property_id is an integer FK to properties.id (the canonical property
    PK), not a text reference. The entity registry stores property native_id
    as text (matching properties.id cast to text) for polymorphic FK bookkeeping.

    Compliance: no financial data, no pricing/term/commitment fields.
    """

    __tablename__ = "fa_max_property_associations"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    person_id: Mapped[Any] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("fa_max_persons.person_id", name="fk_fa_max_prop_assoc_person"),
        nullable=False,
    )
    property_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("properties.id", name="fk_fa_max_prop_assoc_property"),
        nullable=False,
    )
    opportunity_id: Mapped[Optional[Any]] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("fa_max_opportunities.opportunity_id", name="fk_fa_max_prop_assoc_opp"),
        nullable=True,
    )
    role: Mapped[str] = mapped_column(
        String(30), nullable=False, server_default=text("'subject'")
    )
    valid_from: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    valid_to: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    source: Mapped[Optional[str]] = mapped_column(String(60), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    timeline_seq: Mapped[int] = mapped_column(
        BigInteger,
        FA_MAX_TIMELINE_SEQUENCE,
        server_default=FA_MAX_TIMELINE_SEQUENCE.next_value(),
        nullable=False,
    )

    __table_args__ = (
        CheckConstraint(
            "role IN ('subject', 'collateral', 'current_project', 'exit_property', 'owned')",
            name="ck_fa_max_prop_assoc_role",
        ),
        Index("ix_fa_max_prop_assoc_person_id", "person_id"),
        Index("ix_fa_max_prop_assoc_property_id", "property_id"),
        Index("ix_fa_max_prop_assoc_person_timeline_seq", "person_id", "timeline_seq"),
        Index(
            "ix_fa_max_prop_assoc_current",
            "person_id",
            "property_id",
            postgresql_where=text("valid_to IS NULL"),
        ),
    )

    def __repr__(self) -> str:
        return (
            f"<FaMaxPropertyAssociation(person={self.person_id!r}, "
            f"property={self.property_id!r}, role={self.role!r}, "
            f"valid_to={self.valid_to!r})>"
        )


class FaMaxPropertyAssociationEvent(Base):
    """Append-only facts for association lifecycle changes after creation."""

    __tablename__ = "fa_max_property_association_events"

    event_id: Mapped[Any] = mapped_column(
        PG_UUID(as_uuid=True), primary_key=True, server_default=text("generate_uuidv7()")
    )
    association_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("fa_max_property_associations.id"), nullable=False
    )
    person_id: Mapped[Any] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("fa_max_persons.person_id"), nullable=False
    )
    event_type: Mapped[str] = mapped_column(String(20), nullable=False)
    actor: Mapped[str] = mapped_column(String(120), nullable=False)
    source: Mapped[Optional[str]] = mapped_column(String(60), nullable=True)
    occurred_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    timeline_seq: Mapped[int] = mapped_column(
        BigInteger, FA_MAX_TIMELINE_SEQUENCE,
        server_default=FA_MAX_TIMELINE_SEQUENCE.next_value(), nullable=False,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        CheckConstraint("event_type IN ('closed')", name="ck_fa_max_property_association_event_type"),
        UniqueConstraint("association_id", "event_type", name="uq_fa_max_property_association_event"),
        Index("ix_fa_max_prop_assoc_event_person_timeline_seq", "person_id", "timeline_seq"),
    )


class FaMaxWorkQueue(Base):
    """Durable leased work queue for FA Max background operations.

    Designed for exactly-once processing with crash recovery:
    - claim_next() acquires a row using FOR UPDATE SKIP LOCKED, sets
      status='claimed', and writes a lease_expires_at deadline.
    - If the worker dies, reclaim_expired() returns rows with
      lease_expires_at < NOW() and status='claimed' back to 'available',
      incrementing attempt_count.
    - Workers must write status='done' before their lease expires.
    - idempotency_key prevents duplicate enqueuing of the same logical work.

    WP-1 Done When: a worker killed mid-task leaves state recoverable by a
    fresh instance — proven by test_real_worker_kill_work_queue_recovered.
    """

    __tablename__ = "fa_max_work_queue"

    work_item_id: Mapped[Any] = mapped_column(
        PG_UUID(as_uuid=True), primary_key=True, server_default=text("generate_uuidv7()")
    )
    person_id: Mapped[Optional[Any]] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("fa_max_persons.person_id", name="fk_fa_max_work_queue_person"),
        nullable=True,
    )
    queue_name: Mapped[str] = mapped_column(String(60), nullable=False)
    payload: Mapped[dict] = mapped_column(
        JSONB, nullable=False, server_default=text("'{}'::jsonb")
    )
    status: Mapped[str] = mapped_column(
        String(20), nullable=False, server_default=text("'available'")
    )
    idempotency_key: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    available_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    claimed_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    lease_expires_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    done_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    attempt_count: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default=text("0")
    )
    worker_id: Mapped[Optional[str]] = mapped_column(String(120), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        CheckConstraint(
            "status IN ('available', 'claimed', 'done', 'failed')",
            name="ck_fa_max_work_queue_status",
        ),
        Index(
            "uq_fa_max_work_queue_idempotency",
            "idempotency_key",
            unique=True,
            postgresql_where=text("idempotency_key IS NOT NULL"),
        ),
        Index("ix_fa_max_work_queue_queue_name", "queue_name"),
        Index(
            "ix_fa_max_work_queue_claimable",
            "queue_name",
            "available_at",
            postgresql_where=text("status = 'available'"),
        ),
        Index(
            "ix_fa_max_work_queue_expired_leases",
            "lease_expires_at",
            postgresql_where=text("status = 'claimed'"),
        ),
    )

    def __repr__(self) -> str:
        return (
            f"<FaMaxWorkQueue(work_item_id={self.work_item_id!r}, "
            f"queue={self.queue_name!r}, status={self.status!r})>"
        )


# ============================================================================
# WP-9 — DIAL-LIST FAILURE-BEHAVIOR STATE
# ============================================================================


class DialListSnapshot(Base):
    """Last successful dial list, retained for safe cached fallback delivery."""

    __tablename__ = "dial_list_snapshot"

    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True,
        autoincrement=True,
    )
    county_id: Mapped[Optional[str]] = mapped_column(Text)
    generated_for: Mapped[date] = mapped_column(Date, nullable=False)
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        Index("idx_dial_list_snapshot_county_date", "county_id", "generated_for"),
    )


class DialListTouch(Base):
    """Durable audit record for each Called or Skip interaction."""

    __tablename__ = "dial_list_touch"

    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True,
        autoincrement=True,
    )
    opportunity_thread_id: Mapped[Optional[str]] = mapped_column(Text)
    property_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    action: Mapped[str] = mapped_column(Text, nullable=False)
    actor: Mapped[Optional[str]] = mapped_column(Text)
    generation_date: Mapped[date] = mapped_column(Date, nullable=False)
    touched_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class DialListNeedsEnrichment(Base):
    """Candidate held from the dial list until it has a verified phone number."""

    __tablename__ = "dial_list_needs_enrichment"

    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True,
        autoincrement=True,
    )
    property_id: Mapped[int] = mapped_column(BigInteger, nullable=False, unique=True)
    reason: Mapped[str] = mapped_column(Text, nullable=False)
    first_seen: Mapped[date] = mapped_column(Date, nullable=False)
    last_seen: Mapped[date] = mapped_column(Date, nullable=False)
    retry_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)


# ============================================================================
# WP-5B — Borrower Buy Box, Velocity & Next-Need Prediction
# ============================================================================

class FaMaxPersonProfile(Base):
    """Per-person intelligence profile: buy-box, velocity, and predicted next
    financing need.

    Keyed on fa_max_persons.person_id (WP-1 canonical anchor). Updated nightly
    by src/tasks/fa_max_profile_sweep.py, which joins through the Hunter
    buyer_entities/buyer_entity_links resolution layer to aggregate deed/permit/
    financing-intent signals into a borrower-level view.

    Compliance: contains NO borrower financial data (no credit score, income,
    bank statements, tax returns, SSN). buy_box_price_band derives entirely
    from public-record deed sale prices (>$1 000 nominal-consideration floor,
    same as Hunter's portfolio_profiling). predicted_next_need is an internal
    product-category label; it is never a rate, term, or commitment to a
    borrower.

    buyer_entity_id is a provisional FK to buyer_entities.id populated by
    WP-5B's profile sweep when a matching BuyerEntity is found. WP-3/WP-4
    (person identity + entity-to-principal graph) will formalize this bridge
    with reversible-merge logging once those work packages ship. Until then
    the sweep does a best-effort name/address match and sets confidence_tier
    to 'low' or 'unknown' when no entity link is established.
    """

    __tablename__ = "fa_max_person_profiles"

    person_id: Mapped[str] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("fa_max_persons.person_id", name="fk_fa_max_profile_person"),
        primary_key=True,
    )

    # --- Provisional entity bridge (WP-3/WP-4 will formalize) ---------------
    # NULL when no BuyerEntity has been resolved for this person yet.
    buyer_entity_id: Mapped[Optional[int]] = mapped_column(
        Integer,
        ForeignKey("buyer_entities.id", name="fk_fa_max_profile_buyer_entity"),
        nullable=True,
        index=True,
    )

    # --- Buy-box profile (from deed history via entity links) ----------------
    # All three are NULL when confidence_tier = 'unknown'.
    buy_box_geography: Mapped[Optional[Any]] = mapped_column(
        JSONB,
        nullable=True,
        comment="city/county distribution: [{city, county_id, count}]",
    )
    buy_box_property_types: Mapped[Optional[Any]] = mapped_column(
        JSONB,
        nullable=True,
        comment="property type distribution: [{property_type, property_use_code, count}]",
    )
    buy_box_price_band: Mapped[Optional[Any]] = mapped_column(
        JSONB,
        nullable=True,
        comment="arm's-length sale price stats: {min_cents, median_cents, max_cents, sample_count}",
    )
    buy_box_preferences: Mapped[Optional[Any]] = mapped_column(
        JSONB,
        nullable=True,
        comment="property condition/size preferences: condition distribution, year_built range, beds/baths/lot averages",
    )

    # --- Deal velocity (mirrored from BuyerEntity cadence fields) ------------
    velocity_purchases_per_year: Mapped[Optional[Decimal]] = mapped_column(Numeric(6, 2), nullable=True)
    last_transaction_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    avg_days_between_transactions: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 1), nullable=True)
    active_property_count: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    # --- Predicted next need (rolled up from FinancingIntentScore per-property) ---
    # Product category only — never a rate, term, or commitment.
    predicted_next_need: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    predicted_next_need_date: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    next_need_evidence: Mapped[Optional[Any]] = mapped_column(
        JSONB,
        nullable=True,
        comment="top-3 source properties with financing_intent signal details",
    )

    # --- Confidence / data-sufficiency ---------------------------------------
    confidence_tier: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        server_default=text("'unknown'"),
    )

    computed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        CheckConstraint(
            "confidence_tier IN ('high', 'medium', 'low', 'unknown')",
            name="ck_fa_max_person_profile_confidence_tier",
        ),
        CheckConstraint(
            "predicted_next_need IS NULL OR predicted_next_need IN ("
            "'bridge', 'hard_money_purchase', 'renovation_capital', "
            "'heloc', 'cash_out_refi', 'buyout_refi')",
            name="ck_fa_max_person_profile_next_need",
        ),
        Index("ix_fa_max_person_profile_buyer_entity", "buyer_entity_id",
              postgresql_where=text("buyer_entity_id IS NOT NULL")),
        Index("ix_fa_max_person_profile_confidence", "confidence_tier"),
        Index("ix_fa_max_person_profile_computed_at", "computed_at",
              postgresql_where=text("computed_at IS NOT NULL")),
    )

    def __repr__(self) -> str:
        return (
            f"<FaMaxPersonProfile(person_id={self.person_id!r}, "
            f"confidence={self.confidence_tier!r}, "
            f"next_need={self.predicted_next_need!r})>"
        )


# ============================================================================
# WP-7 — Self-serve pre-fill path (tracked links)
# ============================================================================

class TrackedLink(Base):
    """A partner/campaign/source URL into the self-serve pre-fill flow.

    `property_id` is nullable and unset by the `/tracked-link` Slack command —
    every link falls back to address entry resolved through BaseLoader's
    matching waterfall. The column stays available for a future
    admin-API-minted link bound to one known property, but there is no
    per-property-mailer path (physical mail campaigns are out of scope; the
    kind was removed 2026-09-18 — it was never in the client's spec). See
    tasks/FA_Max_build/dev2_wp7_selfserve_prefill_plan.md WI-1.

    `buyer_entity_id` is a separate, independent axis from `property_id` —
    added 2026-09-18 so `/tracked-link` can bind a link to a known repeat
    borrower (exact canonical_name match, disambiguated by
    primary_mailing_address when the name alone is ambiguous — see
    src/services/tracked_links.py:find_buyer_entity_by_name) without
    requiring a target property address, which usually doesn't exist yet for
    a borrower's *next* deal. A property's current owner-of-record is not
    assumed to be the borrower buying it next, so this is never derived from
    `property_id` — it is only ever set from an explicit name match at mint
    time, or left NULL.
    """
    __tablename__ = "tracked_links"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    slug: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    kind: Mapped[str] = mapped_column(Text, nullable=False)
    label: Mapped[str] = mapped_column(Text, nullable=False)
    partner_ref: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    campaign_ref: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    property_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("properties.id"), nullable=True
    )
    buyer_entity_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("buyer_entities.id"), nullable=True
    )
    destination: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_by: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False,
        default=lambda: datetime.now(timezone.utc), server_default=func.now(),
    )

    __table_args__ = (
        CheckConstraint(
            "kind IN ('partner', 'campaign', 'source')",
            name="ck_tracked_links_kind",
        ),
    )

    def __repr__(self) -> str:
        return f"<TrackedLink(id={self.id}, slug={self.slug!r}, kind={self.kind!r})>"


class TrackedLinkClick(Base):
    """One row per click on a TrackedLink. `ip_hash` is a salted hash, never
    the raw IP. `session_token` ties this click to the selfserve session the
    borrower then fills out."""
    __tablename__ = "tracked_link_clicks"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    tracked_link_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("tracked_links.id"), nullable=False
    )
    clicked_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False,
        default=lambda: datetime.now(timezone.utc), server_default=func.now(),
    )
    ip_hash: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    user_agent: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    referer: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    session_token: Mapped[str] = mapped_column(Text, nullable=False)

    __table_args__ = (
        Index("idx_link_clicks_link", "tracked_link_id", "clicked_at"),
        Index("idx_link_clicks_session", "session_token"),
    )

    def __repr__(self) -> str:
        return f"<TrackedLinkClick(id={self.id}, link={self.tracked_link_id})>"


class SelfserveSession(Base):
    """One self-serve pre-fill session (WP-7 WI-3).

    Two identity FKs, not one — a real, confirmed gap in this codebase, not
    speculative design (see plan §1.5): `buyer_entity_id` is the WP-3/WP-4
    deed-side identity (BuyerEntity), `person_id` is the WP-1 governance-side
    identity (FaMaxPerson, required by relay_approval_queue's live CHECK
    constraint for any outbound send this session later triggers). Nothing in
    this codebase bridges the two yet — both are resolved independently here.

    `prefill_snapshot` is immutable after creation — same discipline as
    DealRoom.properties_snapshot, and for the same reason: the audit record of
    what the borrower was actually shown.
    """
    __tablename__ = "selfserve_sessions"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    token: Mapped[str] = mapped_column(PG_UUID(as_uuid=False), nullable=False, unique=True)
    tracked_link_id: Mapped[Optional[int]] = mapped_column(
        BigInteger, ForeignKey("tracked_links.id"), nullable=True
    )
    property_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("properties.id"), nullable=True
    )
    buyer_entity_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("buyer_entities.id"), nullable=True
    )
    person_id: Mapped[Optional[str]] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("fa_max_persons.person_id"), nullable=True
    )
    prefill_snapshot: Mapped[dict] = mapped_column(JSONB, nullable=False)
    corrections: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    confirmations: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    contact: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    status: Mapped[str] = mapped_column(Text, nullable=False, default="started")
    handoff_ref: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    handed_off_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False,
        default=lambda: datetime.now(timezone.utc), server_default=func.now(),
    )
    last_activity_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False,
        default=lambda: datetime.now(timezone.utc), server_default=func.now(),
    )
    expires_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False,
        default=lambda: datetime.now(timezone.utc) + timedelta(days=30),
        server_default=text("now() + interval '30 days'"),
    )

    __table_args__ = (
        CheckConstraint(
            "status IN ('started', 'prefilled', 'confirmed', 'handed_off', 'abandoned')",
            name="ck_selfserve_sessions_status",
        ),
        Index("idx_selfserve_status", "status", "started_at"),
        Index("idx_selfserve_person", "person_id"),
    )

    def __repr__(self) -> str:
        return f"<SelfserveSession(id={self.id}, token={self.token!r}, status={self.status!r})>"


class FaMaxArvResult(Base):
    """WP-8B canonical ARV result — one row per computed valuation of a property.

    Property-keyed and spine-independent. New computations that change the
    determinative inputs insert a row and supersede the previous current row;
    identical recomputes are no-ops. Published figures are stored rounded to
    the nearest $5,000, while comp details remain internal-only provenance.
    """

    __tablename__ = "fa_max_arv_results"

    arv_result_id: Mapped[str] = mapped_column(
        PG_UUID(as_uuid=True), primary_key=True, server_default=text("generate_uuidv7()")
    )
    property_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    computed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    low: Mapped[Optional[float]] = mapped_column(Numeric(14, 2))
    high: Mapped[Optional[float]] = mapped_column(Numeric(14, 2))
    point: Mapped[Optional[float]] = mapped_column(Numeric(14, 2))
    confidence: Mapped[Optional[str]] = mapped_column(String(10))
    comp_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    weak_comp: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("true"))
    locality_tier: Mapped[Optional[str]] = mapped_column(String(20))
    selected_comps: Mapped[Optional[list]] = mapped_column(JSONB)
    source: Mapped[str] = mapped_column(String(50), nullable=False)
    arv_unknown: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default=text("false")
    )
    calculation_version: Mapped[str] = mapped_column(String(20), nullable=False)
    input_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(
        String(20), nullable=False, server_default=text("'computed'")
    )
    supersedes_result_id: Mapped[Optional[str]] = mapped_column(PG_UUID(as_uuid=True))
    # Manual override with audit trail (WP-8B scope item, migrations/apply_wp8b_arv_override.py).
    # low/high/point above are the ORIGINAL computed figures and are never modified by an
    # override — see override_arv_result()'s docstring in arv_persistence.py.
    override_low: Mapped[Optional[float]] = mapped_column(Numeric(14, 2))
    override_point: Mapped[Optional[float]] = mapped_column(Numeric(14, 2))
    override_high: Mapped[Optional[float]] = mapped_column(Numeric(14, 2))
    override_reason: Mapped[Optional[str]] = mapped_column(Text)
    overridden_by: Mapped[Optional[str]] = mapped_column(Text)
    overridden_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))

    __table_args__ = (
        Index("idx_fa_max_arv_property_computed", "property_id", "computed_at"),
        Index("idx_fa_max_arv_property_status", "property_id", "status"),
        Index(
            "uq_fa_max_arv_one_computed_per_property",
            "property_id",
            unique=True,
            postgresql_where=text("status = 'computed'"),
            sqlite_where=text("status = 'computed'"),
        ),
        CheckConstraint(
            "status IN ('computed','superseded','overridden')", name="ck_fa_max_arv_status"
        ),
    )


class FaMaxThreadFallbackLog(Base):
    """Audit log for WP-T2-12 FA Max Slack LLM responder.

    One row per invocation — every authorized-approver message the responder
    classifies, whether a card-thread reply (relay_item_id set) or a top-level
    channel message (relay_item_id NULL). Feeds catalog tuning and human
    follow-up on 'other' bucket rows.
    """

    __tablename__ = "fa_max_thread_fallback_log"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    # NULL for top-level channel messages (no originating card).
    relay_item_id: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    slack_user_id: Mapped[str] = mapped_column(String(60), nullable=False)
    thread_ts: Mapped[str] = mapped_column(String(40), nullable=False)
    lane: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    raw_text: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    bucket: Mapped[str] = mapped_column(String(20), nullable=False)
    lookup_id: Mapped[Optional[str]] = mapped_column(String(60), nullable=True)
    reply_sent: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    tokens_in: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    tokens_out: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    cost_usd: Mapped[Optional[float]] = mapped_column(Numeric(12, 6), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        CheckConstraint(
            "bucket IN ('simple_lookup','cc_query','social','other')",
            name="ck_fa_max_thread_fallback_bucket",
        ),
        Index("ix_fa_max_thread_fallback_relay_item", "relay_item_id"),
        Index("ix_fa_max_thread_fallback_created", "created_at"),
    )


class FaMaxGyrRoutingLog(Base):
    """Immutable audit log — one row per GYR routing decision (WP-T2-11).

    Never updated. Every classify() call produces one row so routing history is
    fully reconstructable independent of the mutable gyr_* columns on
    fa_max_opportunities.
    """

    __tablename__ = "fa_max_gyr_routing_log"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    opportunity_id: Mapped[str] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("fa_max_opportunities.opportunity_id", name="fk_fa_max_gyr_log_opp"),
        nullable=False,
    )
    color: Mapped[str] = mapped_column(String(10), nullable=False)
    expected_revenue_cents: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    reason_codes: Mapped[Optional[list]] = mapped_column(JSONB, nullable=True)
    disqualifying_rule: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    queue: Mapped[Optional[str]] = mapped_column(String(12), nullable=True)
    decided_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        CheckConstraint("color IN ('green','yellow','red')", name="ck_fa_max_gyr_log_color"),
        CheckConstraint(
            "queue IN ('MONEY','EXCEPTIONS') OR queue IS NULL",
            name="ck_fa_max_gyr_log_queue",
        ),
        Index("ix_fa_max_gyr_log_opp_decided", "opportunity_id", "decided_at"),
    )


class FaMaxOpportunityFacts(Base):
    """WP-T3-7 — property/project facts intake for the Qualification Agent.

    One row per opportunity (1:1). The single write path is
    src.services.fa_max_qualification.set_facts() via
    POST /api/admin/fa-max/opportunities/{id}/facts — nothing else may
    INSERT/UPDATE this table. Schema mirrors
    migrations/apply_fa_max_opportunity_facts.py exactly; this model exists
    so Base.metadata.create_all() (this repo's test-fixture source of
    truth per CLAUDE.md) creates the table in a fresh/test environment
    without requiring the migration to have run first (code-review finding,
    2026-09: these tables previously existed only in the migration script).

    COMPLIANCE BOUNDARY (SOT.md Part 1): no borrower financial data column
    may ever be added here — credit_score, income, bank_statement,
    tax_return, ssn are permanently forbidden regardless of source.
    """

    __tablename__ = "fa_max_opportunity_facts"

    opportunity_id: Mapped[str] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("fa_max_opportunities.opportunity_id", name="fk_fa_max_opp_facts_opp"),
        primary_key=True,
    )
    facts_revision: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    property_id: Mapped[Optional[int]] = mapped_column(
        BigInteger, ForeignKey("properties.id", name="fk_fa_max_opp_facts_property")
    )
    purchase_price: Mapped[Optional[float]] = mapped_column(Numeric(14, 2))
    estimated_value: Mapped[Optional[float]] = mapped_column(Numeric(14, 2))
    assessed_value_mkt: Mapped[Optional[float]] = mapped_column(Numeric(14, 2))
    last_sale_price: Mapped[Optional[float]] = mapped_column(Numeric(14, 2))
    rehab_estimate: Mapped[Optional[float]] = mapped_column(Numeric(14, 2))
    rehab_source: Mapped[Optional[str]] = mapped_column(String(30))
    rehab_confidence: Mapped[Optional[str]] = mapped_column(String(10))
    arv: Mapped[Optional[float]] = mapped_column(Numeric(14, 2))
    arv_source: Mapped[Optional[str]] = mapped_column(String(80))
    arv_confidence: Mapped[Optional[str]] = mapped_column(String(10))
    expected_exit_strategy: Mapped[Optional[str]] = mapped_column(String(30))
    current_use: Mapped[Optional[str]] = mapped_column(String(60))
    existing_sqft: Mapped[Optional[int]] = mapped_column(Integer)
    facts_provenance: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        CheckConstraint(
            "rehab_source IS NULL OR rehab_source IN ('job_estimator', 'override')",
            name="ck_fa_max_opp_facts_rehab_source",
        ),
        CheckConstraint(
            "rehab_confidence IS NULL OR rehab_confidence IN ('high', 'medium', 'low')",
            name="ck_fa_max_opp_facts_rehab_confidence",
        ),
        CheckConstraint(
            "arv_source IS NULL OR (arv_source ~ '^[a-z0-9_.:-]+$'"
            " AND arv_source !~* '(credit_score|income|bank_statement|tax_return|ssn|fico|dti|debt_to_income)')",
            name="fa_max_opportunity_facts_arv_source_check",
        ),
        CheckConstraint(
            "arv_confidence IS NULL OR arv_confidence IN ('high', 'medium', 'low')",
            name="ck_fa_max_opp_facts_arv_confidence",
        ),
        CheckConstraint(
            "expected_exit_strategy IS NULL OR expected_exit_strategy IN"
            " ('sale','rent','dscr','refinance','unknown')",
            name="ck_fa_max_opp_facts_exit_strategy",
        ),
        CheckConstraint(
            "current_use IS NULL OR current_use IN"
            " ('single_family','multi_family_2_4','multi_family_5plus',"
            " 'condo','townhouse','vacant_land','commercial','mixed_use','other')",
            name="fa_max_opportunity_facts_current_use_check",
        ),
        CheckConstraint(
            "existing_sqft IS NULL OR existing_sqft >= 0",
            name="ck_fa_max_opp_facts_sqft",
        ),
        Index("ix_fa_max_opp_facts_rev", "opportunity_id", "facts_revision"),
    )


class FaMaxQualificationDecision(Base):
    """WP-T3-7 — append-only audit row per sufficiency evaluation.

    Written by src.services.fa_max_qualification.evaluate_sufficiency(). See
    FaMaxOpportunityFacts's docstring for why this model exists alongside
    the migration.
    """

    __tablename__ = "fa_max_qualification_decisions"

    decision_id: Mapped[str] = mapped_column(
        PG_UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    opportunity_id: Mapped[str] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("fa_max_opportunities.opportunity_id", name="fk_fa_max_qual_dec_opp"),
        nullable=False,
    )
    facts_revision: Mapped[int] = mapped_column(Integer, nullable=False)
    checklist_version: Mapped[str] = mapped_column(String(40), nullable=False)
    verdict: Mapped[str] = mapped_column(String(30), nullable=False)
    gaps: Mapped[list] = mapped_column(JSONB, nullable=False, server_default=text("'[]'::jsonb"))
    gap_content_hash: Mapped[Optional[str]] = mapped_column(String(64))
    decided_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    decided_by: Mapped[str] = mapped_column(String(80), nullable=False)

    __table_args__ = (
        CheckConstraint(
            "verdict IN ('sufficient', 'insufficient', 'pending_enrichment',"
            " 'sufficient_pending_contract')",
            name="ck_fa_max_qual_dec_verdict",
        ),
        Index("ix_fa_max_qd_opp_decided", "opportunity_id", "decided_at"),
        Index("ix_fa_max_qd_opp_rev", "opportunity_id", "facts_revision", "checklist_version"),
    )
