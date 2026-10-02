"""Wave 0 Calling Pool Extraction — WP-W0-1.

Reads the existing FA database (Hillsborough, Pinellas, Pasco) and produces
three calling pools for the Lending Engine (Cora):

    Pool 1  wholesaler_flipper  — buyer_entities with buyer_type IN ('wholesaler','flipper')
    Pool 2  active_builder      — two tagged sub-lists:
                                    List 3 = DBPR-licensed Cert Building/Residential contractors
                                    List 7 = property owners with active structural permits (NOC)
    Pool 3  mortgage_broker     — OFR "Ch 494 MBR-MBRB" registry, loaded via ofr_broker_load;
                                   fail-closed (0 records) until the table exists and holds rows

Output lands in ``lending_calling_pool_staging`` in the FA database.  This is
a Wave 0 placeholder location.  The final isolated lending schema (O1) is
owned by Developer 2 — once that schema ships, this writer swaps in the real
target table without logic changes.

Open items this file is waiting on:
  O1   — Lending-schema location (Dev 2). Staging table used as placeholder.
  O11  — Wholesaler definition (buyer_type vs raw deed-velocity). Using buyer_type.
  O12  — Builder permit_type definitions. Using STRUCTURAL_KEYWORDS from config.
  O14  — Intent filter applicability to non-property pools. Applied where property
          anchor exists; non-anchored records pass through at tier='unscored'.
  O15  — No-phone handling: RESOLVED by lead — Tracerfy only (both skip-trace AND
          DNC check), per client. BatchData is explicitly NOT used (no credits
          available). Rows without a normalised phone are staged with
          phone_available=False for WP-W0-3's Tracerfy-only enrichment pass.
  O16  — Multi-pool dedup precedence. Pool 2 > Pool 1 (same entity in both pools
          keeps the Pool 2 row; pools never share a unique phone).
  O28  — Estimated Loan Value formula. Interim formulas in POOL_ELV_FACTORS below.
  O29  — Recent Permit Details for non-builder pools. Omitted; field is NULL.

Pasco/Builders (RESOLVED by lead): include Pasco in Pool 2 scope IF DBPR data
exists for it — no separate code gate needed, since the DBPR query already
filters by county_id and returns zero Pasco rows naturally if none exist
(never fabricates builders). Actual Pasco DBPR row-count verification is
still blocked on live-DB access (connection to the production host timed out
from this environment both in this pass and the prior session) — run the
query below once connected to confirm real coverage:
    SELECT license_type_desc, COUNT(*) FROM dbpr_contacts
    WHERE county_id = 'pasco' GROUP BY license_type_desc;

IMPORTANT: estimated_loan_value is an INTERNAL CALLER REFERENCE only.  It is
derived from public-record job_value / sale_price.  It is never a quote, term,
rate commitment, or offer to the borrower.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from config.financing_intent import STRUCTURAL_KEYWORDS
from src.services.phone_utils import normalize as normalize_phone

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration constants
# Wave 0 geography — hardcoded per developer split §1 "Scope: Hillsborough
# and Pinellas counties only."
# ---------------------------------------------------------------------------

WAVE0_COUNTY_NAMES: tuple[str, ...] = ("Hillsborough", "Pinellas", "Pasco")

# Aircall campaign tags — spec §4.3
AIRCALL_TAG: dict[str, str] = {
    "wholesaler_flipper": "DESK_CAPITAL_LOOP",
    "active_builder": "DESK_CONSTRUCTION",
    "mortgage_broker": "DESK_RESCUE",
}

# Josh's List 1-9 campaign taxonomy (client comments answers doc + questionnaire v2).
# Builders map to two lists depending on source:
#   List 3 = DBPR-licensed contractors (license registry — active builders by credential)
#   List 7 = NOC/permit property owners (building_permits — active construction projects)
# Brokers: List 4.
# Wholesaler/Flipper: PROVISIONAL — the client taxonomy names Lists 3, 4, 7 explicitly
# but has no explicit number for the wholesaler/flipper pool; left None until Josh
# confirms in the next launch prep session.
CAMPAIGN_LIST: dict[str, str | None] = {
    "wholesaler_flipper": None,          # provisional — needs Josh confirmation
    "active_builder_dbpr": "List 3",     # DBPR-licensed contractors
    "active_builder_noc": "List 7",      # NOC/permit property owners
    "mortgage_broker": "List 4",
}

# Permit lookback window — spec §4.1 "12-month active permits"
BUILDER_PERMIT_LOOKBACK_MONTHS: int = 12

# Intent-filter minimum tier — O14 deferred; 'medium' used as interim default.
# 'medium' maps to financing_intent_score >= 45 (config/financing_intent.py).
# Change to 'high' or 'low' here when O14 is resolved.
INTENT_MIN_TIER: str = "medium"
TIER_ORDER: dict[str, int] = {"high": 2, "medium": 1, "low": 0}


def meets_intent_threshold(intent_tier: Optional[str], min_tier: str = INTENT_MIN_TIER) -> bool:
    """O14 intent gate: True if intent_tier is at or above min_tier.

    'unscored'/None always passes — those records have no property anchor, so
    intent scoring does not apply. Used by the downstream Aircall-load step; the
    Wave 0 staging export keeps every row (Dev 2 needs the full A1 denominator).
    """
    if intent_tier == "unscored" or intent_tier is None:
        return True
    return TIER_ORDER.get(intent_tier, -1) >= TIER_ORDER.get(min_tier, 0)

# Estimated Loan Value — O28, anchored to the spec's stated loan economics
# (INTERNAL caller-context only, NEVER quoted to a borrower; the copy-safety rule
# in spec §5 forbids stating LTC/rate/terms in any outbound message).
#   Construction (spec p1/p4): $500K min, $525K avg, 85% LTC ground-up program.
#   Fix & Flip (spec p8): $100K min loan size.
CONSTRUCTION_LTC = 0.85          # 85% loan-to-cost (spec p4)
CONSTRUCTION_MIN_LOAN = 500_000  # spec p1/p8
CONSTRUCTION_AVG_LOAN = 525_000  # spec p1 (fallback when no job_value)
FLIP_MIN_LOAN = 100_000          # spec p8
FLIP_LOAN_FACTOR = 0.75          # bridge estimate off last sale_price

# O12 — Builder identification. The spec (§5.9 / p18) defines a builder via DBPR
# licensing + permit history. Prod permit data barely captures the contractor
# (only 61 of 52k permits have a name), so the permit-only path finds ~32.
# The real builder population is the DBPR construction registry. Pool 2 sources
# from DBPR (matching §5.9's "maps active state license numbers from DBPR") —
# phones come via skip-trace (WP-W0-3), same as the other pools.
BUILDER_MIN_PROJECTS = 1  # retained for the permit-history refinement (§5.9 Wave 1)

# DBPR license types scoped to spec §4.1's "single-family and infill" builder,
# per FL Statute 489 license classes (verified, not keyword-guessed):
#   Cert Residential (CRC) — single/duplex/triplex/fourplex only, <=2 stories:
#       the exact "single-family" match.
#   Cert Building (CBC) — up to 3 stories, residential + light commercial:
#       covers infill/small multi-family.
#   Cert General (CGC) — EXCLUDED: unlimited scope (high-rise, commercial,
#       industrial) — too broad, does not match "single-family and infill."
# ('Residental' is a real misspelling in the source data — matched verbatim,
# not by pattern, so this list is an exact license_type_desc match, not ILIKE.)
BUILDER_DBPR_LICENSE_TYPES: list[str] = ["Cert Building", "Cert Residental"]

# SQL ILIKE patterns built once from STRUCTURAL_KEYWORDS
_STRUCTURAL_PATTERNS: list[str] = [f"%{kw}%" for kw in STRUCTURAL_KEYWORDS]

# Wave 0 geography is Florida only (spec §4.1). Every row carries state='FL' so
# Dev 2's Georgia entity rule has a value to read (Georgia ingestion is Wave 2).
WAVE0_STATE: str = "FL"

# entity_status vocabulary — mapping agreed with Dev 2 (compliance).
# buyer_entities.entity_type → compliance vocab. Anything unmapped/missing → None
# (fail-closed: Dev 2's Georgia LLC/LP/CORPORATION allow-list blocks NULL).
_ENTITY_STATUS_MAP: dict[str, str] = {
    "llc": "LLC",
    "corporate": "CORPORATION",
    "individual": "NATURAL_PERSON",
    "trust": "TRUST",
}


def _map_entity_status(entity_type: Optional[str]) -> Optional[str]:
    """Map buyer_entities.entity_type to Dev 2's compliance vocabulary, or None."""
    return _ENTITY_STATUS_MAP.get((entity_type or "").strip().lower())


# Name-match threshold for the owner-builder fallback. Set at 70 (not FA's
# general 75) because permit owner records carry co-owners and truncated
# surnames ("ABEL AND ADRIAN CALV"), which hold genuine matches at ~72 while
# unrelated owners score ≤ ~45 — a wide, safe separation on observed data.
_NAME_MATCH_MIN = 70


def _names_match(a: Optional[str], b: Optional[str]) -> bool:
    """True if two person/company names are the same party (rapidfuzz ≥ 75).

    Used to decide whether a permit property's OWNER is actually the builder
    (owner-builder / infill spec builder) before borrowing the owner's phone.
    Guards against attaching a random homeowner's phone to a contractor.
    """
    if not a or not b:
        return False
    try:
        from rapidfuzz.fuzz import token_set_ratio
    except Exception:
        return False
    # token_set_ratio (not token_sort): a builder whose name is a subset of a
    # multi-owner record ("ADRIAN CALVO" within "ABEL AND ADRIAN CALVO") still
    # scores high, while an unrelated owner stays low.
    return token_set_ratio(a.strip().lower(), b.strip().lower()) >= _NAME_MATCH_MIN


# ---------------------------------------------------------------------------
# Output record
# ---------------------------------------------------------------------------

@dataclass
class CallingPoolRecord:
    """One row destined for lending_calling_pool_staging.

    The five spec §4.3 dialer-display attributes are:
        borrower_name          → Borrower Name
        entity_name            → Entity Name (LLC / company)
        target_property_address → Target Property Address
        estimated_loan_value   → Estimated Loan Value (INTERNAL estimate)
        recent_permit_details  → Recent Permit Details
    """

    run_id: str
    pool_name: str                        # wholesaler_flipper | active_builder | mortgage_broker
    county_id: str
    county_name: str

    # ── Spec §4.3 dialer display attributes ─────────────────────────────
    borrower_name: Optional[str]          # "Borrower Name"
    entity_name: Optional[str]            # "Entity Name" (LLC / company)
    target_property_address: Optional[str]  # "Target Property Address"
    estimated_loan_value: Optional[Decimal]  # "Estimated Loan Value" — INTERNAL ESTIMATE
    recent_permit_details: Optional[str]  # "Recent Permit Details"

    # ── Compliance / geo (for Dev 2's DNC + Georgia rules) ──────────────
    entity_status: Optional[str]          # LLC | CORPORATION | NATURAL_PERSON | TRUST | None
    parcel_id: Optional[str]
    zip: Optional[str]
    state: Optional[str]                  # 'FL' for every Wave 0 row

    # ── Contact ─────────────────────────────────────────────────────────
    normalized_phone: Optional[str]       # E.164 or None (see O15)
    phone_available: bool
    line_type: Optional[str]              # 'mobile' | 'landline' | 'unknown' | None
    email: Optional[str]

    # ── Intent filter (spec §4.1 "Filter: Intent Scoring") ──────────────
    financing_intent_score: Optional[Decimal]
    intent_tier: Optional[str]            # high | medium | low | unscored
    recommended_product: Optional[str]

    # ── Routing + campaign taxonomy ─────────────────────────────────────
    aircall_campaign_tag: str
    campaign_list: Optional[str]          # Josh's List 1-9; see CAMPAIGN_LIST
    buyer_entity_id: Optional[int]        # set for Pool 1
    permit_number: Optional[str]          # set for Pool 2
    dbpr_license_number: Optional[str]    # reserved for Pool 3
    source_property_id: Optional[int]
    source_table: str
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def extract_calling_pools(
    session: Session,
    *,
    dry_run: bool = False,
    county_names: tuple[str, ...] = WAVE0_COUNTY_NAMES,
) -> dict[str, Any]:
    """Extract all three calling pools and write to lending_calling_pool_staging.

    Returns a summary dict with per-pool counts, phone-available counts, and
    the run_id so callers can audit the exact rows written.

    Args:
        session:      Active SQLAlchemy session (read-only queries + one batch write).
        dry_run:      If True, extract and return counts but skip all DB writes.
        county_names: County names to include; defaults to Wave 0 scope.
    """
    run_id = str(uuid.uuid4())
    county_ids = _resolve_county_ids(session, county_names)
    if not county_ids:
        logger.error("No county rows found for %s — aborting extraction", county_names)
        return {"error": "county_ids_not_found", "county_names": county_names}

    logger.info("run_id=%s county_ids=%s", run_id, county_ids)

    pool1 = _extract_pool1_wholesaler_flipper(session, county_ids)
    pool2 = _extract_pool2_active_builder(session, county_ids)
    pool3 = _extract_pool3_mortgage_broker(session, county_ids)

    all_records = _dedup_across_pools(pool1, pool2, pool3)
    _attach_intent_scores(session, all_records)
    _finalize_run_metadata(all_records, run_id)

    summary: dict[str, Any] = {
        "run_id": run_id,
        "dry_run": dry_run,
        "county_ids": county_ids,
        "pools": {
            "wholesaler_flipper": {
                "total": sum(1 for r in all_records if r.pool_name == "wholesaler_flipper"),
                "phone_available": sum(
                    1 for r in all_records
                    if r.pool_name == "wholesaler_flipper" and r.phone_available
                ),
            },
            "active_builder": {
                "total": sum(1 for r in all_records if r.pool_name == "active_builder"),
                "phone_available": sum(
                    1 for r in all_records
                    if r.pool_name == "active_builder" and r.phone_available
                ),
            },
            "mortgage_broker": {
                "total": sum(1 for r in all_records if r.pool_name == "mortgage_broker"),
                "note": (
                    "Spec §4.1 source (OFR/NMLS professional licensing registry) not ingested "
                    "in FA — fail-closed, 0 records. Blocked on O4/O13."
                ),
            },
        },
        "total_records": len(all_records),
        "total_phone_available": sum(1 for r in all_records if r.phone_available),
    }

    if not dry_run:
        written = _write_to_staging(session, all_records)
        summary["rows_written"] = written
        logger.info("run_id=%s wrote %d rows to lending_calling_pool_staging", run_id, written)
    else:
        logger.info("run_id=%s dry_run=True skipping DB write (%d records)", run_id, len(all_records))

    return summary


# ---------------------------------------------------------------------------
# Pool 1 — Wholesalers / Flippers
# ---------------------------------------------------------------------------

def _extract_pool1_wholesaler_flipper(
    session: Session,
    county_ids: list[str],
) -> list[CallingPoolRecord]:
    """Query buyer_entities with buyer_type IN ('wholesaler','flipper').

    Primary source of truth for Pool 1 is the existing buyer_entity_resolution
    pipeline output — avoids building a second, drifting flipper definition on
    top of raw deed-velocity.  Raw deed-velocity fallback (O11) deferred.

    County filter: entity must have at least one linked property in the target
    counties (via buyer_entity_links → properties).
    """
    rows = session.execute(
        text("""
            SELECT DISTINCT ON (be.id)
                be.id                           AS buyer_entity_id,
                be.canonical_name               AS canonical_name,
                be.entity_type                  AS entity_type,
                be.principal_name               AS principal_name,
                -- Entity's own phone/email: resolver's modal value first, then
                -- the entity's OWN linked owner records (the same person, clustered
                -- by the resolver). NOT enriched_contacts by the transacted
                -- property (that's the seller/owner of that deal, a different person).
                COALESCE(be.primary_phone, oc.owner_phone, ecc.ec_phone) AS raw_phone,
                COALESCE(be.primary_email, oc.owner_email, ecc.ec_email) AS email,
                d.property_id                   AS source_property_id,
                p.county_id                     AS county_id,
                c.display_name                  AS county_name,
                p.parcel_id                     AS parcel_id,
                p.address                       AS prop_address,
                p.city                          AS prop_city,
                p.state                         AS prop_state,
                p.zip                           AS prop_zip,
                d.sale_price                    AS last_sale_price
            FROM buyer_entities be
            JOIN buyer_entity_links bel ON bel.buyer_entity_id = be.id
                                       AND bel.source_table IN ('deeds', 'deed_wholesaler')
            JOIN deeds d    ON d.id = bel.source_id
            JOIN properties p ON p.id = d.property_id
            JOIN counties c   ON c.county_id = p.county_id
            -- The entity's OWN contact, via its clustered owner records
            -- (buyer_entity_links source_table='owners' → owners). These records
            -- ARE this buyer (the resolver clustered them), so their skip-traced
            -- phone is the buyer's — unlike the transacted property's owner.
            LEFT JOIN LATERAL (
                SELECT COALESCE(o.phone_1, o.phone_2, o.phone_3) AS owner_phone,
                       o.email_1                                  AS owner_email
                FROM buyer_entity_links bl
                JOIN owners o ON o.id = bl.source_id
                WHERE bl.buyer_entity_id = be.id
                  AND bl.source_table = 'owners'
                  AND COALESCE(o.phone_1, o.phone_2, o.phone_3) IS NOT NULL
                LIMIT 1
            ) oc ON TRUE
            -- Enriched (skip-traced) phone on the entity's OWN properties, via its
            -- clustered owner records — the same person, not the transacted deed's owner.
            LEFT JOIN LATERAL (
                SELECT COALESCE(ec.mobile_phone, ec.landline) AS ec_phone,
                       ec.email                                AS ec_email
                FROM buyer_entity_links bl
                JOIN owners o ON o.id = bl.source_id
                JOIN enriched_contacts ec ON ec.property_id = o.property_id
                                         AND ec.superseded_at IS NULL
                                         AND ec.match_success
                WHERE bl.buyer_entity_id = be.id
                  AND bl.source_table = 'owners'
                  AND COALESCE(ec.mobile_phone, ec.landline) IS NOT NULL
                ORDER BY ec.confidence DESC NULLS LAST, ec.enriched_at DESC
                LIMIT 1
            ) ecc ON TRUE
            WHERE be.buyer_type IN ('wholesaler', 'flipper')
              AND p.county_id = ANY(:county_ids)
            ORDER BY be.id, d.record_date DESC NULLS LAST
        """),
        {"county_ids": county_ids},
    ).fetchall()

    records: list[CallingPoolRecord] = []
    for row in rows:
        norm = normalize_phone(row.raw_phone)
        # O28 Fix & Flip: bridge estimate off last sale price, floored at the
        # spec's $100K flip minimum. Internal reference only.
        if row.last_sale_price:
            elv = max(
                Decimal(str(FLIP_MIN_LOAN)),
                Decimal(str(row.last_sale_price)) * Decimal(str(FLIP_LOAN_FACTOR)),
            )
        else:
            elv = Decimal(str(FLIP_MIN_LOAN))
        # Entity vs. person split for the §4.3 display: an LLC/Corporate/Trust
        # entity's canonical_name is the company; the pierced principal (if any)
        # is the borrower. An Individual entity is the borrower itself.
        is_company = (row.entity_type or "").lower() in ("llc", "corporate", "trust")
        borrower_name = row.principal_name if is_company else row.canonical_name
        entity_name = row.canonical_name if is_company else None

        records.append(CallingPoolRecord(
            run_id="",  # filled by _finalize_run_metadata
            pool_name="wholesaler_flipper",
            county_id=str(row.county_id),
            county_name=row.county_name,
            borrower_name=borrower_name,
            entity_name=entity_name,
            target_property_address=_compose_address(
                row.prop_address, row.prop_city, row.prop_state, row.prop_zip
            ),
            estimated_loan_value=elv,
            recent_permit_details=None,   # O29 — not applicable to wholesalers
            entity_status=_map_entity_status(row.entity_type),
            parcel_id=row.parcel_id,
            zip=row.prop_zip,
            state=row.prop_state or WAVE0_STATE,
            normalized_phone=norm,
            phone_available=norm is not None,
            line_type="unknown",           # buyer_entity phone source doesn't distinguish mobile/landline
            email=row.email,
            financing_intent_score=None,   # attached by _attach_intent_scores
            intent_tier=None,
            recommended_product=None,
            aircall_campaign_tag=AIRCALL_TAG["wholesaler_flipper"],
            campaign_list=CAMPAIGN_LIST["wholesaler_flipper"],  # None — provisional, needs Josh confirmation
            buyer_entity_id=row.buyer_entity_id,
            permit_number=None,
            dbpr_license_number=None,
            source_property_id=row.source_property_id,
            source_table="buyer_entities",
        ))

    logger.info("Pool 1 wholesaler_flipper: %d raw rows", len(records))
    return records


# ---------------------------------------------------------------------------
# Pool 2 — Active Builders
# ---------------------------------------------------------------------------

def _extract_pool2_active_builder(
    session: Session,
    county_ids: list[str],
) -> list[CallingPoolRecord]:
    """Builders = DBPR-licensed construction contractors (spec §5.9 / p18).

    O12: prod permit data barely captures the contractor (only 61 of 52k permits
    carry a name), so the permit-only path finds ~32. The real builder population
    is the DBPR construction registry, scoped to Cert Building + Cert Residential
    (single-family/infill per spec §4.1 — Cert General excluded as out-of-scope,
    see BUILDER_DBPR_LICENSE_TYPES). Pool 2 sources from DBPR (this is §5.9's
    "maps active state license numbers from DBPR"), one row per licensed contractor.

    Phone: DBPR's own phone fields first (mobile/phone/landline). DBPR phones are
    populated by the separate DBPR enrichment; anything still phone-less flows to
    the shared skip-trace queue (WP-W0-3) with every other phone-less pool row.
    """
    rows = session.execute(
        text("""
            SELECT
                dc.license_number,
                dc.full_name                AS borrower_name,
                dc.company_name             AS entity_name,
                dc.license_type_desc,
                dc.license_expiry,
                dc.address                  AS prop_address,
                dc.city                     AS prop_city,
                dc.state                    AS prop_state,
                dc.zip_code                 AS prop_zip,
                dc.county_id,
                c.display_name              AS county_name,
                COALESCE(dc.mobile_phone, dc.phone, dc.landline_phone) AS raw_phone,
                -- Capture which DBPR phone field was used so line_type is deterministic.
                -- DBPR stores mobile and landline in separate columns; 'phone' is untyped.
                CASE
                    WHEN dc.mobile_phone IS NOT NULL THEN 'mobile'
                    WHEN dc.phone IS NOT NULL THEN 'unknown'
                    WHEN dc.landline_phone IS NOT NULL THEN 'landline'
                    ELSE NULL
                END AS phone_line_type,
                dc.email
            FROM dbpr_contacts dc
            JOIN counties c ON c.county_id = dc.county_id
            WHERE dc.county_id = ANY(:county_ids)
              AND dc.license_type_desc = ANY(:lic_types)
        """),
        {
            "county_ids": county_ids,
            "lic_types": BUILDER_DBPR_LICENSE_TYPES,
        },
    ).fetchall()

    records: list[CallingPoolRecord] = []
    for row in rows:
        norm = normalize_phone(row.raw_phone)
        # O28 New Construction: no per-deal job value from DBPR → the spec's
        # $525K construction average as the internal estimate.
        elv = Decimal(str(CONSTRUCTION_AVG_LOAN))
        detail = row.license_type_desc + (
            f" · lic {row.license_number}" if row.license_number else ""
        ) + (f" · exp {row.license_expiry}" if row.license_expiry else "")

        records.append(CallingPoolRecord(
            run_id="",
            pool_name="active_builder",
            county_id=str(row.county_id),
            county_name=row.county_name,
            borrower_name=row.borrower_name,
            entity_name=row.entity_name,
            target_property_address=_compose_address(
                row.prop_address, row.prop_city, row.prop_state, row.prop_zip
            ),
            estimated_loan_value=elv,
            recent_permit_details=detail,
            entity_status=_entity_status_from_firm_name(row.entity_name),
            parcel_id=None,                       # DBPR is contractor-level, no property anchor
            zip=row.prop_zip,
            state=row.prop_state or WAVE0_STATE,
            normalized_phone=norm,
            phone_available=norm is not None,
            line_type=row.phone_line_type,        # 'mobile'|'landline'|'unknown' from DBPR columns
            email=row.email,
            financing_intent_score=None,
            intent_tier=None,
            recommended_product=None,
            aircall_campaign_tag=AIRCALL_TAG["active_builder"],
            campaign_list=CAMPAIGN_LIST["active_builder_dbpr"],  # List 3
            buyer_entity_id=None,
            permit_number=None,
            dbpr_license_number=row.license_number,
            source_property_id=None,
            source_table="dbpr_contacts",
        ))

    noc_records = _extract_pool2b_noc_permits(session, county_ids)
    records.extend(noc_records)

    logger.info(
        "Pool 2 active_builder: %d DBPR (List 3) + %d NOC/permits (List 7), %d with phone",
        len(records) - len(noc_records),
        len(noc_records),
        sum(1 for r in records if r.phone_available),
    )
    return records


# ---------------------------------------------------------------------------
# Pool 2b — NOC / Permits (List 7)
# ---------------------------------------------------------------------------

def _extract_pool2b_noc_permits(
    session: Session,
    county_ids: list[str],
) -> list[CallingPoolRecord]:
    """List 7: property owners with active structural/new-construction permits.

    Targets the PROPERTY OWNER side of a construction project — investors or
    owner-builders who have filed a structural permit in the last 12 months.
    Distinct from List 3 (DBPR contractors): these are the project owners who
    may need construction or bridge financing, not the licensed builders per se.

    Phone sourced from the owners table (skip-traced). Records without a phone
    flow to the shared skip-trace queue (WP-W0-3) same as every other pool.

    Only structural permit types are included; enforcement/violation permits
    (code_violation, stop-work) are explicitly excluded to avoid conflating
    a distressed property with an active development project.
    """
    rows = session.execute(
        text("""
            SELECT DISTINCT ON (p.id)
                p.id                        AS source_property_id,
                p.parcel_id,
                p.address                   AS prop_address,
                p.city                      AS prop_city,
                p.state                     AS prop_state,
                p.zip                       AS prop_zip,
                p.county_id,
                c.display_name              AS county_name,
                o.owner_name                AS borrower_name,
                COALESCE(o.phone_1, o.phone_2, o.phone_3) AS raw_phone,
                o.email_1                   AS email,
                bp.permit_number,
                bp.permit_type,
                bp.issue_date,
                bp.job_value
            FROM building_permits bp
            JOIN properties p ON p.id = bp.property_id
            JOIN counties c   ON c.county_id = p.county_id
            LEFT JOIN owners o ON o.property_id = p.id
            WHERE p.county_id = ANY(:county_ids)
              AND bp.issue_date > NOW() - INTERVAL '12 months'
              AND (
                    bp.permit_type ILIKE '%new construction%'
                 OR bp.permit_type ILIKE '%ground up%'
                 OR bp.permit_type ILIKE '%foundation%'
                 OR bp.permit_type ILIKE '%structural%'
                 OR bp.permit_type ILIKE '%addition%'
                 OR bp.permit_type ILIKE '%residential new%'
              )
              AND bp.permit_type NOT ILIKE '%code violation%'
              AND bp.permit_type NOT ILIKE '%enforcement%'
              AND bp.permit_type NOT ILIKE '%stop work%'
            ORDER BY p.id, bp.issue_date DESC NULLS LAST
        """),
        {"county_ids": county_ids},
    ).fetchall()

    records: list[CallingPoolRecord] = []
    for row in rows:
        norm = normalize_phone(row.raw_phone)
        # O28 New Construction: use actual job_value if present; fall back to avg.
        if row.job_value:
            raw_elv = Decimal(str(row.job_value)) * Decimal(str(CONSTRUCTION_LTC))
            elv = max(Decimal(str(CONSTRUCTION_MIN_LOAN)), raw_elv)
        else:
            elv = Decimal(str(CONSTRUCTION_AVG_LOAN))
        detail = _compose_permit_details(row.permit_type, row.issue_date, row.job_value)

        records.append(CallingPoolRecord(
            run_id="",
            pool_name="active_builder",
            county_id=str(row.county_id),
            county_name=row.county_name,
            borrower_name=row.borrower_name,
            entity_name=None,                     # property-owner sourced — entity resolution deferred
            target_property_address=_compose_address(
                row.prop_address, row.prop_city, row.prop_state, row.prop_zip
            ),
            estimated_loan_value=elv,
            recent_permit_details=detail,
            entity_status=None,                   # no entity_type for permit-owner records
            parcel_id=row.parcel_id,
            zip=row.prop_zip,
            state=row.prop_state or WAVE0_STATE,
            normalized_phone=norm,
            phone_available=norm is not None,
            line_type="unknown",                  # owners table phone_1/2/3 has no line type
            email=row.email,
            financing_intent_score=None,          # attached by _attach_intent_scores
            intent_tier=None,
            recommended_product=None,
            aircall_campaign_tag=AIRCALL_TAG["active_builder"],
            campaign_list=CAMPAIGN_LIST["active_builder_noc"],  # List 7
            buyer_entity_id=None,
            permit_number=row.permit_number,
            dbpr_license_number=None,
            source_property_id=row.source_property_id,
            source_table="building_permits",
        ))

    logger.info(
        "Pool 2b NOC/permits (List 7): %d structural-permit owners (%d with phone)",
        len(records), sum(1 for r in records if r.phone_available),
    )
    return records


# ---------------------------------------------------------------------------
# Pool 3 — Mortgage Brokers (STUB)
# ---------------------------------------------------------------------------

def _extract_pool3_mortgage_broker(session: Session, county_ids: list[str]) -> list[CallingPoolRecord]:
    """Extract mortgage brokers per spec §4.1.

    The spec names three sources for this pool:
        (1) corporate records
        (2) commercial deed signings
        (3) professional licensing registries

    FAIL-CLOSED STATUS — verified against the branch, dev, and the live
    production DB (2026-09-29):

      (3) professional licensing registries is the ONLY authoritative source
          for who is a licensed mortgage broker.  In Florida that is the OFR
          (Office of Financial Regulation) / NMLS registry.  FA does NOT
          ingest it — there is no scraper on any branch and no table in the
          live DB.  dbpr_contacts holds CILB *contractor* licences only
          (verified: 23 license types, all construction trades, zero broker
          types).

      (1) corporate records exist in FA only as sunbiz_snapshots — a raw
          scrape audit scoped to property-owner LLC piercing, NOT a Florida
          business directory.  It cannot identify brokers who do not own
          distressed property.

      (2) commercial deed signings (deeds) do not carry a broker-role tag; a
          name on a deed is a grantor/grantee, not a licensed broker.

    Because the one identity-bearing source (registry) is not ingested and the
    other two cannot classify a broker on their own, this pool returns zero
    records rather than emit a name-keyword guess that would produce false
    brokers.  This is a documented, fail-closed placeholder — NOT a completed
    feature (production-execution: never claim an unimplemented dependency is
    done).

    RESOLVED (O4/O13): the registry source is the Florida OFR "Ch 494 Businesses
    - NMLS (MBR-MBRB)" bulk download, loaded into ofr_mortgage_brokers by
    src/tasks/ofr_broker_load.py.  We use the BUSINESS (MBR/MBRB) file — the
    broker firms, which in FL includes solo brokers licensed as their own LLC.
    The individual Loan Originator (LO) file is intentionally NOT used: those
    are employees (not brokers), carry no employer link, and have no phone.

    This pool stays fail-closed until the table exists AND holds rows, so an
    empty/absent load never fabricates brokers.

    Aircall tag: DESK_RESCUE.
    """
    if not _ofr_registry_available(session):
        logger.warning(
            "Pool 3 mortgage_broker: ofr_mortgage_brokers table not present — "
            "returning 0 records (fail-closed). Run migrations/apply_ofr_mortgage_brokers.py "
            "+ src.tasks.ofr_broker_load."
        )
        return []

    # Wave 0 target counties, upper-cased to match OFR's COUNTY text column.
    target_counties = [c.upper() for c in WAVE0_COUNTY_NAMES]

    rows = session.execute(
        text("""
            SELECT
                license_number, nmls_id, firm_name,
                prim_address_1, prim_address_2, prim_city, county, prim_state, prim_zip,
                normalized_phone
            FROM ofr_mortgage_brokers
            WHERE status = 'Approved'
              AND UPPER(COALESCE(county, '')) = ANY(:counties)
        """),
        {"counties": target_counties},
    ).fetchall()

    records: list[CallingPoolRecord] = []
    for row in rows:
        addr_line = " ".join(p for p in [row.prim_address_1, row.prim_address_2] if p)
        records.append(CallingPoolRecord(
            run_id="",
            pool_name="mortgage_broker",
            county_id=(row.county or "").lower(),
            county_name=(row.county or "").title(),
            borrower_name=None,                       # firm-level; no individual (LO file excluded)
            entity_name=row.firm_name,
            target_property_address=_compose_address(
                addr_line, row.prim_city, row.prim_state, row.prim_zip
            ),
            estimated_loan_value=None,                # O28 — no basis for brokers
            recent_permit_details=None,               # O29 — n/a
            entity_status=_entity_status_from_firm_name(row.firm_name),
            parcel_id=None,                           # brokers have no property anchor
            zip=row.prim_zip,
            state=row.prim_state or WAVE0_STATE,
            normalized_phone=row.normalized_phone,
            phone_available=row.normalized_phone is not None,
            line_type="unknown",                      # OFR does not distinguish mobile/landline
            email=None,                               # not in OFR — skip-trace optional
            financing_intent_score=None,
            intent_tier=None,
            recommended_product=None,
            aircall_campaign_tag=AIRCALL_TAG["mortgage_broker"],
            campaign_list=CAMPAIGN_LIST["mortgage_broker"],  # List 4
            buyer_entity_id=None,
            permit_number=None,
            dbpr_license_number=row.license_number,    # OFR license # (repurposed provenance field)
            source_property_id=None,
            source_table="ofr_mortgage_brokers",
        ))

    logger.info(
        "Pool 3 mortgage_broker: %d Approved brokers in %s (%d with phone)",
        len(records), target_counties,
        sum(1 for r in records if r.phone_available),
    )
    return records


def _entity_status_from_firm_name(firm_name: Optional[str]) -> Optional[str]:
    """Infer entity_status from a firm-name suffix; None when not determinable.

    Wave 0 is FL-only so entity_status does not gate any send (Dev 2's Georgia
    rule ignores FL rows), but the field is populated best-effort for consistency.
    """
    if not firm_name:
        return None
    name = firm_name.upper()
    if "LLC" in name or "L.L.C" in name:
        return "LLC"
    if "CORP" in name or "INC" in name or "INCORPORATED" in name:
        return "CORPORATION"
    return None


def _ofr_registry_available(session: Session) -> bool:
    """True if ofr_mortgage_brokers exists AND holds at least one row.

    Fail-closed on both a missing table and an empty table, so Pool 3 never
    activates before the OFR CSV has actually been loaded.
    """
    exists = session.execute(
        text("""
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_name = 'ofr_mortgage_brokers'
            LIMIT 1
        """)
    ).first()
    if not exists:
        return False
    return session.execute(text("SELECT 1 FROM ofr_mortgage_brokers LIMIT 1")).first() is not None


# ---------------------------------------------------------------------------
# Dedup across pools
# ---------------------------------------------------------------------------

def _dedup_across_pools(
    pool1: list[CallingPoolRecord],
    pool2: list[CallingPoolRecord],
    pool3: list[CallingPoolRecord],
) -> list[CallingPoolRecord]:
    """Remove duplicate phones across pools using Pool 2 > Pool 1 > Pool 3 precedence.

    A unique phone may appear in at most one pool in the staging table.  When the
    same normalised phone appears across pools, the higher-priority pool wins.
    Rows with no phone (phone_available=False) are never deduplicated against each
    other — each is kept (they'll be skip-traced individually in WP-W0-3).
    """
    seen_phones: set[str] = set()
    result: list[CallingPoolRecord] = []

    for record in [*pool2, *pool1, *pool3]:
        if record.phone_available and record.normalized_phone:
            if record.normalized_phone in seen_phones:
                logger.debug(
                    "Dedup: dropping borrower=%s pool=%s (already claimed by higher-priority pool)",
                    record.borrower_name, record.pool_name,
                )
                continue
            seen_phones.add(record.normalized_phone)
        result.append(record)

    logger.info(
        "After dedup: %d records (%d unique phones, %d no-phone rows)",
        len(result),
        len(seen_phones),
        sum(1 for r in result if not r.phone_available),
    )
    return result


# ---------------------------------------------------------------------------
# Intent score attachment (O14)
# ---------------------------------------------------------------------------

def _attach_intent_scores(session: Session, records: list[CallingPoolRecord]) -> None:
    """Join financing_intent_scores for property-anchored records.

    Only records with a source_property_id receive a score.  Non-property-
    anchored records (Pool 1 entities with no recent property, Pool 3) are
    left with intent_tier='unscored' and pass through the intent filter.

    Intent filter (O14 deferred): records with intent_tier below INTENT_MIN_TIER
    are logged but NOT removed here — filtering happens in the CLI layer so the
    full staging table is preserved for analysis.  The CLI task applies the
    threshold when building the Aircall export.
    """
    property_ids = [
        r.source_property_id for r in records if r.source_property_id is not None
    ]
    if not property_ids:
        return

    rows = session.execute(
        text("""
            SELECT DISTINCT ON (fis.property_id)
                fis.property_id,
                fis.financing_intent_score,
                fis.intent_tier,
                fis.recommended_product
            FROM financing_intent_scores fis
            WHERE fis.property_id = ANY(CAST(:ids AS bigint[]))
            ORDER BY fis.property_id, fis.score_date DESC
        """),
        {"ids": property_ids},
    ).fetchall()

    score_by_property: dict[int, Any] = {row.property_id: row for row in rows}

    for record in records:
        if record.source_property_id is None:
            record.intent_tier = "unscored"
            continue
        row = score_by_property.get(record.source_property_id)
        if row:
            record.financing_intent_score = row.financing_intent_score
            record.intent_tier = row.intent_tier
            record.recommended_product = row.recommended_product
        else:
            record.intent_tier = "unscored"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _compose_address(
    address: Optional[str],
    city: Optional[str],
    state: Optional[str],
    zip_code: Optional[str],
) -> Optional[str]:
    """Build the §4.3 'Target Property Address' display string, or None."""
    line = (address or "").strip()
    tail = " ".join(p for p in [(city or "").strip(), (state or "").strip(), (zip_code or "").strip()] if p)
    parts = [p for p in [line, tail] if p]
    return ", ".join(parts) if parts else None


def _compose_permit_details(
    permit_type: Optional[str],
    issue_date: Any,
    job_value: Any,
) -> Optional[str]:
    """Build the §4.3 'Recent Permit Details' display string, or None."""
    bits: list[str] = []
    if permit_type:
        bits.append(str(permit_type).strip())
    if issue_date:
        bits.append(f"issued {issue_date}")
    if job_value:
        bits.append(f"${Decimal(str(job_value)):,.0f}")
    return " · ".join(bits) if bits else None


def _finalize_run_metadata(records: list[CallingPoolRecord], run_id: str) -> None:
    for r in records:
        r.run_id = run_id


def _resolve_county_ids(session: Session, county_names: tuple[str, ...]) -> list[str]:
    """Return county_id slugs (e.g. 'hillsborough') for the given county names.

    Resilient to display-name variants: matches an exact display_name, a
    'Name%' prefix (prod stores 'Hillsborough County'), or the lowercased slug
    directly (county_id = 'hillsborough'). Falls back to the slug so a missing/
    differently-named counties row never silently drops a whole county.
    """
    names = list(county_names)
    prefixes = [f"{n}%" for n in names]
    slugs = [n.strip().lower() for n in names]
    rows = session.execute(
        text("""
            SELECT DISTINCT county_id FROM counties
            WHERE display_name ILIKE ANY(CAST(:names AS text[]))
               OR display_name ILIKE ANY(CAST(:prefixes AS text[]))
               OR county_id     = ANY(CAST(:slugs AS text[]))
        """),
        {"names": names, "prefixes": prefixes, "slugs": slugs},
    ).fetchall()
    resolved = [row.county_id for row in rows]
    # Fallback: if the counties table has no matching row at all, use the
    # lowercased slugs directly (properties.county_id uses these).
    return resolved or slugs


def _write_to_staging(session: Session, records: list[CallingPoolRecord]) -> int:
    """Bulk-insert records into lending_calling_pool_staging.

    NOTE: Table location is a Wave 0 placeholder in the FA database.
    Final location (O1) will be set by Dev 2 when the isolated lending schema
    is provisioned.  When that happens, swap the table name here and in the
    migration script only — no logic changes required.
    """
    if not records:
        return 0

    # Column order shared by the INSERT and the per-row tuple builder.
    cols = [
        "run_id", "pool_name", "county_id", "county_name",
        "borrower_name", "entity_name", "target_property_address",
        "estimated_loan_value", "recent_permit_details",
        "entity_status", "parcel_id", "zip", "state",
        "normalized_phone", "phone_available", "line_type", "email",
        "financing_intent_score", "intent_tier", "recommended_product",
        "aircall_campaign_tag", "campaign_list",
        "buyer_entity_id", "permit_number", "dbpr_license_number",
        "source_property_id", "source_table", "created_at",
    ]
    insert_sql = f"INSERT INTO lending_calling_pool_staging ({', '.join(cols)}) VALUES %s"

    def _tuple(r: CallingPoolRecord) -> tuple:
        return (
            r.run_id, r.pool_name, r.county_id, r.county_name,
            r.borrower_name, r.entity_name, r.target_property_address,
            r.estimated_loan_value, r.recent_permit_details,
            r.entity_status, r.parcel_id, r.zip, r.state,
            r.normalized_phone, r.phone_available, r.line_type, r.email,
            r.financing_intent_score, r.intent_tier, r.recommended_product,
            r.aircall_campaign_tag, r.campaign_list,
            r.buyer_entity_id, r.permit_number, r.dbpr_license_number,
            r.source_property_id, r.source_table, r.created_at,
        )

    # Bulk insert via psycopg2 execute_values — ONE network round-trip per batch.
    # Plain executemany sends one INSERT per row, which crawls over a remote
    # connection (a ~19k-row run took minutes). Dedicated raw connection so
    # raw-cursor commits don't collide with the passed session.
    import psycopg2.extras

    batch_size = 1000
    written = 0
    raw = session.get_bind().raw_connection()
    try:
        for i in range(0, len(records), batch_size):
            values = [_tuple(r) for r in records[i:i + batch_size]]
            with raw.cursor() as cur:
                psycopg2.extras.execute_values(cur, insert_sql, values, page_size=batch_size)
            raw.commit()
            written += len(values)
    finally:
        raw.close()

    return written
