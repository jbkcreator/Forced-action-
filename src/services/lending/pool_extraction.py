"""Wave 0 Calling Pool Extraction — WP-W0-1 / WP-GL-2.

Reconciled 2026-10-02 from two branches that diverged after a common ancestor
and independently built overlapping work: this branch (originally
feat/calling-pool-extraction-intent-filter, PR #318) and
feat/lending-w0-dev2-compliance-floor (PR #320). #320's schema move
(lending.calling_pool_staging), List 6/7/9 extractors, and List-7 financing-
exclusion check were kept as the base; this branch's Pasco scope, line_type
field, corrected DBPR-based Pool 2, and per-campaign summary/export were
ported on top. See PR #318's comment thread for the full before/after.

Reads the existing FA database (Hillsborough, Pinellas, Pasco) and produces
five calling pools for the Lending Engine (Cora), each tagged with Josh's
List 1-9 taxonomy (source_tag, client_commnets_answers.md Section 2):

    Pool 1  wholesaler_flipper — buyer_entities with buyer_type IN ('wholesaler','flipper')
                                  List 2 "cash buyers" normally, List 9 "stalled flips"
                                  once the purchase is >=90 days old and unsold
    Pool 2  active_builder     — List 3: DBPR-licensed Cert Building/Residential contractors
    Pool 3  mortgage_broker    — List 4: OFR "Ch 494 MBR-MBRB" registry (brokers only, not
                                  individual LOs — see the source_tag_for() docstring)
    permit_owner               — List 7: property owners behind a new-construction permit,
                                  with no permanent financing recorded since
    auction_winner             — List 6: tax-deed auction winners (phoneless, trace-only)

Output lands in ``lending.calling_pool_staging``.

Open items this file is waiting on:
  O11  — Wholesaler definition (buyer_type vs raw deed-velocity). Using buyer_type.
  O12  — Builder permit_type definitions. Using STRUCTURAL_KEYWORDS from config.
  O14  — Intent filter applicability to non-property pools. Applied where property
          anchor exists; non-anchored records pass through at tier='unscored'.
  O15  — No-phone handling: RESOLVED by lead — Tracerfy only (both skip-trace AND
          DNC check), per client. BatchData is explicitly NOT used (no credits
          available). Rows without a normalised phone are staged with
          phone_available=False for WP-W0-3's Tracerfy-only enrichment pass.
  O16  — Multi-pool dedup precedence. Pool 2 > Pool 1 > Pool 3 (same entity in
          multiple pools keeps the higher-priority pool's row; List 6/7 are
          phoneless or dedup against already-claimed phones separately).
  O28  — Estimated Loan Value formula. Interim formulas below (CONSTRUCTION_LTC etc).
  O29  — Recent Permit Details for non-builder pools. Omitted; field is NULL.

Pasco/Builders (RESOLVED by lead + verified 2026-10-02 on prod): Pasco has ZERO
DBPR builder records today (Cert Building + Cert Residental both empty, query
run directly on the production server). Pool 2 (List 3) correctly returns 0
Pasco rows as a result — no fabrication, no code change needed. List 7
(permit_owner) does not depend on DBPR and has NOT yet been checked for Pasco
building_permits coverage — worth a follow-up query:
    SELECT COUNT(*) FROM building_permits WHERE county_id = 'pasco';

List 4 "brokers and LOs" gap: Pool 3 only loads the OFR broker-business file
(MBR/MBRB), not the individual Loan Originator file. Confirmed from real OFR
sample data (2026-10-02): the LO file is a nationwide NMLS registry (most
records are out-of-state), and phone coverage is ~0% even after narrowing to
our target counties. Flagged pending a client scope decision — not built
blind this close to launch.

IMPORTANT: estimated_loan_value is an INTERNAL CALLER REFERENCE only.  It is
derived from public-record job_value / sale_price.  It is never a quote, term,
rate commitment, or offer to the borrower.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
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
    "auction_winner": "DESK_CAPITAL_LOOP",
    "permit_owner": "DESK_CONSTRUCTION",
}

# Go Live Brief 2.5 source lists (Josh's List 1-9 taxonomy, client_commnets_answers.md
# Section 2 table). A flipper is List 9 only while its flip is stalled: the latest
# purchase is at least this old and the property has not been resold.
# 90 days: the median flipper hold is 8 days, and deeds only reach back to 2026-01-01.
STALLED_FLIP_MIN_DAYS: int = 90
# List 7 = owners behind a new-construction permit (brief: "new construction is the repeat
# business and the biggest checks").
LIST7_PERMIT_PATTERN: str = "%new construction%"


def is_stalled_flip(bought: Optional[date], *, resold: bool, today: Optional[date] = None) -> bool:
    if bought is None or resold:
        return False
    return ((today or date.today()) - bought).days >= STALLED_FLIP_MIN_DAYS


def source_tag_for(pool_name: str, source_table: str, *, stalled: bool = False) -> Optional[str]:
    """Josh's List 1-9 taxonomy for a staged record; None when it belongs to no launch list.

    wholesaler_flipper -> List 2 "cash buyers" while NOT stalled (the spec's own Pool 1
    data source, buyer_entities.total_cash_volume, matches Josh's "cash buyers" label —
    INFERRED, not confirmed by him by number; gates his "Lists 2 and 4 blocked from
    booking" rule, client_commnets_answers.md "Two booking gates" item — confirm with
    Josh if there's any doubt before relying on it operationally), List 9 "stalled
    flips" once stalled.

    mortgage_broker -> List 4 "brokers and LOs" — NOTE: Pool 3 currently loads the OFR
    business (MBR/MBRB) file only, NOT the individual Loan Originator file, so this
    list is under-covered relative to Josh's own naming. Flagged to the team pending a
    scope decision on whether to ingest the LO file before launch — NOT silently built
    (the file's phone-field availability needs checking first; real sample data from
    OFR's bulk "LO" download shows ~0% phone coverage even after narrowing to FL).
    """
    if pool_name == "active_builder":
        return "list_3"
    if pool_name == "mortgage_broker":
        return "list_4"
    if pool_name == "auction_winner":
        return "list_6"
    if pool_name == "permit_owner":
        return "list_7"
    if pool_name == "wholesaler_flipper":
        return "list_9" if stalled else "list_2"
    return None

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
    """One row destined for lending.calling_pool_staging.

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
    buyer_entity_id: Optional[int]        # set for Pool 1
    permit_number: Optional[str]          # set for Pool 2
    dbpr_license_number: Optional[str]    # reserved for Pool 3
    source_property_id: Optional[int]
    source_table: str
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    # Go Live: brief source list (list_1..list_9); set by _finalize_run_metadata.
    source_tag: Optional[str] = None
    stalled_flip: bool = False
    # F8 (Josh, Oct 4 §2): owner-occupied status of the target property, from
    # financials.homestead_exempt. None for pools with no single subject property
    # (mortgage_broker: the record is a professional, not a property owner).
    homestead_exempt: Optional[bool] = None


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def extract_calling_pools(
    session: Session,
    *,
    dry_run: bool = False,
    county_names: tuple[str, ...] = WAVE0_COUNTY_NAMES,
) -> dict[str, Any]:
    """Extract all three calling pools and write to lending.calling_pool_staging.

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
    claimed = {r.normalized_phone for r in all_records if r.normalized_phone}
    all_records.extend(drop_claimed_phones(_extract_list7_permit_owners(session, county_ids), claimed))
    all_records.extend(_extract_auction_winners(session, county_ids))  # phoneless: nothing to dedup
    _attach_intent_scores(session, all_records)
    _finalize_run_metadata(all_records, run_id)

    summary: dict[str, Any] = {
        "run_id": run_id,
        "dry_run": dry_run,
        "county_ids": county_ids,
        "pools": _summarize_by_pool(all_records),
        "source_tags": _summarize_by_source_tag(all_records),
        "total_records": len(all_records),
        "total_phone_available": sum(1 for r in all_records if r.phone_available),
    }

    if not dry_run:
        written = _write_to_staging(session, all_records)
        summary["rows_written"] = written
        logger.info("run_id=%s wrote %d rows to lending.calling_pool_staging", run_id, written)
    else:
        logger.info("run_id=%s dry_run=True skipping DB write (%d records)", run_id, len(all_records))

    return summary


def _summarize_by_pool(records: list[CallingPoolRecord]) -> dict[str, dict[str, int]]:
    """Total / phone_available per internal pool_name (wholesaler_flipper, active_builder,
    mortgage_broker, permit_owner, auction_winner).

    Coarser than _summarize_by_source_tag — wholesaler_flipper mixes List 2 (not stalled)
    and List 9 (stalled) here. Kept for backward-compat callers that key off pool_name.
    """
    out: dict[str, dict[str, int]] = {}
    for r in records:
        bucket = out.setdefault(r.pool_name, {"total": 0, "phone_available": 0})
        bucket["total"] += 1
        if r.phone_available:
            bucket["phone_available"] += 1
    return out


def _summarize_by_source_tag(records: list[CallingPoolRecord]) -> dict[str, dict[str, Any]]:
    """Total / phone_available per Josh's List 1-9 taxonomy (source_tag) — the shape of
    the table he asked for (client_commnets_answers.md Section 2): "each campaign...
    raw records, records with a phone". A record with source_tag=None (not yet mapped
    to any launch list) groups under 'unassigned' rather than silently vanishing.

    NOTE: this is "raw records" / "records with a phone" only — NOT "dialable after DNC
    and suppression", which needs WP-W0-2's compliance pass output, not this extraction step.
    """
    out: dict[str, dict[str, Any]] = {}
    for r in records:
        key = r.source_tag or "unassigned"
        bucket = out.setdefault(key, {"total": 0, "phone_available": 0, "pool_names": set()})
        bucket["total"] += 1
        bucket["pool_names"].add(r.pool_name)
        if r.phone_available:
            bucket["phone_available"] += 1
    # Sets aren't JSON-serializable — convert to a sorted list for the summary dict.
    for bucket in out.values():
        bucket["pool_names"] = sorted(bucket["pool_names"])
    return out


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
                f.homestead_exempt              AS homestead_exempt,
                d.sale_price                    AS last_sale_price,
                d.record_date                   AS last_purchase_date,
                EXISTS (SELECT 1 FROM deeds later
                        WHERE later.property_id = d.property_id
                          AND later.record_date > d.record_date) AS resold
            FROM buyer_entities be
            JOIN buyer_entity_links bel ON bel.buyer_entity_id = be.id
                                       AND bel.source_table IN ('deeds', 'deed_wholesaler')
            JOIN deeds d    ON d.id = bel.source_id
            JOIN properties p ON p.id = d.property_id
            LEFT JOIN financials f ON f.property_id = p.id
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
            buyer_entity_id=row.buyer_entity_id,
            permit_number=None,
            dbpr_license_number=None,
            source_property_id=row.source_property_id,
            source_table="buyer_entities",
            stalled_flip=is_stalled_flip(_as_date(row.last_purchase_date), resold=bool(row.resold)),
            homestead_exempt=_buyer_homestead(row.homestead_exempt, bool(row.resold)),
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

    This is List 3 only. List 7 (NOC/permits — property owners with active
    construction permits, a related but distinct population) is a separate
    extractor, _extract_list7_permit_owners(), called directly from
    extract_calling_pools() rather than nested here.
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
            buyer_entity_id=None,
            permit_number=None,
            dbpr_license_number=row.license_number,
            source_property_id=None,
            source_table="dbpr_contacts",
        ))

    logger.info(
        "Pool 2 active_builder (List 3): %d DBPR contractors, %d with phone",
        len(records),
        sum(1 for r in records if r.phone_available),
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

    List 4 is "brokers AND LOs" per Josh's own taxonomy — individual Loan
    Originators are now included too, via _extract_pool3b_loan_originators()
    (OFR's separate "LO" bulk file, ofr_loan_originators table). Confirmed from
    real sample data: the LO file is a NATIONWIDE NMLS registry (most records
    are out-of-state individuals holding a remote FL license), and phone
    coverage is ~0% even after narrowing to our target counties — virtually
    every LO record needs skip-trace. Same county-match filter as brokers
    narrows the out-of-state noise down to locally-based LOs, consistent with
    the referral-relationship use case (Caller Playbook's broker/LO hook).

    This pool stays fail-closed until each table exists AND holds rows, so an
    empty/absent load never fabricates brokers or LOs.

    Aircall tag: DESK_RESCUE.
    """
    # Target counties, upper-cased to match OFR's COUNTY text column. Derived
    # from the actually-requested county_ids (slugs, e.g. "hillsborough" ->
    # "HILLSBOROUGH"), not the global WAVE0_COUNTY_NAMES constant — a
    # single-county CLI run (--counties Hillsborough) must not silently stage
    # every other county's brokers/LOs too.
    target_counties = [c.upper() for c in county_ids]

    # Brokers and LOs are independent OFR datasets with independent load status —
    # one being missing/empty must never silently suppress the other.
    if not _ofr_registry_available(session):
        logger.warning(
            "Pool 3 mortgage_broker: ofr_mortgage_brokers table not present — "
            "0 broker records (fail-closed). Run migrations/apply_ofr_mortgage_brokers.py "
            "+ src.tasks.ofr_broker_load."
        )
        return _extract_pool3b_loan_originators(session, target_counties)

    rows = session.execute(
        text("""
            SELECT
                license_number, nmls_id, firm_name,
                prim_address_1, prim_address_2, prim_city, county, prim_state, prim_zip,
                normalized_phone
            FROM ofr_mortgage_brokers
            WHERE status = 'Approved'
              AND UPPER(COALESCE(county, '')) = ANY(:counties)
              AND UPPER(COALESCE(prim_state, '')) = 'FL'
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
            buyer_entity_id=None,
            permit_number=None,
            dbpr_license_number=row.license_number,    # OFR license # (repurposed provenance field)
            source_property_id=None,
            source_table="ofr_mortgage_brokers",
        ))

    lo_records = _extract_pool3b_loan_originators(session, target_counties)
    records.extend(lo_records)

    logger.info(
        "Pool 3 mortgage_broker: %d broker businesses + %d LOs in %s, %d with phone",
        len(records) - len(lo_records), len(lo_records), target_counties,
        sum(1 for r in records if r.phone_available),
    )
    return records


def _extract_pool3b_loan_originators(session: Session, target_counties: list[str]) -> list[CallingPoolRecord]:
    """List 4 (part 2): individual Loan Originators — Josh's 'brokers and LOs' naming.

    Same county-match filter as the broker query, PLUS a prim_state='FL' check:
    the OFR LO file is a nationwide NMLS registry where 'county' is the
    individual's own home county, frequently out-of-state (confirmed from real
    sample data — Michigan, Oregon addresses). County-name matching alone is
    not sufficient: "Hillsborough" is also a real county in New Hampshire, so
    an NH-based LO holding a remote FL license would otherwise match on county
    name and get staged with state='NH', silently entering the Florida nurture
    queue and inflating Hillsborough-FL counts in client reports (found in
    code review). The state filter is required, not optional.

    Fail-closed until ofr_loan_originators exists AND holds rows, same as
    the broker table — never fabricates LOs from an absent/empty load.
    """
    if not _ofr_lo_registry_available(session):
        logger.warning(
            "Pool 3b loan_originator: ofr_loan_originators table not present — "
            "returning 0 records (fail-closed). Run migrations/apply_ofr_loan_originators.py "
            "+ src.tasks.ofr_lo_load."
        )
        return []

    rows = session.execute(
        text("""
            SELECT
                license_number, nmls_id, last_name, first_name, middle_name,
                prim_address_1, prim_address_2, prim_city, county, prim_state, prim_zip,
                normalized_phone
            FROM ofr_loan_originators
            WHERE status = 'Approved'
              AND UPPER(COALESCE(county, '')) = ANY(:counties)
              AND UPPER(COALESCE(prim_state, '')) = 'FL'
        """),
        {"counties": target_counties},
    ).fetchall()

    records: list[CallingPoolRecord] = []
    for row in rows:
        addr_line = " ".join(p for p in [row.prim_address_1, row.prim_address_2] if p)
        full_name = " ".join(p for p in [row.first_name, row.middle_name, row.last_name] if p)
        records.append(CallingPoolRecord(
            run_id="",
            pool_name="mortgage_broker",
            county_id=(row.county or "").lower(),
            county_name=(row.county or "").title(),
            borrower_name=full_name or None,          # individual — unlike the firm-level broker rows
            entity_name=None,                         # no firm/employer link in the LO file
            target_property_address=_compose_address(
                addr_line, row.prim_city, row.prim_state, row.prim_zip
            ),
            estimated_loan_value=None,                # O28 — no basis for LOs
            recent_permit_details=None,               # O29 — n/a
            entity_status="NATURAL_PERSON",
            parcel_id=None,                           # LOs have no property anchor
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
            buyer_entity_id=None,
            permit_number=None,
            dbpr_license_number=row.license_number,    # OFR license # (repurposed provenance field)
            source_property_id=None,
            source_table="ofr_loan_originators",
        ))

    logger.info(
        "Pool 3b loan_originator: %d Approved LOs in %s (%d with phone)",
        len(records), target_counties,
        sum(1 for r in records if r.phone_available),
    )
    return records


def _ofr_lo_registry_available(session: Session) -> bool:
    """True if ofr_loan_originators exists AND holds at least one row.

    Fail-closed on both a missing table and an empty table, so the LO half of
    Pool 3 never activates before the OFR LO files have actually been loaded.
    """
    exists = session.execute(
        text("""
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_name = 'ofr_loan_originators'
            LIMIT 1
        """)
    ).first()
    if not exists:
        return False
    return session.execute(text("SELECT 1 FROM ofr_loan_originators LIMIT 1")).first() is not None


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
                    "Dedup: dropping a %s record (phone already claimed by a higher-priority pool)",
                    record.pool_name,
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
        if r.source_tag is None:
            r.source_tag = source_tag_for(r.pool_name, r.source_table, stalled=r.stalled_flip)


def _as_date(value: Any) -> Optional[date]:
    if value is None:
        return None
    return value.date() if isinstance(value, datetime) else value


# ---------------------------------------------------------------------------
# List 7 — Owners pulling construction permits (NOCs / permits, Go Live Brief 2.5)
# ---------------------------------------------------------------------------

def permit_owner_record(row: Any) -> CallingPoolRecord:
    """The property OWNER behind a recent structural permit: they are the one who needs
    construction financing. Phone is the owner's own traced phone (never the contractor's)."""
    norm = normalize_phone(row.owner_phone) if row.owner_phone else None
    elv = (Decimal(str(row.job_value)) * Decimal(str(CONSTRUCTION_LTC))).quantize(Decimal("0.01"))         if row.job_value else Decimal(str(CONSTRUCTION_AVG_LOAN))
    return CallingPoolRecord(
        run_id="", pool_name="permit_owner",
        county_id=str(row.county_id), county_name=row.county_name,
        borrower_name=None, entity_name=row.owner_name,
        target_property_address=_compose_address(row.prop_address, row.prop_city, row.prop_state, row.prop_zip),
        estimated_loan_value=elv,
        recent_permit_details=_compose_permit_details(row.permit_type, row.issue_date, row.job_value),
        entity_status=_entity_status_from_firm_name(row.owner_name),
        parcel_id=row.parcel_id, zip=row.prop_zip, state=row.prop_state or WAVE0_STATE,
        normalized_phone=norm, phone_available=norm is not None,
        line_type="unknown",  # owners table phone_1/2/3 has no line type
        email=(row.owner_email or "").strip().lower() or None,
        financing_intent_score=None, intent_tier=None, recommended_product=None,
        aircall_campaign_tag=AIRCALL_TAG["permit_owner"],
        buyer_entity_id=None, permit_number=row.permit_number, dbpr_license_number=None,
        source_property_id=row.source_property_id, source_table="building_permits",
        source_tag="list_7",
        homestead_exempt=row.homestead_exempt,
    )


def _buyer_homestead(homestead_exempt: Optional[bool], resold: bool) -> Optional[bool]:
    """A resold deed property's appraiser flag describes its new (often owner-occupant)
    owner, not the flipper, so it must not screen the flipper out."""
    return None if resold else homestead_exempt


def drop_claimed_phones(records: list[CallingPoolRecord], claimed: set[str]) -> list[CallingPoolRecord]:
    """Keep records whose phone no higher-priority pool (or earlier record) already holds.
    Phoneless records are always kept (they are traced individually)."""
    kept: list[CallingPoolRecord] = []
    for record in records:
        if record.normalized_phone:
            if record.normalized_phone in claimed:
                continue
            claimed.add(record.normalized_phone)
        kept.append(record)
    return kept


def _extract_list7_permit_owners(session: Session, county_ids: list[str]) -> list[CallingPoolRecord]:
    """One row per property with a NEW-CONSTRUCTION permit issued in the last 12 months in a
    target county and no permanent financing recorded since (Pool 2's financing rule).

    New construction only: the broader structural set is dominated by express and trade
    permits (roofs, windows, repairs), which are not construction-financing leads."""
    rows = session.execute(
        text("""
            SELECT DISTINCT ON (bp.property_id)
                bp.property_id AS source_property_id, bp.permit_number, bp.permit_type, bp.issue_date,
                bp.job_value, bp.county_id, c.display_name AS county_name,
                p.parcel_id, p.address AS prop_address, p.city AS prop_city, p.state AS prop_state,
                p.zip AS prop_zip, f.homestead_exempt, o.owner_name,
                COALESCE(o.phone_1, o.phone_2, o.phone_3) AS owner_phone,
                o.email_1 AS owner_email
            FROM building_permits bp
            JOIN counties c ON c.county_id = bp.county_id
            JOIN properties p ON p.id = bp.property_id
            LEFT JOIN financials f ON f.property_id = bp.property_id
            LEFT JOIN owners o ON o.property_id = bp.property_id
            WHERE bp.is_enforcement_permit = FALSE
              AND bp.county_id = ANY(:county_ids)
              AND bp.issue_date >= CURRENT_DATE - INTERVAL '12 months'
              AND o.owner_name IS NOT NULL
              AND (bp.permit_type ILIKE :new_construction OR bp.description ILIKE :new_construction)
              AND NOT EXISTS (SELECT 1 FROM deeds d WHERE d.property_id = bp.property_id
                              AND d.mortgage_amount > 0 AND d.record_date >= bp.issue_date)
            ORDER BY bp.property_id, bp.issue_date DESC
        """),
        {"county_ids": county_ids, "new_construction": LIST7_PERMIT_PATTERN},
    ).fetchall()
    records = [permit_owner_record(row) for row in rows]
    logger.info("List 7 permit_owner: %d properties", len(records))
    return records


# ---------------------------------------------------------------------------
# List 6 — Tax-deed auction winners (Go Live Brief 2.5)
# ---------------------------------------------------------------------------

def auction_winner_record(row: Any) -> CallingPoolRecord:
    """A winner has no phone in FA; the record is staged for tracing, never dialed as-is."""
    return CallingPoolRecord(
        run_id="", pool_name="auction_winner",
        county_id=str(row.county_id), county_name=row.county_name,
        borrower_name=None, entity_name=row.sold_to,
        target_property_address=_compose_address(row.prop_address, row.prop_city, row.prop_state, row.prop_zip),
        estimated_loan_value=Decimal(str(row.sold_amount)) if row.sold_amount else None,
        recent_permit_details=None,
        entity_status=_entity_status_from_firm_name(row.sold_to),
        parcel_id=row.parcel_id, zip=row.prop_zip, state=row.prop_state or WAVE0_STATE,
        normalized_phone=None, phone_available=False, line_type="unknown", email=None,
        financing_intent_score=None, intent_tier=None, recommended_product=None,
        aircall_campaign_tag=AIRCALL_TAG["auction_winner"],
        buyer_entity_id=None, permit_number=None, dbpr_license_number=None,
        source_property_id=row.property_id, source_table="tax_deed_auctions",
        source_tag="list_6",
        # The appraiser flag on this parcel describes the former owner the winner just
        # bought it from, not the winner, so it is never used to screen them.
        homestead_exempt=None,
    )


def _extract_auction_winners(session: Session, county_ids: list[str]) -> list[CallingPoolRecord]:
    rows = session.execute(
        text("""
            SELECT tda.id AS auction_id, tda.sold_to, tda.sold_amount, tda.property_id,
                   COALESCE(tda.parcel_id, p.parcel_id) AS parcel_id,
                   tda.county_id, c.display_name AS county_name,
                   p.address AS prop_address, p.city AS prop_city, p.state AS prop_state, p.zip AS prop_zip
            FROM tax_deed_auctions tda
            LEFT JOIN properties p ON p.id = tda.property_id
            LEFT JOIN counties c ON c.county_id = tda.county_id
            WHERE tda.sold_to IS NOT NULL AND btrim(tda.sold_to) <> ''
              AND tda.county_id = ANY(:county_ids)
        """),
        {"county_ids": county_ids},
    ).fetchall()
    records = [auction_winner_record(row) for row in rows]
    logger.info("List 6 auction_winner: %d rows", len(records))
    return records


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
    """Bulk-insert records into lending.calling_pool_staging.

    NOTE: Table location is a Wave 0 placeholder in the FA database.
    Final location (O1) will be set by Dev 2 when the isolated lending schema
    is provisioned.  When that happens, swap the table name here and in the
    migration script only — no logic changes required.
    """
    if not records:
        return 0

    from sqlalchemy import insert

    from src.core.models import LendingCallingPoolStaging

    cols = [
        "run_id", "pool_name", "county_id", "county_name",
        "borrower_name", "entity_name", "target_property_address",
        "estimated_loan_value", "recent_permit_details",
        "entity_status", "parcel_id", "zip", "state",
        "normalized_phone", "phone_available", "line_type", "email",
        "financing_intent_score", "intent_tier", "recommended_product",
        "aircall_campaign_tag",
        "buyer_entity_id", "permit_number", "dbpr_license_number",
        "source_property_id", "source_table", "created_at", "source_tag", "homestead_exempt",
    ]

    # One multi-row INSERT per batch: a single round trip, unlike per-row executemany
    # (a ~19k-row run took minutes over the remote connection). All batches share one
    # transaction, committed only once every batch has succeeded — a mid-run failure
    # (a dropped connection on batch 3 of 5, say) must leave nothing committed from
    # this run_id, not a partial run that latest_run_id() would otherwise pick as
    # "newest" over the previous complete one (finding #12).
    batch_size = 1000
    written = 0
    try:
        for i in range(0, len(records), batch_size):
            batch = [{c: getattr(r, c) for c in cols} for r in records[i:i + batch_size]]
            session.execute(insert(LendingCallingPoolStaging).values(batch))
            written += len(batch)
        session.commit()
    except Exception:
        session.rollback()
        raise
    return written
