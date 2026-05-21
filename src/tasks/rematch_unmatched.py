"""
Re-match Unmatched Records Task
================================
Retries property matching for records in the unmatched_records staging table.
Run manually or wired to the quarterly master parcel refresh.

Usage:
    python -m src.tasks.rematch_unmatched
    python -m src.tasks.rematch_unmatched --source liens --limit 1000
"""
import logging
import re
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy.orm import Session

from config.matching import for_county
from src.core.database import get_db_context
from src.core.models import UnmatchedRecord
from src.loaders.liens import LienLoader
from src.loaders.deeds import DeedLoader
from src.loaders.legal_proceedings import ProbateLoader, EvictionLoader, BankruptcyLoader

# Mirrors _LEGAL_DESC_RE in legal_proceedings.py — detects legal descriptions
# stored in PartyAddress fields (LOT/BLOCK/UNIT prefix, no house number).
_LEGAL_DESC_RE = re.compile(
    r'^\s*(LOT\s|BLOCK\s|UNIT\s|BLDG\s|BUILDING\s|TRACT\s|PARCEL\s|'
    r'[A-Z]-\d|UNIT\s*NO\.?\s*\d)',
    re.IGNORECASE,
)

logger = logging.getLogger(__name__)


def rematch_unmatched(
    source_type: Optional[str] = None,
    limit: int = 5000,
    county_id: str = "hillsborough",
) -> dict:
    """
    Retry property matching for unmatched and pending_review staged records.

    Args:
        source_type: Filter by source (liens, deeds, evictions, etc.). None = all.
        limit: Max records to process per run.
        county_id: County to process.

    Returns:
        dict with counts: total, matched, pending_review, still_unmatched, errors
    """
    stats = {"total": 0, "matched": 0, "pending_review": 0, "still_unmatched": 0, "errors": 0}

    with get_db_context() as db:
        query = db.query(UnmatchedRecord).filter(
            UnmatchedRecord.match_status.in_(["unmatched", "pending_review"]),
            UnmatchedRecord.county_id == county_id,
        )
        if source_type:
            query = query.filter(UnmatchedRecord.source_type == source_type)

        records = query.order_by(UnmatchedRecord.date_added).limit(limit).all()
        stats["total"] = len(records)
        logger.info("Re-matching %d records (source=%s)", len(records), source_type or "all")

        # Instantiate loaders for matching logic — county_id must be passed so
        # all find_property_by_* calls filter against the correct county's properties.
        lien_loader = LienLoader(db, county_id=county_id)
        deed_loader = DeedLoader(db, county_id=county_id)
        lp_loader = EvictionLoader(db, county_id=county_id)

        BATCH_SIZE = 500
        LOG_INTERVAL = 25
        for i, record in enumerate(records):
            if i % LOG_INTERVAL == 0:
                logger.info(
                    "Progress: %d/%d — matched=%d pending_review=%d still_unmatched=%d errors=%d",
                    i, len(records), stats["matched"], stats["pending_review"],
                    stats["still_unmatched"], stats["errors"],
                )
            if i > 0 and i % BATCH_SIZE == 0:
                db.commit()
                logger.info("Committed batch at record %d", i)

            try:
                record.match_attempted_at = datetime.now(timezone.utc)
                raw = record.raw_data or {}

                # County-aware thresholds — Pinellas has wider pending_review band
                thr = for_county(record.county_id)

                property_record = None
                match_score = 0
                match_method = None

                loader = lien_loader  # default — has both matching strategies
                if record.source_type == "deeds":
                    loader = deed_loader
                elif record.source_type in ("evictions", "probate", "bankruptcies", "divorce_filings"):
                    loader = lp_loader

                # ── Probate / eviction / divorce: use source-specific fields ──────
                if record.source_type in ("probate", "evictions", "divorce_filings"):
                    party_address = raw.get("PartyAddress") or record.address_string
                    if party_address:
                        addr_str = str(party_address).strip()
                        if _LEGAL_DESC_RE.match(addr_str):
                            result = loader.find_property_by_legal_description(addr_str)
                            if result:
                                property_record, match_score = result
                                match_method = 'legal_desc'
                        else:
                            result = loader.find_property_by_address(
                                addr_str, threshold=thr.address_floor
                            )
                            if result:
                                property_record, match_score = result
                                match_method = 'address'

                    if not property_record:
                        last = str(raw.get("LastName/CompanyName") or "").strip()
                        first = str(raw.get("FirstName") or "").strip()
                        middle = str(raw.get("MiddleName") or "").strip()
                        full_name = " ".join(p for p in [first, middle, last] if p)
                        full_name = " ".join(full_name.split())
                        if full_name:
                            result = loader.find_property_by_owner_name(
                                full_name, threshold=thr.owner_name_floor
                            )
                            if result:
                                property_record, match_score = result
                                match_method = 'owner_name'
                        # first+last only if middle added noise
                        if not property_record and first and last and middle:
                            result = loader.find_property_by_owner_name(
                                f"{first} {last}", threshold=thr.owner_name_floor
                            )
                            if result:
                                property_record, match_score = result
                                match_method = 'owner_name'

                # ── Bankruptcies: name only ───────────────────────────────────────
                elif record.source_type == "bankruptcies":
                    lead_name = record.grantor or raw.get("Lead Name") or raw.get("lead_name")
                    if lead_name:
                        result = loader.find_property_by_owner_name(
                            str(lead_name), threshold=thr.owner_name_floor
                        )
                        if result:
                            property_record, match_score = result
                            match_method = 'owner_name'

                # ── Liens / deeds / all others ────────────────────────────────────
                else:
                    legal = raw.get("Legal") or raw.get("legal_description")
                    grantor_val = record.grantor or raw.get("Grantor") or raw.get("grantor")
                    grantee_val = raw.get("Grantee") or raw.get("grantee")

                    # Determine which name field to use — mirrors LienLoader field logic.
                    doc_type_str      = str(raw.get('document_type', '')).upper()
                    doc_type_raw      = str(raw.get('DocType', '')).upper()
                    is_tax_lien       = 'TAX LIEN' in doc_type_str
                    is_code_lien      = (
                        'TCL' in doc_type_str or 'CCL' in doc_type_str
                        or 'CODE LIEN' in doc_type_raw
                    )
                    is_mechanics_lien = 'ML' in doc_type_str or 'MECHANIC' in doc_type_str
                    is_judgment       = 'JUDGMENT' in doc_type_raw or 'CERTIFIED' in doc_type_raw

                    _filer_keywords = lien_loader._city_filer_keywords

                    if is_tax_lien:
                        name_to_try = grantee_val
                    elif is_code_lien:
                        grantor_is_filer = grantor_val and any(
                            kw in str(grantor_val).upper() for kw in _filer_keywords
                        )
                        name_to_try = grantee_val if grantor_is_filer else grantor_val
                    elif is_mechanics_lien or is_judgment:
                        # ML: Grantor = contractor, Grantee = property owner
                        # JUDGMENT: Grantor = creditor, Grantee = debtor/property owner
                        name_to_try = grantee_val
                    else:
                        name_to_try = grantor_val

                    if legal:
                        result = loader.find_property_by_legal_description(str(legal))
                        if result:
                            property_record, match_score = result
                            match_method = 'legal_desc'

                    if not property_record and name_to_try:
                        result = loader.find_property_by_owner_name(
                            str(name_to_try), threshold=thr.owner_name_floor
                        )
                        if result:
                            property_record, match_score = result
                            match_method = 'owner_name'

                    # Address fallback for source types where address is the primary path.
                    if not property_record and record.source_type in (
                        "lis_pendens", "deeds", "violations", "permits"
                    ):
                        address_candidate = (
                            raw.get("Address") or raw.get("address") or record.address_string
                        )
                        if address_candidate and len(str(address_candidate).strip()) > 5:
                            result = loader.find_property_by_address(
                                str(address_candidate), threshold=thr.address_floor
                            )
                            if result:
                                property_record, match_score = result
                                match_method = 'address'

                if property_record:
                    tier = lien_loader._classify_match(match_score, match_method)
                    if tier == "matched":
                        record.match_status = "matched"
                        record.matched_property_id = property_record.id
                        record.match_confidence = round(match_score / 100.0, 3)
                        record.match_method = match_method
                        record.candidate_property_id = None
                        stats["matched"] += 1
                        logger.debug(
                            "Re-matched %s record instrument=%s -> property_id=%s (score=%.3f, method=%s)",
                            record.source_type, record.instrument_number,
                            property_record.id, match_score / 100.0, match_method,
                        )
                    else:  # pending_review
                        record.match_status = "pending_review"
                        record.candidate_property_id = property_record.id
                        record.match_confidence = round(match_score / 100.0, 3)
                        record.match_method = match_method
                        stats["pending_review"] += 1
                        logger.debug(
                            "Pending review %s record instrument=%s candidate_property_id=%s (score=%.3f)",
                            record.source_type, record.instrument_number,
                            property_record.id, match_score / 100.0,
                        )
                else:
                    stats["still_unmatched"] += 1

            except Exception as e:
                logger.warning("Error re-matching record id=%s: %s", record.id, e)
                stats["errors"] += 1

        db.commit()

    logger.info(
        "Re-match complete: total=%d matched=%d pending_review=%d still_unmatched=%d errors=%d",
        stats["total"], stats["matched"], stats["pending_review"],
        stats["still_unmatched"], stats["errors"],
    )
    return stats


if __name__ == "__main__":
    import argparse
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(description="Re-match unmatched staged records")
    parser.add_argument("--source", default=None, help="Filter by source type (liens, deeds, etc.)")
    parser.add_argument("--limit", type=int, default=5000, help="Max records to process")
    parser.add_argument("--county", "--county-id", dest="county_id", default="hillsborough", help="County ID")
    args = parser.parse_args()

    result = rematch_unmatched(source_type=args.source, limit=args.limit, county_id=args.county_id)
    print(result)
