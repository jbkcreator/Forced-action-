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
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy.orm import Session

from src.core.database import get_db_context
from src.core.models import UnmatchedRecord
from src.loaders.liens import LienLoader
from src.loaders.deeds import DeedLoader
from src.loaders.legal_proceedings import ProbateLoader, EvictionLoader, BankruptcyLoader

logger = logging.getLogger(__name__)


def rematch_unmatched(
    source_type: Optional[str] = None,
    limit: int = 5000,
    county_id: str = "hillsborough",
) -> dict:
    """
    Retry property matching for unmatched staged records.

    Args:
        source_type: Filter by source (liens, deeds, evictions, etc.). None = all.
        limit: Max records to process per run.
        county_id: County to process.

    Returns:
        dict with counts: total, matched, still_unmatched
    """
    stats = {"total": 0, "matched": 0, "still_unmatched": 0, "errors": 0}

    with get_db_context() as db:
        query = db.query(UnmatchedRecord).filter(
            UnmatchedRecord.match_status == "unmatched",
            UnmatchedRecord.county_id == county_id,
        )
        if source_type:
            query = query.filter(UnmatchedRecord.source_type == source_type)

        records = query.order_by(UnmatchedRecord.date_added).limit(limit).all()
        stats["total"] = len(records)
        logger.info("Re-matching %d unmatched records (source=%s)", len(records), source_type or "all")

        # Instantiate loaders for matching logic
        lien_loader = LienLoader(db)
        deed_loader = DeedLoader(db)
        lp_loader = EvictionLoader(db)  # all LP loaders share the same base matching logic

        BATCH_SIZE = 500
        LOG_INTERVAL = 25
        for i, record in enumerate(records):
            if i % LOG_INTERVAL == 0:
                logger.info(
                    "Progress: %d/%d processed — matched=%d still_unmatched=%d errors=%d",
                    i, len(records), stats["matched"], stats["still_unmatched"], stats["errors"],
                )
            if i > 0 and i % BATCH_SIZE == 0:
                db.commit()
                logger.info("Committed batch at record %d", i)

            try:
                record.match_attempted_at = datetime.now(timezone.utc)
                raw = record.raw_data or {}

                property_record = None

                # Try legal description first, then owner name.
                # For code liens and tax liens the owner may be in Grantee rather than
                # Grantor — apply the same filer-keyword detection used in LienLoader
                # so re-matched records follow the same fixed logic as new records.
                legal = raw.get("Legal") or raw.get("legal_description")
                grantor_val = record.grantor or raw.get("Grantor") or raw.get("grantor")
                grantee_val = raw.get("Grantee") or raw.get("grantee")

                loader = lien_loader  # default — has both matching strategies
                if record.source_type == "deeds":
                    loader = deed_loader
                elif record.source_type in ("evictions", "probate", "bankruptcies"):
                    loader = lp_loader

                # Determine which name field to use based on lien type
                doc_type_str = str(raw.get('document_type', '')).upper()
                is_tax_lien  = 'TAX LIEN' in doc_type_str
                is_code_lien = 'TCL' in doc_type_str or 'CCL' in doc_type_str
                _FILER_KEYWORDS = {'CITY OF TAMPA', 'HILLSBOROUGH COUNTY'}

                if is_tax_lien:
                    name_to_try = grantee_val
                elif is_code_lien:
                    grantor_is_filer = grantor_val and any(
                        kw in str(grantor_val).upper() for kw in _FILER_KEYWORDS
                    )
                    name_to_try = grantee_val if grantor_is_filer else grantor_val
                else:
                    name_to_try = grantor_val

                if legal:
                    result = loader.find_property_by_legal_description(str(legal))
                    if result:
                        property_record, _ = result

                if not property_record and name_to_try:
                    result = loader.find_property_by_owner_name(str(name_to_try))
                    if result:
                        property_record, _ = result

                # For lis_pendens, deeds, violations and permits: also try address matching
                # at 80% threshold (vs the default 85%) to recover records that narrowly
                # missed on address.  Violations/permits have no legal-description or
                # owner-name fallback so address is their only matching path.
                if not property_record and record.source_type in ("lis_pendens", "deeds", "violations", "permits"):
                    address_candidate = (
                        raw.get("Address") or raw.get("address") or record.address_string
                    )
                    if address_candidate and len(str(address_candidate).strip()) > 5:
                        result = loader.find_property_by_address(
                            str(address_candidate), threshold=80
                        )
                        if result:
                            property_record, _ = result

                if property_record:
                    record.match_status = "matched"
                    record.matched_property_id = property_record.id
                    stats["matched"] += 1
                    logger.debug("Re-matched %s record instrument=%s -> property_id=%s",
                                 record.source_type, record.instrument_number, property_record.id)
                else:
                    stats["still_unmatched"] += 1

            except Exception as e:
                logger.warning("Error re-matching record id=%s: %s", record.id, e)
                stats["errors"] += 1

        db.commit()

    logger.info(
        "Re-match complete: total=%d matched=%d still_unmatched=%d errors=%d",
        stats["total"], stats["matched"], stats["still_unmatched"], stats["errors"]
    )
    return stats


if __name__ == "__main__":
    import argparse
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(description="Re-match unmatched staged records")
    parser.add_argument("--source", default=None, help="Filter by source type (liens, deeds, etc.)")
    parser.add_argument("--limit", type=int, default=5000, help="Max records to process")
    parser.add_argument("--county-id", default="hillsborough", help="County ID")
    args = parser.parse_args()

    result = rematch_unmatched(source_type=args.source, limit=args.limit, county_id=args.county_id)
    print(result)
