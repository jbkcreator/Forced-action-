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

        for record in records:
            try:
                record.match_attempted_at = datetime.now(timezone.utc)
                raw = record.raw_data or {}

                property_record = None

                # Try legal description first, then grantor name
                legal = raw.get("Legal") or raw.get("legal_description")
                grantor = record.grantor or raw.get("Grantor") or raw.get("grantor")

                loader = lien_loader  # default — has both matching strategies
                if record.source_type == "deeds":
                    loader = deed_loader
                elif record.source_type in ("evictions", "probate", "bankruptcies"):
                    loader = lp_loader

                if legal:
                    result = loader.find_property_by_legal_description(str(legal))
                    if result:
                        property_record, _ = result

                if not property_record and grantor:
                    result = loader.find_property_by_owner_name(str(grantor))
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
