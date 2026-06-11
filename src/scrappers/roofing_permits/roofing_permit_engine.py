"""
Roofing Permit Keyword Filter — M1-F Scraper #5

SQL classifier on the existing building_permits table.
No new scraping required — runs entirely on already-loaded permit data.

Matches permits whose permit_type contains roofing-related keywords.
For Pinellas, also matches Express Building Permits where the description
field contains a roofing keyword (Pinellas uses generic permit types;
roofing details only appear in the Description field).
Creates an Incident record (type='roofing_permit') on the linked property
so the CDS engine can score it.

Entry point:
    scrape_roofing_permits(county_id, date_range)
"""

import logging
from datetime import date, timedelta
from typing import Optional, Tuple

from sqlalchemy import select, and_, or_, func
from sqlalchemy.orm import Session

from src.core.database import get_db_context
from src.core.models import BuildingPermit, Property, Incident

logger = logging.getLogger(__name__)

# Matches against permit_type for all counties (Hillsborough behavior unchanged)
ROOFING_KEYWORDS = [
    "roof", "shingle", "tpo", "tile", "flashing", "underlayment",
]

# Pinellas only — matched against description when permit_type is an Express permit
PINELLAS_DESCRIPTION_KEYWORDS = [
    "roof",        # re-roof, reroof, metal roof, reroof metal
    "shingle",     # shingle, shingles
    "tile",
    "tro",
    "tpo",
    "aluminium",
    "aluminum",
]


def _keyword_filter(county_id: str):
    """
    All counties: keyword match on permit_type.
    Pinellas only: additionally match Express Building Permits where description
    contains a roofing keyword. The Express Permit guard prevents false positives
    from non-roofing permits that happen to have matching description text.
    """
    type_filters = [func.lower(BuildingPermit.permit_type).contains(kw) for kw in ROOFING_KEYWORDS]

    if county_id == "pinellas":
        desc_filters = [func.lower(BuildingPermit.description).contains(kw) for kw in PINELLAS_DESCRIPTION_KEYWORDS]
        express_desc_match = and_(
            func.lower(BuildingPermit.permit_type).contains("express"),
            or_(*desc_filters),
        )
        return or_(*type_filters, express_desc_match)

    return or_(*type_filters)


def scrape_roofing_permits(
    county_id: str = "hillsborough",
    date_range: Optional[Tuple[date, date]] = None,
) -> int:
    """
    Classify roofing permits from the existing building_permits table and
    upsert Incident records so CDS scoring picks them up.

    Args:
        county_id:   County to process.
        date_range:  (start_date, end_date) tuple. Defaults to last 30 days.

    Returns:
        Number of new Incident records created.
    """
    if date_range is None:
        end_date = date.today()
        start_date = end_date - timedelta(days=1)
    else:
        start_date, end_date = date_range

    created = 0
    skipped_no_property = 0
    skipped_duplicate = 0

    with get_db_context() as db:
        permits = db.execute(
            select(BuildingPermit)
            .where(
                and_(
                    BuildingPermit.county_id == county_id,
                    BuildingPermit.issue_date >= start_date,
                    BuildingPermit.issue_date <= end_date,
                    _keyword_filter(county_id),
                )
            )
        ).scalars().all()

        for permit in permits:
            if not permit.property_id:
                skipped_no_property += 1
                continue

            existing = db.execute(
                select(Incident).where(
                    and_(
                        Incident.property_id == permit.property_id,
                        Incident.incident_type == "roofing_permit",
                        Incident.incident_date == permit.issue_date,
                    )
                )
            ).scalars().first()

            if existing:
                skipped_duplicate += 1
                continue

            incident = Incident(
                property_id=permit.property_id,
                incident_type="roofing_permit",
                incident_date=permit.issue_date,
                county_id=county_id,
            )
            db.add(incident)
            created += 1

        db.commit()

    if skipped_no_property:
        logger.warning(
            "[roofing_permits] %d permits had no property_id and were skipped",
            skipped_no_property,
        )

    logger.info(
        "[roofing_permits] %s %s→%s: created=%d duplicate=%d no_property=%d",
        county_id, start_date, end_date, created, skipped_duplicate, skipped_no_property,
    )
    try:
        from src.utils.scraper_db_helper import record_scraper_stats
        record_scraper_stats(
            source_type='roofing_permits',
            total_scraped=created + skipped_duplicate + skipped_no_property,
            matched=created,
            unmatched=skipped_no_property,
            skipped=skipped_duplicate,
            scored=created,
            county_id=county_id,
        )
    except Exception as stats_err:
        logger.warning("⚠ Could not record scraper stats (non-critical): %s", stats_err)
    return created


if __name__ == "__main__":
    import argparse
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    parser = argparse.ArgumentParser(description="Scrape roofing permit incidents")
    parser.add_argument("--county-id", dest="county_id", default="hillsborough", help="County identifier (default: hillsborough)")
    parser.add_argument("--backfill", action="store_true", help="Classify all permits ever loaded for this county (duplicates are skipped automatically)")
    args = parser.parse_args()

    if args.backfill:
        from src.core.database import get_db_context
        from sqlalchemy import text
        with get_db_context() as db:
            row = db.execute(
                text("SELECT MIN(issue_date) FROM building_permits WHERE county_id = :cid AND issue_date IS NOT NULL"),
                {"cid": args.county_id},
            ).fetchone()
        earliest = row[0] if row and row[0] else date.today()
        date_range = (earliest, date.today())
        logger.info("[backfill] %s: classifying permits from %s to %s", args.county_id, earliest, date.today())
    else:
        date_range = None

    n = scrape_roofing_permits(county_id=args.county_id, date_range=date_range)
    print(f"Done — {n} new roofing permit incidents created")
