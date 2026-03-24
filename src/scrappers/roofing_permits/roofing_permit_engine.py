"""
Roofing Permit Keyword Filter — M1-F Scraper #5

SQL classifier on the existing building_permits table.
No new scraping required — runs entirely on already-loaded permit data.

Matches permits whose permit_type contains roofing-related keywords and
creates/updates an Incident record (type='roofing_permit') on the linked
property so the CDS engine can score it.

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

# Keywords that indicate a roofing job
ROOFING_KEYWORDS = [
    "roof", "shingle", "tpo", "tile", "fascia",
    "soffit", "gutters", "flashing", "underlayment",
    "re-roof", "reroof",
]


def _keyword_filter():
    """SQLAlchemy OR filter across all roofing keywords (case-insensitive)."""
    return or_(
        *[
            func.lower(BuildingPermit.permit_type).contains(kw)
            for kw in ROOFING_KEYWORDS
        ]
    )


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
                    _keyword_filter(),
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
    args = parser.parse_args()
    n = scrape_roofing_permits(county_id=args.county_id)
    print(f"Done — {n} new roofing permit incidents created")
