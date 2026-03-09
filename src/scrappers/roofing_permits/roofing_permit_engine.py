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
        start_date = end_date - timedelta(days=30)
    else:
        start_date, end_date = date_range

    logger.info(
        f"[roofing_permits] Classifying roofing permits for {county_id} "
        f"{start_date} → {end_date}"
    )

    created = 0

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

        logger.info(f"[roofing_permits] Found {len(permits)} roofing permits to classify")

        for permit in permits:
            # Skip if an Incident for this permit already exists
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

    logger.info(f"[roofing_permits] Created {created} new Incident records")
    return created


if __name__ == "__main__":
    import sys
    county = sys.argv[1] if len(sys.argv) > 1 else "hillsborough"
    n = scrape_roofing_permits(county_id=county)
    print(f"Done — {n} new roofing permit incidents created")
