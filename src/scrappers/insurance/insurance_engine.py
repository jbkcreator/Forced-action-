"""
Insurance Claim Filings — M1-F Scraper #1

Sources:
  1. FEMA Individual Assistance registrations (public API) — flood/storm claims
  2. Hillsborough County building adjuster permits (Accela portal, permit_type
     contains 'insurance', 'adjuster', 'claim', 'damage assessment')

Insurance claims are the #1 revenue signal — $12K–$25K roofing/remediation jobs,
10-20% close rate. Flag immediately.

Creates Incident records (incident_type='insurance_claim') on matched properties.

Entry point:
    scrape_insurance_claims(county_id, date_range)
"""

import logging
from datetime import date, timedelta
from typing import Optional, Tuple, List, Dict

import requests

from src.core.database import get_db_context
from src.core.models import Property, Incident, BuildingPermit
from src.utils.county_config import get_county
from sqlalchemy import select, and_, or_, func

logger = logging.getLogger(__name__)

_FEMA_IA_URL = "https://www.fema.gov/api/open/v2/HousingAssistanceOwners"

# Permit keywords indicating insurance/adjuster activity
INSURANCE_PERMIT_KEYWORDS = [
    "insurance",
    "adjuster",
    "claim",
    "damage assessment",
    "damage repair",
    "storm damage",
    "flood damage",
    "fire damage",
    "wind damage",
    "hail damage",
]


def _insurance_permit_filter():
    """SQLAlchemy OR filter for insurance-related permit types."""
    return or_(
        *[
            func.lower(BuildingPermit.permit_type).contains(kw)
            for kw in INSURANCE_PERMIT_KEYWORDS
        ]
    )


def _fetch_fema_ia_registrants(state: str, start_date: date) -> List[Dict]:
    """
    Fetch FEMA Housing Assistance Owners records for a state (grouped by ZIP).
    These represent homeowners who received FEMA disaster housing assistance.
    """
    # Build query string manually — requests.params URL-encodes $ which breaks FEMA API
    url = (
        f"{_FEMA_IA_URL}"
        f"?$filter=state eq '{state}'"
        f"&$select=zipCode,county,city,totalDamage,repairReplaceAmount,validRegistrations"
        f"&$top=1000&$format=json"
    )
    try:
        resp = requests.get(url, headers={"Accept": "application/json"}, timeout=20)
        resp.raise_for_status()
        return resp.json().get("HousingAssistanceOwners", [])
    except Exception as e:
        logger.warning("[insurance] FEMA IA API failed: %s", e, exc_info=True)
        return []


def _get_insurance_permits(db, county_id: str, start_date: date, end_date: date) -> List:
    """Query existing building_permits for insurance/adjuster permit types."""
    return db.execute(
        select(BuildingPermit).where(
            and_(
                BuildingPermit.county_id == county_id,
                BuildingPermit.issue_date >= start_date,
                BuildingPermit.issue_date <= end_date,
                _insurance_permit_filter(),
            )
        )
    ).scalars().all()


def scrape_insurance_claims(
    county_id: str = "hillsborough",
    date_range: Optional[Tuple[date, date]] = None,
) -> int:
    """
    Collect insurance claim signals from FEMA IA API + adjuster permits
    and create Incident records for matched properties.

    Args:
        county_id:  County to process.
        date_range: (start_date, end_date). Defaults to last 30 days.

    Returns:
        Number of new Incident records created.
    """
    config = get_county(county_id)
    fips = config.get("fips", "")
    state = config.get("state", "FL")
    zip_prefixes = config.get("zip_prefixes", [])

    if date_range is None:
        end_date = date.today()
        start_date = end_date - timedelta(days=30)
    else:
        start_date, end_date = date_range

    created = 0
    skipped_duplicate = 0
    skipped_no_property = 0

    with get_db_context() as db:
        # ── Source 1: adjuster permits from existing permit table ──────────
        insurance_permits = _get_insurance_permits(db, county_id, start_date, end_date)

        for permit in insurance_permits:
            if not permit.property_id:
                skipped_no_property += 1
                continue

            existing = db.execute(
                select(Incident).where(
                    and_(
                        Incident.property_id == permit.property_id,
                        Incident.incident_type == "insurance_claim",
                        Incident.incident_date == permit.issue_date,
                    )
                )
            ).scalars().first()

            if existing:
                skipped_duplicate += 1
                continue

            db.add(Incident(
                property_id=permit.property_id,
                incident_type="insurance_claim",
                incident_date=permit.issue_date or date.today(),
                county_id=county_id,
            ))
            created += 1

        db.commit()

        # ── Source 2: FEMA IA registrants → match by ZIP ──────────────────
        fema_registrants = _fetch_fema_ia_registrants(state, start_date)

        affected_zips = set()
        for reg in fema_registrants:
            z = str(reg.get("zipCode", "")).zfill(5)
            if any(z.startswith(p) for p in zip_prefixes):
                affected_zips.add(z)

        if affected_zips:
            properties = db.execute(
                select(Property).where(
                    and_(
                        Property.county_id == county_id,
                        Property.zip.in_(affected_zips),
                    )
                )
            ).scalars().all()

            # Pre-fetch all property IDs that already have a FEMA insurance_claim
            # to avoid N+1 queries. FEMA data is cumulative so dedup on property+type only.
            existing_ids = set(
                r[0] for r in db.execute(
                    select(Incident.property_id).where(
                        and_(
                            Incident.incident_type == "insurance_claim",
                            Incident.county_id == county_id,
                        )
                    ).distinct()
                ).fetchall()
            )

            claim_date = date.today()
            fema_created = 0
            fema_skipped = 0
            for prop in properties:
                if prop.id in existing_ids:
                    fema_skipped += 1
                    continue

                db.add(Incident(
                    property_id=prop.id,
                    incident_type="insurance_claim",
                    incident_date=claim_date,
                    county_id=county_id,
                ))
                fema_created += 1
                created += 1

            db.commit()

            if skipped_no_property:
                logger.warning(
                    "[insurance] %d permits had no property_id and were skipped",
                    skipped_no_property,
                )

            logger.info(
                "[insurance] %s %s→%s: created=%d duplicate=%d "
                "(permits=%d fema_zips=%d fema_props=%d)",
                county_id, start_date, end_date, created, skipped_duplicate + fema_skipped,
                len(insurance_permits), len(affected_zips), len(properties),
            )
        else:
            if skipped_no_property:
                logger.warning(
                    "[insurance] %d permits had no property_id and were skipped",
                    skipped_no_property,
                )
            logger.info(
                "[insurance] %s %s→%s: created=%d duplicate=%d "
                "(permits=%d fema_zips=0)",
                county_id, start_date, end_date, created, skipped_duplicate,
                len(insurance_permits),
            )

    return created


if __name__ == "__main__":
    import argparse
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    parser = argparse.ArgumentParser(description="Scrape insurance claim incidents")
    parser.add_argument("--county-id", dest="county_id", default="hillsborough", help="County identifier (default: hillsborough)")
    args = parser.parse_args()
    n = scrape_insurance_claims(county_id=args.county_id)
    print(f"Done — {n} insurance claim incidents created")
