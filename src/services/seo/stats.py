"""Gather per-cell data spine for SEO page generation (Task 5.2)."""
import logging
from typing import Optional

from sqlalchemy.orm import Session
from sqlalchemy import text

logger = logging.getLogger(__name__)

# ponytail: hardcoded to live counties — keep in sync with grid._LIVE_COUNTIES
_LIVE_COUNTIES = ("hillsborough", "pinellas")


def gather_page_data(
    db: Session, city: str, vertical: str, *, county_qualified: Optional[int] = None
) -> dict:
    """Return the data dict for a city×vertical SEO page.

    The `latest`-score-per-property sort is scoped to the city's own parcels so
    this stays cheap when called once per cell (no full-table DISTINCT ON).
    county_qualified is the county-wide qualified count for `vertical` — pass it
    from the bulk count query to avoid a county-wide scan per cell; when None it
    is computed here (correct but slower — used by ad-hoc callers/tests).
    Caller already confirmed the cell is eligible, so an empty result is safe.
    """
    row = db.execute(
        text("""
            -- ponytail: TRIM(city) filter seq-scans properties (~522k rows) per
            -- cell. Fine for a weekly job; if it gets slow add a functional index
            -- CONCURRENTLY: CREATE INDEX ON properties (TRIM(city), county_id).
            WITH city_ids AS (
                SELECT id FROM properties
                WHERE TRIM(city) = :city
                  AND county_id = ANY(:counties)
            ),
            latest AS (
                SELECT DISTINCT ON (ds.property_id)
                    ds.property_id,
                    ds.vertical_scores,
                    ds.lead_tier
                FROM distress_scores ds
                WHERE ds.property_id IN (SELECT id FROM city_ids)
                ORDER BY ds.property_id, ds.score_date DESC
            ),
            city_props AS (
                SELECT
                    l.lead_tier,
                    f.assessed_value_mkt,
                    o.absentee_status
                FROM latest l
                JOIN properties p ON p.id = l.property_id
                LEFT JOIN financials f ON f.property_id = p.id
                LEFT JOIN owners o ON o.property_id = p.id
                WHERE (l.vertical_scores ->> :vertical)::float > 0
            )
            SELECT
                COUNT(*)                                                             AS qualified_count,
                AVG(assessed_value_mkt)                                              AS avg_value,
                COUNT(*) FILTER (WHERE lead_tier = 'Ultra Platinum')                AS ultra_platinum_count,
                COUNT(*) FILTER (WHERE lead_tier = 'Platinum')                      AS platinum_count,
                COUNT(*) FILTER (WHERE lead_tier = 'Gold')                          AS gold_count,
                COUNT(*) FILTER (WHERE absentee_status IN ('Out-of-County', 'Out-of-State')) AS absentee_count
            FROM city_props
        """),
        {"city": city.strip(), "counties": list(_LIVE_COUNTIES), "vertical": vertical},
    ).mappings().fetchone()

    if not row:
        logger.warning("gather_page_data: no data for city=%s vertical=%s", city, vertical)
        return {}

    city_count = int(row["qualified_count"] or 0)

    if county_qualified is None:
        county_qualified = _county_qualified_count(db, vertical)

    return {
        "qualified_count": city_count,
        "avg_value": float(row["avg_value"] or 0),
        "ultra_platinum_count": int(row["ultra_platinum_count"] or 0),
        "platinum_count": int(row["platinum_count"] or 0),
        "gold_count": int(row["gold_count"] or 0),
        "absentee_count": int(row["absentee_count"] or 0),
        "city_vs_county_pct": round(city_count / max(county_qualified or 1, 1) * 100, 1),
    }


def _county_qualified_count(db: Session, vertical: str) -> int:
    """County-wide qualified count for a vertical (fallback; full-table scan)."""
    row = db.execute(
        text("""
            WITH latest AS (
                SELECT DISTINCT ON (property_id) property_id, vertical_scores
                FROM distress_scores
                ORDER BY property_id, score_date DESC
            )
            SELECT COUNT(*) AS cnt
            FROM latest l
            JOIN properties p ON p.id = l.property_id
            WHERE p.county_id = ANY(:counties)
              AND (l.vertical_scores ->> :vertical)::float > 0
        """),
        {"counties": list(_LIVE_COUNTIES), "vertical": vertical},
    ).scalar()
    return int(row or 0)
