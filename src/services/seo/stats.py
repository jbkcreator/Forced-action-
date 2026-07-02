"""Gather per-cell data spine for SEO page generation (Task 5.2)."""
import logging
from typing import Sequence

from sqlalchemy.orm import Session
from sqlalchemy import text

logger = logging.getLogger(__name__)

# ponytail: hardcoded to live counties — keep in sync with grid._LIVE_COUNTIES
_LIVE_COUNTIES = ("hillsborough", "pinellas")


def gather_page_data(
    db: Session, cities: Sequence[str], vertical: str, *, county_qualified: int
) -> dict:
    """Return the data dict for a city×vertical SEO page.

    cities: all raw spellings of the one city this page covers (GridCell.variants)
    — 'Tampa' and 'TAMPA' rows are the same page. The `latest`-score-per-property
    sort is scoped to the city's own parcels so this stays cheap per cell.
    median_value (not mean) because assessed_value_mkt has commercial outliers
    that make a mean non-credible on a distressed-homes page.
    county_qualified comes summed from grid.all_qualified_counts() — one bulk
    query, no per-cell county scan.
    """
    row = db.execute(
        text("""
            -- ponytail: TRIM(city) filter seq-scans properties (~522k rows) per
            -- cell. Fine for a weekly job; if it gets slow add a functional index
            -- CONCURRENTLY: CREATE INDEX ON properties (TRIM(city), county_id).
            WITH city_ids AS (
                SELECT id FROM properties
                WHERE TRIM(city) = ANY(:cities)
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
                percentile_cont(0.5) WITHIN GROUP (ORDER BY assessed_value_mkt)      AS median_value,
                COUNT(*) FILTER (WHERE lead_tier = 'Ultra Platinum')                AS ultra_platinum_count,
                COUNT(*) FILTER (WHERE lead_tier = 'Platinum')                      AS platinum_count,
                COUNT(*) FILTER (WHERE lead_tier = 'Gold')                          AS gold_count,
                COUNT(*) FILTER (WHERE absentee_status IN ('Out-of-County', 'Out-of-State')) AS absentee_count
            FROM city_props
        """),
        {"cities": [c.strip() for c in cities], "counties": list(_LIVE_COUNTIES),
         "vertical": vertical},
    ).mappings().fetchone()

    if not row:
        logger.warning("gather_page_data: no data for cities=%s vertical=%s", cities, vertical)
        return {}

    city_count = int(row["qualified_count"] or 0)

    return {
        "qualified_count": city_count,
        "median_value": float(row["median_value"] or 0),
        "ultra_platinum_count": int(row["ultra_platinum_count"] or 0),
        "platinum_count": int(row["platinum_count"] or 0),
        "gold_count": int(row["gold_count"] or 0),
        "absentee_count": int(row["absentee_count"] or 0),
        "city_vs_county_pct": round(city_count / max(county_qualified, 1) * 100, 1),
    }
