"""Discovery and eligibility for the programmatic SEO page grid (Task 5.2)."""
import re
import logging
from typing import NamedTuple

from sqlalchemy.orm import Session
from sqlalchemy import text

logger = logging.getLogger(__name__)

VERTICALS: list[str] = [
    "wholesalers", "fix_flip", "restoration", "roofing", "public_adjusters", "attorneys"
]

# ponytail: hardcoded to live counties; read from DB when 3+ counties active
_LIVE_COUNTIES = ("hillsborough", "pinellas")


class GridCell(NamedTuple):
    city_raw: str
    city_slug: str
    vertical: str
    topic_slug: str


def city_to_slug(city: str) -> str:
    """'St. Petersburg' → 'st-petersburg'"""
    s = city.lower().strip()
    s = re.sub(r"[^a-z0-9\s-]", "", s)   # strip punctuation except hyphen
    s = re.sub(r"[\s-]+", "-", s)         # collapse whitespace/hyphens
    return s.strip("-")


def vertical_to_slug(vertical: str) -> str:
    """'fix_flip' → 'fix-flip'"""
    return vertical.replace("_", "-")


def is_eligible(count: int, floor: int) -> bool:
    return count >= floor


def discover_cells(db: Session) -> list[GridCell]:
    """Return one GridCell per (city, vertical) from live counties.

    Cities are DB-derived from properties. Slug collisions across distinct city
    names get a numeric suffix (city-slug-2, city-slug-3, …).
    """
    rows = db.execute(
        text("""
            SELECT DISTINCT TRIM(city) AS city
            FROM properties
            WHERE county_id = ANY(:counties)
              AND city IS NOT NULL
              AND TRIM(city) != ''
            ORDER BY 1
        """),
        {"counties": list(_LIVE_COUNTIES)},
    ).fetchall()

    cells: list[GridCell] = []
    slug_seen: dict[str, int] = {}

    for row in rows:
        city_raw = row.city
        base = city_to_slug(city_raw)

        if base in slug_seen:
            slug_seen[base] += 1
            city_slug = f"{base}-{slug_seen[base]}"
        else:
            slug_seen[base] = 1
            city_slug = base

        for vertical in VERTICALS:
            cells.append(GridCell(
                city_raw=city_raw,
                city_slug=city_slug,
                vertical=vertical,
                topic_slug=vertical_to_slug(vertical),
            ))

    return cells


def all_qualified_counts(db: Session) -> dict[tuple[str, str], int]:
    """Return {(city_raw, vertical): count} for all live-county cities × verticals.

    One query for all cells — no query-in-loop in the compiler.
    """
    verticals_sql = ", ".join(f"('{v}')" for v in VERTICALS)
    rows = db.execute(
        text(f"""
            WITH latest AS (
                SELECT DISTINCT ON (property_id)
                    property_id, vertical_scores
                FROM distress_scores
                ORDER BY property_id, score_date DESC
            )
            SELECT
                TRIM(p.city) AS city,
                v.vertical,
                COUNT(*) AS cnt
            FROM properties p
            JOIN latest l ON l.property_id = p.id
            CROSS JOIN (VALUES {verticals_sql}) v(vertical)
            WHERE p.county_id = ANY(:counties)
              AND p.city IS NOT NULL
              AND TRIM(p.city) != ''
              AND (l.vertical_scores ->> v.vertical)::float > 0
            GROUP BY TRIM(p.city), v.vertical
        """),
        {"counties": list(_LIVE_COUNTIES)},
    ).fetchall()

    return {(row.city, row.vertical): row.cnt for row in rows}


def qualified_count(db: Session, city: str, vertical: str) -> int:
    """Count properties with a positive score for the vertical in the given city.

    The latest-per-property sort is scoped to the city's parcels so this is cheap
    per call (no full distress_scores DISTINCT ON).
    """
    row = db.execute(
        text("""
            WITH city_ids AS (
                SELECT id FROM properties
                WHERE TRIM(city) = :city
                  AND county_id = ANY(:counties)
            ),
            latest AS (
                SELECT DISTINCT ON (property_id)
                    property_id, vertical_scores
                FROM distress_scores
                WHERE property_id IN (SELECT id FROM city_ids)
                ORDER BY property_id, score_date DESC
            )
            SELECT COUNT(*) AS cnt
            FROM latest l
            WHERE (l.vertical_scores ->> :vertical)::float > 0
        """),
        {"city": city.strip(), "counties": list(_LIVE_COUNTIES), "vertical": vertical},
    ).scalar()
    return int(row or 0)
