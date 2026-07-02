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

# Pseudo-city values in properties.city that must never become pages.
_CITY_BLOCKLIST = {"UNINCORPORATED", "UNKNOWN", "N/A", "NA", "NONE"}


class GridCell(NamedTuple):
    city_raw: str                    # display variant (most properties)
    city_slug: str
    vertical: str
    topic_slug: str
    variants: tuple[str, ...] = ()   # all raw spellings this slug covers, for SQL matching


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


def group_city_variants(rows: list[tuple[str, int]]) -> list[tuple[str, str, tuple[str, ...]]]:
    """Merge raw city spellings that slug identically into one city identity.

    rows: (raw_city, property_count) pairs. Returns (display, slug, variants)
    per city — display is the highest-count spelling. Same-city case/punctuation
    variants ('Tampa'/'TAMPA', 'ST PETERSBURG'/'ST. PETERSBURG') would otherwise
    each mint a page (tampa + tampa-2) and compete against themselves in Google.
    Blocklisted pseudo-cities are dropped.
    """
    by_slug: dict[str, list[tuple[str, int]]] = {}
    for raw, n in rows:
        if raw.upper() in _CITY_BLOCKLIST:
            continue
        slug = city_to_slug(raw)
        if not slug:
            continue
        by_slug.setdefault(slug, []).append((raw, n))

    out = []
    for slug, variants in sorted(by_slug.items()):
        variants.sort(key=lambda v: -v[1])
        display = variants[0][0]
        out.append((display, slug, tuple(raw for raw, _ in variants)))
    return out


def discover_cells(db: Session) -> list[GridCell]:
    """Return one GridCell per (city, vertical) from live counties.

    City identity = slug: raw spellings that slug identically are one city
    (see group_city_variants), so a city can never split into competing pages.
    """
    rows = db.execute(
        text("""
            SELECT TRIM(city) AS city, COUNT(*) AS n
            FROM properties
            WHERE county_id = ANY(:counties)
              AND city IS NOT NULL
              AND TRIM(city) != ''
            GROUP BY TRIM(city)
        """),
        {"counties": list(_LIVE_COUNTIES)},
    ).fetchall()

    cells: list[GridCell] = []
    for display, slug, variants in group_city_variants([(r.city, r.n) for r in rows]):
        for vertical in VERTICALS:
            cells.append(GridCell(
                city_raw=display,
                city_slug=slug,
                vertical=vertical,
                topic_slug=vertical_to_slug(vertical),
                variants=variants,
            ))
    return cells


def all_qualified_counts(db: Session) -> dict[tuple[str, str], int]:
    """Return {(city_slug, vertical): count} for all live-county cities × verticals.

    One query for all cells — no query-in-loop in the compiler. Counts for raw
    spellings of the same city are summed under one slug (same identity rule
    as discover_cells).
    """
    verticals_sql = ", ".join(f"('{v}')" for v in VERTICALS)
    rows = db.execute(
        text(f"""
            WITH latest AS (
                SELECT DISTINCT ON (property_id)
                    property_id, vertical_scores, qualified, is_guess_lead
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
              -- qualified = the platform-wide lead definition (cds_engine routing
              -- threshold); is_guess_lead = A2 low-confidence gate. Pages must
              -- count only inventory a subscriber would actually receive.
              AND l.qualified = TRUE
              AND l.is_guess_lead = FALSE
              AND (l.vertical_scores ->> v.vertical)::float > 0
            GROUP BY TRIM(p.city), v.vertical
        """),
        {"counties": list(_LIVE_COUNTIES)},
    ).fetchall()

    counts: dict[tuple[str, str], int] = {}
    for row in rows:
        if row.city.upper() in _CITY_BLOCKLIST:
            continue
        key = (city_to_slug(row.city), row.vertical)
        counts[key] = counts.get(key, 0) + row.cnt
    return counts
