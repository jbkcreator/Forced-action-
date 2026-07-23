"""
County-level territory scarcity.

Derives open vs locked ZIP pressure for a county from the existing
`ZipTerritory.status` field (available | locked | grace). No per-ZIP seat
capacity — the model is exclusive (one investor per ZIP per vertical), so
scarcity is expressed as "how many ZIPs in this county are still open".

Counts are read straight from `zip_territories`; there is no new schema.
"""

import logging
from typing import Optional

from sqlalchemy import text as sa_text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


# Most-restrictive-wins precedence when a ZIP's status differs by vertical and
# no vertical was requested — locked/grace pressure should never be masked by
# an unrelated vertical happening to still be available.
_STATUS_PRECEDENCE = {"locked": 0, "grace": 1, "available": 2}


def _resolve_county_id(db: Session, zip_code: str, vertical: Optional[str]) -> Optional[str]:
    """Return the county_id a ZIP belongs to, or None if the ZIP is unknown.

    A ZIP can appear under several verticals; when `vertical` is given it is
    used to disambiguate, otherwise the first matching territory row wins
    (a ZIP maps to a single county regardless of vertical).
    """
    params: dict = {"zip": zip_code}
    clause = "WHERE zip_code = :zip"
    if vertical:
        clause += " AND vertical = :vertical"
        params["vertical"] = vertical
    row = db.execute(
        sa_text(f"SELECT county_id FROM zip_territories {clause} LIMIT 1"),
        params,
    ).first()
    return row[0] if row else None


def _resolve_zip_status(
    db: Session, county_id: str, zip_code: str, vertical: Optional[str]
) -> Optional[str]:
    """Return the queried ZIP's status.

    When `vertical` is given, the status is unambiguous. When omitted, a ZIP
    may carry different statuses across verticals — pick the most restrictive
    (locked > grace > available) rather than an arbitrary row, so the visitor
    is never shown a falsely-open status.
    """
    params: dict = {"county_id": county_id, "zip": zip_code}
    clause = "WHERE county_id = :county_id AND zip_code = :zip"
    if vertical:
        clause += " AND vertical = :vertical"
        params["vertical"] = vertical

    rows = db.execute(
        sa_text(f"SELECT DISTINCT status FROM zip_territories {clause}"),
        params,
    ).all()
    if not rows:
        return None
    statuses = [r[0] for r in rows]
    return min(statuses, key=lambda s: _STATUS_PRECEDENCE.get(s, 99))


def county_scarcity(
    db: Session,
    zip_code: str,
    vertical: Optional[str] = None,
) -> Optional[dict]:
    """Compute open/locked ZIP counts for the county a ZIP belongs to.

    Returns None if the ZIP is not present in `zip_territories`.

    When `vertical` is supplied, counts are scoped to that vertical (clean
    per-vertical inventory). When omitted, every (zip, vertical) territory row
    in the county is counted.

    A single round trip: one query resolves the county + queried-ZIP status,
    a second aggregates the county's statuses.
    """
    county_id = _resolve_county_id(db, zip_code, vertical)
    if not county_id:
        return None

    params: dict = {"county_id": county_id}
    vertical_clause = ""
    if vertical:
        vertical_clause = " AND vertical = :vertical"
        params["vertical"] = vertical

    rows = db.execute(
        sa_text(
            f"""
            SELECT status, COUNT(DISTINCT zip_code) AS n
              FROM zip_territories
             WHERE county_id = :county_id{vertical_clause}
             GROUP BY status
            """
        ),
        params,
    ).all()

    counts = {"available": 0, "locked": 0, "grace": 0}
    for status, n in rows:
        if status in counts:
            counts[status] = int(n)

    zip_status = _resolve_zip_status(db, county_id, zip_code, vertical)

    open_count = counts["available"]
    grace_count = counts["grace"]
    # A ZIP in grace is not freely lockable — pressure, not supply — but it is
    # NOT the same as hard-locked (it may reopen). Keep locked_count as the
    # hard-locked-only figure and expose grace_count separately so callers can
    # label it honestly instead of collapsing both into "locked".
    locked_count = counts["locked"]
    total = open_count + locked_count + grace_count

    county_name = county_id
    try:
        from src.utils.county_config import get_county

        county_name = get_county(county_id).get("display_name") or county_id
    except Exception as exc:
        logger.warning("territory_scarcity: county name lookup failed for %s: %s", county_id, exc)

    return {
        "zip_code": zip_code,
        "zip_status": zip_status,
        "county_id": county_id,
        "county_name": county_name,
        "vertical": vertical,
        "open_count": open_count,
        "locked_count": locked_count,
        "grace_count": grace_count,
        "total_count": total,
    }
