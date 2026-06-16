"""
Lead pool service — thin wrappers over read_tools queries for use outside the agents layer.

api/main.py and services/wallet_to_lock.py need lead-pool and ZIP-activity data
but should not import directly from src.agents.tools.read_tools (crosses the
process boundary). This module exposes the same queries via service functions.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from config.triangulation import PACK_CONTACTABLE_LABELS, PACK_MIN_CONTACTABLE_PCT


def get_lead_pool(
    zip_code: str,
    vertical: Optional[str] = None,
    min_score: int = 0,
    limit: int = 25,
    exclude_trade: Optional[str] = None,
    county_id: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Return scored leads available in a ZIP.
    Delegates to read_tools.get_lead_pool — same query, same return shape.

    Pass exclude_trade + county_id to apply cross-trade exclusivity (so Cora
    upsell paths never surface a lead already sold to another trade).
    """
    from src.agents.tools.read_tools import get_lead_pool as _get_lead_pool
    return _get_lead_pool(
        zip_code=zip_code, vertical=vertical, min_score=min_score, limit=limit,
        exclude_trade=exclude_trade, county_id=county_id,
    )


def check_pack_contactability(
    session: Session,
    property_ids: List[int],
) -> Dict[str, Any]:
    """
    Validate that a lead pack meets the minimum contactability threshold (ADR 0015).

    Queries owners for the given property_ids, counts how many have a
    contact_info_confidence in PACK_CONTACTABLE_LABELS ('high' or 'medium'),
    and returns whether the pack passes the PACK_MIN_CONTACTABLE_PCT gate.

    Returns:
        {
            "pct_contactable": float,   # 0.0–1.0
            "passes": bool,
            "counts": {"high": n, "medium": n, "low": n, "stale": n, "unknown": n},
            "total": int,
        }
    """
    if not property_ids:
        return {"pct_contactable": 0.0, "passes": False, "counts": {}, "total": 0}

    rows = session.execute(
        text("""
            SELECT contact_info_confidence, COUNT(*) AS n
            FROM owners
            WHERE property_id = ANY(:pids)
            GROUP BY contact_info_confidence
        """),
        {"pids": property_ids},
    ).fetchall()

    counts: Dict[str, int] = {}
    for row in rows:
        label = row.contact_info_confidence or "unknown"
        counts[label] = counts.get(label, 0) + int(row.n)

    total = sum(counts.values())
    contactable = sum(counts.get(lbl, 0) for lbl in PACK_CONTACTABLE_LABELS)
    pct = contactable / total if total > 0 else 0.0

    return {
        "pct_contactable": round(pct, 4),
        "passes": pct >= PACK_MIN_CONTACTABLE_PCT,
        "counts": counts,
        "total": total,
    }


def get_zip_activity(
    zip_code: str,
    vertical: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Live activity snapshot for a ZIP — active urgency-window count + 24h message volume.
    Delegates to read_tools.get_zip_activity — same query, same return shape.
    """
    from src.agents.tools.read_tools import get_zip_activity as _get_zip_activity
    return _get_zip_activity(zip_code=zip_code, vertical=vertical)
