"""
Lead pool service — thin wrappers over read_tools queries for use outside the agents layer.

api/main.py and services/wallet_to_lock.py need lead-pool and ZIP-activity data
but should not import directly from src.agents.tools.read_tools (crosses the
process boundary). This module exposes the same queries via service functions.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from sqlalchemy import and_, exists, or_, select, text
from sqlalchemy.orm import Session

from config.triangulation import PACK_CONTACTABLE_LABELS, PACK_MIN_CONTACTABLE_PCT

# ADR 0032 — insurance-distress segment qualifiers. insurance_claim is
# deliberately excluded: it's the FEMA bulk source reverted from CDS weight 72
# on 2026-05-06 (config/scoring.py:130) after it auto-qualified ~38k junk leads.
INSURANCE_DISTRESS_INCIDENT_TYPES = ("storm_damage", "flood_damage")
INSURANCE_DISTRESS_VERTICALS = ("wholesalers", "fix_flip")


def insurance_distress_segment_clause(now: datetime):
    """
    SQLAlchemy WHERE clause selecting properties that qualify for the
    insurance-distress lead segment (ADR 0032): a genuine flip/investment
    signal (wholesalers or fix_flip >= Silver floor) AND a storm/flood
    damage incident within STACKING_WINDOW_DAYS.

    Must be used inside a query that already selects from Property joined
    to DistressScore (for correlation of the Incident EXISTS subquery).
    """
    from config.scoring import LEAD_TIER_THRESHOLDS, STACKING_WINDOW_DAYS
    from src.core.models import DistressScore, Incident, Property

    silver_floor = next(score for score, tier in LEAD_TIER_THRESHOLDS if tier == "Silver")
    cutoff = now - timedelta(days=STACKING_WINDOW_DAYS)

    return and_(
        or_(*(
            DistressScore.vertical_scores[v].as_float() >= silver_floor
            for v in INSURANCE_DISTRESS_VERTICALS
        )),
        exists().where(
            Incident.property_id == Property.id,
            Incident.incident_type.in_(INSURANCE_DISTRESS_INCIDENT_TYPES),
            Incident.incident_date >= cutoff,
        ),
    )


def apply_segment_filter(filters: List[Any], segment: Optional[str], now: datetime) -> None:
    """
    Append the segment-specific WHERE clause to `filters` in place, if any.
    Single call site for the checkout gate, the webhook reservation, and the
    availability endpoint so they can never drift apart (ADR 0032 D5).
    """
    if segment == "insurance_distress":
        filters.append(insurance_distress_segment_clause(now))


def sellable_lead_filters(settings: Any) -> List[Any]:
    """
    Shared sellability predicate for a lead-pack lead (ADR 0032 D5). Every
    surface that COUNTS or RESERVES a sellable lead — the availability feed
    card, the checkout gate, and the webhook reservation — must build on this
    so they cannot drift and advertise/charge for a lead that later fails to
    fulfil:

      * DistressScore.qualified — passes the CDS qualification bar.
      * is_guess_lead is False  — never a guessed/imputed lead in a paid pack (A2).
      * contactable             — at least one phone/email on Owner (unless debug).

    Cross-trade exclusivity and the segment clause are appended by the caller
    (they need per-request county/ZIP/now context) via get_exclusive_property_ids
    and apply_segment_filter.

    The returned filters reference DistressScore and Owner, so the caller's
    query must join DistressScore and outer-join Owner to Property.
    """
    from src.core.models import DistressScore
    from src.utils.lead_filters import has_contact_filter

    filters: List[Any] = [
        DistressScore.qualified == True,  # noqa: E712 — SQLAlchemy needs ==, not is
        DistressScore.is_guess_lead.is_(False),
    ]
    contact_clause = has_contact_filter(settings)
    if contact_clause is not None:
        filters.append(contact_clause)
    return filters


# Minimum sellable, exclusive leads a ZIP must have before it can be sold.
# The lead-pack checkout gate (main.py) and the demo deal-room generator
# (deal_room_router.py) both gate on this same number via count_available_leads
# so a demo can never represent inventory the real checkout would then reject.
MIN_EXCLUSIVE_LEADS = 5


def count_available_leads(
    db: Session,
    *,
    county_id: str,
    zip_code: str,
    segment: Optional[str],
    now: datetime,
    limit: int = MIN_EXCLUSIVE_LEADS,
) -> int:
    """
    Count sellable, exclusive leads for a (county, zip[, segment]) combination,
    capped at `limit` — callers only need to know "is there at least N", never
    the true total. Same predicate as the lead-pack checkout gate and webhook
    reservation (ADR 0032 D5): qualified, non-guess, contactable,
    cross-trade-exclusive, segment-filtered. `qualified` is computed from the
    overall CDS score (cds_engine.py), not per-vertical, so this count is not
    scoped by vertical either — matching the existing checkout gate exactly.
    """
    from config.settings import get_settings
    from src.core.models import DistressScore, Owner, Property
    from src.services.lead_exclusivity import get_exclusive_property_ids

    settings = get_settings()
    excl_ids = get_exclusive_property_ids(db, county_id, now, zip_code=zip_code)

    filters = sellable_lead_filters(settings)
    filters.append(Property.zip == zip_code)
    filters.append(Property.county_id == county_id)
    if excl_ids:
        filters.append(Property.id.not_in(excl_ids))
    apply_segment_filter(filters, segment, now)

    rows = db.execute(
        select(Property.id)
        .join(DistressScore, DistressScore.property_id == Property.id)
        .outerjoin(Owner, Owner.property_id == Property.id)
        .where(and_(*filters))
        .limit(limit)
    ).scalars().all()
    return len(rows)


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

    Pass exclude_trade + county_id to apply cross-trade exclusivity (so Lifecycle
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
