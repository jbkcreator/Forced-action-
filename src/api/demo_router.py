"""
Live Demo Mode endpoints (3g + 3h + 3i).

Prefix: /api/demo

Endpoints:
  GET  /zip-reveal          — all distress-scored properties in a ZIP for the past N days
  POST /prepare-call        — pick + store the featured lead (masked) for this demo session
  POST /reveal-lead/{prep_id} — un-blur the featured lead, record revealed_at
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import select, text
from sqlalchemy.exc import OperationalError, SQLAlchemyError

from src.api.deps import VALID_VERTICALS, ZIP_RE, get_db, resolve_phone_with_quality
from src.core.models import DemoSession, DistressScore, Owner, Property, Subscriber
from src.services.proof_moment import _blur_address
from src.utils.lead_filters import has_contact_filter, phone_priority_order
from config.settings import get_settings

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/demo", tags=["demo"])

GOLD_PLUS_TIERS = {"Gold", "Platinum", "Ultra Platinum"}
# Featured lead looks back further than the zip-reveal list to maximise coverage
_FEATURED_LEAD_DAYS = 30


# ---------------------------------------------------------------------------
# Shared auth guard — all three endpoints require is_demo=true
# ---------------------------------------------------------------------------

def _require_demo_sub(feed_uuid: str, db) -> Subscriber:
    sub = db.execute(
        select(Subscriber).where(Subscriber.event_feed_uuid == feed_uuid)
    ).scalar_one_or_none()
    if not sub or not sub.is_demo:
        raise HTTPException(status_code=403, detail="Demo access only")
    return sub


# ---------------------------------------------------------------------------
# Endpoint A — ZIP Reveal
# ---------------------------------------------------------------------------

@router.get("/zip-reveal")
def zip_reveal(
    feed_uuid: str,
    zip_code: str,
    county_id: str = "hillsborough",
    days: int = Query(default=7, ge=1, le=30),
    db=Depends(get_db),
):
    """Full-screen list of distress-scored properties in a ZIP for the past N days."""
    _require_demo_sub(feed_uuid, db)

    if not ZIP_RE.match(zip_code):
        raise HTTPException(status_code=422, detail="Invalid ZIP code")

    try:
        # CTE: latest score_date per property prevents duplicates when a property
        # was scored multiple times in the window.
        rows = db.execute(
            text("""
            WITH latest AS (
                SELECT property_id, MAX(score_date) AS max_date
                FROM distress_scores
                WHERE score_date >= NOW() - INTERVAL '1 day' * :days
                  AND county_id = :county_id
                GROUP BY property_id
            )
            SELECT DISTINCT ON (p.id)
                p.id          AS property_id,
                p.address,
                p.city,
                p.state,
                p.zip,
                ds.lead_tier,
                ds.distress_types,
                ds.final_cds_score,
                ds.score_date
            FROM properties p
            JOIN distress_scores ds ON ds.property_id = p.id
            JOIN latest ON latest.property_id = ds.property_id
                       AND latest.max_date = ds.score_date
            WHERE p.zip = :zip_code
              AND p.county_id = :county_id
              AND ds.qualified = true
              AND ds.is_guess_lead = false
            ORDER BY p.id, ds.final_cds_score DESC NULLS LAST
            LIMIT 25
            """),
            {"zip_code": zip_code, "county_id": county_id, "days": days},
        ).fetchall()
    except (OperationalError, SQLAlchemyError) as exc:
        logger.error("[demo] zip-reveal DB error: %s", exc)
        raise HTTPException(status_code=503, detail="Database unavailable, try again")

    leads = []
    for r in rows:
        dt = r.distress_types or []
        if isinstance(dt, dict):
            dt = list(dt.keys())
        leads.append({
            "property_id":    r.property_id,
            "address_masked": _blur_address(r.address),
            "city":           r.city,
            "state":          r.state,
            "zip":            r.zip,
            "lead_tier":      r.lead_tier,
            "distress_types": dt,
            "cds_score":      float(r.final_cds_score or 0),
            "scored_at":      r.score_date.isoformat() if r.score_date else None,
        })

    # Sort by score descending after dedup (DISTINCT ON preserves insertion order)
    leads.sort(key=lambda x: x["cds_score"], reverse=True)

    return {"zip_code": zip_code, "county_id": county_id, "total": len(leads), "leads": leads}


# ---------------------------------------------------------------------------
# Endpoint B — Prepare Call (select + store featured lead)
# ---------------------------------------------------------------------------

class PrepareCallBody(BaseModel):
    feed_uuid: str
    zip_code: str
    vertical: str
    county_id: str = "hillsborough"


@router.post("/prepare-call")
def prepare_call(body: PrepareCallBody, db=Depends(get_db)):
    """Store the top Gold+ lead for this demo session (masked address)."""
    sub = _require_demo_sub(body.feed_uuid, db)

    if not ZIP_RE.match(body.zip_code):
        raise HTTPException(status_code=422, detail="Invalid ZIP code")
    if body.vertical not in VALID_VERTICALS:
        raise HTTPException(status_code=422, detail="Invalid vertical")

    try:
        # Idempotent: same sub + zip + vertical within 2 hours → return existing row.
        # Use limit(1) + first() instead of scalar_one_or_none() to avoid
        # MultipleResultsFound on a double-click race condition.
        cutoff = datetime.now(timezone.utc) - timedelta(hours=2)
        existing = db.execute(
            select(DemoSession)
            .where(
                DemoSession.subscriber_id == sub.id,
                DemoSession.zip_code == body.zip_code,
                DemoSession.vertical == body.vertical,
                DemoSession.created_at >= cutoff,
            )
            .order_by(DemoSession.created_at.desc())
            .limit(1)
        ).first()

        if existing:
            return _prep_response(existing[0])

        # Find top Gold+ lead with contact data for this ZIP+vertical, recent window
        county_fallback = False
        prop, score = _find_featured_lead(db, body.zip_code, body.vertical, body.county_id)

        if prop is None:
            # Fallback: county-wide top lead (flag so UI can warn the closer)
            prop, score = _find_featured_lead(db, None, body.vertical, body.county_id)
            county_fallback = prop is not None

        session_row = DemoSession(
            subscriber_id=sub.id,
            zip_code=body.zip_code,
            vertical=body.vertical,
            county_id=body.county_id,
            property_id=prop.id if prop else None,
            masked_address=_blur_address(prop.address) if prop else None,
            lead_tier=score.lead_tier if score else None,
            distress_types=score.distress_types if score else None,
        )
        db.add(session_row)
        db.flush()

        return _prep_response(session_row, county_fallback=county_fallback)

    except (OperationalError, SQLAlchemyError) as exc:
        logger.error("[demo] prepare-call DB error: %s", exc)
        raise HTTPException(status_code=503, detail="Database unavailable, try again")


def _find_featured_lead(db, zip_code: Optional[str], vertical: str, county_id: str):
    """
    Return (Property, DistressScore) for the top Gold+ lead with contact data.
    Searches within the past _FEATURED_LEAD_DAYS days.
    Uses phone_priority_order so leads without contact are deprioritised.
    """
    score_col = DistressScore.vertical_scores[vertical].as_float()

    cutoff = datetime.now(timezone.utc) - timedelta(days=_FEATURED_LEAD_DAYS)
    contact_clause = has_contact_filter(get_settings())

    filters = [
        DistressScore.lead_tier.in_(GOLD_PLUS_TIERS),
        DistressScore.qualified == True,  # noqa: E712
        DistressScore.is_guess_lead == False,  # noqa: E712
        DistressScore.score_date >= cutoff,
        Property.county_id == county_id,
    ]
    if zip_code:
        filters.append(Property.zip == zip_code)
    if contact_clause is not None:
        filters.append(contact_clause)

    row = db.execute(
        select(Property, DistressScore)
        .join(DistressScore, DistressScore.property_id == Property.id)
        .outerjoin(Owner, Owner.property_id == Property.id)
        .where(*filters)
        .order_by(*phone_priority_order(score_col))
        .limit(1)
    ).first()

    if not row:
        return None, None
    return row[0], row[1]


def _prep_response(s: DemoSession, county_fallback: bool = False) -> dict[str, Any]:
    dt = s.distress_types or []
    if isinstance(dt, dict):
        dt = list(dt.keys())
    return {
        "prep_id":        s.id,
        "masked_address": s.masked_address,
        "lead_tier":      s.lead_tier,
        "distress_types": dt,
        "zip_code":       s.zip_code,
        "county_fallback": county_fallback,
        "revealed":       s.revealed_at is not None,
        "has_lead":       s.property_id is not None,
    }


# ---------------------------------------------------------------------------
# Endpoint C — Reveal Lead
# ---------------------------------------------------------------------------

@router.post("/reveal-lead/{prep_id}")
def reveal_lead(prep_id: int, feed_uuid: str, db=Depends(get_db)):
    """Record reveal moment and return full address + PII."""
    sub = _require_demo_sub(feed_uuid, db)

    try:
        session_row = db.execute(
            select(DemoSession).where(
                DemoSession.id == prep_id,
                DemoSession.subscriber_id == sub.id,
            )
        ).scalar_one_or_none()

        if not session_row:
            raise HTTPException(status_code=404, detail="Demo session not found")

        # Idempotent — already revealed: return cached data
        if session_row.revealed_at is not None:
            return _reveal_response(session_row, db)

        session_row.revealed_at = datetime.now(timezone.utc)
        db.flush()

        return _reveal_response(session_row, db)

    except HTTPException:
        raise
    except (OperationalError, SQLAlchemyError) as exc:
        logger.error("[demo] reveal-lead DB error: %s", exc)
        raise HTTPException(status_code=503, detail="Database unavailable, try again")


def _reveal_response(s: DemoSession, db) -> dict[str, Any]:
    prop = db.get(Property, s.property_id) if s.property_id else None
    owner = None
    if prop:
        owner = db.execute(
            select(Owner).where(Owner.property_id == prop.id).limit(1)
        ).scalar_one_or_none()

    phone, _ = resolve_phone_with_quality(owner)

    dt = s.distress_types or []
    if isinstance(dt, dict):
        dt = list(dt.keys())

    has_contact = bool(owner and (phone or (owner.email_1 or owner.email_2)))

    return {
        "prep_id":        s.id,
        "address":        prop.address if prop else None,
        "city":           prop.city if prop else None,
        "state":          prop.state if prop else None,
        "zip":            prop.zip if prop else s.zip_code,
        "lead_tier":      s.lead_tier,
        "distress_types": dt,
        "owner_name":     owner.owner_name if owner else None,
        "phone":          phone,
        "email":          (owner.email_1 or owner.email_2) if owner else None,
        "has_contact":    has_contact,
        "revealed":       True,
        "revealed_at":    s.revealed_at.isoformat() if s.revealed_at else None,
    }
