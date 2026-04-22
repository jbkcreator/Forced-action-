"""
Monetization Wall — Item 25 (backend).

Tracks first-session state for new signups. Presents ROI frame + countdown
to drive first payment. Redis-backed with 24h TTL; degrades gracefully when
Redis is unavailable.

Session lifecycle:
  1. Frontend calls POST /api/wall/session at page load → create_session()
  2. Frontend polls GET /api/wall/{session_id} for countdown + converted flag
  3. On payment: mark_converted(session_id)
  4. Wall expires automatically after 24h (Redis TTL)
"""
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from src.core.redis_client import get_redis, redis_available

logger = logging.getLogger(__name__)

_WALL_TTL = 24 * 3600       # 24h session window
_COUNTDOWN_SEC = 15 * 60    # 15-min payment countdown

_ROI_FRAMES: dict = {
    "roofing": {
        "headline": "Roofers in Hillsborough close 12+ storm/distress jobs/mo",
        "avg_job_value": 8500,
        "avg_jobs_month": 12,
        "monthly_revenue": 102000,
    },
    "remediation": {
        "headline": "Remediation contractors average 8 distress calls/mo at ~$6,500 each",
        "avg_job_value": 6500,
        "avg_jobs_month": 8,
        "monthly_revenue": 52000,
    },
    "investor": {
        "headline": "Distressed-property investors average 2 deals/mo at $22K profit each",
        "avg_deal_value": 22000,
        "avg_deals_month": 2,
        "monthly_revenue": 44000,
    },
    "plumbing": {
        "headline": "Plumbers on distressed leads average 15 emergency jobs/mo",
        "avg_job_value": 3200,
        "avg_jobs_month": 15,
        "monthly_revenue": 48000,
    },
    "hvac": {
        "headline": "HVAC contractors find 10+ urgent replacements/mo via distress leads",
        "avg_job_value": 4800,
        "avg_jobs_month": 10,
        "monthly_revenue": 48000,
    },
}
_DEFAULT_ROI = {
    "headline": "Contractors using Forced Action data close 30–50% more distressed jobs",
    "avg_job_value": 5000,
    "avg_jobs_month": 10,
    "monthly_revenue": 50000,
}


def create_session(subscriber_id: int, session_id: str) -> dict:
    """
    Start tracking a monetization wall session.
    Returns the session state dict (also stored in Redis when available).
    """
    now = datetime.now(timezone.utc)
    state = {
        "subscriber_id": subscriber_id,
        "session_id": session_id,
        "created_at": now.isoformat(),
        "countdown_expires": (now + timedelta(seconds=_COUNTDOWN_SEC)).isoformat(),
        "converted": False,
    }
    if redis_available():
        get_redis().setex(f"mwall:{session_id}", _WALL_TTL, json.dumps(state))
    return state


def get_session_state(session_id: str) -> Optional[dict]:
    """Return current session state, or None if expired/missing."""
    if not redis_available():
        return None
    raw = get_redis().get(f"mwall:{session_id}")
    return json.loads(raw) if raw else None


def mark_converted(session_id: str) -> None:
    """Flag the session as converted (payment received)."""
    if not redis_available():
        return
    r = get_redis()
    key = f"mwall:{session_id}"
    raw = r.get(key)
    if not raw:
        return
    state = json.loads(raw)
    state["converted"] = True
    state["converted_at"] = datetime.now(timezone.utc).isoformat()
    r.setex(key, max(r.ttl(key), 300), json.dumps(state))


def is_active(session_id: str) -> bool:
    """True if this session still has an active wall."""
    return get_session_state(session_id) is not None


def get_roi_frame(vertical: str, county_id: str, db: Session) -> dict:
    """
    Return ROI framing data for the monetization wall.
    Augments static copy with a live qualified-lead count for credibility.
    """
    frame = _ROI_FRAMES.get(vertical, _DEFAULT_ROI).copy()

    try:
        from src.core.models import DistressScore, Property
        live_count = db.execute(
            select(func.count(Property.id))
            .join(DistressScore, DistressScore.property_id == Property.id)
            .where(
                Property.county_id == county_id,
                DistressScore.qualified == True,  # noqa: E712
            )
        ).scalar_one_or_none() or 0
        frame["live_lead_count"] = live_count
    except Exception as exc:
        logger.warning("ROI frame live count failed: %s", exc)
        frame["live_lead_count"] = None

    frame["vertical"] = vertical
    frame["county_id"] = county_id
    return frame
