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


def _safe_redis_op(op_name: str, fn):
    """
    Run a Redis operation. On any error (TimeoutError, ConnectionError, etc.)
    log a warning and return None. Prevents a dropped Redis connection from
    500'ing endpoints that call into this module.
    """
    if not redis_available():
        return None
    try:
        return fn(get_redis())
    except Exception as exc:
        logger.warning("monetization_wall: %s failed: %s", op_name, exc)
        # Invalidate the cached client so the next call re-tests connectivity.
        try:
            from src.core.redis_client import reset_client_cache
            reset_client_cache()
        except Exception:
            pass
        return None


def create_session(subscriber_id: int, session_id: str) -> dict:
    """
    Start tracking a monetization wall session.
    Returns the session state dict (also stored in Redis when available).
    Redis failures are logged but do not propagate — the dict is always
    returned so the caller (API endpoint) always gets a usable response.
    """
    now = datetime.now(timezone.utc)
    state = {
        "subscriber_id": subscriber_id,
        "session_id": session_id,
        "created_at": now.isoformat(),
        "countdown_expires": (now + timedelta(seconds=_COUNTDOWN_SEC)).isoformat(),
        "converted": False,
    }
    _safe_redis_op(
        "create_session.setex",
        lambda r: r.setex(f"mwall:{session_id}", _WALL_TTL, json.dumps(state)),
    )
    return state


def get_session_state(session_id: str) -> Optional[dict]:
    """Return current session state, or None if expired/missing/Redis-down."""
    raw = _safe_redis_op("get_session_state.get", lambda r: r.get(f"mwall:{session_id}"))
    if not raw:
        return None
    try:
        return json.loads(raw)
    except (TypeError, ValueError):
        return None


def mark_converted(session_id: str) -> None:
    """Flag the session as converted (payment received). No-op on Redis failure."""
    key = f"mwall:{session_id}"

    def _op(r):
        raw = r.get(key)
        if not raw:
            return
        state = json.loads(raw)
        state["converted"] = True
        state["converted_at"] = datetime.now(timezone.utc).isoformat()
        r.setex(key, max(r.ttl(key), 300), json.dumps(state))

    _safe_redis_op("mark_converted", _op)


def is_active(session_id: str) -> bool:
    """True if this session still has an active wall."""
    return get_session_state(session_id) is not None


def get_roi_frame(vertical: str, county_id: str, db: Session) -> dict:
    """
    Return ROI framing data for the monetization wall.
    Augments static copy with a live qualified-lead count for credibility.

    The live count is:
      - UNIQUE properties (a property re-scored on multiple runs counts once)
      - filtered to this vertical (only properties whose CDS engine output
        has a non-zero score for the requested vertical qualify as e.g.
        "qualified roofing leads")
      - county-scoped
    """
    frame = _ROI_FRAMES.get(vertical, _DEFAULT_ROI).copy()

    try:
        from src.core.models import DistressScore, Property

        # `vertical_scores` is a JSONB dict like {"roofing": 72, "investor": 15}.
        # We filter on `...[vertical] > 0` so properties with no signal for
        # this vertical are excluded — otherwise an investor-signal-only
        # property would be counted as a "qualified roofing lead".
        try:
            v_score = DistressScore.vertical_scores[vertical].as_float()
        except (KeyError, TypeError):
            v_score = None

        query = (
            select(func.count(func.distinct(Property.id)))
            .join(DistressScore, DistressScore.property_id == Property.id)
            .where(
                Property.county_id == county_id,
                DistressScore.qualified == True,  # noqa: E712
            )
        )
        if v_score is not None:
            query = query.where(v_score > 0)

        live_count = db.execute(query).scalar_one_or_none() or 0
        frame["live_lead_count"] = live_count
    except Exception as exc:
        logger.warning("ROI frame live count failed: %s", exc)
        frame["live_lead_count"] = None

    frame["vertical"] = vertical
    frame["county_id"] = county_id
    return frame
