"""Closer Cockpit router (Sprint S1b).

  POST /api/admin/closer-calls                  — dial-time correlation (pending row)
  POST /api/admin/closer-calls/{id}/feedback    — one-tap closer feedback
  GET  /api/admin/closer-calls                  — list/filter
  GET  /api/admin/subscribers/{id}/closer-calls — per-subscriber call timeline
  GET  /api/admin/closer-calls/{id}/recording   — on-demand fresh Aircall playback URL
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Response
from pydantic import BaseModel
from sqlalchemy import select, text
from sqlalchemy.orm import Session

from config.closer import (
    LEAD_QUALITY_MAX,
    LEAD_QUALITY_MIN,
    OBJECTION_TAXONOMY_SET,
    PITCH_VARIANTS_SET,
)
from src.api.admin_router import get_current_admin
from src.api.deps import get_db
from src.core.models import CloserCall, Subscriber
from src.services import aircall_client
from src.services.phone_utils import normalize as normalize_phone

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/admin", tags=["closer_cockpit"])


def _row_to_dict(r) -> dict:
    """Serialize a closer_calls row (SQLAlchemy Row from text() select)."""
    return {
        "id": r.id,
        "aircall_call_id": r.aircall_call_id,
        "subscriber_id": r.subscriber_id,
        "escalation_id": r.escalation_id,
        "closer_aircall_user_id": r.closer_aircall_user_id,
        "closer_name": r.closer_name,
        "direction": r.direction,
        "dialed_e164": r.dialed_e164,
        "duration_sec": r.duration_sec,
        "started_at": r.started_at.isoformat() if r.started_at else None,
        "ended_at": r.ended_at.isoformat() if r.ended_at else None,
        "sentiment": r.sentiment,
        "topics": r.topics,
        "objections": r.objections,
        "objection_resolved": r.objection_resolved,
        "call_outcome": r.call_outcome,
        "follow_ups": r.follow_ups,
        "tagged_at": r.tagged_at.isoformat() if r.tagged_at else None,
        "objection_type": r.objection_type,
        "pitch_variant": r.pitch_variant,
        "lead_quality_rating": r.lead_quality_rating,
        "transcript_available": bool(r.transcript_text),
    }


_LIST_COLS = (
    "id, aircall_call_id, subscriber_id, escalation_id, closer_aircall_user_id, "
    "closer_name, direction, dialed_e164, duration_sec, started_at, ended_at, "
    "sentiment, topics, objections, objection_resolved, call_outcome, follow_ups, "
    "tagged_at, objection_type, pitch_variant, lead_quality_rating, "
    "(transcript_text IS NOT NULL) AS transcript_text"
)


class CorrelateCallRequest(BaseModel):
    aircall_call_id: str
    subscriber_id: int
    escalation_id: Optional[int] = None
    dialed_e164: Optional[str] = None


def _serialize(row: CloserCall) -> dict:
    return {
        "id": row.id,
        "aircall_call_id": row.aircall_call_id,
        "subscriber_id": row.subscriber_id,
        "escalation_id": row.escalation_id,
        "status": "tagged" if row.tagged_at else "pending",
        "created_at": row.created_at.isoformat() if row.created_at else None,
    }


@router.post("/closer-calls")
def correlate_call(
    req: CorrelateCallRequest,
    response: Response,
    db: Session = Depends(get_db),
    _admin: dict = Depends(get_current_admin),
):
    """Create (or return existing) pending closer_calls row at dial time.

    Idempotent: a re-fired dial event for the same aircall_call_id returns the
    existing row (200) instead of erroring.
    """
    if not req.aircall_call_id:
        raise HTTPException(status_code=422, detail="aircall_call_id required")

    if not db.get(Subscriber, req.subscriber_id):
        raise HTTPException(status_code=404, detail="subscriber not found")

    existing = db.execute(
        select(CloserCall).where(CloserCall.aircall_call_id == req.aircall_call_id)
    ).scalar_one_or_none()
    if existing is not None:
        response.status_code = 200
        return _serialize(existing)

    row = CloserCall(
        aircall_call_id=req.aircall_call_id,
        subscriber_id=req.subscriber_id,
        escalation_id=req.escalation_id,
        direction="outbound",
        dialed_e164=normalize_phone(req.dialed_e164) if req.dialed_e164 else None,
    )
    db.add(row)
    db.flush()
    logger.info(
        "[closer] correlated call aircall_call_id=%s sub=%s esc=%s",
        req.aircall_call_id, req.subscriber_id, req.escalation_id,
    )
    response.status_code = 201
    return _serialize(row)


class FeedbackRequest(BaseModel):
    objection_type: Optional[str] = None
    pitch_variant: Optional[str] = None
    lead_quality_rating: Optional[int] = None


@router.post("/closer-calls/{call_id}/feedback")
def submit_feedback(
    call_id: int,
    req: FeedbackRequest,
    db: Session = Depends(get_db),
    admin: dict = Depends(get_current_admin),
):
    """Record the closer's one-tap feedback for a call (per-call)."""
    row = db.get(CloserCall, call_id)
    if row is None:
        raise HTTPException(status_code=404, detail="closer call not found")

    if req.objection_type is not None and req.objection_type not in OBJECTION_TAXONOMY_SET:
        raise HTTPException(status_code=422, detail="invalid objection_type")
    if req.pitch_variant is not None and req.pitch_variant not in PITCH_VARIANTS_SET:
        raise HTTPException(status_code=422, detail="invalid pitch_variant")
    if req.lead_quality_rating is not None and not (
        LEAD_QUALITY_MIN <= req.lead_quality_rating <= LEAD_QUALITY_MAX
    ):
        raise HTTPException(status_code=422, detail="lead_quality_rating must be 1-5")

    row.objection_type = req.objection_type
    row.pitch_variant = req.pitch_variant
    row.lead_quality_rating = req.lead_quality_rating
    row.feedback_by = admin.get("sub") or "admin"
    row.feedback_at = datetime.now(timezone.utc)
    db.flush()
    return {
        "id": row.id,
        "objection_type": row.objection_type,
        "pitch_variant": row.pitch_variant,
        "lead_quality_rating": row.lead_quality_rating,
        "feedback_by": row.feedback_by,
        "feedback_at": row.feedback_at.isoformat(),
    }


@router.get("/closer-calls")
def list_closer_calls(
    closer: Optional[str] = Query(None, description="closer_aircall_user_id"),
    subscriber_id: Optional[int] = Query(None),
    from_: Optional[str] = Query(None, alias="from", description="ISO; started_at >="),
    to: Optional[str] = Query(None, description="ISO; started_at <"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
    _admin: dict = Depends(get_current_admin),
):
    """List closer calls, newest first, with optional filters."""
    where = []
    params: dict = {"limit": limit, "offset": offset}
    if closer:
        where.append("closer_aircall_user_id = :closer")
        params["closer"] = closer
    if subscriber_id is not None:
        where.append("subscriber_id = :sid")
        params["sid"] = subscriber_id
    if from_:
        where.append("started_at >= :from_ts")
        params["from_ts"] = from_
    if to:
        where.append("started_at < :to_ts")
        params["to_ts"] = to
    clause = (" WHERE " + " AND ".join(where)) if where else ""

    total = db.execute(
        text(f"SELECT count(*) FROM closer_calls{clause}"), params
    ).scalar()
    rows = db.execute(
        text(
            f"SELECT {_LIST_COLS} FROM closer_calls{clause} "
            "ORDER BY started_at DESC NULLS LAST, id DESC LIMIT :limit OFFSET :offset"
        ),
        params,
    ).all()
    return {"total": total, "limit": limit, "offset": offset,
            "items": [_row_to_dict(r) for r in rows]}


@router.get("/subscribers/{subscriber_id}/closer-calls")
def subscriber_closer_calls(
    subscriber_id: int,
    limit: int = Query(100, ge=1, le=500),
    db: Session = Depends(get_db),
    _admin: dict = Depends(get_current_admin),
):
    """All closer calls for one subscriber, newest first (the call timeline)."""
    rows = db.execute(
        text(
            f"SELECT {_LIST_COLS} FROM closer_calls WHERE subscriber_id = :sid "
            "ORDER BY started_at DESC NULLS LAST, id DESC LIMIT :limit"
        ),
        {"sid": subscriber_id, "limit": limit},
    ).all()
    return {"subscriber_id": subscriber_id, "count": len(rows),
            "items": [_row_to_dict(r) for r in rows]}


@router.get("/closer-calls/{call_id}/recording")
def closer_call_recording(
    call_id: int,
    db: Session = Depends(get_db),
    _admin: dict = Depends(get_current_admin),
):
    """Fetch a fresh (~10-min) Aircall recording URL for on-demand playback."""
    row = db.get(CloserCall, call_id)
    if row is None:
        raise HTTPException(status_code=404, detail="closer call not found")
    url, ttl = aircall_client.fresh_recording_url(row.aircall_call_id)
    if not url:
        raise HTTPException(status_code=404, detail="recording not available")
    return {"recording_url": url, "expires_in_sec": ttl}
