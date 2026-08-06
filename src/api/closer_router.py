"""Closer Cockpit router (Sprint S1b).

  POST /api/admin/closer-calls                    — dial-time correlation (pending row)
  POST /api/admin/closer-calls/{id}/feedback      — one-tap closer feedback
  GET  /api/admin/closer-calls                    — list/filter
  GET  /api/admin/subscribers/{id}/closer-calls   — per-subscriber call timeline
  GET  /api/admin/buyer-entities/{id}/closer-calls — per-whale call timeline (item 49)
  GET  /api/admin/closer-calls/whale-queue        — Hunter's ranked whale queue, dial-ready (item 49)
  GET  /api/admin/closer-calls/{id}/recording     — on-demand fresh Aircall playback URL
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
    CORRECTION_REASONS_SET,
    CORRECTION_REASON_WRONG_DISTRESS,
    DAMPENING_REASONS_SET,
    LEAD_QUALITY_MAX,
    LEAD_QUALITY_MIN,
    OBJECTION_TAXONOMY_SET,
    PITCH_VARIANTS_SET,
    TEACHABLE_SIGNAL_TYPES_SET,
)
from src.api.admin_router import get_current_admin
from src.api.deps import get_db
from src.core.models import BuyerEntity, CloserCall, LifecycleTrainingOverride, Subscriber
from src.services import aircall_client
from src.services.cds_engine import MultiVerticalScorer as CDSEngine
from src.services.phone_utils import normalize_closer as normalize_phone

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/admin", tags=["closer_cockpit"])


def _row_to_dict(r) -> dict:
    """Serialize a closer_calls row (SQLAlchemy Row from text() select)."""
    return {
        "id": r.id,
        "aircall_call_id": r.aircall_call_id,
        "subscriber_id": r.subscriber_id,
        "buyer_entity_id": r.buyer_entity_id,
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
    "id, aircall_call_id, subscriber_id, buyer_entity_id, escalation_id, closer_aircall_user_id, "
    "closer_name, direction, dialed_e164, duration_sec, started_at, ended_at, "
    "sentiment, topics, objections, objection_resolved, call_outcome, follow_ups, "
    "tagged_at, objection_type, pitch_variant, lead_quality_rating, "
    "(transcript_text IS NOT NULL) AS transcript_text"
)


class CorrelateCallRequest(BaseModel):
    aircall_call_id: str
    subscriber_id: Optional[int] = None
    buyer_entity_id: Optional[int] = None
    escalation_id: Optional[int] = None
    dialed_e164: Optional[str] = None


def _serialize(row: CloserCall) -> dict:
    return {
        "id": row.id,
        "aircall_call_id": row.aircall_call_id,
        "subscriber_id": row.subscriber_id,
        "buyer_entity_id": row.buyer_entity_id,
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

    Exactly one of subscriber_id/buyer_entity_id must be set — a call is
    either with an existing customer or a cold whale prospect sourced from
    Hunter's ranked queue (item 49), matching ck_closer_calls_one_identity.
    Idempotent: a re-fired dial event for the same aircall_call_id returns the
    existing row (200) instead of erroring.
    """
    if not req.aircall_call_id:
        raise HTTPException(status_code=422, detail="aircall_call_id required")

    if bool(req.subscriber_id) == bool(req.buyer_entity_id):
        raise HTTPException(
            status_code=422,
            detail="exactly one of subscriber_id or buyer_entity_id is required",
        )

    if req.subscriber_id is not None and not db.get(Subscriber, req.subscriber_id):
        raise HTTPException(status_code=404, detail="subscriber not found")
    if req.buyer_entity_id is not None and not db.get(BuyerEntity, req.buyer_entity_id):
        raise HTTPException(status_code=404, detail="buyer entity not found")

    existing = db.execute(
        select(CloserCall).where(CloserCall.aircall_call_id == req.aircall_call_id)
    ).scalar_one_or_none()
    if existing is not None:
        response.status_code = 200
        return _serialize(existing)

    row = CloserCall(
        aircall_call_id=req.aircall_call_id,
        subscriber_id=req.subscriber_id,
        buyer_entity_id=req.buyer_entity_id,
        escalation_id=req.escalation_id,
        direction="outbound",
        dialed_e164=normalize_phone(req.dialed_e164) if req.dialed_e164 else None,
    )
    db.add(row)
    db.flush()
    logger.info(
        "[closer] correlated call aircall_call_id=%s sub=%s buyer_entity=%s esc=%s",
        req.aircall_call_id, req.subscriber_id, req.buyer_entity_id, req.escalation_id,
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
    buyer_entity_id: Optional[int] = Query(None),
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
    if buyer_entity_id is not None:
        where.append("buyer_entity_id = :beid")
        params["beid"] = buyer_entity_id
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


@router.get("/buyer-entities/{buyer_entity_id}/closer-calls")
def buyer_entity_closer_calls(
    buyer_entity_id: int,
    limit: int = Query(100, ge=1, le=500),
    db: Session = Depends(get_db),
    _admin: dict = Depends(get_current_admin),
):
    """All closer calls for one whale/buyer entity, newest first (item 49)."""
    rows = db.execute(
        text(
            f"SELECT {_LIST_COLS} FROM closer_calls WHERE buyer_entity_id = :beid "
            "ORDER BY started_at DESC NULLS LAST, id DESC LIMIT :limit"
        ),
        {"beid": buyer_entity_id, "limit": limit},
    ).all()
    return {"buyer_entity_id": buyer_entity_id, "count": len(rows),
            "items": [_row_to_dict(r) for r in rows]}


@router.get("/closer-calls/whale-queue")
def whale_call_queue(
    limit: int = Query(25, ge=1, le=100),
    county_id: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    _admin: dict = Depends(get_current_admin),
):
    """Hunter's ranked whale queue, dial-ready for closers (item 49).

    Read-only passthrough to get_ranked_whales() — a closer works this list
    top to bottom, then POSTs /closer-calls with buyer_entity_id=entity_id to
    log the call against the same identity.
    """
    from src.services.whale_ranking import get_ranked_whales

    whales = get_ranked_whales(db, limit=limit, county_id=county_id)
    return {"count": len(whales), "items": whales}


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


# ── A6: Closer-to-Lifecycle Teaching Interface ────────────────────────────────────

class TeachRequest(BaseModel):
    subject_id: int
    correction_reason: str
    signal_type: Optional[str] = None
    note: Optional[str] = None
    closer_call_id: Optional[int] = None


def _serialize_correction(row: LifecycleTrainingOverride) -> dict:
    return {
        "id": row.id,
        "subject_id": row.subject_id,
        "correction_reason": row.correction_reason,
        "signal_type": row.signal_type,
        "dampener_active": row.dampener_active,
        "queue_status": row.queue_status,
        "created_at": row.created_at.isoformat() if row.created_at else None,
    }


@router.post("/closer/teach", status_code=201)
def create_teaching_correction(
    req: TeachRequest,
    response: Response,
    db: Session = Depends(get_db),
    admin: dict = Depends(get_current_admin),
):
    """Record a Teaching Correction for a mis-scored lead.

    Immediately applies a Score Dampener (for dampening reasons) by synchronously
    rescoring the property.  Retains the row as a fine-tuning label for future
    Lifecycle model training.  Idempotent: a duplicate active correction returns 409
    with the existing row.
    """
    if req.correction_reason not in CORRECTION_REASONS_SET:
        raise HTTPException(
            status_code=422,
            detail=f"invalid correction_reason; valid: {sorted(CORRECTION_REASONS_SET)}",
        )

    if req.correction_reason == CORRECTION_REASON_WRONG_DISTRESS:
        if not req.signal_type:
            raise HTTPException(
                status_code=422,
                detail="signal_type required for wrong_distress corrections",
            )
        if req.signal_type not in TEACHABLE_SIGNAL_TYPES_SET:
            raise HTTPException(
                status_code=422,
                detail=f"invalid signal_type; valid: {sorted(TEACHABLE_SIGNAL_TYPES_SET)}",
            )
    elif req.signal_type is not None:
        raise HTTPException(
            status_code=422,
            detail="signal_type is only valid for wrong_distress corrections",
        )

    # Verify property exists.
    prop_row = db.execute(
        text("SELECT 1 FROM properties WHERE id = :pid"),
        {"pid": req.subject_id},
    ).first()
    if prop_row is None:
        raise HTTPException(status_code=404, detail="property not found")

    dampener_active = req.correction_reason in DAMPENING_REASONS_SET

    row = LifecycleTrainingOverride(
        source="closer_teach",
        subject_type="property",
        subject_ref=str(req.subject_id),
        closer_call_id=req.closer_call_id,
        correction_reason=req.correction_reason,
        signal_type=req.signal_type,
        note=req.note,
        dampener_active=dampener_active,
        queue_status="pending",
        created_by=admin.get("sub") or admin.get("email") or "admin",
    )
    db.add(row)

    try:
        db.flush()
    except Exception as exc:
        from sqlalchemy.exc import IntegrityError as _IE
        if isinstance(exc, _IE):
            db.rollback()
            # Return the existing active correction.
            existing = db.execute(
                text(
                    "SELECT * FROM lifecycle_training_overrides "
                    "WHERE subject_ref = :sid AND correction_reason = :reason "
                    "  AND COALESCE(signal_type, '') = COALESCE(:sig, '') "
                    "  AND dampener_active "
                    "LIMIT 1"
                ),
                {
                    "sid": str(req.subject_id),
                    "reason": req.correction_reason,
                    "sig": req.signal_type,
                },
            ).first()
            response.status_code = 409
            if existing:
                return {"id": existing.id, "detail": "correction already active"}
            return {"detail": "correction already active"}
        raise

    db.commit()

    logger.info(
        "[teach] correction created id=%s property=%s reason=%s signal=%s by=%s",
        row.id, req.subject_id, req.correction_reason, req.signal_type,
        row.created_by,
    )

    if dampener_active:
        try:
            engine = CDSEngine(db)
            engine.score_properties_by_ids([req.subject_id], save_to_db=True)
            logger.info("[teach] rescore triggered for property=%s", req.subject_id)
        except Exception:
            logger.error(
                "[teach] rescore failed for property=%s after correction=%s",
                req.subject_id, row.id, exc_info=True,
            )

    return _serialize_correction(row)


@router.delete("/closer/teach/{correction_id}", status_code=200)
def delete_teaching_correction(
    correction_id: int,
    db: Session = Depends(get_db),
    admin: dict = Depends(get_current_admin),
):
    """Undo a Teaching Correction.

    Sets dampener_active=False and queue_status='discarded', then rescores
    the property so the dampener is lifted immediately.
    """
    row = db.get(LifecycleTrainingOverride, correction_id)
    if row is None:
        raise HTTPException(status_code=404, detail="correction not found")

    was_dampening = row.dampener_active
    row.dampener_active = False
    row.queue_status = "discarded"
    db.flush()
    db.commit()

    logger.info(
        "[teach] correction deleted id=%s property=%s by=%s",
        correction_id, row.subject_id, admin.get("sub") or admin.get("email"),
    )

    if was_dampening:
        try:
            engine = CDSEngine(db)
            engine.score_properties_by_ids([row.subject_id], save_to_db=True)
            logger.info("[teach] rescore triggered for property=%s (undo)", row.subject_id)
        except Exception:
            logger.error(
                "[teach] rescore failed for property=%s after undo correction=%s",
                row.subject_id, correction_id, exc_info=True,
            )

    return _serialize_correction(row)


@router.get("/subscribers/{subscriber_id}/delivered-leads")
def subscriber_delivered_leads(
    subscriber_id: int,
    limit: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
    _admin: dict = Depends(get_current_admin),
):
    """Leads delivered to a subscriber, newest first, with current score and
    the signals present (for the Teach panel's wrong_distress picker) plus any
    active Teaching Corrections already applied (for the undo affordance).
    """
    lead_rows = db.execute(
        text(
            """
            SELECT
                p.id            AS property_id,
                p.address       AS address,
                p.city          AS city,
                ds.final_cds_score AS cds_score,
                ds.lead_tier    AS lead_tier,
                ds.distress_types  AS signals,
                sl.sent_at      AS sent_at
            FROM sent_leads sl
            JOIN properties p ON p.id = sl.property_id
            LEFT JOIN LATERAL (
                SELECT final_cds_score, lead_tier, distress_types
                FROM distress_scores
                WHERE property_id = p.id
                ORDER BY score_date DESC
                LIMIT 1
            ) ds ON TRUE
            WHERE sl.subscriber_id = :sid
            ORDER BY sl.sent_at DESC
            LIMIT :limit
            """
        ),
        {"sid": subscriber_id, "limit": limit},
    ).fetchall()

    property_ids = [r.property_id for r in lead_rows]

    corrections_by_pid: dict[int, list] = {}
    if property_ids:
        corr_rows = db.execute(
            text(
                """
                SELECT id, subject_ref AS subject_id, correction_reason, signal_type
                FROM lifecycle_training_overrides
                WHERE subject_type = 'property'
                  AND subject_ref = ANY(:pids)
                  AND dampener_active
                ORDER BY created_at
                """
            ),
            {"pids": [str(pid) for pid in property_ids]},
        ).fetchall()
        for c in corr_rows:
            corrections_by_pid.setdefault(c.subject_id, []).append({
                "id": c.id,
                "correction_reason": c.correction_reason,
                "signal_type": c.signal_type,
            })

    items = []
    for r in lead_rows:
        items.append({
            "property_id": r.property_id,
            "address": r.address,
            "city": r.city,
            "cds_score": float(r.cds_score) if r.cds_score is not None else None,
            "lead_tier": r.lead_tier,
            "signals": list(r.signals) if r.signals else [],
            "sent_at": r.sent_at.isoformat() if r.sent_at else None,
            "active_corrections": corrections_by_pid.get(r.property_id, []),
        })

    return {"subscriber_id": subscriber_id, "count": len(items), "items": items}
