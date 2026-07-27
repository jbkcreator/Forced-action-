from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session

from src.api.admin_router import get_current_admin
from src.api.deps import get_db
from src.core.models import LifecycleTrainingOverride
from src.services.feedback_ritual import (
    apply_feedback_ritual_review,
    serialize_feedback_ritual,
)

router = APIRouter(prefix="/api/admin", tags=["feedback-ritual"])


class FeedbackRitualReviewRequest(BaseModel):
    review_outcome: str
    correction_reason: Optional[str] = None
    corrected_output: Optional[str] = None
    note: Optional[str] = None


@router.get("/feedback-ritual")
def list_feedback_ritual(
    queue_status: str = Query("pending"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
    _: dict = Depends(get_current_admin),
):
    rows = (
        db.query(LifecycleTrainingOverride)
        .filter(
            LifecycleTrainingOverride.source == "feedback_ritual",
            LifecycleTrainingOverride.queue_status == queue_status,
        )
        .order_by(LifecycleTrainingOverride.created_at.desc())
        .limit(limit)
        .offset(offset)
        .all()
    )
    items = [serialize_feedback_ritual(row) for row in rows]
    return {"count": len(items), "items": items}


@router.post("/feedback-ritual/{queue_id}/review")
def review_feedback_ritual(
    queue_id: int,
    req: FeedbackRitualReviewRequest,
    db: Session = Depends(get_db),
    admin: dict = Depends(get_current_admin),
):
    row = db.get(LifecycleTrainingOverride, queue_id)
    if row is None or row.source != "feedback_ritual":
        raise HTTPException(status_code=404, detail="feedback ritual row not found")

    try:
        updated = apply_feedback_ritual_review(
            db,
            row,
            review_outcome=req.review_outcome,
            correction_reason=req.correction_reason,
            corrected_output=req.corrected_output,
            note=req.note,
            reviewer=admin.get("sub") or admin.get("email") or "admin",
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))

    return serialize_feedback_ritual(updated)


@router.get("/feedback-ritual/{queue_id}")
def get_feedback_ritual(
    queue_id: int,
    db: Session = Depends(get_db),
    _: dict = Depends(get_current_admin),
):
    row = db.get(LifecycleTrainingOverride, queue_id)
    if row is None or row.source != "feedback_ritual":
        raise HTTPException(status_code=404, detail="feedback ritual row not found")
    return serialize_feedback_ritual(row)
