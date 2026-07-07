"""M5 — CDS Score Feedback API (spec §4.4, §8B).

  POST /api/admin/score-feedback/outcome     — record realized outcome for a prospect
  GET  /api/admin/score-feedback             — list resolved feedback rows
  GET  /api/admin/score-feedback/inversion   — tier inversion acceptance gate
  GET  /api/admin/score-feedback/outcome-map — call_outcome → realized_outcome reference
"""
from __future__ import annotations

import logging
import uuid
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, field_validator
from sqlalchemy.orm import Session

from src.api.admin_router import get_current_admin
from src.api.deps import get_db
from src.services.score_feedback_service import (
    CALL_OUTCOME_MAP,
    _VALID_OUTCOMES,
    check_tier_inversion,
    get_feedback_rows,
    post_outcome,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/admin", tags=["score_feedback"])


class PostOutcomeRequest(BaseModel):
    prospect_id: str
    outcome: str
    # B0-02 Outcome Sanity Filter: optional buyer-capacity reason (low_fico /
    # no_capital). Free text; the service ignores any value outside the
    # canonical set, so no enum validation here.
    reason: Optional[str] = None
    closer_call_id: Optional[int] = None

    @field_validator("prospect_id")
    @classmethod
    def validate_prospect_id(cls, v: str) -> str:
        try:
            return str(uuid.UUID(v))
        except (ValueError, AttributeError):
            raise ValueError("prospect_id must be a valid UUID")

    @field_validator("outcome")
    @classmethod
    def validate_outcome(cls, v: str) -> str:
        if v not in _VALID_OUTCOMES:
            raise ValueError(f"outcome must be one of {sorted(_VALID_OUTCOMES)}")
        return v


@router.post("/score-feedback/outcome", status_code=200)
def post_outcome_endpoint(
    req: PostOutcomeRequest,
    db: Session = Depends(get_db),
    _admin: dict = Depends(get_current_admin),
):
    """Record the realized outcome of a homeowner call for CDS model feedback (spec §8B).

    Idempotent per prospect_id — re-posting with a new outcome updates in place.
    closer_call_id is optional; supply it when homeowner calling is wired up.
    """
    result = post_outcome(
        db,
        prospect_id=req.prospect_id,
        outcome=req.outcome,
        reason=req.reason,
        closer_call_id=req.closer_call_id,
    )
    if result is None:
        raise HTTPException(status_code=404, detail="prospect not found")
    db.commit()
    return result


@router.get("/score-feedback")
def list_score_feedback(
    predicted_tier: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    db: Session = Depends(get_db),
    _admin: dict = Depends(get_current_admin),
):
    """List resolved score_feedback rows, optionally filtered by predicted_tier."""
    rows = get_feedback_rows(db, predicted_tier=predicted_tier, limit=limit)
    return {"count": len(rows), "items": rows}


@router.get("/score-feedback/inversion")
def tier_inversion_check(
    db: Session = Depends(get_db),
    _admin: dict = Depends(get_current_admin),
):
    """Acceptance gate — returns inversion_fixed=true when Ultra outranks Bronze."""
    return check_tier_inversion(db)


@router.get("/score-feedback/outcome-map")
def outcome_map(_admin: dict = Depends(get_current_admin)):
    """Reference: maps closer_calls.call_outcome values to realized_outcome enum."""
    return CALL_OUTCOME_MAP
