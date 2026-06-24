"""Loss Autopsy admin endpoints (Phase 3 A1).

POST /api/admin/loss-autopsies/trigger  — manual trigger for a property/deal
GET  /api/admin/loss-autopsies          — list with filters
GET  /api/admin/loss-autopsies/{id}     — detail
"""
from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import text as sa_text
from sqlalchemy.orm import Session

from src.api.admin_router import get_current_admin
from src.api.deps import get_db
from src.services.loss_autopsy import run_loss_autopsy

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/admin", tags=["loss_autopsy"])

_VALID_TRIGGERS = {"CLOSED_LOST", "DECLINED", "GHOSTED_SLA"}


class TriggerRequest(BaseModel):
    property_id: int
    trigger_reason: str  # CLOSED_LOST | DECLINED | GHOSTED_SLA
    deal_outcome_id: Optional[int] = None


def _row_to_dict(r) -> dict:
    return {
        "id": str(r.id),
        "property_id": r.property_id,
        "prospect_id": str(r.prospect_id) if r.prospect_id else None,
        "deal_outcome_id": r.deal_outcome_id,
        "trigger_reason": r.trigger_reason,
        "primary_rejection_reason": r.primary_rejection_reason,
        "competitor_rate_delta": float(r.competitor_rate_delta) if r.competitor_rate_delta is not None else None,
        "underwriting_blocker": r.underwriting_blocker,
        "cora_behavior_adjustment": r.cora_behavior_adjustment,
        "claude_cost_usd": float(r.claude_cost_usd) if r.claude_cost_usd is not None else None,
        "created_at": r.created_at.isoformat() if r.created_at else None,
    }


@router.post("/loss-autopsies/trigger", status_code=201)
def trigger_loss_autopsy(
    req: TriggerRequest,
    db: Session = Depends(get_db),
    _admin: dict = Depends(get_current_admin),
):
    """Manually trigger a loss autopsy for any property. Useful for backfilling
    or re-running after a Claude outage."""
    if req.trigger_reason not in _VALID_TRIGGERS:
        raise HTTPException(status_code=422, detail=f"trigger_reason must be one of {sorted(_VALID_TRIGGERS)}")

    prop_exists = db.execute(
        sa_text("SELECT 1 FROM properties WHERE id = :pid"),
        {"pid": req.property_id},
    ).scalar_one_or_none()
    if not prop_exists:
        raise HTTPException(status_code=404, detail="property not found")

    autopsy = run_loss_autopsy(
        property_id=req.property_id,
        trigger_reason=req.trigger_reason,
        db=db,
        deal_outcome_id=req.deal_outcome_id,
    )
    if autopsy is None:
        raise HTTPException(
            status_code=409,
            detail="loss autopsy already exists for this deal_outcome_id",
        )

    return {
        "id": str(autopsy.id),
        "property_id": autopsy.property_id,
        "deal_outcome_id": autopsy.deal_outcome_id,
        "trigger_reason": autopsy.trigger_reason,
        "primary_rejection_reason": autopsy.primary_rejection_reason,
        "competitor_rate_delta": float(autopsy.competitor_rate_delta) if autopsy.competitor_rate_delta is not None else None,
        "underwriting_blocker": autopsy.underwriting_blocker,
        "cora_behavior_adjustment": autopsy.cora_behavior_adjustment,
        "claude_cost_usd": float(autopsy.claude_cost_usd) if autopsy.claude_cost_usd is not None else None,
    }


@router.get("/loss-autopsies")
def list_loss_autopsies(
    property_id: Optional[int] = Query(None),
    trigger_reason: Optional[str] = Query(None),
    from_: Optional[str] = Query(None, alias="from", description="ISO datetime; created_at >="),
    to: Optional[str] = Query(None, description="ISO datetime; created_at <"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
    _admin: dict = Depends(get_current_admin),
):
    """List loss autopsies with optional filters, newest first."""
    where: list[str] = []
    params: dict = {"limit": limit, "offset": offset}

    if property_id is not None:
        where.append("property_id = :pid")
        params["pid"] = property_id
    if trigger_reason:
        where.append("trigger_reason = :reason")
        params["reason"] = trigger_reason
    if from_:
        where.append("created_at >= :from_ts")
        params["from_ts"] = from_
    if to:
        where.append("created_at < :to_ts")
        params["to_ts"] = to

    clause = (" WHERE " + " AND ".join(where)) if where else ""
    total = db.execute(sa_text(f"SELECT count(*) FROM loss_autopsies{clause}"), params).scalar()
    rows = db.execute(
        sa_text(
            f"SELECT id, property_id, prospect_id, deal_outcome_id, trigger_reason, "
            f"primary_rejection_reason, competitor_rate_delta, underwriting_blocker, "
            f"cora_behavior_adjustment, claude_cost_usd, created_at "
            f"FROM loss_autopsies{clause} "
            f"ORDER BY created_at DESC LIMIT :limit OFFSET :offset"
        ),
        params,
    ).all()
    return {"total": total, "limit": limit, "offset": offset, "items": [_row_to_dict(r) for r in rows]}


@router.get("/loss-autopsies/{autopsy_id}")
def get_loss_autopsy(
    autopsy_id: str,
    db: Session = Depends(get_db),
    _admin: dict = Depends(get_current_admin),
):
    """Fetch a single loss autopsy including the raw context and full model response."""
    row = db.execute(
        sa_text(
            "SELECT id, property_id, prospect_id, deal_outcome_id, trigger_reason, "
            "primary_rejection_reason, competitor_rate_delta, underwriting_blocker, "
            "cora_behavior_adjustment, raw_context, model_response, claude_cost_usd, created_at "
            "FROM loss_autopsies WHERE id = :aid"
        ),
        {"aid": autopsy_id},
    ).mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="loss autopsy not found")

    return {
        "id": str(row["id"]),
        "property_id": row["property_id"],
        "prospect_id": str(row["prospect_id"]) if row["prospect_id"] else None,
        "deal_outcome_id": row["deal_outcome_id"],
        "trigger_reason": row["trigger_reason"],
        "primary_rejection_reason": row["primary_rejection_reason"],
        "competitor_rate_delta": float(row["competitor_rate_delta"]) if row["competitor_rate_delta"] is not None else None,
        "underwriting_blocker": row["underwriting_blocker"],
        "cora_behavior_adjustment": row["cora_behavior_adjustment"],
        "raw_context": row["raw_context"],
        "model_response": row["model_response"],
        "claude_cost_usd": float(row["claude_cost_usd"]) if row["claude_cost_usd"] is not None else None,
        "created_at": row["created_at"].isoformat() if row["created_at"] else None,
    }
