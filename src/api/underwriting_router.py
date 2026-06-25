"""Sprint 4.6 — Underwriting Reason-Code Feedback API.

  POST /api/loans/underwriting-feedback
      Receive a broker decline reason for a property.
      Writes the audit record, nudges scoring_weight_overrides (A3),
      and immediately rescores the property with updated CDS weights.

  GET  /api/loans/underwriting-feedback/{parcel_id}
      Return the full decline history for a property.
"""
from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, field_validator
from sqlalchemy import text as sa_text
from sqlalchemy.orm import Session

from src.api.admin_router import get_current_admin
from src.api.deps import get_db
from src.services.underwriting_feedback_service import (
    VALID_REASON_CODES,
    apply_signal_nudges,
    record_feedback,
    rescore_property,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/loans", tags=["underwriting"])


class UnderwritingFeedbackRequest(BaseModel):
    parcel_id: str
    reason_code: str
    reason_detail: Optional[str] = None
    lender_id: Optional[str] = None
    loan_amount: Optional[float] = None

    @field_validator("reason_code")
    @classmethod
    def validate_reason_code(cls, v: str) -> str:
        if v not in VALID_REASON_CODES:
            raise ValueError(
                f"reason_code '{v}' is not valid. "
                f"Accepted values: {sorted(VALID_REASON_CODES)}"
            )
        return v


@router.post("/underwriting-feedback", status_code=200)
def submit_underwriting_feedback(
    req: UnderwritingFeedbackRequest,
    db: Session = Depends(get_db),
    admin: dict = Depends(get_current_admin),
):
    """Record a broker underwriting decline and retrain CDS distress weights.

    Steps:
      1. Resolve parcel_id → property_id (404 if not found).
      2. Insert into underwriting_feedback (per-property audit log).
      3. Upsert signal nudges into scoring_weight_overrides (A3 global table).
      4. Invalidate the heuristic cache so the next CDS scorer reads fresh weights.
      5. Immediately rescore this property and persist the updated DistressScore.
    """
    prop_row = db.execute(
        sa_text("SELECT id FROM properties WHERE parcel_id = :pid"),
        {"pid": req.parcel_id},
    ).first()
    if not prop_row:
        raise HTTPException(status_code=404, detail="Property not found")

    property_id: int = prop_row.id

    try:
        record_feedback(
            property_id=property_id,
            reason_code=req.reason_code,
            submitted_by=admin["sub"],
            db=db,
            reason_detail=req.reason_detail,
            lender_id=req.lender_id,
            loan_amount=req.loan_amount,
        )
        db.flush()

        nudges_applied = apply_signal_nudges(req.reason_code, db)
        db.flush()

        score_data = rescore_property(property_id, req.parcel_id, db)
    except Exception as exc:
        logger.error(
            "[underwriting] feedback processing failed for parcel=%s reason=%s: %s",
            req.parcel_id, req.reason_code, exc, exc_info=True,
        )
        raise HTTPException(status_code=500, detail="Failed to process underwriting feedback")

    return {
        "status":          "ok",
        "parcel_id":       req.parcel_id,
        "reason_code":     req.reason_code,
        "nudges_applied":  len(nudges_applied),
        "new_cds_score":   score_data["final_cds_score"],
        "new_lead_tier":   score_data["lead_tier"],
        "vertical_scores": score_data["vertical_scores"],
    }


@router.get("/underwriting-feedback/{parcel_id}", status_code=200)
def get_underwriting_feedback(
    parcel_id: str,
    db: Session = Depends(get_db),
    _admin: dict = Depends(get_current_admin),
):
    """Return the full underwriting decline history for a property."""
    prop_row = db.execute(
        sa_text("SELECT id FROM properties WHERE parcel_id = :pid"),
        {"pid": parcel_id},
    ).first()
    if not prop_row:
        raise HTTPException(status_code=404, detail="Property not found")

    rows = db.execute(
        sa_text("""
            SELECT id, reason_code, reason_detail, lender_id, loan_amount,
                   submitted_by, submitted_at
            FROM underwriting_feedback
            WHERE property_id = :pid
            ORDER BY submitted_at DESC
        """),
        {"pid": prop_row.id},
    ).mappings().all()

    return {
        "parcel_id": parcel_id,
        "total":     len(rows),
        "feedback": [
            {
                "id":            r["id"],
                "reason_code":   r["reason_code"],
                "reason_detail": r["reason_detail"],
                "lender_id":     r["lender_id"],
                "loan_amount":   float(r["loan_amount"]) if r["loan_amount"] else None,
                "submitted_by":  r["submitted_by"],
                "submitted_at":  r["submitted_at"].isoformat() if r["submitted_at"] else None,
            }
            for r in rows
        ],
    }
