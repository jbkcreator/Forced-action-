"""FA Max borrower intelligence API.

Routes:
  GET  /api/fa-max/persons/{person_id}/profile — buy-box/velocity/next-need profile.
  POST /api/fa-max/gates                        — submit a caller booking gate (WP-GL-5).

Auth: admin JWT required on all routes. These endpoints expose internal
borrower intelligence and caller gate results; they must never be public.
"""
from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from src.api.admin_router import get_current_admin
from src.api.deps import get_db
from src.services.borrower_profile_service import get_person_profile

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/fa-max", tags=["fa-max"])


class GateSubmission(BaseModel):
    tracked_link_id: int
    liquidity_source: str = Field(description="cash | loc | partner | none")
    liquidity_amount: Optional[str] = Field(
        default=None,
        description="Caller-reported rough amount (brief p.1) — free text, not validated, never gates the booking on its own.",
    )
    completed_projects: str = Field(description="0 | 1_to_2 | 3_plus")
    exit_strategy: str = Field(description="sale | refinance | other")
    occupancy: str = Field(description="investment | homestead")
    decision_maker: str = Field(description="yes | no")
    property_address: str = Field(min_length=1, max_length=500)
    person_id: Optional[int] = None
    list_key: Optional[str] = None
    captured_by: Optional[str] = None


@router.post("/gates", status_code=201)
def submit_gate(
    payload: GateSubmission,
    db: Session = Depends(get_db),
    admin: dict = Depends(get_current_admin),
) -> dict:
    """Record a caller's booking gate evaluation for a tracked link.

    Returns the gate_id and whether it passed. If it failed, the contact is
    automatically enqueued in fa_max_nurture_queue and surfaced to EXCEPTIONS.

    Gate answers are stored as enum codes only — never free text financial
    data — per the _FINANCIAL_TERMS and relay-payload CHECK constraints.
    """
    from src.services.calendar.gate import GateAnswers, store_gate

    answers = GateAnswers(
        liquidity_source=payload.liquidity_source,
        liquidity_amount=payload.liquidity_amount,
        completed_projects=payload.completed_projects,
        exit_strategy=payload.exit_strategy,
        occupancy=payload.occupancy,
        decision_maker=payload.decision_maker,
        property_address=payload.property_address,
    )

    gate_id, result = store_gate(
        db,
        answers=answers,
        tracked_link_id=payload.tracked_link_id,
        person_id=payload.person_id,
        list_key=payload.list_key,
        captured_by=payload.captured_by or admin.get("sub"),
    )

    logger.info(
        "gate.submit: gate_id=%s link=%s result=%s caller=%s",
        gate_id, payload.tracked_link_id,
        "pass" if result.passed else "fail",
        payload.captured_by or admin.get("sub"),
    )
    return {
        "gate_id": gate_id,
        "passed": result.passed,
        "failed_field": result.failed_field,
        "reason": result.reason,
    }


@router.get("/persons/{person_id}/profile")
def get_borrower_profile(
    person_id: str,
    db: Session = Depends(get_db),
    _admin: dict = Depends(get_current_admin),
) -> dict:
    """Return the stored FA Max borrower profile for *person_id*.

    Requires an admin JWT. 404 if no profile has been computed yet.
    """
    profile = get_person_profile(db, person_id)
    if profile is None:
        raise HTTPException(status_code=404, detail="Profile not found for this person.")
    return profile
