"""R1 0.1: move a loan file one stage forward.

POST /api/lending/loan-files/{loan_file_id}/stage on lending-api, behind the admin token (X-Admin-Token).

An out-of-order jump (INTAKE -> FUNDED, a backwards move, any move on a closed file) is refused with HTTP 400; the
refused event is kept and a card is queued for Josh (``state_machine.transition``).  A replayed ``source_event_id``
or a move to the stage the file is already at changes nothing and answers 200 with the outcome.
"""
from __future__ import annotations

import hashlib
import hmac
import logging
from typing import Optional

from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy.orm import Session

from config.settings import get_settings
from src.lending.contracts import LoanStage
from src.lending.db import get_lending_db
from src.lending.state_machine import IllegalStageTransitionError, LoanFileNotFound, transition

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/lending/loan-files", tags=["lending-loan-files"])

ADMIN_TOKEN_HEADER = "X-Admin-Token"


class StageIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    to_stage: LoanStage
    actor: str = Field(min_length=1, max_length=80)
    source_event_id: Optional[str] = Field(default=None, max_length=120)
    reason: Optional[str] = Field(default=None, max_length=200)


class StageOut(BaseModel):
    loan_file_id: int
    from_stage: LoanStage
    to_stage: LoanStage
    outcome: str


def require_admin_token(supplied: Optional[str] = Header(default=None, alias=ADMIN_TOKEN_HEADER)) -> None:
    expected = get_settings().lending_admin_token
    secret = expected.get_secret_value() if expected is not None else ""
    if not secret or not supplied or not hmac.compare_digest(
            hashlib.sha256(supplied.encode()).digest(), hashlib.sha256(secret.encode()).digest()):
        raise HTTPException(status_code=401, detail="Invalid admin token")


@router.post("/{loan_file_id}/stage", response_model=StageOut, dependencies=[Depends(require_admin_token)])
def set_stage(loan_file_id: int, body: StageIn, db: Session = Depends(get_lending_db)) -> StageOut:
    try:
        result = transition(db, loan_file_id, body.to_stage, source="api", actor=body.actor,
                            source_event_id=body.source_event_id, reason=body.reason)
        db.commit()
    except LoanFileNotFound:
        db.rollback()
        raise HTTPException(status_code=404, detail="Loan file not found") from None
    except IllegalStageTransitionError as exc:
        db.commit()  # keep the rejected event and the card queued for Josh
        logger.warning("loan file %d: stage change %s -> %s refused (%s)", loan_file_id, exc.current.value,
                       exc.target.value, exc.reason)
        raise HTTPException(
            status_code=400,
            detail=f"Stage change not allowed: {exc.current.value} to {exc.target.value} ({exc.reason}).") from None
    except Exception:
        db.rollback()
        logger.exception("loan file %d: stage change failed", loan_file_id)
        raise HTTPException(status_code=500, detail="Stage change failed") from None
    return StageOut(loan_file_id=result.loan_file_id, from_stage=result.from_stage, to_stage=result.to_stage,
                    outcome=result.outcome)
