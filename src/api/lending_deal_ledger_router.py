"""R1 0.8: a loan file's whole lifecycle from the audit ledger, behind the admin token (X-Admin-Token).

GET /api/lending/loan-files/{loan_file_id}/lifecycle   every event, oldest first
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.orm import Session

from src.api.lending_loan_stage_router import require_admin_token
from src.lending.audit import deal_ledger
from src.lending.db import get_lending_db

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/lending/loan-files", tags=["lending-deal-ledger"],
                   dependencies=[Depends(require_admin_token)])


class EventOut(BaseModel):
    id: int
    event_type: str
    actor: str
    rule_version: Optional[int]
    diff: dict[str, Any]
    detail: dict[str, Any]
    ref_table: Optional[str]
    ref_id: Optional[str]
    occurred_at: datetime


@router.get("/{loan_file_id}/lifecycle", response_model=list[EventOut])
def lifecycle(loan_file_id: int, db: Session = Depends(get_lending_db)) -> list[dict]:
    try:
        if db.execute(text("SELECT 1 FROM lending.loan_files WHERE id = :id"), {"id": loan_file_id}).first() is None:
            raise HTTPException(status_code=404, detail="Loan file not found")
        return deal_ledger.lifecycle(db, loan_file_id)
    except HTTPException:
        raise
    except Exception:
        logger.exception("lifecycle of loan file %d could not be read", loan_file_id)
        raise HTTPException(status_code=500, detail="Could not read the lifecycle") from None
