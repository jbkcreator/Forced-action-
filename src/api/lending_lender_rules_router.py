"""R1 0.7: lender rule versions and the 1-click rollback, behind the admin token (X-Admin-Token).

GET  /api/lending/lender-rules/versions   newest first
POST /api/lending/lender-rules/rollback   restore an earlier version; recorded as a new version, posted to #cora
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy.orm import Session

from src.api.lending_loan_stage_router import require_admin_token
from src.lending import lender_rules_store as store
from src.lending.db import get_lending_db

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/lending/lender-rules", tags=["lending-lender-rules"],
                   dependencies=[Depends(require_admin_token)])


class VersionOut(BaseModel):
    version_no: int
    created_by: str
    reason: str
    created_at: datetime


class RollbackIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version: int = Field(ge=1)
    actor: str = Field(min_length=1, max_length=80)
    reason: Optional[str] = Field(default=None, max_length=200)


class RollbackOut(BaseModel):
    from_version: Optional[int]
    to_version: int
    new_version: int
    changed: bool


@router.get("/versions", response_model=list[VersionOut])
def versions(limit: int = Query(default=20, ge=1, le=200), db: Session = Depends(get_lending_db)) -> list[dict]:
    try:
        return store.list_snapshots(db, limit)
    except Exception:
        logger.exception("lender rule versions could not be read")
        raise HTTPException(status_code=500, detail="Could not read the versions") from None


@router.post("/rollback", response_model=RollbackOut)
def rollback(body: RollbackIn, db: Session = Depends(get_lending_db)) -> RollbackOut:
    try:
        result = store.rollback_to(db, body.version, actor=body.actor, reason=body.reason)
        db.commit()
    except store.SnapshotNotFound:
        db.rollback()
        raise HTTPException(status_code=404, detail="Version not found") from None
    except Exception:
        db.rollback()
        logger.exception("lender rule rollback to v%d failed", body.version)
        raise HTTPException(status_code=500, detail="Rollback failed") from None
    logger.warning("lender rules rolled back by %s: v%s -> rules of v%d (now v%d)", body.actor, result.from_version,
                   result.to_version, result.new_version)
    return RollbackOut(from_version=result.from_version, to_version=result.to_version,
                       new_version=result.new_version, changed=result.changed)
