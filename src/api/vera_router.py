"""
Vera Promise Management API.

JWT-authenticated admin endpoints for managing vera_promises.
All endpoints require Depends(get_current_admin).

Routes:
    GET    /api/admin/vera/promises            — list promises (filterable by status)
    POST   /api/admin/vera/promises            — create a promise
    PATCH  /api/admin/vera/promises/{id}       — mark fulfilled (closed) or cancelled
    DELETE /api/admin/vera/promises/{id}       — hard delete
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from src.api.admin_router import get_current_admin
from src.api.deps import get_db
from src.agents.vera.promises import record_promise, close_promise

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/admin/vera", tags=["vera"])

_VALID_STATUS_FILTERS = {"open", "closed", "cancelled", "all"}
_VALID_PATCH_STATUSES = {"closed", "cancelled"}


# ── Schemas ──────────────────────────────────────────────────────────────────

class PromiseCreateRequest(BaseModel):
    description: str = Field(..., min_length=1, max_length=2000)
    owner: str = Field(..., min_length=1, max_length=120)
    due_at: Optional[datetime] = None
    mrr_at_risk_cents: Optional[int] = Field(default=None, ge=0)
    thread_id: Optional[str] = Field(default=None, max_length=64)


class PromisePatchRequest(BaseModel):
    status: str = Field(..., description="'closed' to mark fulfilled, 'cancelled' to discard")


class PromiseOut(BaseModel):
    id: int
    description: str
    owner: str
    source: str
    status: str
    due_at: Optional[datetime]
    observed_at: datetime
    closed_at: Optional[datetime]
    mrr_at_risk_cents: Optional[int]
    thread_id: Optional[str]

    class Config:
        from_attributes = True


# ── Endpoints ────────────────────────────────────────────────────────────────

@router.get("/promises")
def list_promises(
    status: str = Query(default="all"),
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    db: Session = Depends(get_db),
    _admin: dict = Depends(get_current_admin),
):
    if status not in _VALID_STATUS_FILTERS:
        raise HTTPException(status_code=400, detail=f"status must be one of {sorted(_VALID_STATUS_FILTERS)}")

    where = "" if status == "all" else "WHERE status = :status"
    params: dict = {"limit": limit, "offset": offset}
    if status != "all":
        params["status"] = status

    from sqlalchemy import text
    rows = db.execute(
        text(
            f"SELECT id, description, owner, source, status, due_at, observed_at, "
            f"closed_at, mrr_at_risk_cents, thread_id "
            f"FROM vera_promises {where} "
            f"ORDER BY observed_at DESC LIMIT :limit OFFSET :offset"
        ),
        params,
    ).mappings().all()

    count_row = db.execute(
        text(f"SELECT count(*) FROM vera_promises {where}"),
        {k: v for k, v in params.items() if k not in ("limit", "offset")},
    ).scalar()

    return {"promises": [dict(r) for r in rows], "total": count_row}


@router.post("/promises", status_code=201)
def create_promise(
    body: PromiseCreateRequest,
    _admin: dict = Depends(get_current_admin),
):
    promise = record_promise(
        description=body.description,
        owner=body.owner,
        source="admin_ui",
        thread_id=body.thread_id,
        mrr_at_risk_cents=body.mrr_at_risk_cents,
        due_at=body.due_at,
    )
    return {
        "id": promise.id,
        "description": promise.description,
        "owner": promise.owner,
        "source": promise.source,
        "status": promise.status,
        "due_at": promise.due_at,
        "observed_at": promise.observed_at,
        "closed_at": promise.closed_at,
        "mrr_at_risk_cents": promise.mrr_at_risk_cents,
        "thread_id": promise.thread_id,
    }


@router.patch("/promises/{promise_id}")
def patch_promise(
    promise_id: int,
    body: PromisePatchRequest,
    _admin: dict = Depends(get_current_admin),
):
    if body.status not in _VALID_PATCH_STATUSES:
        raise HTTPException(status_code=400, detail=f"status must be one of {sorted(_VALID_PATCH_STATUSES)}")

    updated = close_promise(promise_id, status=body.status)
    if not updated:
        raise HTTPException(status_code=404, detail="Promise not found or already closed")
    return {"id": promise_id, "status": body.status}


@router.delete("/promises/{promise_id}", status_code=204)
def delete_promise(
    promise_id: int,
    db: Session = Depends(get_db),
    _admin: dict = Depends(get_current_admin),
):
    from sqlalchemy import text
    result = db.execute(
        text("DELETE FROM vera_promises WHERE id = :id"),
        {"id": promise_id},
    )
    db.commit()
    if result.rowcount == 0:
        raise HTTPException(status_code=404, detail="Promise not found")
