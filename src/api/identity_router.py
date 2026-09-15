"""Admin Identity-Resolution Exceptions Router — WP-4 (WI-4).

JWT-protected (admin). Surfaces the durable EXCEPTIONS queue the resolver
writes to whenever it correctly declines to auto-merge -- an ambiguous pair
or a cluster touching 2+ existing buyer_entities anchors. Client spec:
"Identity resolution is uncertain. Records stay separate and a possible-match
flag routes to EXCEPTIONS." Mounted at /api/admin/ by main.py.

    GET  /api/admin/identity/exceptions?limit=
    POST /api/admin/identity/exceptions/{id}/merge   {surviving_id, absorbed_id, reason}
    POST /api/admin/identity/exceptions/{id}/reject  {reason}

The merge endpoint calls merge_entities() and resolve_exception() in one
transaction -- an exception row is never marked 'merged' without a real
merge_log_id pointing at the audit record of what actually happened.
"""
from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session

from src.api.admin_router import get_current_admin
from src.api.deps import get_db as _get_db
from src.services.buyer_entity_exceptions import list_open_exceptions, resolve_exception
from src.services.buyer_entity_merge import merge_entities

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/admin", tags=["admin-identity"])


class _MergeRequest(BaseModel):
    surviving_id: int
    absorbed_id: int
    reason: Optional[str] = None


class _RejectRequest(BaseModel):
    reason: Optional[str] = None


@router.get("/identity/exceptions")
def get_open_exceptions(
    limit: int = Query(default=100, ge=1, le=1000),
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(_get_db),
):
    """Open exception rows, oldest first."""
    rows = list_open_exceptions(db, limit=limit)
    return {"total": len(rows), "items": rows}


@router.post("/identity/exceptions/{exception_id}/merge")
def merge_exception(
    exception_id: int,
    body: _MergeRequest,
    admin: dict = Depends(get_current_admin),
    db: Session = Depends(_get_db),
):
    """Resolve an exception by merging surviving_id/absorbed_id. Calls
    merge_entities() then resolve_exception() in one transaction -- the
    exception row is stamped 'merged' with the real merge_log_id, never a
    guess."""
    admin_id = admin.get("sub", "admin")
    try:
        log = merge_entities(
            db, surviving_id=body.surviving_id, absorbed_id=body.absorbed_id,
            merged_by=f"admin:{admin_id}", reason=body.reason,
        )
        resolve_exception(
            db, exception_id, status="merged", resolved_by=f"admin:{admin_id}",
            merge_log_id=log.id,
        )
        db.commit()
    except ValueError as exc:
        db.rollback()
        raise HTTPException(status_code=422, detail=str(exc))
    except Exception:
        db.rollback()
        logger.error("[Admin] identity exception merge failed (id=%s)", exception_id, exc_info=True)
        raise HTTPException(status_code=500, detail="Merge failed")

    return {"exception_id": exception_id, "status": "merged", "merge_log_id": log.id}


@router.post("/identity/exceptions/{exception_id}/reject")
def reject_exception(
    exception_id: int,
    body: _RejectRequest,
    admin: dict = Depends(get_current_admin),
    db: Session = Depends(_get_db),
):
    """Resolve an exception as 'rejected' -- the two records/entities stay
    separate; this is a human confirming the resolver's caution was
    correct."""
    admin_id = admin.get("sub", "admin")
    try:
        resolve_exception(db, exception_id, status="rejected", resolved_by=f"admin:{admin_id}")
        db.commit()
        logger.info(
            "[Admin] identity exception %d rejected by admin:%s (reason=%r)",
            exception_id, admin_id, body.reason,
        )
    except ValueError as exc:
        db.rollback()
        raise HTTPException(status_code=404, detail=str(exc))
    except Exception:
        db.rollback()
        logger.error("[Admin] identity exception reject failed (id=%s)", exception_id, exc_info=True)
        raise HTTPException(status_code=500, detail="Reject failed")

    return {"exception_id": exception_id, "status": "rejected"}
