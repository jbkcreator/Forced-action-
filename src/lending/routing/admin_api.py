"""Admin API for R-1 lender routing rules.

Mounted on lending-api (/api/lending/routing/...).  Auth: shared secret in
X-Routing-Admin-Secret (LENDING_ROUTING_ADMIN_SECRET).

Q7 (open): UI is in forced-action-ui (separate repo), out of scope for this PR.
Josh uses these endpoints directly until the UI ships.

Endpoints:
  GET    /api/lending/routing/lenders             list all lenders (sorted by priority)
  POST   /api/lending/routing/lenders             add a lender
  PATCH  /api/lending/routing/lenders/{id}        update a lender
  POST   /api/lending/routing/lenders/{id}/make-primary  move the primary flag here (atomic)
  GET    /api/lending/routing/lenders/{id}/boxes  list product boxes
  POST   /api/lending/routing/lenders/{id}/boxes  add a product box
  DELETE /api/lending/routing/lenders/{id}/boxes/{box_id}  remove a product box
  GET    /api/lending/routing/decline-reasons      list decline reason codes
  POST   /api/lending/routing/decline-reasons      add a reason code
  POST   /api/lending/routing/events/{event_id}/decline  one-click decline (R-1)
  POST   /api/lending/routing/files/{lead_id}/outcome    record approved / funded (moves the seat cap)
  GET    /api/lending/routing/hold-queue           current hold queue summary
"""
from __future__ import annotations

import hmac
import logging
from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel, Field, field_validator
from sqlalchemy import text
from sqlalchemy.orm import Session

from config.lending_routing import (
    AGREEMENT_STATUS_CHOICES,
    DEFAULT_DECLINE_REASON,
    OUTCOME_ACTIONS,
    PRIMARY_DEFAULT_CONSTRUCTION_MIN,
    PRIMARY_DEFAULT_SEAT_CAP,
)
from config.settings import get_settings
from src.lending.contracts import LoanType
from src.lending.db import get_lending_db

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/lending/routing", tags=["routing-admin"])


# ── Auth ──────────────────────────────────────────────────────────────────────


def _verify_admin(x_routing_admin_secret: Optional[str] = Header(default=None)) -> None:
    settings = get_settings()
    secret = settings.lending_routing_admin_secret
    if not secret:
        raise HTTPException(status_code=503, detail="Routing admin not configured")
    if not x_routing_admin_secret or not hmac.compare_digest(
        x_routing_admin_secret.encode(),
        secret.get_secret_value().encode(),
    ):
        raise HTTPException(status_code=401, detail="Unauthorized")


# ── Schemas ───────────────────────────────────────────────────────────────────


class LenderIn(BaseModel):
    name: str
    active: bool = True
    is_primary: bool = False
    agreement_status: str = "pending"
    priority: int
    monthly_cap: Optional[int] = None
    construction_min_amount: Optional[int] = None
    points_paid_to_broker: Optional[float] = None
    takes_ground_up: bool = False


class LenderPatch(BaseModel):
    name: Optional[str] = None
    active: Optional[bool] = None
    is_primary: Optional[bool] = None
    agreement_status: Optional[str] = None
    priority: Optional[int] = None
    monthly_cap: Optional[int] = None
    construction_min_amount: Optional[int] = None
    points_paid_to_broker: Optional[float] = None
    takes_ground_up: Optional[bool] = None


class ProductBoxIn(BaseModel):
    loan_types: list[str]

    @field_validator("loan_types")
    @classmethod
    def _known_loan_types(cls, value: list[str]) -> list[str]:
        """A box must name real LendingFlow loan types; a typo would silently never match."""
        known = {m.value for m in LoanType}
        normalised = [v.strip().upper() for v in value]
        unknown = [v for v in normalised if v not in known]
        if not normalised or unknown:
            raise ValueError(f"loan_types must be a non-empty subset of {sorted(known)}; got {unknown or value}")
        return normalised

    min_loan_amount: Optional[int] = None
    max_loan_amount: Optional[int] = None
    approved_states: Optional[list[str]] = None
    credit_floor: Optional[int] = None
    max_ltv: Optional[float] = Field(default=None, ge=0, le=10)
    max_ltc: Optional[float] = Field(default=None, ge=0, le=10)
    min_completed_projects: int = 0
    requires_ground_up_build: bool = False


class DeclineReasonIn(BaseModel):
    code: str
    label: str
    active: bool = True


class DeclineIn(BaseModel):
    reason_code: str = DEFAULT_DECLINE_REASON
    actor: str = "josh"
    free_text: Optional[str] = None


class OutcomeIn(BaseModel):
    action: str
    actor: str = "josh"
    free_text: Optional[str] = None


# ── Lender CRUD ───────────────────────────────────────────────────────────────


@router.get("/lenders")
def list_lenders(
    _: None = Depends(_verify_admin),
    db: Session = Depends(get_lending_db),
) -> list[dict]:
    rows = db.execute(
        text(
            "SELECT id, name, active, is_primary, agreement_status, priority,"
            "       monthly_cap, construction_min_amount, takes_ground_up,"
            "       created_at, updated_at"
            "  FROM lending.lender_routing_rules"
            " ORDER BY priority ASC, id ASC"
        )
    ).mappings().all()
    return [dict(r) for r in rows]


@router.post("/lenders", status_code=201)
def create_lender(
    body: LenderIn,
    _: None = Depends(_verify_admin),
    db: Session = Depends(get_lending_db),
) -> dict:
    if body.agreement_status not in AGREEMENT_STATUS_CHOICES:
        raise HTTPException(status_code=422, detail=f"Invalid agreement_status: {body.agreement_status!r}")
    if body.is_primary:
        _enforce_single_primary(None, db)
        if body.monthly_cap is None:
            body.monthly_cap = PRIMARY_DEFAULT_SEAT_CAP
        if body.construction_min_amount is None:
            body.construction_min_amount = PRIMARY_DEFAULT_CONSTRUCTION_MIN
    now = datetime.now(timezone.utc)
    row = db.execute(
        text(
            "INSERT INTO lending.lender_routing_rules"
            "  (name, active, is_primary, agreement_status, priority,"
            "   monthly_cap, construction_min_amount, points_paid_to_broker,"
            "   takes_ground_up, created_at, updated_at)"
            " VALUES (:name, :active, :primary, :status, :priority,"
            "         :cap, :cmin, :pts, :gu, :now, :now)"
            " RETURNING id"
        ),
        {
            "name": body.name, "active": body.active, "primary": body.is_primary,
            "status": body.agreement_status, "priority": body.priority,
            "cap": body.monthly_cap, "cmin": body.construction_min_amount,
            "pts": body.points_paid_to_broker, "gu": body.takes_ground_up,
            "now": now,
        },
    ).scalar_one()
    db.commit()
    return {"id": row, "name": body.name}


@router.patch("/lenders/{lender_id}")
def update_lender(
    lender_id: int,
    body: LenderPatch,
    _: None = Depends(_verify_admin),
    db: Session = Depends(get_lending_db),
) -> dict:
    _require_lender(lender_id, db)
    if body.agreement_status is not None and body.agreement_status not in AGREEMENT_STATUS_CHOICES:
        raise HTTPException(status_code=422, detail=f"Invalid agreement_status: {body.agreement_status!r}")
    if body.is_primary is True:
        _enforce_single_primary(lender_id, db)

    updates = {k: v for k, v in body.model_dump(exclude_none=True).items()}
    if not updates:
        return {"detail": "no changes"}

    updates["id"] = lender_id
    updates["now"] = datetime.now(timezone.utc)
    set_clause = ", ".join(
        f"{k} = :{k}" for k in updates if k not in ("id", "now")
    ) + ", updated_at = :now"
    db.execute(
        text(f"UPDATE lending.lender_routing_rules SET {set_clause} WHERE id = :id"),
        updates,
    )
    db.commit()
    return {"id": lender_id, "updated": list(updates.keys())}


@router.post("/lenders/{lender_id}/make-primary")
def make_primary(
    lender_id: int,
    _: None = Depends(_verify_admin),
    db: Session = Depends(get_lending_db),
) -> dict:
    """Move the primary flag to this lender in one transaction.

    The previous primary is demoted and this lender gets first look.  The spec defaults
    (seat cap 15, ground-up minimum $500,000) fill any value the lender does not already have, so
    the seat cap applies to the new primary straight away.
    """
    _require_lender(lender_id, db)
    now = datetime.now(timezone.utc)
    demoted = db.execute(
        text(
            "UPDATE lending.lender_routing_rules SET is_primary = FALSE, updated_at = :now"
            " WHERE is_primary AND id <> :id RETURNING id"
        ),
        {"id": lender_id, "now": now},
    ).scalars().all()
    db.execute(
        text(
            "UPDATE lending.lender_routing_rules"
            "   SET is_primary = TRUE,"
            "       monthly_cap = COALESCE(monthly_cap, :cap),"
            "       construction_min_amount = COALESCE(construction_min_amount, :cmin),"
            "       updated_at = :now"
            " WHERE id = :id"
        ),
        {"id": lender_id, "cap": PRIMARY_DEFAULT_SEAT_CAP, "cmin": PRIMARY_DEFAULT_CONSTRUCTION_MIN, "now": now},
    )
    db.commit()
    logger.info("make_primary lender_id=%d demoted=%s", lender_id, demoted)
    return {"id": lender_id, "demoted": demoted}


# ── Product boxes ─────────────────────────────────────────────────────────────


@router.get("/lenders/{lender_id}/boxes")
def list_boxes(
    lender_id: int,
    _: None = Depends(_verify_admin),
    db: Session = Depends(get_lending_db),
) -> list[dict]:
    _require_lender(lender_id, db)
    rows = db.execute(
        text(
            "SELECT id, loan_types, min_loan_amount, max_loan_amount, approved_states,"
            "       credit_floor, max_ltv, max_ltc, min_completed_projects,"
            "       requires_ground_up_build, created_at"
            "  FROM lending.lender_product_boxes"
            " WHERE lender_id = :lid"
        ),
        {"lid": lender_id},
    ).mappings().all()
    return [dict(r) for r in rows]


@router.post("/lenders/{lender_id}/boxes", status_code=201)
def add_box(
    lender_id: int,
    body: ProductBoxIn,
    _: None = Depends(_verify_admin),
    db: Session = Depends(get_lending_db),
) -> dict:
    _require_lender(lender_id, db)
    row_id = db.execute(
        text(
            "INSERT INTO lending.lender_product_boxes"
            "  (lender_id, loan_types, min_loan_amount, max_loan_amount,"
            "   approved_states, credit_floor, max_ltv, max_ltc,"
            "   min_completed_projects, requires_ground_up_build)"
            " VALUES (:lid, :lt, :min, :max, :states, :credit,"
            "         :ltv, :ltc, :proj, :gu)"
            " RETURNING id"
        ),
        {
            "lid": lender_id, "lt": body.loan_types,
            "min": body.min_loan_amount, "max": body.max_loan_amount,
            "states": body.approved_states,
            "credit": body.credit_floor, "ltv": body.max_ltv, "ltc": body.max_ltc,
            "proj": body.min_completed_projects, "gu": body.requires_ground_up_build,
        },
    ).scalar_one()
    db.commit()
    return {"id": row_id}


@router.delete("/lenders/{lender_id}/boxes/{box_id}", status_code=204)
def delete_box(
    lender_id: int,
    box_id: int,
    _: None = Depends(_verify_admin),
    db: Session = Depends(get_lending_db),
) -> None:
    result = db.execute(
        text("DELETE FROM lending.lender_product_boxes WHERE id = :bid AND lender_id = :lid"),
        {"bid": box_id, "lid": lender_id},
    )
    db.commit()
    if result.rowcount == 0:
        raise HTTPException(status_code=404, detail="Box not found")


# ── Decline reasons ───────────────────────────────────────────────────────────


@router.get("/decline-reasons")
def list_decline_reasons(
    _: None = Depends(_verify_admin),
    db: Session = Depends(get_lending_db),
) -> list[dict]:
    rows = db.execute(
        text("SELECT code, label, active FROM lending.decline_reason_codes ORDER BY code")
    ).mappings().all()
    return [dict(r) for r in rows]


@router.post("/decline-reasons", status_code=201)
def add_decline_reason(
    body: DeclineReasonIn,
    _: None = Depends(_verify_admin),
    db: Session = Depends(get_lending_db),
) -> dict:
    db.execute(
        text(
            "INSERT INTO lending.decline_reason_codes (code, label, active)"
            " VALUES (:code, :label, :active)"
            " ON CONFLICT (code) DO UPDATE SET label = EXCLUDED.label, active = EXCLUDED.active"
        ),
        {"code": body.code, "label": body.label, "active": body.active},
    )
    db.commit()
    return {"code": body.code}


# ── One-click decline ─────────────────────────────────────────────────────────


@router.post("/events/{event_id}/decline", status_code=200)
def decline_event(
    event_id: int,
    body: DeclineIn,
    _: None = Depends(_verify_admin),
    db: Session = Depends(get_lending_db),
) -> dict[str, Any]:
    from src.lending.routing.decline import record_decline

    try:
        record_decline(
            event_id,
            reason_code=body.reason_code,
            actor=body.actor,
            db=db,
            free_text=body.free_text,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    return {"detail": "decline recorded"}


# ── Outcome (approved / funded) ───────────────────────────────────────────────


@router.post("/files/{lead_id}/outcome", status_code=200)
def file_outcome(
    lead_id: int,
    body: OutcomeIn,
    _: None = Depends(_verify_admin),
    db: Session = Depends(get_lending_db),
) -> dict[str, Any]:
    from src.lending.routing.outcome import record_outcome

    if body.action not in OUTCOME_ACTIONS:
        raise HTTPException(status_code=422, detail=f"action must be one of {list(OUTCOME_ACTIONS)}")
    try:
        recorded = record_outcome(
            lead_id, action=body.action, actor=body.actor, db=db, free_text=body.free_text,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    return {"detail": "recorded" if recorded else "already recorded"}


# ── Hold queue ────────────────────────────────────────────────────────────────


@router.get("/hold-queue")
def hold_queue(
    _: None = Depends(_verify_admin),
    db: Session = Depends(get_lending_db),
) -> dict:
    rows = db.execute(
        text(
            "SELECT fr.lead_id, fr.status, fr.hold_reason,"
            "       ll.first_name, ll.last_name, ll.loan_type, ll.loan_amount,"
            "       fr.updated_at"
            "  FROM lending.file_routing fr"
            "  JOIN lending.lendingflow_leads ll ON ll.id = fr.lead_id"
            " WHERE fr.status IN ('held','exhausted')"
            " ORDER BY fr.updated_at DESC"
            " LIMIT 200"
        )
    ).mappings().all()
    return {
        "count": len(rows),
        "files": [dict(r) for r in rows],
    }


# ── Internal helpers ──────────────────────────────────────────────────────────


def _require_lender(lender_id: int, db: Session) -> None:
    exists = db.execute(
        text("SELECT 1 FROM lending.lender_routing_rules WHERE id = :id"),
        {"id": lender_id},
    ).scalar()
    if not exists:
        raise HTTPException(status_code=404, detail=f"Lender {lender_id} not found")


def _enforce_single_primary(except_id: Optional[int], db: Session) -> None:
    """Raise 409 if another lender already has is_primary=TRUE."""
    row = db.execute(
        text(
            "SELECT id FROM lending.lender_routing_rules"
            " WHERE is_primary = TRUE AND (:except_id IS NULL OR id != :except_id)"
        ),
        {"except_id": except_id},
    ).scalar()
    if row:
        raise HTTPException(
            status_code=409,
            detail=(
                f"Lender {row} is already marked primary. "
                f"Use POST lenders/{{id}}/make-primary to move the primary flag in one step."
            ),
        )
