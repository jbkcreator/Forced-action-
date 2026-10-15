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
  POST   /api/lending/routing/lenders/reorder     set the waterfall order (atomic)
  GET    /api/lending/routing/meta                vocabularies for the admin screen
  GET    /api/lending/routing/files               file list, both kinds (status, last-name, source filters)
  GET    /api/lending/routing/files/{file_id}     file record: current lender, next lender, history  (?source=partner)
  POST   /api/lending/routing/files/{file_id}/decline  one-click decline of the current lender  (?source=partner)
  POST   /api/lending/routing/events/{event_id}/decline  one-click decline (R-1)
  POST   /api/lending/routing/files/{file_id}/requeue    send a held file back to the router (?source=partner)
  POST   /api/lending/routing/hold-queue/requeue         reopen every file held for a reason (no_match / exhausted)
  POST   /api/lending/routing/files/{file_id}/outcome    record approved / funded (moves the seat cap)  (?source=partner)
  GET    /api/lending/routing/hold-queue           current hold queue summary
"""
from __future__ import annotations

import hmac
import logging
from datetime import datetime, timezone
from typing import Annotated, Any, Optional

from fastapi import APIRouter, Depends, Header, HTTPException, Path, Query
from pydantic import BaseModel, Field, field_validator, model_validator
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from config.lending_routing import (
    AGREEMENT_STATUS_CHOICES,
    DEFAULT_DECLINE_REASON,
    OUTCOME_ACTIONS,
    PRIMARY_DEFAULT_CONSTRUCTION_MIN,
    PRIMARY_DEFAULT_SEAT_CAP,
    SEAT_CAP_COUNTED_ACTION,
)
from config.settings import get_settings
from config.lender_matrix import LENDER_MATRIX
from config.lending_partners import PRODUCTS
from src.lending.contracts import LoanType
from src.lending.db import get_lending_db
from src.lending.routing.file_key import FileKey, key_from_source

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

_MAX_INT = 2_147_483_647  # the id columns' range; anything larger would be a database error, not a 404
IdPath = Annotated[int, Path(ge=1, le=_MAX_INT)]
_MAX_MONEY = 1_000_000_000_000


def _check_rule_keys(value: Optional[list[str]]) -> Optional[list[str]]:
    """``rule_keys`` must be T-05 lender keys (config/lender_matrix.py); a typo would leave the lender with no rules."""
    if value is None:
        return None
    known = {r.key for r in LENDER_MATRIX}
    cleaned = [k.strip() for k in value]
    unknown = [k for k in cleaned if k not in known]
    if not cleaned or unknown:
        raise ValueError(f"rule_keys must be a non-empty subset of {sorted(known)}; got {unknown or value}")
    return cleaned


def _check_name(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    cleaned = value.strip()
    if not cleaned:
        raise ValueError("name cannot be blank")
    return cleaned


class LenderIn(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    active: bool = True
    is_primary: bool = False
    agreement_status: str = "pending"
    priority: int = Field(ge=1, le=100_000)
    monthly_cap: Optional[int] = Field(default=None, ge=0, le=100_000)
    construction_min_amount: Optional[int] = Field(default=None, ge=0, le=_MAX_MONEY)
    points_paid_to_broker: Optional[float] = Field(default=None, ge=0, lt=10)  # NUMERIC(5,4)
    takes_ground_up: bool = False
    rule_keys: Optional[list[str]] = None

    @field_validator("name")
    @classmethod
    def _name_not_blank(cls, value: str) -> str:
        return _check_name(value)

    @field_validator("rule_keys")
    @classmethod
    def _rule_keys_known(cls, value: Optional[list[str]]) -> Optional[list[str]]:
        return _check_rule_keys(value)


class LenderPatch(BaseModel):
    name: Optional[str] = Field(default=None, min_length=1, max_length=100)
    active: Optional[bool] = None
    is_primary: Optional[bool] = None
    agreement_status: Optional[str] = None
    priority: Optional[int] = Field(default=None, ge=1, le=100_000)
    monthly_cap: Optional[int] = Field(default=None, ge=0, le=100_000)
    construction_min_amount: Optional[int] = Field(default=None, ge=0, le=_MAX_MONEY)
    points_paid_to_broker: Optional[float] = Field(default=None, ge=0, lt=10)
    takes_ground_up: Optional[bool] = None
    rule_keys: Optional[list[str]] = None

    @field_validator("name")
    @classmethod
    def _name_not_blank(cls, value: Optional[str]) -> Optional[str]:
        return _check_name(value)

    @field_validator("rule_keys")
    @classmethod
    def _rule_keys_known(cls, value: Optional[list[str]]) -> Optional[list[str]]:
        return _check_rule_keys(value)


class ProductBoxIn(BaseModel):
    loan_types: list[str]
    min_loan_amount: Optional[int] = Field(default=None, ge=0, le=_MAX_MONEY)
    max_loan_amount: Optional[int] = Field(default=None, ge=0, le=_MAX_MONEY)
    approved_states: Optional[list[str]] = None
    credit_floor: Optional[int] = Field(default=None, ge=0, le=900)
    max_ltv: Optional[float] = Field(default=None, ge=0, lt=10)  # NUMERIC(5,4): 10 or more overflows it
    max_ltc: Optional[float] = Field(default=None, ge=0, lt=10)
    min_completed_projects: int = Field(default=0, ge=0, le=1000)
    requires_ground_up_build: bool = False

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

    @field_validator("approved_states")
    @classmethod
    def _states_as_codes(cls, value: Optional[list[str]]) -> Optional[list[str]]:
        """Two-letter codes in capitals, as the files carry them; a lowercase "fl" would never match."""
        if not value:
            return None
        cleaned = [s.strip().upper() for s in value]
        bad = [s for s in cleaned if len(s) != 2 or not s.isalpha()]
        if bad:
            raise ValueError(f"approved_states must be two-letter state codes; got {bad}")
        return cleaned

    @model_validator(mode="after")
    def _min_not_above_max(self) -> "ProductBoxIn":
        if (self.min_loan_amount is not None and self.max_loan_amount is not None
                and self.min_loan_amount > self.max_loan_amount):
            raise ValueError("min_loan_amount cannot be above max_loan_amount")
        return self


class DeclineReasonIn(BaseModel):
    code: str = Field(min_length=1, max_length=50, pattern=r"^[a-z0-9_]+$")
    label: str = Field(min_length=1, max_length=200)
    active: bool = True


class DeclineIn(BaseModel):
    reason_code: str = Field(default=DEFAULT_DECLINE_REASON, min_length=1, max_length=50)
    actor: str = Field(default="josh", min_length=1, max_length=100)
    free_text: Optional[str] = Field(default=None, max_length=2000)


class OutcomeIn(BaseModel):
    action: str = Field(min_length=1, max_length=20)
    actor: str = Field(default="josh", min_length=1, max_length=100)
    free_text: Optional[str] = Field(default=None, max_length=2000)


class ReorderIn(BaseModel):
    ordered_ids: list[Annotated[int, Field(ge=1, le=_MAX_INT)]] = Field(min_length=1, max_length=500)


class RequeueIn(BaseModel):
    reason: str = Field(default="no_match", pattern=r"^(no_match|exhausted)$")
    limit: int = Field(default=200, ge=1, le=1000)


_NULLABLE_LENDER_FIELDS = frozenset({"monthly_cap", "construction_min_amount", "points_paid_to_broker", "rule_keys"})
_FILE_STATUSES = ("unrouted", "routed", "held", "exhausted")
_FILE_SOURCES = ("lendingflow", "partner")
_HOLD_QUEUE_LIMIT = 200


def _priority_taken() -> HTTPException:
    return HTTPException(
        status_code=409,
        detail="That priority is already used by an active lender. Use the reorder control to change the order.",
    )


def _conflict(exc: IntegrityError) -> HTTPException:
    """409 for a unique-index clash, saying which rule was broken (a second primary, or a duplicate priority)."""
    constraint = getattr(getattr(exc.orig, "diag", None), "constraint_name", "") or ""
    if "primary" in constraint:
        return HTTPException(
            status_code=409,
            detail="Another lender is already primary. Use POST lenders/{id}/make-primary to move the flag in one step.",
        )
    return _priority_taken()


def _lock_lenders(db: Session) -> None:
    """Serialise changes to the waterfall order and the primary flag (two admins acting at once)."""
    db.execute(text("SELECT id FROM lending.lender_routing_rules ORDER BY id FOR UPDATE"))


# ── Lender CRUD ───────────────────────────────────────────────────────────────


@router.get("/lenders")
def list_lenders(
    _: None = Depends(_verify_admin),
    db: Session = Depends(get_lending_db),
) -> list[dict]:
    rows = db.execute(
        text(
            "SELECT id, name, active, is_primary, agreement_status, priority,"
            "       monthly_cap, construction_min_amount, points_paid_to_broker, takes_ground_up, rule_keys,"
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
    try:
        row = db.execute(
            text(
                "INSERT INTO lending.lender_routing_rules"
                "  (name, active, is_primary, agreement_status, priority,"
                "   monthly_cap, construction_min_amount, points_paid_to_broker,"
                "   takes_ground_up, rule_keys, created_at, updated_at)"
                " VALUES (:name, :active, :primary, :status, :priority,"
                "         :cap, :cmin, :pts, :gu, :keys, :now, :now)"
                " RETURNING id"
            ),
            {
                "name": body.name, "active": body.active, "primary": body.is_primary,
                "status": body.agreement_status, "priority": body.priority,
                "cap": body.monthly_cap, "cmin": body.construction_min_amount,
                "pts": body.points_paid_to_broker, "gu": body.takes_ground_up,
                "keys": body.rule_keys, "now": now,
            },
        ).scalar_one()
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise _conflict(exc)
    return {"id": row, "name": body.name}


@router.patch("/lenders/{lender_id}")
def update_lender(
    lender_id: IdPath,
    body: LenderPatch,
    _: None = Depends(_verify_admin),
    db: Session = Depends(get_lending_db),
) -> dict:
    _require_lender(lender_id, db)
    if body.agreement_status is not None and body.agreement_status not in AGREEMENT_STATUS_CHOICES:
        raise HTTPException(status_code=422, detail=f"Invalid agreement_status: {body.agreement_status!r}")
    if body.is_primary is True:
        _enforce_single_primary(lender_id, db)

    # Only fields the caller sent. An explicit null clears a nullable limit (for example "no seat cap");
    # for any other field null is meaningless and is rejected.
    updates = body.model_dump(exclude_unset=True)
    if not updates:
        return {"detail": "no changes"}
    bad_null = [k for k, v in updates.items() if v is None and k not in _NULLABLE_LENDER_FIELDS]
    if bad_null:
        raise HTTPException(status_code=422, detail=f"{bad_null} cannot be null")

    updates["id"] = lender_id
    updates["now"] = datetime.now(timezone.utc)
    set_clause = ", ".join(
        f"{k} = :{k}" for k in updates if k not in ("id", "now")
    ) + ", updated_at = :now"
    try:
        db.execute(
            text(f"UPDATE lending.lender_routing_rules SET {set_clause} WHERE id = :id"),
            updates,
        )
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise _conflict(exc)
    return {"id": lender_id, "updated": [k for k in updates if k not in ("id", "now")]}


@router.post("/lenders/reorder")
def reorder_lenders(
    body: ReorderIn,
    _: None = Depends(_verify_admin),
    db: Session = Depends(get_lending_db),
) -> dict:
    """Set the waterfall order in one transaction: ``ordered_ids[0]`` becomes priority 1, and so on.

    Priorities are unique among active lenders, so they cannot be swapped one PATCH at a time. Every
    active lender must be in the list, otherwise the new numbers could collide with one left out.
    """
    ids = body.ordered_ids
    if len(set(ids)) != len(ids):
        raise HTTPException(status_code=422, detail="ordered_ids contains duplicates")
    _lock_lenders(db)
    known = {r["id"]: r["active"] for r in db.execute(
        text("SELECT id, active FROM lending.lender_routing_rules")).mappings()}
    unknown = [i for i in ids if i not in known]
    if unknown:
        raise HTTPException(status_code=404, detail=f"Unknown lender ids: {unknown}")
    missing_active = sorted(i for i, active in known.items() if active and i not in set(ids))
    if missing_active:
        raise HTTPException(status_code=422, detail=f"Every active lender must be listed; missing {missing_active}")
    now = datetime.now(timezone.utc)
    try:
        # Two passes: park the listed rows on negative numbers, then assign the final order.
        db.execute(
            text("UPDATE lending.lender_routing_rules SET priority = -id WHERE id = ANY(:ids)"), {"ids": ids},
        )
        for position, lender_id in enumerate(ids, start=1):
            db.execute(
                text("UPDATE lending.lender_routing_rules SET priority = :p, updated_at = :now WHERE id = :id"),
                {"p": position, "now": now, "id": lender_id},
            )
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise _conflict(exc)
    return {"ordered_ids": ids}


@router.post("/lenders/{lender_id}/make-primary")
def make_primary(
    lender_id: IdPath,
    _: None = Depends(_verify_admin),
    db: Session = Depends(get_lending_db),
) -> dict:
    """Move the primary flag to this lender in one transaction.

    The previous primary is demoted and this lender gets first look.  The spec defaults
    (seat cap 15, ground-up minimum $500,000) fill any value the lender does not already have, so
    the seat cap applies to the new primary straight away.
    """
    _require_lender(lender_id, db)
    _lock_lenders(db)
    active = db.execute(
        text("SELECT active FROM lending.lender_routing_rules WHERE id = :id"), {"id": lender_id},
    ).scalar()
    if not active:
        raise HTTPException(status_code=422, detail="Resume the lender before making it primary.")
    now = datetime.now(timezone.utc)
    try:
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
    except IntegrityError as exc:
        db.rollback()
        raise _conflict(exc)
    logger.info("make_primary lender_id=%d demoted=%s", lender_id, demoted)
    return {"id": lender_id, "demoted": demoted}


# ── Product boxes ─────────────────────────────────────────────────────────────


@router.get("/lenders/{lender_id}/boxes")
def list_boxes(
    lender_id: IdPath,
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
    lender_id: IdPath,
    body: ProductBoxIn,
    _: None = Depends(_verify_admin),
    db: Session = Depends(get_lending_db),
) -> dict:
    _require_lender(lender_id, db)
    keys = db.execute(
        text("SELECT rule_keys FROM lending.lender_routing_rules WHERE id = :id"), {"id": lender_id},
    ).scalar()
    if keys:
        raise HTTPException(
            status_code=409,
            detail=(f"This lender's rules come from the T-05 rule values ({', '.join(keys)}), so a box added here "
                    "would be ignored. Edit its values in lending.lender_rule_values instead."),
        )
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
    lender_id: IdPath,
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
    if body.code == DEFAULT_DECLINE_REASON and not body.active:
        raise HTTPException(status_code=422, detail="The default decline reason cannot be switched off.")
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
    event_id: IdPath,
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


# ── Files: LendingFlow leads and partner files ────────────────────────────────
#
# A file is a LendingFlow lead (source "lendingflow") or a partner file from the Partner Door (source
# "partner"). Every file endpoint takes ``source`` (default "lendingflow") and the file's id within it.


def _file_key(source: str, file_id: int) -> FileKey:
    try:
        return key_from_source(source, file_id)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))


def _loan_type_of_product(product: Optional[str]) -> Optional[str]:
    """A partner file stores a product (flip, ground_up...); show it as the lender-box loan type."""
    mapped = PRODUCTS.get(product) if product else None
    return mapped.value if mapped is not None else None


def _partner_files_available(db: Session) -> bool:
    """True when the Partner Door tables exist (its migration may not have run in this database yet)."""
    return db.execute(text("SELECT to_regclass('lending.loan_files') IS NOT NULL")).scalar() is True


_LEAD_FILE_ROWS = (
    "SELECT 'lendingflow' AS source, fr.lead_id AS file_id, ll.first_name, ll.last_name, ll.loan_type,"
    "       ll.loan_amount, ll.property_state, NULL::text AS partner_name, fr.status,"
    "       fr.current_lender_name, fr.route_position, fr.hold_reason, fr.updated_at"
    "  FROM lending.file_routing fr"
    "  JOIN lending.lendingflow_leads ll ON ll.id = fr.lead_id"
)
_PARTNER_FILE_ROWS = (
    "SELECT 'partner' AS source, fr.loan_file_id AS file_id, lf.borrower_first_name AS first_name,"
    "       lf.borrower_last_name AS last_name, lf.product AS loan_type,"
    "       lf.loan_amount, lf.property_state, p.name AS partner_name, fr.status,"
    "       fr.current_lender_name, fr.route_position, fr.hold_reason, fr.updated_at"
    "  FROM lending.file_routing fr"
    "  JOIN lending.loan_files lf ON lf.id = fr.loan_file_id"
    "  LEFT JOIN lending.partners p ON p.id = lf.partner_id"
)


def _file_rows(db: Session, *, where: str, params: dict, order_limit: str, source: Optional[str] = None) -> list[dict]:
    """File rows from both kinds of file, newest first. ``where`` references the aliases ``f.*`` of the union."""
    parts = []
    if source in (None, "lendingflow"):
        parts.append(_LEAD_FILE_ROWS)
    if source in (None, "partner") and _partner_files_available(db):
        parts.append(_PARTNER_FILE_ROWS)
    if not parts:
        return []
    rows = db.execute(
        text(f"SELECT * FROM ({' UNION ALL '.join(parts)}) f WHERE {where} {order_limit}"), params,
    ).mappings().all()
    out = []
    for row in rows:
        item = dict(row)
        if item["source"] == "partner":
            item["loan_type"] = _loan_type_of_product(item["loan_type"])
        item["lead_id"] = item["file_id"] if item["source"] == "lendingflow" else None  # kept for older callers
        out.append(item)
    return out


# ── Outcome (approved / funded) ───────────────────────────────────────────────


@router.post("/files/{file_id}/outcome", status_code=200)
def file_outcome(
    file_id: IdPath,
    body: OutcomeIn,
    source: str = Query(default="lendingflow"),
    _: None = Depends(_verify_admin),
    db: Session = Depends(get_lending_db),
) -> dict[str, Any]:
    from src.lending.routing.outcome import record_outcome

    key = _file_key(source, file_id)
    if body.action not in OUTCOME_ACTIONS:
        raise HTTPException(status_code=422, detail=f"action must be one of {list(OUTCOME_ACTIONS)}")
    try:
        recorded = record_outcome(
            key.id, action=body.action, actor=body.actor, db=db, free_text=body.free_text, kind=key.kind,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    return {"detail": "recorded" if recorded else "already recorded"}


# ── Meta ──────────────────────────────────────────────────────────────────────


@router.get("/meta")
def meta(_: None = Depends(_verify_admin)) -> dict:
    """Vocabularies the admin screen offers, so it never hard-codes them."""
    return {
        "loan_types": [m.value for m in LoanType],
        "agreement_statuses": list(AGREEMENT_STATUS_CHOICES),
        "file_statuses": list(_FILE_STATUSES),
        "file_sources": list(_FILE_SOURCES),
        "outcome_actions": list(OUTCOME_ACTIONS),
        "default_decline_reason": DEFAULT_DECLINE_REASON,
        "seat_cap_counted_action": SEAT_CAP_COUNTED_ACTION,
    }


# ── File record (list, detail with next lender + history, one-click decline) ──


@router.get("/files")
def list_files(
    status: Optional[str] = Query(default=None),
    q: Optional[str] = Query(default=None, max_length=60, description="borrower last name contains"),
    source: Optional[str] = Query(default=None, description="lendingflow or partner; omit for both"),
    limit: int = Query(default=100, ge=1, le=500),
    _: None = Depends(_verify_admin),
    db: Session = Depends(get_lending_db),
) -> dict:
    if status is not None and status not in _FILE_STATUSES:
        raise HTTPException(status_code=422, detail=f"status must be one of {list(_FILE_STATUSES)}")
    if source is not None and source not in _FILE_SOURCES:
        raise HTTPException(status_code=422, detail=f"source must be one of {list(_FILE_SOURCES)}")
    q = (q or "").strip() or None
    pattern = None if q is None else "%" + q.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%"
    files = _file_rows(
        db,
        where="(CAST(:status AS text) IS NULL OR f.status = :status)"
              " AND (CAST(:q AS text) IS NULL OR f.last_name ILIKE :qlike ESCAPE '\\')",
        params={"status": status, "q": q, "qlike": pattern, "limit": limit},
        order_limit="ORDER BY f.updated_at DESC, f.file_id DESC LIMIT :limit",
        source=source,
    )
    return {"count": len(files), "files": files}


@router.get("/files/{file_id}")
def file_record(
    file_id: IdPath,
    source: str = Query(default="lendingflow"),
    _: None = Depends(_verify_admin),
    db: Session = Depends(get_lending_db),
) -> dict:
    """One file: where it is, who is next, and its full route history."""
    from src.lending.routing.router import preview_next

    key = _file_key(source, file_id)
    summary = _file_rows(
        db, where="f.file_id = :id", params={"id": key.id}, order_limit="LIMIT 1", source=key.source,
    )
    if not summary:
        raise HTTPException(status_code=404, detail=f"No routing record for file {file_id}")
    file = summary[0]

    routing = db.execute(
        text(
            "SELECT current_lender_id, lenders_tried FROM lending.file_routing"
            f" WHERE {key.column} = :id"
        ),
        {"id": key.id},
    ).mappings().one()
    history = db.execute(
        text(
            "SELECT id, action, lender_id, lender_name, reason_code, free_text, actor, created_at"
            f"  FROM lending.file_routing_events WHERE {key.column} = :id ORDER BY id"
        ),
        {"id": key.id},
    ).mappings().all()

    routed = file["status"] == "routed"
    return {
        "file": {k: file[k] for k in (
            "source", "file_id", "lead_id", "first_name", "last_name", "loan_type", "loan_amount",
            "property_state", "partner_name", "status", "hold_reason", "updated_at",
        )},
        "current": (
            {"lender_id": routing["current_lender_id"], "name": file["current_lender_name"],
             "position": file["route_position"]} if routed else None
        ),
        "next": preview_next(key.id, db, kind=key.kind) if file["status"] in ("routed", "unrouted") else None,
        "tried_lender_ids": list(routing["lenders_tried"] or []),
        "history": [dict(h) for h in history],
        "can_decline": routed,
        "can_record_outcome": routed,
    }


@router.post("/files/{file_id}/decline", status_code=200)
def decline_file(
    file_id: IdPath,
    body: DeclineIn,
    source: str = Query(default="lendingflow"),
    _: None = Depends(_verify_admin),
    db: Session = Depends(get_lending_db),
) -> dict[str, Any]:
    """One-click decline of the file's current lender (the file record's Decline button)."""
    from src.lending.routing.decline import record_decline

    key = _file_key(source, file_id)
    event_id = db.execute(
        text(
            "SELECT e.id FROM lending.file_routing_events e"
            f"  JOIN lending.file_routing fr ON fr.{key.column} = e.{key.column} AND fr.status = 'routed'"
            f" WHERE e.{key.column} = :id AND e.action = 'routed' ORDER BY e.id DESC LIMIT 1"
        ),
        {"id": key.id},
    ).scalar()
    if event_id is None:
        raise HTTPException(status_code=422, detail=f"File {file_id} is not currently routed to a lender")
    try:
        record_decline(
            event_id, reason_code=body.reason_code, actor=body.actor, db=db, free_text=body.free_text,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    return {"detail": "decline recorded"}


@router.post("/files/{file_id}/requeue", status_code=200)
def requeue_file(
    file_id: IdPath,
    source: str = Query(default="lendingflow"),
    _: None = Depends(_verify_admin),
    db: Session = Depends(get_lending_db),
) -> dict[str, Any]:
    """Send a held or exhausted file back to the router now (after a lender was signed or a box was edited)."""
    from src.lending.routing.file_key import LEAD
    from src.lending.routing.router import reopen_file, route_file, route_loan_file

    key = _file_key(source, file_id)
    if not reopen_file(key, db):
        raise HTTPException(status_code=422, detail=f"File {file_id} is not in the hold queue")
    if key.kind == LEAD:
        route_file(key.id, db)
    else:
        route_loan_file(key.id, db)
    return {"detail": "sent back to the router"}


@router.post("/hold-queue/requeue", status_code=200)
def requeue_held(
    body: RequeueIn,
    _: None = Depends(_verify_admin),
    db: Session = Depends(get_lending_db),
) -> dict[str, Any]:
    """Reopen every file held for ``reason``; the routing sweep then routes them in batches, oldest first."""
    from src.lending.routing.router import reopen_held

    return {"reopened": reopen_held(db, body.reason, body.limit),
            "detail": "reopened; the routing sweep routes them in batches"}


# ── Hold queue ────────────────────────────────────────────────────────────────


@router.get("/hold-queue")
def hold_queue(
    _: None = Depends(_verify_admin),
    db: Session = Depends(get_lending_db),
) -> dict:
    files = _file_rows(
        db,
        where="f.status IN ('held','exhausted')",
        params={"limit": _HOLD_QUEUE_LIMIT + 1},
        order_limit="ORDER BY f.updated_at DESC LIMIT :limit",
    )
    truncated = len(files) > _HOLD_QUEUE_LIMIT
    return {"count": min(len(files), _HOLD_QUEUE_LIMIT), "truncated": truncated, "files": files[:_HOLD_QUEUE_LIMIT]}


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
