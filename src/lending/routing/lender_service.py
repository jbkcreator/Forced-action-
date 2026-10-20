"""R-1 lender rules and file record: the operations behind the Slack form and buttons.

Plain business logic: no HTTP, no Slack. ``slack_admin.py`` (the ``/lenders`` form) and ``slack_actions.py`` (the
buttons on a routing post) call these functions; each takes an open session and commits its own change. Anything the
caller must show a person is raised as :class:`RoutingError` with a plain message (and a ``kind``), never a database
error. The same shape as soft approval: Slack files talk to a service, the service never imports Slack.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Annotated, Optional

from pydantic import BaseModel, Field, field_validator, model_validator
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from config.lender_matrix import LENDER_MATRIX
from config.lending_partners import PRODUCTS
from config.lending_routing import AGREEMENT_STATUS_CHOICES, PRIMARY_DEFAULT_CONSTRUCTION_MIN, PRIMARY_DEFAULT_SEAT_CAP
from src.lending.contracts import LoanType
from src.lending.routing.file_key import LEAD, FileKey

logger = logging.getLogger(__name__)

NOT_FOUND, CONFLICT, INVALID = "not_found", "conflict", "invalid"

_MAX_INT = 2_147_483_647  # the id columns' range
_MAX_MONEY = 1_000_000_000_000


class RoutingError(Exception):
    """A refusal with a message fit to show a person. ``kind``: ``not_found``, ``conflict`` or ``invalid``."""

    def __init__(self, message: str, kind: str = INVALID) -> None:
        super().__init__(message)
        self.kind = kind


# ── Schemas (validation shared by every caller) ───────────────────────────────

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


class ReorderIn(BaseModel):
    ordered_ids: list[Annotated[int, Field(ge=1, le=_MAX_INT)]] = Field(min_length=1, max_length=500)


_NULLABLE_LENDER_FIELDS = frozenset({"monthly_cap", "construction_min_amount", "points_paid_to_broker", "rule_keys"})


# ── Helpers ───────────────────────────────────────────────────────────────────

def _priority_taken() -> RoutingError:
    return RoutingError("That priority is already used by an active lender. Reorder the lenders instead.", CONFLICT)


def _conflict(exc: IntegrityError) -> RoutingError:
    """A unique-index clash, saying which rule was broken (a second primary, or a duplicate priority)."""
    constraint = getattr(getattr(exc.orig, "diag", None), "constraint_name", "") or ""
    if "primary" in constraint:
        return RoutingError("Another lender is already primary. Make this lender primary to move the flag in one step.", CONFLICT)
    return _priority_taken()


def _lock_lenders(db: Session) -> None:
    """Serialise changes to the waterfall order and the primary flag (two people acting at once)."""
    db.execute(text("SELECT id FROM lending.lender_routing_rules ORDER BY id FOR UPDATE"))


def _require_lender(lender_id: int, db: Session) -> None:
    if not db.execute(text("SELECT 1 FROM lending.lender_routing_rules WHERE id = :id"), {"id": lender_id}).scalar():
        raise RoutingError(f"Lender {lender_id} not found", NOT_FOUND)


def _enforce_single_primary(except_id: Optional[int], db: Session) -> None:
    other = db.execute(
        text("SELECT id FROM lending.lender_routing_rules"
             " WHERE is_primary = TRUE AND (:except_id IS NULL OR id != :except_id)"),
        {"except_id": except_id},
    ).scalar()
    if other:
        raise RoutingError(f"Lender {other} is already marked primary. Make this lender primary to move the flag.", CONFLICT)


# ── Lenders ───────────────────────────────────────────────────────────────────

def create_lender(body: LenderIn, db: Session) -> dict:
    if body.agreement_status not in AGREEMENT_STATUS_CHOICES:
        raise RoutingError(f"Invalid agreement status: {body.agreement_status!r}")
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


def update_lender(lender_id: int, body: LenderPatch, db: Session) -> dict:
    _require_lender(lender_id, db)
    if body.agreement_status is not None and body.agreement_status not in AGREEMENT_STATUS_CHOICES:
        raise RoutingError(f"Invalid agreement status: {body.agreement_status!r}")
    if body.is_primary is True:
        _enforce_single_primary(lender_id, db)

    # Only fields that were set. An explicit None clears a nullable limit (for example "no seat cap");
    # for any other field None is meaningless and is rejected.
    updates = body.model_dump(exclude_unset=True)
    if not updates:
        return {"id": lender_id, "updated": []}
    bad_null = [k for k, v in updates.items() if v is None and k not in _NULLABLE_LENDER_FIELDS]
    if bad_null:
        raise RoutingError(f"{bad_null} cannot be empty")

    updates["id"] = lender_id
    updates["now"] = datetime.now(timezone.utc)
    set_clause = ", ".join(f"{k} = :{k}" for k in updates if k not in ("id", "now")) + ", updated_at = :now"
    try:
        db.execute(text(f"UPDATE lending.lender_routing_rules SET {set_clause} WHERE id = :id"), updates)
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise _conflict(exc)
    return {"id": lender_id, "updated": [k for k in updates if k not in ("id", "now")]}


def delete_lender(lender_id: int, db: Session) -> dict:
    """Remove a lender row for good. Refused for the primary lender and for any lender that has routed a file,
    because the routing history must keep pointing at it; pause it or set its agreement to terminated instead."""
    _require_lender(lender_id, db)
    _lock_lenders(db)
    name, is_primary = db.execute(
        text("SELECT name, is_primary FROM lending.lender_routing_rules WHERE id = :id"), {"id": lender_id},
    ).one()
    if is_primary:
        raise RoutingError(f"{name} is the primary lender. Make another lender primary first.", CONFLICT)
    used = db.execute(
        text("SELECT (SELECT COUNT(*) FROM lending.file_routing_events WHERE lender_id = :id)"
             "     + (SELECT COUNT(*) FROM lending.file_routing WHERE current_lender_id = :id)"),
        {"id": lender_id},
    ).scalar()
    if used:
        raise RoutingError(
            f"{name} has routing history, so it cannot be deleted. Pause it or set its agreement to terminated.", CONFLICT)
    db.execute(text("DELETE FROM lending.lender_product_boxes WHERE lender_id = :id"), {"id": lender_id})
    db.execute(text("DELETE FROM lending.lender_routing_rules WHERE id = :id"), {"id": lender_id})
    db.commit()
    return {"id": lender_id, "deleted": True}


def reorder_lenders(body: ReorderIn, db: Session) -> dict:
    """Set the waterfall order in one transaction: ``ordered_ids[0]`` becomes priority 1, and so on.

    Priorities are unique among active lenders, so they cannot be swapped one update at a time. Every
    active lender must be in the list, otherwise the new numbers could collide with one left out.
    """
    ids = body.ordered_ids
    if len(set(ids)) != len(ids):
        raise RoutingError("ordered_ids contains duplicates")
    _lock_lenders(db)
    known = {r["id"]: r["active"] for r in db.execute(text("SELECT id, active FROM lending.lender_routing_rules")).mappings()}
    unknown = [i for i in ids if i not in known]
    if unknown:
        raise RoutingError(f"Unknown lender ids: {unknown}", NOT_FOUND)
    missing_active = sorted(i for i, active in known.items() if active and i not in set(ids))
    if missing_active:
        raise RoutingError(f"Every active lender must be listed; missing {missing_active}")
    now = datetime.now(timezone.utc)
    try:
        # Two passes: park the listed rows on negative numbers, then assign the final order.
        db.execute(text("UPDATE lending.lender_routing_rules SET priority = -id WHERE id = ANY(:ids)"), {"ids": ids})
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


def make_primary(lender_id: int, db: Session) -> dict:
    """Move the primary flag to this lender in one transaction.

    The previous primary is demoted and this lender gets first look. The spec defaults (seat cap 15,
    ground-up minimum $500,000) fill any value the lender does not already have, so the seat cap applies to
    the new primary straight away.
    """
    _require_lender(lender_id, db)
    _lock_lenders(db)
    if not db.execute(text("SELECT active FROM lending.lender_routing_rules WHERE id = :id"), {"id": lender_id}).scalar():
        raise RoutingError("Resume the lender before making it primary.")
    now = datetime.now(timezone.utc)
    try:
        demoted = db.execute(
            text("UPDATE lending.lender_routing_rules SET is_primary = FALSE, updated_at = :now"
                 " WHERE is_primary AND id <> :id RETURNING id"),
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

def add_box(lender_id: int, body: ProductBoxIn, db: Session) -> dict:
    _require_lender(lender_id, db)
    keys = db.execute(text("SELECT rule_keys FROM lending.lender_routing_rules WHERE id = :id"), {"id": lender_id}).scalar()
    if keys:
        raise RoutingError(
            f"This lender's rules come from the T-05 rule values ({', '.join(keys)}), so a box added here would be "
            "ignored. Edit its values in lending.lender_rule_values instead.", CONFLICT)
    row_id = db.execute(
        text(
            "INSERT INTO lending.lender_product_boxes"
            "  (lender_id, loan_types, min_loan_amount, max_loan_amount,"
            "   approved_states, credit_floor, max_ltv, max_ltc,"
            "   min_completed_projects, requires_ground_up_build)"
            " VALUES (:lid, :lt, :min, :max, :states, :credit, :ltv, :ltc, :proj, :gu)"
            " RETURNING id"
        ),
        {
            "lid": lender_id, "lt": body.loan_types, "min": body.min_loan_amount, "max": body.max_loan_amount,
            "states": body.approved_states, "credit": body.credit_floor, "ltv": body.max_ltv, "ltc": body.max_ltc,
            "proj": body.min_completed_projects, "gu": body.requires_ground_up_build,
        },
    ).scalar_one()
    db.commit()
    return {"id": row_id}


def delete_box(lender_id: int, box_id: int, db: Session) -> None:
    result = db.execute(
        text("DELETE FROM lending.lender_product_boxes WHERE id = :bid AND lender_id = :lid"),
        {"bid": box_id, "lid": lender_id},
    )
    db.commit()
    if result.rowcount == 0:
        raise RoutingError("Box not found", NOT_FOUND)


# ── Decline reasons ───────────────────────────────────────────────────────────

class DeclineReasonIn(BaseModel):
    code: str = Field(min_length=1, max_length=50, pattern=r"^[a-z0-9_]+$")
    label: str = Field(min_length=1, max_length=200)

    @field_validator("label")
    @classmethod
    def _label_not_blank(cls, value: str) -> str:
        return _check_name(value)


def add_decline_reason(body: DeclineReasonIn, db: Session) -> dict:
    """Add a reason to Josh's list, or change the wording of one that exists (and switch it back on)."""
    existed = db.execute(text("SELECT 1 FROM lending.decline_reason_codes WHERE code = :c"), {"c": body.code}).scalar()
    db.execute(
        text("INSERT INTO lending.decline_reason_codes (code, label, active) VALUES (:code, :label, TRUE)"
             " ON CONFLICT (code) DO UPDATE SET label = EXCLUDED.label, active = TRUE"),
        {"code": body.code, "label": body.label},
    )
    db.commit()
    return {"code": body.code, "created": not existed}


# ── The file record: where a file is, who is next, and its history ────────────

_LEAD_FILE_ROW = (
    "SELECT 'lendingflow' AS source, fr.lead_id AS file_id, ll.first_name, ll.last_name, ll.loan_type,"
    "       ll.loan_amount, ll.property_state, NULL::text AS partner_name, fr.status,"
    "       fr.current_lender_name, fr.route_position, fr.hold_reason, fr.updated_at"
    "  FROM lending.file_routing fr"
    "  JOIN lending.lendingflow_leads ll ON ll.id = fr.lead_id"
    " WHERE fr.lead_id = :id"
)
_PARTNER_FILE_ROW = (
    "SELECT 'partner' AS source, fr.loan_file_id AS file_id, lf.borrower_first_name AS first_name,"
    "       lf.borrower_last_name AS last_name, lf.product AS loan_type,"
    "       lf.loan_amount, lf.property_state, p.name AS partner_name, fr.status,"
    "       fr.current_lender_name, fr.route_position, fr.hold_reason, fr.updated_at"
    "  FROM lending.file_routing fr"
    "  JOIN lending.loan_files lf ON lf.id = fr.loan_file_id"
    "  LEFT JOIN lending.partners p ON p.id = lf.partner_id"
    " WHERE fr.loan_file_id = :id"
)


def file_record(key: FileKey, db: Session) -> dict:
    """One file: where it is, who is next, and its full route history."""
    from src.lending.routing.router import preview_next

    row = db.execute(text(_LEAD_FILE_ROW if key.kind == LEAD else _PARTNER_FILE_ROW), {"id": key.id}).mappings().one_or_none()
    if row is None:
        raise RoutingError(f"No routing record for file {key.id}", NOT_FOUND)
    file = dict(row)
    if file["source"] == "partner":  # a partner file stores its product (flip, ground_up...); show the lender-box loan type
        product = PRODUCTS.get(file["loan_type"]) if file["loan_type"] else None
        file["loan_type"] = product.value if product is not None else None

    routing = db.execute(
        text(f"SELECT current_lender_id, lenders_tried FROM lending.file_routing WHERE {key.column} = :id"), {"id": key.id},
    ).mappings().one()
    history = db.execute(
        text("SELECT id, action, lender_id, lender_name, reason_code, free_text, actor, created_at"
             f"  FROM lending.file_routing_events WHERE {key.column} = :id ORDER BY id"),
        {"id": key.id},
    ).mappings().all()

    routed = file["status"] == "routed"
    return {
        "file": {k: file[k] for k in (
            "source", "file_id", "first_name", "last_name", "loan_type", "loan_amount",
            "property_state", "partner_name", "status", "hold_reason", "updated_at",
        )},
        "current": ({"lender_id": routing["current_lender_id"], "name": file["current_lender_name"],
                     "position": file["route_position"]} if routed else None),
        "next": preview_next(key.id, db, kind=key.kind) if file["status"] in ("routed", "unrouted") else None,
        "tried_lender_ids": list(routing["lenders_tried"] or []),
        "history": [dict(h) for h in history],
    }
