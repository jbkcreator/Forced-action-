"""Database-backed lender rule loader and box evaluator.

Replaces the in-code ``config/lender_matrix.py`` for routing purposes.  The
T-05 evaluator (``src.lending.lender_fit``) continues to exist for the
enrichment card; this module is the routing engine's own, DB-backed
equivalent.

Design: all lenders — including the primary lender — are rows in
``lending.lender_routing_rules``.  Their eligibility constraints live in
``lending.lender_product_boxes``.  The router never names any specific lender.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


# ── Data structures ────────────────────────────────────────────────────────


@dataclass(frozen=True)
class ProductBox:
    """One eligibility rule set for one lender + loan-type group."""

    loan_types: frozenset[str]
    min_loan_amount: Optional[int]
    max_loan_amount: Optional[int]
    approved_states: Optional[frozenset[str]]  # None = all states
    credit_floor: Optional[int]
    max_ltv: Optional[float]
    max_ltc: Optional[float]
    min_completed_projects: int
    requires_ground_up_build: bool


@dataclass(frozen=True)
class LenderRow:
    """A fully-loaded lender rule row (lender + its boxes)."""

    id: int
    name: str
    active: bool
    is_primary: bool
    agreement_status: str
    priority: int
    monthly_cap: Optional[int]
    construction_min_amount: Optional[int]
    takes_ground_up: bool
    boxes: tuple[ProductBox, ...]


@dataclass(frozen=True)
class FileProfile:
    """Minimal file facts the router needs to evaluate rules.

    Q6 (open): loan_amount uses the band minimum from lendingflow_leads.loan_amount.
    Josh to confirm whether min or max should be used for threshold checks.
    """

    loan_type: str               # LoanType value, e.g. 'fix_and_flip'
    loan_amount: int             # USD, band minimum (Q6)
    state: str                   # 2-letter USPS code
    credit_band_min_fico: Optional[int] = None
    completed_projects: Optional[int] = None
    arv: Optional[int] = None
    purchase_price: Optional[int] = None
    rehab_budget: Optional[int] = None


# ── Loader ────────────────────────────────────────────────────────────────


def load_active_lenders(db: Session) -> list[LenderRow]:
    """Load all active lenders with their product boxes, ordered by priority asc.

    Only ``active=True`` rows are returned.  ``agreement_status`` is kept in the
    result so the router can filter on 'signed' without a second query.
    """
    lender_rows = db.execute(
        text(
            "SELECT id, name, active, is_primary, agreement_status, priority,"
            "       monthly_cap, construction_min_amount, takes_ground_up"
            "  FROM lending.lender_routing_rules"
            " WHERE active = TRUE"
            " ORDER BY priority ASC, id ASC"
        )
    ).mappings().all()

    if not lender_rows:
        return []

    lender_ids = [r["id"] for r in lender_rows]
    box_rows = db.execute(
        text(
            "SELECT lender_id, loan_types, min_loan_amount, max_loan_amount,"
            "       approved_states, credit_floor, max_ltv, max_ltc,"
            "       min_completed_projects, requires_ground_up_build"
            "  FROM lending.lender_product_boxes"
            " WHERE lender_id = ANY(:ids)"
        ),
        {"ids": lender_ids},
    ).mappings().all()

    boxes_by_lender: dict[int, list[ProductBox]] = {}
    for b in box_rows:
        box = ProductBox(
            loan_types=frozenset(b["loan_types"] or []),
            min_loan_amount=b["min_loan_amount"],
            max_loan_amount=b["max_loan_amount"],
            approved_states=frozenset(b["approved_states"]) if b["approved_states"] else None,
            credit_floor=b["credit_floor"],
            max_ltv=float(b["max_ltv"]) if b["max_ltv"] is not None else None,
            max_ltc=float(b["max_ltc"]) if b["max_ltc"] is not None else None,
            min_completed_projects=b["min_completed_projects"] or 0,
            requires_ground_up_build=bool(b["requires_ground_up_build"]),
        )
        boxes_by_lender.setdefault(b["lender_id"], []).append(box)

    return [
        LenderRow(
            id=r["id"],
            name=r["name"],
            active=r["active"],
            is_primary=r["is_primary"],
            agreement_status=r["agreement_status"],
            priority=r["priority"],
            monthly_cap=r["monthly_cap"],
            construction_min_amount=r["construction_min_amount"],
            takes_ground_up=r["takes_ground_up"],
            boxes=tuple(boxes_by_lender.get(r["id"], [])),
        )
        for r in lender_rows
    ]


# ── Box evaluator ────────────────────────────────────────────────────────


def _box_matches(box: ProductBox, profile: FileProfile) -> tuple[bool, list[str]]:
    """Return (matches, rejection_reasons) for one product box.

    A lender with no boxes has no restrictions and accepts everything.
    """
    reasons: list[str] = []

    if box.loan_types and profile.loan_type not in box.loan_types:
        reasons.append(f"loan_type {profile.loan_type!r} not accepted")
        return False, reasons  # short-circuit: rest is moot

    if box.approved_states and profile.state not in box.approved_states:
        reasons.append(f"state {profile.state!r} not in approved states")

    if box.min_loan_amount is not None and profile.loan_amount < box.min_loan_amount:
        reasons.append(
            f"loan_amount ${profile.loan_amount:,} below ${box.min_loan_amount:,} minimum"
        )
    if box.max_loan_amount is not None and profile.loan_amount > box.max_loan_amount:
        reasons.append(
            f"loan_amount ${profile.loan_amount:,} exceeds ${box.max_loan_amount:,} maximum"
        )

    if box.credit_floor is not None and profile.credit_band_min_fico is not None:
        if profile.credit_band_min_fico < box.credit_floor:
            reasons.append(
                f"credit {profile.credit_band_min_fico} below floor {box.credit_floor}"
            )

    if box.min_completed_projects > 0 and profile.completed_projects is not None:
        if profile.completed_projects < box.min_completed_projects:
            reasons.append(
                f"{profile.completed_projects} project(s) below "
                f"{box.min_completed_projects}+ minimum"
            )

    return len(reasons) == 0, reasons


def lender_passes_box(lender: LenderRow, profile: FileProfile) -> bool:
    """True when the file passes at least one of the lender's product boxes."""
    if not lender.boxes:
        return True  # no boxes = no restrictions
    return any(_box_matches(box, profile)[0] for box in lender.boxes)


def primary_lender(lenders: list[LenderRow]) -> Optional[LenderRow]:
    """Return the primary signed-active lender, or None."""
    for lr in lenders:
        if lr.is_primary and lr.agreement_status in {"signed"}:
            return lr
    return None
