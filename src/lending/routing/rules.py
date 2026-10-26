"""Database-backed lender rule loader and box evaluator.

Replaces the in-code ``config/lender_matrix.py`` for routing purposes.  The
T-05 evaluator (``src.lending.lender_fit``) continues to exist for the
enrichment card; this module is the routing engine's own, DB-backed
equivalent.

Design: all lenders, including the primary lender, are rows in
``lending.lender_routing_rules``.  The router never names any specific lender.

Where a lender's eligibility comes from (one source of truth, no copies of the same numbers):
  - a lender with ``rule_keys`` uses the T-05 rule values for those keys (``config/lender_matrix.py``, with
    ``lending.lender_rule_values`` applied when ``LENDING_RULES_FROM_DB`` is on): the same values and
    statuses the lead card's fit score reads.
  - a lender without ``rule_keys`` (one T-05 does not define) uses its own ``lending.lender_product_boxes``.
  - a lender with neither has no known rules and is never routed to; the file goes to the hold queue.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from config.lender_matrix import LENDER_MATRIX, LenderRules
from config.settings import get_settings
from src.lending import leverage
from src.lending.lender_rules_store import load_matrix

logger = logging.getLogger(__name__)


# ── Data structures ────────────────────────────────────────────────────────


class FailureKind(StrEnum):
    """What a broken box rule was about, so the primary's bypass reason never depends on message wording."""

    STATE = "state"
    LEVERAGE = "leverage"
    OTHER = "other"


class BypassReason(StrEnum):
    """``file_routing_events.metadata.primary_bypass_reason`` (Chunk 1 Addendum 1.2.1)."""

    SEAT_CAP = "SEAT_CAP"
    BOX_FAIL = "BOX_FAIL"
    LEVERAGE_EXCEEDED = "LEVERAGE_EXCEEDED"
    OUT_OF_FOOTPRINT = "OUT_OF_FOOTPRINT"


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

    An unknown credit band or experience count (None) means that check is not applied, in line with
    "possible, confirm on the call": the router recommends a lender and Josh submits manually.  A box's
    max_ltv / max_ltc apply only when the file carries the figures (partner files do; LendingFlow leads do not,
    so their leverage is checked on the call: PENDING_CALL_FACTS).
    """

    loan_type: str               # LoanType value, e.g. 'FIX_AND_FLIP'
    loan_amount: int             # USD, band minimum (Q6)
    state: str                   # 2-letter USPS code
    credit_band_min_fico: Optional[int] = None
    completed_projects: Optional[int] = None
    purchase_price: Optional[int] = None
    rehab_budget: Optional[int] = None
    arv: Optional[int] = None


# ── T-05 rule values -> boxes ─────────────────────────────────────────────


def _box_from_t05(rules: LenderRules) -> ProductBox:
    """A routing box from one T-05 rule row.  An unset limit means "not enforced", exactly as in T-05."""
    return ProductBox(
        loan_types=frozenset(t.value for t in rules.loan_types),
        min_loan_amount=int(rules.min_loan_amount) if rules.min_loan_amount is not None else None,
        max_loan_amount=int(rules.max_loan_amount) if rules.max_loan_amount is not None else None,
        approved_states=frozenset(rules.approved_states) if rules.approved_states else None,
        credit_floor=rules.credit_floor,
        max_ltv=float(rules.max_ltv) if rules.max_ltv is not None else None,
        max_ltc=float(rules.max_ltc) if rules.max_ltc is not None else None,
        min_completed_projects=rules.min_completed_projects,
        requires_ground_up_build=rules.requires_ground_up_build,
    )


def _t05_boxes(db: Session, keys_by_lender: dict[int, list[str]]) -> dict[int, list[ProductBox]]:
    """Boxes for every lender that names T-05 ``rule_keys``, read once per load.

    Uses the same switch as the fit card: the rule-values table when ``LENDING_RULES_FROM_DB`` is on, else the
    config.  A key T-05 does not define yields no box for it (so, with no other key, the lender is never routed).
    """
    if not keys_by_lender:
        return {}
    matrix = load_matrix(db) if get_settings().lending_rules_from_db else LENDER_MATRIX
    by_key = {rules.key: rules for rules in matrix}
    boxes: dict[int, list[ProductBox]] = {}
    for lender_id, keys in keys_by_lender.items():
        found = [by_key[k] for k in keys if k in by_key]
        for missing in (k for k in keys if k not in by_key):
            logger.error("lender_routing_rules id=%d names unknown T-05 rule key %r; it adds no box", lender_id, missing)
        boxes[lender_id] = [_box_from_t05(r) for r in found]
    return boxes


# ── Loader ────────────────────────────────────────────────────────────────


def load_active_lenders(db: Session) -> list[LenderRow]:
    """Load all active lenders with their product boxes, ordered by priority asc.

    Only ``active=True`` rows are returned.  ``agreement_status`` is kept in the
    result so the router can filter on 'signed' without a second query.
    """
    lender_rows = db.execute(
        text(
            "SELECT id, name, active, is_primary, agreement_status, priority,"
            "       monthly_cap, construction_min_amount, takes_ground_up, rule_keys"
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
            loan_types=frozenset(str(t).upper() for t in (b["loan_types"] or [])),
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

    t05_boxes = _t05_boxes(db, {r["id"]: r["rule_keys"] for r in lender_rows if r["rule_keys"]})
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
            boxes=tuple(t05_boxes[r["id"]] if r["rule_keys"] else boxes_by_lender.get(r["id"], [])),
        )
        for r in lender_rows
    ]


# ── Box evaluator ────────────────────────────────────────────────────────


def _box_failures(box: ProductBox, profile: FileProfile) -> list[tuple[FailureKind, str]]:
    """Every rule of one box the file breaks, each with its kind. A box with an unset limit does not enforce it."""
    if box.loan_types and profile.loan_type not in box.loan_types:
        return [(FailureKind.OTHER, f"loan_type {profile.loan_type!r} not accepted")]  # the rest is moot

    failures: list[tuple[FailureKind, str]] = []
    if box.approved_states and profile.state not in box.approved_states:
        failures.append((FailureKind.STATE, f"state {profile.state!r} not in approved states"))
    if box.min_loan_amount is not None and profile.loan_amount < box.min_loan_amount:
        failures.append((FailureKind.OTHER,
                         f"loan_amount ${profile.loan_amount:,} below ${box.min_loan_amount:,} minimum"))
    if box.max_loan_amount is not None and profile.loan_amount > box.max_loan_amount:
        failures.append((FailureKind.OTHER,
                         f"loan_amount ${profile.loan_amount:,} exceeds ${box.max_loan_amount:,} maximum"))
    if box.credit_floor is not None and profile.credit_band_min_fico is not None:
        if profile.credit_band_min_fico < box.credit_floor:
            failures.append((FailureKind.OTHER,
                             f"credit {profile.credit_band_min_fico} below floor {box.credit_floor}"))
    if box.min_completed_projects > 0 and profile.completed_projects is not None:
        if profile.completed_projects < box.min_completed_projects:
            failures.append((FailureKind.OTHER, f"{profile.completed_projects} project(s) below "
                                                f"{box.min_completed_projects}+ minimum"))

    deal_ltv = leverage.ltv(profile.loan_amount, profile.arv)
    if leverage.over_cap(deal_ltv, box.max_ltv):
        failures.append((FailureKind.LEVERAGE, f"LTV ({leverage.requested_vs_max(deal_ltv, box.max_ltv)})"))
    deal_ltc = leverage.ltc(profile.loan_amount, profile.purchase_price, profile.rehab_budget)
    if leverage.over_cap(deal_ltc, box.max_ltc):
        failures.append((FailureKind.LEVERAGE, f"LTC ({leverage.requested_vs_max(deal_ltc, box.max_ltc)})"))
    return failures


def _box_matches(box: ProductBox, profile: FileProfile) -> tuple[bool, list[str]]:
    """Return (matches, rejection_reasons) for one product box."""
    failures = _box_failures(box, profile)
    return not failures, [reason for _, reason in failures]


def primary_bypass_reason(lender: LenderRow, profile: FileProfile, at_cap: bool) -> BypassReason:
    """Why the primary did not take a file it would have by amount (Chunk 1 Addendum 1.2.1).

    SEAT_CAP when its monthly seats are used up. Otherwise, a box that failed on leverage alone gives
    LEVERAGE_EXCEEDED, then one that failed on state alone gives OUT_OF_FOOTPRINT; any other mix is BOX_FAIL.
    """
    if at_cap:
        return BypassReason.SEAT_CAP
    only: set[FailureKind] = set()
    for box in lender.boxes:
        kinds = {kind for kind, _ in _box_failures(box, profile)}
        if len(kinds) == 1:
            only |= kinds
    if FailureKind.LEVERAGE in only:
        return BypassReason.LEVERAGE_EXCEEDED
    if FailureKind.STATE in only:
        return BypassReason.OUT_OF_FOOTPRINT
    return BypassReason.BOX_FAIL


def lender_passes_box(lender: LenderRow, profile: FileProfile) -> bool:
    """True when the file passes at least one of the lender's product boxes.

    A lender with no boxes has no known rules, so it never matches: unknown eligibility must not mean
    "accepts everything" (the file goes on to the next lender or the hold queue instead).
    """
    return any(_box_matches(box, profile)[0] for box in lender.boxes)


def primary_lender(lenders: list[LenderRow]) -> Optional[LenderRow]:
    """Return the primary signed-active lender, or None."""
    for lr in lenders:
        if lr.is_primary and lr.agreement_status in {"signed"}:
            return lr
    return None
