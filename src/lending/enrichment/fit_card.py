"""The lender-fit part of the enrichment card (internal only, never sent to a borrower).

Runs the T-05 evaluator, then presents the result the way Josh asked:

- Score = percent of lenders that fit (A3: 3 of 5 = 60). Backflip's rule rows count as one lender.
  The score is read from the T-05 evaluator (``LenderFitResult.lender_fit_score``), which returns None
  until every lender in the matrix is fully confirmed — a percentage over partly-unknown rules would
  read as a real answer (A1).
- Ranking (A2): Backflip first whenever it fits, then the other fitting lenders cheapest first. When
  Backflip does not fit, every other fitting lender is shown.
- A fitting lender whose cap values are still placeholder (A1): shown with note "subject to lender
  confirmation".  The eligibility checks (state, loan type, credit) used confirmed values only, so a
  fit is real — it just carries provisional sizing figures.
- A credit band that straddles a confirmed floor shows "needs confirmation on the call" (A4).  An
  unconfirmed floor is never straddled here; the evaluator already puts those lenders in ``possible``.
- A lender whose eligibility is unknown (A3) is listed as "possible, confirm on the call": not a fit and
  not a decline, and never counted in the score.
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, time, timezone
from decimal import Decimal, InvalidOperation
from typing import Callable, Mapping, Optional

from config.lender_matrix import LenderRules, field_status_of, fully_confirmed
from config.lending_enrichment import (
    BACKFLIP_KEY_PREFIX,
    BACKFLIP_LENDER,
    CREDIT_STRADDLE_NOTE,
    FIT_RULES_PENDING,
    FIT_SUBJECT_TO_CONFIRMATION,
)
from src.lending.contracts import BorrowerProfile, FieldStatus, LenderFitResult, LoanRequest, LoanType
from src.lending.lender_fit import evaluate_lender_fit
from src.lending.lender_rules_store import current_matrix

logger = logging.getLogger(__name__)

_BAND_RANGE = re.compile(r"^\s*(\d{3})\s*(?:-|to|–)\s*(\d{3})\s*$", re.IGNORECASE)

Evaluator = Callable[[BorrowerProfile, LoanRequest], LenderFitResult]


@dataclass(frozen=True)
class FitView:
    evaluated: bool
    score: Optional[int] = None
    note: Optional[str] = None
    ranked: list[str] = field(default_factory=list)
    possible: list[str] = field(default_factory=list)
    straddles: list[str] = field(default_factory=list)
    missing_fields: list[str] = field(default_factory=list)

    def to_json(self) -> dict:
        return {
            "evaluated": self.evaluated, "score": self.score, "note": self.note, "ranked": self.ranked,
            "possible": self.possible, "straddles": self.straddles, "missing_fields": self.missing_fields,
        }


def lender_group(lender_key: str) -> str:
    return BACKFLIP_LENDER if lender_key.startswith(BACKFLIP_KEY_PREFIX) else lender_key


def _group_name(rules: LenderRules) -> str:
    return "Backflip" if lender_group(rules.key) == BACKFLIP_LENDER else rules.name


def parse_band_bounds(raw: Optional[str]) -> Optional[tuple[int, int]]:
    """``"620-679"`` -> (620, 679). Open-ended (``"740+"``) and unreadable bands have no upper bound."""
    match = _BAND_RANGE.match(raw) if raw else None
    return (int(match.group(1)), int(match.group(2))) if match else None


def _straddles(band: Optional[tuple[int, int]], request: LoanRequest, matrix: tuple[LenderRules, ...]) -> list[str]:
    """Return straddle notes only for lenders with a *confirmed* credit floor.

    An unconfirmed floor is never straddled here: the T-05 evaluator already puts those lenders in
    ``possible`` (eligibility unknown).  Generating a straddle note on top of that would be a
    duplicate and would imply a credit-floor value the lender hasn't actually confirmed (A1).
    """
    if band is None:
        return []
    low, high = band
    notes: dict[str, str] = {}
    for rules in matrix:
        if field_status_of(rules, "credit_floor") is not FieldStatus.CONFIRMED or rules.credit_floor is None:
            continue
        if rules.loan_types and request.loan_type not in rules.loan_types:
            continue
        if low < rules.credit_floor <= high:
            notes.setdefault(_group_name(rules), f"{_group_name(rules)}: credit {CREDIT_STRADDLE_NOTE}")
    return list(notes.values())


def _decimal(value) -> Optional[Decimal]:
    try:
        return Decimal(str(value)) if value is not None else None
    except InvalidOperation:
        return None


def _missing(lead: Mapping) -> list[str]:
    return [name for name, value in (("loan_type", lead.get("loan_type")), ("loan_amount", lead.get("loan_amount")),
                                     ("state", lead.get("property_state"))) if not value]


def evaluate_fit(
    lead: Mapping,
    *,
    address: Optional[str],
    target_close_date=None,
    evaluator: Optional[Evaluator] = None,
    matrix: Optional[tuple[LenderRules, ...]] = None,
) -> FitView:
    """Fit view for one LendingFlow lead row. Never raises for missing inputs: it reports them."""
    matrix = current_matrix() if matrix is None else matrix
    missing = _missing(lead)
    amount = _decimal(lead.get("loan_amount"))
    if missing or amount is None:
        return FitView(evaluated=False, note="Not evaluated: missing " + ", ".join(missing or ["loan_amount"]),
                       missing_fields=missing or ["loan_amount"])
    try:
        loan_type = LoanType(lead["loan_type"])
    except ValueError:
        return FitView(evaluated=False, note="Not evaluated: unrecognised loan type", missing_fields=["loan_type"])
    close_dt = (
        datetime.combine(target_close_date, time.min, tzinfo=timezone.utc) if target_close_date is not None else None
    )
    request = LoanRequest(loan_type=loan_type, loan_amount=amount, state=lead["property_state"],
                          property_address=address, target_close_date=close_dt)
    band = parse_band_bounds(lead.get("credit_band"))
    borrower = BorrowerProfile(credit_band_min_fico=lead.get("credit_band_min_fico"),
                               credit_band_max_fico=band[1] if band else None, state=lead["property_state"])
    result = (evaluator or (lambda b, r: evaluate_lender_fit(b, r, matrix=matrix)))(borrower, request)

    fitting_groups = [lender_group(f.lender_key) for f in result.fitting]  # T-05 order: cheapest first
    names_by_group = {lender_group(r.key): _group_name(r) for r in matrix}
    ordered = [g for g in dict.fromkeys(fitting_groups)]
    if BACKFLIP_LENDER in ordered:
        ordered.remove(BACKFLIP_LENDER)
        ordered.insert(0, BACKFLIP_LENDER)

    fitting_set = set(fitting_groups)
    possible_groups = [g for g in dict.fromkeys(lender_group(p.lender_key) for p in result.possible)
                       if g not in fitting_set]

    # Score = % of unique lenders that fit (Josh: "3 of 5 = 60"). Backflip's two rule rows are one
    # lender, so we group by key prefix before counting — the T-05 evaluator's lender_fit_score
    # counts raw rows (Q4 pending), which would give the wrong denominator (6 vs 5).
    # Shown only once every lender is fully confirmed (fully_confirmed replaces the old r.verified
    # binary flag; a lender with field_status entries can be partially confirmed — A1).
    all_groups = set(names_by_group)
    all_confirmed = bool(matrix) and all(fully_confirmed(r) for r in matrix)
    score = round(100 * len(set(fitting_groups)) / len(all_groups)) if all_confirmed and all_groups else None

    # Note priority (A1):
    # - Some fitting lenders used placeholder cap values → "subject to lender confirmation".
    #   This takes priority over the generic "rules pending" message when fits DO exist, because it
    #   tells Josh that the fit is real but sizing is provisional (e.g., using the 85%/70% defaults).
    # - No fitting lenders and rules are not all confirmed → "Lender rules pending confirmation".
    # - All lenders confirmed (score visible) → no note; nothing is provisional.
    # Note: when all_confirmed=True, fully_confirmed(r) is True for every lender, so
    # subject_to_confirmation=False for all fitting entries, making any_provisional always False
    # in that branch. The note is therefore None, which is correct.
    any_provisional = any(f.subject_to_confirmation for f in result.fitting)
    if any_provisional:
        note: Optional[str] = FIT_SUBJECT_TO_CONFIRMATION
    elif score is None:
        note = FIT_RULES_PENDING
    else:
        note = None

    return FitView(
        evaluated=True,
        score=score,
        note=note,
        ranked=[names_by_group.get(g, g) for g in ordered],
        possible=[names_by_group.get(g, g) for g in possible_groups],
        straddles=_straddles(band, request, matrix),
        missing_fields=list(result.missing_fields),
    )
