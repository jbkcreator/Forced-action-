"""The lender-fit part of the enrichment card (internal only, never sent to a borrower).

Runs the T-05 evaluator, then presents the result the way Josh asked:

- Score = percent of lenders that fit (A3: 3 of 5 = 60). Backflip's rule rows count as one lender.
  The score is withheld while any lender's rules are unverified (A1: all five still pending): a
  percentage over a partly-unknown matrix would read as a real answer.
- Ranking (A2): Backflip first whenever it fits, then the other fitting lenders cheapest first. When
  Backflip does not fit, every other fitting lender is shown.
- A credit band that straddles a lender's floor shows "needs confirmation on the call" (A4).
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, time, timezone
from decimal import Decimal, InvalidOperation
from typing import Callable, Mapping, Optional

from config.lender_matrix import LENDER_MATRIX, LenderRules
from config.lending_enrichment import (
    BACKFLIP_KEY_PREFIX,
    BACKFLIP_LENDER,
    CREDIT_STRADDLE_NOTE,
    FIT_RULES_PENDING,
)
from src.lending.contracts import BorrowerProfile, LenderFitResult, LoanRequest, LoanType
from src.lending.lender_fit import evaluate_lender_fit

logger = logging.getLogger(__name__)

_BAND_RANGE = re.compile(r"^\s*(\d{3})\s*(?:-|to|–)\s*(\d{3})\s*$", re.IGNORECASE)

Evaluator = Callable[[BorrowerProfile, LoanRequest], LenderFitResult]


@dataclass(frozen=True)
class FitView:
    evaluated: bool
    score: Optional[int] = None
    note: Optional[str] = None
    ranked: list[str] = field(default_factory=list)
    straddles: list[str] = field(default_factory=list)
    missing_fields: list[str] = field(default_factory=list)

    def to_json(self) -> dict:
        return {
            "evaluated": self.evaluated, "score": self.score, "note": self.note, "ranked": self.ranked,
            "straddles": self.straddles, "missing_fields": self.missing_fields,
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
    if band is None:
        return []
    low, high = band
    notes: dict[str, str] = {}
    for rules in matrix:
        if not rules.verified or rules.credit_floor is None:
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
    matrix: tuple[LenderRules, ...] = LENDER_MATRIX,
) -> FitView:
    """Fit view for one LendingFlow lead row. Never raises for missing inputs: it reports them."""
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
    borrower = BorrowerProfile(credit_band_min_fico=lead.get("credit_band_min_fico"), state=lead["property_state"])
    result = (evaluator or (lambda b, r: evaluate_lender_fit(b, r, matrix=matrix)))(borrower, request)

    fitting_groups = [lender_group(f.lender_key) for f in result.fitting]  # T-05 order: cheapest first
    names_by_group = {lender_group(r.key): _group_name(r) for r in matrix}
    ordered = [g for g in dict.fromkeys(fitting_groups)]
    if BACKFLIP_LENDER in ordered:
        ordered.remove(BACKFLIP_LENDER)
        ordered.insert(0, BACKFLIP_LENDER)

    all_groups = set(names_by_group)
    all_verified = bool(matrix) and all(r.verified for r in matrix)
    score = round(100 * len(set(fitting_groups)) / len(all_groups)) if all_verified and all_groups else None
    return FitView(
        evaluated=True,
        score=score,
        note=None if all_verified else FIT_RULES_PENDING,
        ranked=[names_by_group.get(g, g) for g in ordered],
        straddles=_straddles(parse_band_bounds(lead.get("credit_band")), request, matrix),
        missing_fields=list(result.missing_fields),
    )
