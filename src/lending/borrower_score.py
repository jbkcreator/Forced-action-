"""Borrower lead score 0–10 (Runbook § 4.3, Item 3), so callers work the best borrower leads first.

Pure function over explicit inputs: no clock, no database. Each criterion earns its points only from a known input;
an unknown input scores 0 and is listed in ``missing``, never guessed. Points and thresholds: config/lending_borrower_score.py.

Data today (2026-10-19): "under contract" is stored nowhere, so it is always missing; completed projects exist only
on website leads (form band, e.g. "3-5"); the property state exists only on LendingFlow leads; the loan amount only on
LendingFlow leads (``loan_amount`` = the low end of the requested band). Whether this score replaces or sits beside
src/lending/lead_scoring.py, and how call queues read it, is open (Runbook Q-18, Dev 2 T-08).
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Mapping, Optional, Union

from config.lending_borrower_score import (
    EXPERIENCE_MIN_PROJECTS,
    EXPERIENCE_POINTS,
    LOAN_SIZE_MIN,
    LOAN_SIZE_POINTS,
    TARGET_STATE_POINTS,
    TARGET_STATES,
    UNDER_CONTRACT_POINTS,
)

UNDER_CONTRACT = "under_contract"
EXPERIENCE = "experience_3_projects_36m"
TARGET_STATE = "property_in_fl_or_ga"
LOAN_SIZE = "loan_300k_or_more"


@dataclass(frozen=True)
class BorrowerFacts:
    under_contract: Optional[bool] = None
    completed_projects_36m: Optional[int] = None
    property_state: Optional[str] = None  # USPS code
    loan_amount: Optional[Union[int, Decimal]] = None


@dataclass(frozen=True)
class BorrowerScore:
    score: int
    earned: tuple[tuple[str, int], ...]  # (criterion, points) for each criterion met
    missing: tuple[str, ...]  # criteria whose input is unknown


def score_borrower(facts: BorrowerFacts) -> BorrowerScore:
    checks = (
        (UNDER_CONTRACT, UNDER_CONTRACT_POINTS, facts.under_contract, lambda v: v is True),
        (EXPERIENCE, EXPERIENCE_POINTS, facts.completed_projects_36m, lambda v: v >= EXPERIENCE_MIN_PROJECTS),
        (TARGET_STATE, TARGET_STATE_POINTS, (facts.property_state or "").strip().upper() or None,
         lambda v: v in TARGET_STATES),
        (LOAN_SIZE, LOAN_SIZE_POINTS, facts.loan_amount, lambda v: v >= LOAN_SIZE_MIN),
    )
    earned = tuple((name, points) for name, points, value, met in checks if value is not None and met(value))
    missing = tuple(name for name, _, value, _ in checks if value is None)
    return BorrowerScore(score=sum(points for _, points in earned), earned=earned, missing=missing)


_FIRST_NUMBER = re.compile(r"\d+")
_NONE_ANSWER = re.compile(r"^(none|zero|no)\b")


def projects_from_band(band: Optional[str]) -> Optional[int]:
    """The lowest project count a website form band states ("1 to 2" -> 1, "6+" -> 6, "None yet" -> 0); None when
    the band is empty or holds no number, so an unclear answer is missing rather than guessed. Bands seen on the live
    form (2026-10-19): "None yet", "1 to 2"."""
    if not band or not band.strip():
        return None
    if _NONE_ANSWER.match(band.strip().lower()):
        return 0
    found = _FIRST_NUMBER.search(band)
    return int(found.group()) if found else None


def facts_from_lendingflow_lead(lead: Mapping[str, Any]) -> BorrowerFacts:
    """From a lending.lendingflow_leads row: state and loan amount; LendingFlow sends no contract or experience."""
    return BorrowerFacts(property_state=lead.get("property_state"), loan_amount=lead.get("loan_amount"))


def facts_from_web_lead(lead: Mapping[str, Any]) -> BorrowerFacts:
    """From a lending.web_leads row: completed projects only; the form asks no state, amount or contract question."""
    return BorrowerFacts(completed_projects_36m=projects_from_band(lead.get("completed_projects_3y")))
