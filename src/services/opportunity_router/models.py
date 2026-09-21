"""WP-T2-11 — pure domain models for the opportunity router.

No I/O, no DB, no network. Everything here is a plain dataclass or enum.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import List, Literal, Optional


class GyrColor(str, Enum):
    GREEN = "green"
    YELLOW = "yellow"
    RED = "red"


class YellowReason(str, Enum):
    MISSING_ARV = "missing_arv"
    LOW_ARV_CONFIDENCE = "low_arv_confidence"
    BORROWER_NOT_CONTACTED = "borrower_not_contacted"
    PROGRAM_MISMATCH_EXCEPTION_POSSIBLE = "program_mismatch_exception_possible"
    BELOW_REVENUE_FLOOR = "below_revenue_floor"


class RedReason(str, Enum):
    ALREADY_FUNDED = "already_funded"
    DEAD_SIGNAL = "dead_signal"
    RECYCLED = "recycled"
    REFERRED = "referred"
    BORROWER_SUPPRESSED = "borrower_suppressed"
    OUT_OF_BOX = "out_of_box"


GyrQueue = Literal["MONEY", "EXCEPTIONS"]


@dataclass(frozen=True)
class RouterConfig:
    green_min_expected_revenue_cents: int
    config_version: str = "1"


@dataclass(frozen=True)
class RouterContext:
    """All assembled inputs needed to classify one opportunity."""

    opportunity_id: str
    person_id: str
    outcome: str
    opportunity_type: str
    loan_amount_cents: Optional[int]
    maturity_months: Optional[int]
    expected_need_date: Optional[datetime]

    # Lender Box
    lender_box_status: Literal["in_box", "out_of_box", "uncertain"]
    lender_box_fail_reasons: List[str]
    lender_box_uncertain_flags: List[str]

    # ARV from scenario builder (WP-8A/8B)
    arv: Optional[Decimal]
    arv_confidence: Optional[Literal["high", "low"]]  # maps to LoanConfidence

    # Dial-list candidate (assembled by assemble.py for scoring reuse)
    dial_candidate_assessed_value: Optional[Decimal]
    dial_candidate_last_sale_price: Optional[Decimal]
    dial_candidate_arv: Optional[Decimal]
    dial_candidate_max_ltc: Optional[Decimal]
    dial_candidate_loan_override: Optional[Decimal]
    dial_candidate_loan_override_confidence: Optional[Literal["high", "low"]]
    dial_candidate_triggers: List[str]
    dial_candidate_intent_tier: Optional[Literal["high", "medium", "low"]]
    dial_candidate_is_builder: bool
    dial_candidate_urgency_date: Optional[date]

    # Suppression
    suppressed: bool  # True if EVERY known channel is suppressed

    # Contact state
    has_interaction: bool  # any fa_max_interactions row for this person

    as_of: date


@dataclass(frozen=True)
class RoutingDecision:
    color: GyrColor
    expected_revenue_cents: int
    reason_codes: List[str]
    disqualifying_rule: Optional[str]
    queue: Optional[GyrQueue]
