"""Runbook §8 Test 2 (split T-27): sample borrowers through the real Lender Box engine.

Each profile carries the pre-qual lead (what the letter shows) plus the call facts the engine
needs (experience, purchase price, rehab, ARV). ``evaluate_profile`` runs ``evaluate_lender_fit``
on the current matrix and returns:

  - qualifies / limits: fitting lenders' loan bounds, the input to the pre-qual letter;
  - ranking: fitting lenders in the engine's order (Josh A2: Backflip first, then lowest
    provisional total borrower cost, unknown cost last), each marked confirmed or
    "subject to lender confirmation";
  - failure_codes: the engine's ``DeclineReason`` codes when no lender fits, from lenders that
    offer the requested loan type only; lenders that do not offer it are listed separately in
    ``loan_type_not_offered`` (the engine reports that miss as the catch-all ``other``).

Internal acceptance output only: never sent to a borrower (lender_fit COMPLIANCE). The letter
PDF itself is unchanged (B1/B4 sign-off pending).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Optional

from config.lender_matrix import LenderRules, effective_rules
from src.lending.contracts import BorrowerProfile, LoanRequest, LoanType
from src.lending.lender_fit import evaluate_lender_fit
from src.lending.prequal import FitLimits, PrequalLead

SUBJECT_TO_CONFIRMATION = "subject to lender confirmation"
CONFIRMED = "confirmed"
# lender_fit._total_borrower_cost; formula pending Josh (Q2), None while points/spread unconfirmed.
COST_BASIS = "provisional: loan x origination points + loan x rate spread x hold months / 12 (Q2 pending)"
COST_UNKNOWN = "unknown (points or rate spread not confirmed)"


# Below every lender's credit floor ("Below 640" band carries no FICO).
FICO_BELOW_EVERY_FLOOR = 0


@dataclass(frozen=True)
class DealFacts:
    purchase: int
    rehab: int
    arv: int


@dataclass(frozen=True)
class RankedLender:
    rank: int
    lender_key: str
    lender_name: str
    est_total_borrower_cost: str
    cost_basis: str
    status: str


@dataclass(frozen=True)
class Test2Profile:
    name: str
    lead: PrequalLead
    borrower: BorrowerProfile
    loan: LoanRequest


@dataclass
class ProfileOutcome:
    name: str
    qualifies: bool
    limits: list[FitLimits] = field(default_factory=list)
    ranking: list[RankedLender] = field(default_factory=list)
    failure_codes: list[str] = field(default_factory=list)
    failure_codes_by_lender: dict[str, list[str]] = field(default_factory=dict)
    loan_type_not_offered: list[str] = field(default_factory=list)
    possible: list[str] = field(default_factory=list)


def _limits(rules: LenderRules) -> FitLimits:
    # TODO(PR #367): replace with prequal_fit.fit_limits_from_result once #367 merges; same bounds
    # as prequal_fit._limits, copied here so this branch does not edit prequal_fit.py.
    return FitLimits(
        min_amount=int(rules.min_loan_amount) if rules.min_loan_amount is not None else 0,
        max_amount=int(rules.max_loan_amount) if rules.max_loan_amount is not None else None,
    )


def _ranked(i: int, fit) -> RankedLender:
    cost = str(fit.total_borrower_cost) if fit.total_borrower_cost is not None else COST_UNKNOWN
    status = SUBJECT_TO_CONFIRMATION if fit.subject_to_confirmation else CONFIRMED
    return RankedLender(i, fit.lender_key, fit.lender_name, cost, COST_BASIS, status)


def evaluate_profile(profile: Test2Profile, *, matrix: tuple[LenderRules, ...]) -> ProfileOutcome:
    result = evaluate_lender_fit(profile.borrower, profile.loan, matrix=matrix)
    effective = {rules.key: effective_rules(rules) for rules in matrix}
    not_offered = sorted(key for key, rules in effective.items()
                         if rules.loan_types and profile.loan.loan_type not in rules.loan_types)
    limits = [_limits(effective[fit.lender_key]) for fit in result.fitting if fit.lender_key in effective]
    by_lender = {miss.lender_key: [code.value for code in miss.reason_codes]
                 for miss in result.non_fitting if miss.lender_key not in not_offered}
    qualifies = bool(result.fitting)
    return ProfileOutcome(
        name=profile.name,
        qualifies=qualifies,
        limits=limits,
        ranking=[_ranked(i, fit) for i, fit in enumerate(result.fitting, start=1)],
        failure_codes=[] if qualifies else sorted({c for codes in by_lender.values() for c in codes}),
        failure_codes_by_lender={} if qualifies else by_lender,
        loan_type_not_offered=[] if qualifies else not_offered,
        possible=[p.lender_key for p in result.possible],
    )


def _profile(name: str, credit_band: str, fico: int, state: str, loan_type: LoanType, amount: int,
             deal: DealFacts, *, projects: int, builds: int = 0) -> Test2Profile:
    return Test2Profile(
        name=name,
        lead=PrequalLead(credit_band, amount, state, loan_type.value),
        borrower=BorrowerProfile(credit_band_min_fico=fico, state=state, completed_projects=projects,
                                 completed_ground_up_builds=builds, has_live_deal=True, has_liquidity=True),
        loan=LoanRequest(loan_type=loan_type, loan_amount=Decimal(amount), state=state, property_type="SFR",
                         purchase_price=Decimal(deal.purchase), rehab_budget=Decimal(deal.rehab),
                         arv=Decimal(deal.arv)),
    )


# Sample borrowers (spec §5 Test 2): fix and flip, experienced builder, credit-challenged investor,
# and a borrower below every credit floor. Demo inputs, not real leads.
TEST2_PROFILES: tuple[Test2Profile, ...] = (
    _profile("1_fix_and_flip", "700+", 700, "FL", LoanType.FIX_AND_FLIP, 360_000,
             DealFacts(purchase=350_000, rehab=100_000, arv=650_000), projects=3),
    _profile("2_experienced_builder", "740+", 740, "FL", LoanType.GROUND_UP_CONSTRUCTION, 1_200_000,
             DealFacts(purchase=400_000, rehab=1_100_000, arv=2_200_000), projects=6, builds=4),
    _profile("3_credit_challenged_investor", "640-679", 640, "GA", LoanType.FIX_AND_FLIP, 220_000,
             DealFacts(purchase=220_000, rehab=60_000, arv=420_000), projects=1),
    _profile("4_non_qualifying", "Below 640", FICO_BELOW_EVERY_FLOOR, "FL", LoanType.FIX_AND_FLIP, 300_000,
             DealFacts(purchase=300_000, rehab=80_000, arv=500_000), projects=0),
)
