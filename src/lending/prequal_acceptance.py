"""Runbook §8 Test 2 (split T-27): sample borrowers through the real Lender Box engine.

Each profile carries the pre-qual lead (what the letter shows) plus the call facts the engine
needs (experience, purchase price, rehab, ARV). ``evaluate_profile`` runs ``evaluate_lender_fit``
on the current matrix and returns:

  - qualifies / limits: fitting lenders' loan bounds, the input to the pre-qual letter;
  - ranking: fitting lenders in the engine's order (Josh A2: Backflip first, then lowest
    provisional total borrower cost, unknown cost last), each marked confirmed or
    "subject to lender confirmation";
  - failure_codes: the engine's ``DeclineReason`` codes when no lender fits.

Internal acceptance output only: never sent to a borrower (lender_fit COMPLIANCE). The letter
PDF itself is unchanged (B1/B4 sign-off pending).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Optional

from config.lender_matrix import LenderRules, effective_rules, fully_confirmed
from src.lending.contracts import BorrowerProfile, LoanRequest, LoanType
from src.lending.lender_fit import evaluate_lender_fit
from src.lending.prequal import FitLimits, PrequalLead

SUBJECT_TO_CONFIRMATION = "subject to lender confirmation"
CONFIRMED = "confirmed"
# lender_fit._total_borrower_cost; formula pending Josh (Q2), None while points/spread unconfirmed.
COST_BASIS = "provisional: loan x origination points + loan x rate spread x hold months / 12 (Q2 pending)"
COST_UNKNOWN = "unknown (points or rate spread not confirmed)"


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
    ranking: list[dict[str, Any]] = field(default_factory=list)
    failure_codes: list[str] = field(default_factory=list)
    failure_codes_by_lender: dict[str, list[str]] = field(default_factory=dict)
    possible: list[str] = field(default_factory=list)


def _limits(rules: LenderRules) -> FitLimits:
    # Same bounds as prequal_fit._limits (PR #367 exposes fit_limits_from_result; switch once merged).
    return FitLimits(
        min_amount=int(rules.min_loan_amount) if rules.min_loan_amount is not None else 0,
        max_amount=int(rules.max_loan_amount) if rules.max_loan_amount is not None else None,
    )


def evaluate_profile(profile: Test2Profile, *, matrix: Optional[tuple[LenderRules, ...]] = None) -> ProfileOutcome:
    result = evaluate_lender_fit(profile.borrower, profile.loan, matrix=matrix)
    sources = {rules.key: rules for rules in (matrix or ())}
    ranking = [{
        "rank": i,
        "lender_key": fit.lender_key,
        "lender_name": fit.lender_name,
        "est_total_borrower_cost": str(fit.total_borrower_cost) if fit.total_borrower_cost is not None else COST_UNKNOWN,
        "cost_basis": COST_BASIS,
        "status": SUBJECT_TO_CONFIRMATION if fit.subject_to_confirmation else CONFIRMED,
    } for i, fit in enumerate(result.fitting, start=1)]
    limits = [_limits(effective_rules(sources[fit.lender_key])) for fit in result.fitting if fit.lender_key in sources]
    by_lender = {miss.lender_key: [code.value for code in miss.reason_codes] for miss in result.non_fitting}
    qualifies = bool(result.fitting)
    return ProfileOutcome(
        name=profile.name,
        qualifies=qualifies,
        limits=limits,
        ranking=ranking,
        failure_codes=[] if qualifies else sorted({c for codes in by_lender.values() for c in codes}),
        failure_codes_by_lender={} if qualifies else by_lender,
        possible=[p.lender_key for p in result.possible],
    )


def _profile(name: str, credit_band: str, fico: int, state: str, loan_type: LoanType, amount: int, *,
             projects: int, builds: int = 0, purchase: int, rehab: int, arv: int) -> Test2Profile:
    return Test2Profile(
        name=name,
        lead=PrequalLead(credit_band, amount, state, loan_type.value),
        borrower=BorrowerProfile(credit_band_min_fico=fico, state=state, completed_projects=projects,
                                 completed_ground_up_builds=builds, has_live_deal=True, has_liquidity=True),
        loan=LoanRequest(loan_type=loan_type, loan_amount=Decimal(amount), state=state, property_type="SFR",
                         purchase_price=Decimal(purchase), rehab_budget=Decimal(rehab), arv=Decimal(arv)),
    )


# Sample borrowers (spec §5 Test 2): fix and flip, experienced builder, credit-challenged investor,
# and a borrower below every credit floor. Demo inputs, not real leads.
TEST2_PROFILES: tuple[Test2Profile, ...] = (
    _profile("1_fix_and_flip", "700+", 700, "FL", LoanType.FIX_AND_FLIP, 360_000,
             projects=3, purchase=350_000, rehab=100_000, arv=650_000),
    _profile("2_experienced_builder", "740+", 740, "FL", LoanType.GROUND_UP_CONSTRUCTION, 1_200_000,
             projects=6, builds=4, purchase=400_000, rehab=1_100_000, arv=2_200_000),
    _profile("3_credit_challenged_investor", "680-699", 680, "GA", LoanType.FIX_AND_FLIP, 220_000,
             projects=1, purchase=220_000, rehab=60_000, arv=420_000),
    _profile("4_non_qualifying", "Below 640", 0, "FL", LoanType.FIX_AND_FLIP, 300_000,
             projects=0, purchase=300_000, rehab=80_000, arv=500_000),
)
