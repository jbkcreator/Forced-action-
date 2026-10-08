"""T-05 — Generic multi-lender fit evaluator.

``evaluate_lender_fit(borrower_profile, loan_request)`` is the production
entry point.  It evaluates the BorrowerProfile + LoanRequest against every
LenderRules row in the matrix and returns a LenderFitResult.

PRODUCTION CALLER STATUS (2026-10-08)
--------------------------------------
This function has no production caller yet.  It is consumed by:
  - T-07 (Minute-5 Pre-Qual PDF) and T-08 (Call-One Soft Approval PDF):
    invoked at PDF-trigger time.  Both tasks build against FakeLenderFitEvaluator
    until their own PRs wire the real function.
  - T-12 (background enrichment card): will call this and append the fit score
    to the GHL contact card.  T-12 is M2 (target Oct 15).

Until T-12 merges, ``evaluate_lender_fit`` is callable but never invoked
automatically.  This is expected and documented here so the gap is visible.

COMPLIANCE
----------
This module is internal analysis only.  The LenderFitResult it returns is
displayed on Josh's contact card and in Slack for his own placement decision.
It is NEVER sent to a borrower as a rate, term, or commitment, and reason
strings must never quote a rate or term (SPEC §4.3, Consolidated §5.2).

OPEN QUESTIONS (block Test 2 sign-off, not the Oct 10 shell — see config/lender_matrix.py)
Q1  credit_band_min_fico=0 encodes "below 640"; None = unknown.  Pending David's schema Oct 10.
Q2  Total-borrower-cost formula provisional: points + spread × hold.  Josh to confirm.
Q3  Ground-up 36-month window: profile carries count but not dates.  Josh to confirm.
Q4  lender_fit_score meaning on card: fraction of verified lenders that fit (0–100 int).
Q5  Real values for rcn_capital, easy_street, kiavi_affiliate, abl.
"""
from __future__ import annotations

import logging
from decimal import Decimal
from typing import Optional

from config.lender_matrix import LENDER_MATRIX, LenderRules
from src.lending.contracts import (
    BorrowerProfile,
    LenderFit,
    LenderFitResult,
    LenderMiss,
    LoanRequest,
    LoanType,
)

logger = logging.getLogger(__name__)

# Credit-band encoding (Q1 — pending David's schema Oct 10):
#   None = unknown / not captured
#   0    = caller answered "below 640"
#   640  = caller answered "at or above 640"
#   680  = etc.
_CREDIT_UNKNOWN = None
_CREDIT_BELOW_640 = 0

# Sentinel used when total_borrower_cost cannot be computed (Q2 pending).
_COST_UNKNOWN: Optional[Decimal] = None


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def evaluate_lender_fit(
    borrower_profile: BorrowerProfile,
    loan_request: LoanRequest,
    *,
    matrix: tuple[LenderRules, ...] = LENDER_MATRIX,
) -> LenderFitResult:
    """Evaluate all active lenders against the borrower + loan.

    Args:
        borrower_profile: Borrower facts captured by the caller.
        loan_request: Loan and property details.
        matrix: Lender rules to evaluate against.  Defaults to
            ``LENDER_MATRIX`` from config; tests inject fixture matrices.

    Returns:
        LenderFitResult with:
          - fitting: qualifying lenders ranked by lowest total_borrower_cost
            (cost=None sorted last — Q2 pending).
          - non_fitting: disqualified lenders with explicit reason strings.
          - missing_fields: inputs the evaluator needed but couldn't find.
          - lender_fit_score: provisional 0–100 int or None (Q4).

    Credit-band note: ``credit_band_min_fico=0`` means "below 640".
    ``credit_band_min_fico=None`` means unknown.  A lender that requires
    credit ≥ 640 will reject a ``0`` borrower and treat ``None`` as
    "unknown — cannot confirm" with a reason string saying so.
    """
    fitting: list[LenderFit] = []
    non_fitting: list[LenderMiss] = []
    all_missing: set[str] = set()

    # Track which lenders are verified (for fit_score numerator/denominator).
    verified_lender_keys: set[str] = set()
    fitting_verified_keys: set[str] = set()

    for rules in matrix:
        if not rules.verified:
            non_fitting.append(LenderMiss(
                lender_key=rules.key,
                lender_name=rules.name,
                reasons=["lender parameters not yet verified — awaiting Josh confirmation (Q5)"],
            ))
            continue

        verified_lender_keys.add(rules.key)
        reasons, missing = _check_rules(rules, borrower_profile, loan_request)
        all_missing.update(missing)

        if reasons:
            non_fitting.append(LenderMiss(
                lender_key=rules.key,
                lender_name=rules.name,
                reasons=reasons,
            ))
        else:
            cost = _total_borrower_cost(rules, loan_request.loan_amount)
            fitting.append(LenderFit(
                lender_key=rules.key,
                lender_name=rules.name,
                total_borrower_cost=cost,
            ))
            fitting_verified_keys.add(rules.key)

    # Rank qualifying lenders by lowest total_borrower_cost; unknown cost last.
    fitting.sort(key=lambda f: (f.total_borrower_cost is None, f.total_borrower_cost or 0))

    fit_score = _fit_score(
        fitting_verified_count=len(fitting_verified_keys),
        verified_total=len(verified_lender_keys),
    )

    result = LenderFitResult(
        fitting=fitting,
        non_fitting=non_fitting,
        missing_fields=sorted(all_missing),
        lender_fit_score=fit_score,
    )

    logger.info(
        "lender_fit evaluated loan_type=%s loan_amount=%s state=%s "
        "fitting=%d non_fitting=%d missing=%s fit_score=%s",
        loan_request.loan_type,
        loan_request.loan_amount,
        loan_request.state,
        len(result.fitting),
        len(result.non_fitting),
        result.missing_fields or "none",
        result.lender_fit_score,
    )
    return result


# ---------------------------------------------------------------------------
# Rule checker — returns (reasons, missing_fields) for one LenderRules row
# ---------------------------------------------------------------------------

def _check_rules(
    rules: LenderRules,
    profile: BorrowerProfile,
    request: LoanRequest,
) -> tuple[list[str], list[str]]:
    """Check a single verified LenderRules row.

    Returns:
        reasons — non-empty means this lender does not qualify.
        missing — fields that were needed but absent (informational only;
                  a missing field causes a conservative failure reason, not
                  a silent pass).
    """
    reasons: list[str] = []
    missing: list[str] = []

    # 1. Loan type
    if rules.loan_types and request.loan_type not in rules.loan_types:
        reasons.append(
            f"loan type {request.loan_type.value} not accepted "
            f"(accepted: {', '.join(lt.value for lt in sorted(rules.loan_types, key=str))})"
        )
        # Remaining checks are moot if loan type is wrong.
        return reasons, missing

    # 2. Geography (state)
    if rules.approved_states and request.state not in rules.approved_states:
        reasons.append(
            f"state {request.state!r} not in approved states "
            f"({', '.join(sorted(rules.approved_states))})"
        )

    # 3. Loan amount
    if rules.min_loan_amount is not None and request.loan_amount < rules.min_loan_amount:
        reasons.append(
            f"loan amount ${request.loan_amount:,.0f} below "
            f"${rules.min_loan_amount:,.0f} minimum"
        )
    if rules.max_loan_amount is not None and request.loan_amount > rules.max_loan_amount:
        reasons.append(
            f"loan amount ${request.loan_amount:,.0f} exceeds "
            f"${rules.max_loan_amount:,.0f} maximum"
        )

    # 4. Minimum purchase price (separate from loan amount — Josh Oct 1)
    if rules.min_purchase_price is not None:
        if request.purchase_price is None:
            missing.append("purchase_price")
            reasons.append(
                f"purchase price unknown — lender requires minimum "
                f"${rules.min_purchase_price:,.0f} (capture on call)"
            )
        elif request.purchase_price < rules.min_purchase_price:
            reasons.append(
                f"purchase price ${request.purchase_price:,.0f} below "
                f"${rules.min_purchase_price:,.0f} minimum"
            )

    # 5. LTV (loan / ARV)
    if rules.max_ltv is not None:
        if request.arv is None:
            missing.append("arv")
            reasons.append(
                f"ARV unknown — cannot verify LTV ≤ {rules.max_ltv:.0%} (capture on call)"
            )
        elif request.arv > 0:
            ltv = request.loan_amount / request.arv
            if ltv > rules.max_ltv:
                reasons.append(
                    f"LTV {ltv:.1%} exceeds {rules.max_ltv:.0%} ARV cap "
                    f"(loan ${request.loan_amount:,.0f} / ARV ${request.arv:,.0f})"
                )

    # 6. LTC (loan / (purchase + rehab))
    if rules.max_ltc is not None:
        purchase = request.purchase_price
        rehab = request.rehab_budget
        if purchase is None or rehab is None:
            for f in ("purchase_price", "rehab_budget"):
                if getattr(request, f.replace("rehab_budget", "rehab_budget")) is None:
                    if f not in missing:
                        missing.append(f)
            reasons.append(
                f"LTC cannot be calculated — purchase price or rehab budget missing"
            )
        else:
            total_cost = purchase + rehab
            if total_cost > 0:
                ltc = request.loan_amount / total_cost
                if ltc > rules.max_ltc:
                    reasons.append(
                        f"LTC {ltc:.1%} exceeds {rules.max_ltc:.0%} limit"
                    )

    # 7. Rehab funding cap
    if rules.max_rehab_funding_pct is not None:
        if request.rehab_budget is None:
            if "rehab_budget" not in missing:
                missing.append("rehab_budget")
        # Pass: "100% of rehab funded" means no limit to check against
        # (it's a feature, not a floor); only modelled as a cap if < 1.0.

    # 8. Credit band (caller-asked, never a pulled score — Josh Oct 1 D2)
    if rules.credit_floor is not None:
        band = profile.credit_band_min_fico
        if band is _CREDIT_UNKNOWN:
            # Unknown credit → conservative: cannot confirm eligibility.
            missing.append("credit_band_min_fico")
            reasons.append(
                f"credit band unknown — caller did not capture above/below "
                f"{rules.credit_floor} (capture on call)"
            )
        elif band < rules.credit_floor:
            # band == 0 means "below 640" per Q1 encoding.
            band_label = f"below {640}" if band == _CREDIT_BELOW_640 else f"{band}"
            reasons.append(
                f"credit band {band_label} below lender floor {rules.credit_floor}"
            )

    # 9. Borrower experience
    if rules.min_completed_projects > 0:
        projects = profile.completed_projects
        if projects is None:
            missing.append("completed_projects")
            reasons.append(
                f"experience unknown — lender requires "
                f"{rules.min_completed_projects}+ completed projects (capture on call)"
            )
        elif projects < rules.min_completed_projects:
            reasons.append(
                f"{projects} completed project(s) below "
                f"{rules.min_completed_projects}+ minimum"
            )
        elif rules.requires_ground_up_build:
            # Ground-up build sub-rule (Josh Oct 1 / Consolidated §1.1, Q3)
            ground_up = profile.completed_ground_up_builds
            if ground_up is None:
                missing.append("completed_ground_up_builds")
                reasons.append(
                    "ground-up build count unknown — lender requires ≥1 "
                    "ground-up build among the 3+ projects (capture on call)"
                )
            elif ground_up < 1:
                reasons.append(
                    "no ground-up builds — lender requires ≥1 ground-up "
                    "build among completed projects"
                )

    return reasons, missing


# ---------------------------------------------------------------------------
# Cost and score helpers
# ---------------------------------------------------------------------------

def _total_borrower_cost(rules: LenderRules, loan_amount: Decimal) -> Optional[Decimal]:
    """Provisional total borrower cost over the assumed hold period.

    Formula (Q2 — pending Josh confirmation):
      cost = loan × origination_points + loan × rate_spread × (hold_months / 12)

    Returns None when origination_points or rate_spread is unconfirmed so
    that the ranking sorts those lenders last (null-last sort in evaluator).
    """
    if rules.origination_points is None or rules.rate_spread is None:
        return _COST_UNKNOWN
    points_cost = loan_amount * rules.origination_points
    interest_cost = loan_amount * rules.rate_spread * Decimal(rules.hold_months) / 12
    return points_cost + interest_cost


def _fit_score(fitting_verified_count: int, verified_total: int) -> Optional[Decimal]:
    """Provisional lender_fit_score (Q4 — pending Josh confirmation on meaning).

    Returns the proportion of verified lenders that qualify, scaled 0–100,
    rounded to one decimal place.  Returns None when there are no verified
    lenders in the matrix (all shells), so the card shows nothing rather than 0.
    """
    if verified_total == 0:
        return None
    return round(
        Decimal(fitting_verified_count) / Decimal(verified_total) * 100,
        1,
    )
