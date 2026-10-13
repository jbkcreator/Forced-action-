"""T-05 — Generic multi-lender fit evaluator.

``evaluate_lender_fit(borrower_profile, loan_request)`` is the production
entry point.  It evaluates the BorrowerProfile + LoanRequest against every
LenderRules row in the matrix and returns a LenderFitResult.

PRODUCTION CALLERS
------------------
  - ``enrichment/fit_card.py`` (T-12): the lead card's fit view.
  - ``soft_approval/service.py`` (T-08): through ``MatrixLenderFitEvaluator``;
    it walks ``fitting`` in order, so the ranking below decides the lender used.
  - T-07's ``prequal_fit.py`` seam is not wired yet.

THREE OUTCOMES PER LENDER (Josh, A1/A3, Oct 8)
----------------------------------------------
  fitting      every check passes.  ``subject_to_confirmation`` is True while any
               of the lender's parameters is not confirmed (placeholder values).
  possible     eligibility cannot be decided: credit, state, or a deal fact is
               unknown, or the credit band straddles the floor.  Shown as
               "possible, confirm on the call" - never a fit and never a decline.
  non_fitting  a known value fails.  Carries the free-text reasons and Josh's
               ``DeclineReason`` codes.

A parameter a lender has no value for takes Josh's placeholder box until he
marks it confirmed (``config.lender_matrix.effective_rules``); it is never
skipped.  A decline that rests on an unconfirmed value says so in its reason.

RANKING (Josh, A2): Backflip rows first whenever they fit, then the others
cheapest first (unknown cost last).  ``lender_fit_score`` stays None until every
lender in the matrix is confirmed (A1).

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
Q4  lender_fit_score meaning on card: fraction of lenders that fit (0–100), shown only
    once every lender is confirmed.
Q5  Real values for rcn_capital, easy_street, kiavi_affiliate, abl (due Oct 14).
"""
from __future__ import annotations

import logging
from decimal import Decimal
from typing import Optional

from config.lender_matrix import (
    LenderRules,
    effective_rules,
    field_status_of,
    fully_confirmed,
)
from config.lending_enrichment import BACKFLIP_KEY_PREFIX
from src.lending.contracts import (
    BorrowerProfile,
    DeclineReason,
    FieldStatus,
    LenderFit,
    LenderFitResult,
    LenderMiss,
    LenderPossible,
    LoanRequest,
    LoanType,
)
from src.lending.lender_rules_store import current_matrix

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

_Decline = tuple[str, DeclineReason]


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def evaluate_lender_fit(
    borrower_profile: BorrowerProfile,
    loan_request: LoanRequest,
    *,
    matrix: Optional[tuple[LenderRules, ...]] = None,
) -> LenderFitResult:
    """Evaluate all active lenders against the borrower + loan.

    Args:
        borrower_profile: Borrower facts captured by the caller.
        loan_request: Loan and property details.
        matrix: Lender rules to evaluate against.  Defaults to
            ``current_matrix()`` (the config, or the table when LENDING_RULES_FROM_DB is on); tests inject fixtures.

    Returns:
        LenderFitResult with:
          - fitting: qualifying lenders, Backflip first then lowest
            total_borrower_cost (cost=None last — Q2 pending).
          - possible: lenders whose eligibility is unknown ("confirm on the call").
          - non_fitting: disqualified lenders with reasons and decline codes.
          - missing_fields: inputs the evaluator needed but couldn't find.
          - lender_fit_score: 0–100, or None until every lender is confirmed (A1).

    Credit-band note: ``credit_band_min_fico=0`` means "below 640" and declines.
    ``None`` means unknown.  A positive band below a lender's floor straddles it
    (the band is read as "this value or higher"), so it is ``possible``.
    """
    matrix = current_matrix() if matrix is None else matrix
    fitting: list[LenderFit] = []
    possible: list[LenderPossible] = []
    non_fitting: list[LenderMiss] = []
    all_missing: set[str] = set()

    for source in matrix:
        rules = effective_rules(source)
        declines, unknowns, missing = _check_rules(rules, borrower_profile, loan_request, source)
        all_missing.update(missing)

        if declines:
            non_fitting.append(LenderMiss(
                lender_key=rules.key,
                lender_name=rules.name,
                reasons=[reason for reason, _ in declines],
                reason_codes=list(dict.fromkeys(code for _, code in declines)),
            ))
        elif unknowns:
            possible.append(LenderPossible(
                lender_key=rules.key,
                lender_name=rules.name,
                why_unknown=unknowns,
            ))
        else:
            fitting.append(LenderFit(
                lender_key=rules.key,
                lender_name=rules.name,
                total_borrower_cost=_total_borrower_cost(rules, loan_request.loan_amount),
                subject_to_confirmation=not fully_confirmed(source),
            ))

    # Backflip first, then lowest total_borrower_cost; unknown cost last.
    fitting.sort(key=lambda f: (
        not f.lender_key.startswith(BACKFLIP_KEY_PREFIX),
        f.total_borrower_cost is None,
        f.total_borrower_cost or 0,
    ))

    all_confirmed = bool(matrix) and all(fully_confirmed(r) for r in matrix)
    fit_score = _fit_score(len(fitting), len(matrix)) if all_confirmed else None

    result = LenderFitResult(
        fitting=fitting,
        possible=possible,
        non_fitting=non_fitting,
        missing_fields=sorted(all_missing),
        lender_fit_score=fit_score,
    )

    logger.info(
        "lender_fit evaluated loan_type=%s loan_amount=%s state=%s "
        "fitting=%d possible=%d non_fitting=%d missing=%s fit_score=%s",
        loan_request.loan_type,
        loan_request.loan_amount,
        loan_request.state,
        len(result.fitting),
        len(result.possible),
        len(result.non_fitting),
        result.missing_fields or "none",
        result.lender_fit_score,
    )
    return result


# ---------------------------------------------------------------------------
# Rule checker — returns (declines, unknowns, missing_fields) for one lender
# ---------------------------------------------------------------------------

def _unconfirmed(source: LenderRules, field: str) -> str:
    """Suffix for a decline that rests on a value Josh has not confirmed."""
    if field_status_of(source, field) is FieldStatus.CONFIRMED:
        return ""
    return " (unconfirmed rule)"


def _check_rules(
    rules: LenderRules,
    profile: BorrowerProfile,
    request: LoanRequest,
    source: Optional[LenderRules] = None,
) -> tuple[list[_Decline], list[str], list[str]]:
    """Check one lender (``rules`` is the effective rules, ``source`` the configured row).

    Returns:
        declines — (reason, code) pairs for known values that fail.
        unknowns — why eligibility cannot be decided (needs a capture on the call).
        missing  — input fields that were needed but absent.
    """
    source = source or rules
    declines: list[_Decline] = []
    unknowns: list[str] = []
    missing: list[str] = []

    # 1. Loan type
    if rules.loan_types and request.loan_type not in rules.loan_types:
        declines.append((
            f"loan type {request.loan_type.value} not accepted "
            f"(accepted: {', '.join(lt.value for lt in sorted(rules.loan_types, key=str))})"
            f"{_unconfirmed(source, 'loan_types')}",
            DeclineReason.OTHER,
        ))
        # Remaining checks are moot if loan type is wrong.
        return declines, unknowns, missing

    # 2. Geography (state)
    if rules.approved_states:
        if not (request.state or "").strip():
            missing.append("state")
            unknowns.append("state not confirmed — capture on call")
        elif request.state not in rules.approved_states:
            declines.append((
                f"state {request.state!r} not in approved states "
                f"({', '.join(sorted(rules.approved_states))}){_unconfirmed(source, 'approved_states')}",
                DeclineReason.STATE_NOT_COVERED,
            ))

    # 3. Loan amount
    if rules.min_loan_amount is not None and request.loan_amount < rules.min_loan_amount:
        declines.append((
            f"loan amount ${request.loan_amount:,.0f} below "
            f"${rules.min_loan_amount:,.0f} minimum{_unconfirmed(source, 'min_loan_amount')}",
            DeclineReason.LOAN_TOO_SMALL,
        ))
    if rules.max_loan_amount is not None and request.loan_amount > rules.max_loan_amount:
        declines.append((
            f"loan amount ${request.loan_amount:,.0f} exceeds "
            f"${rules.max_loan_amount:,.0f} maximum{_unconfirmed(source, 'max_loan_amount')}",
            DeclineReason.LOAN_TOO_LARGE,
        ))

    # 4. Minimum purchase price (separate from loan amount — Josh Oct 1)
    if rules.min_purchase_price is not None:
        if request.purchase_price is None:
            missing.append("purchase_price")
            unknowns.append(
                f"purchase price unknown — lender requires minimum "
                f"${rules.min_purchase_price:,.0f} (capture on call)"
            )
        elif request.purchase_price < rules.min_purchase_price:
            declines.append((
                f"purchase price ${request.purchase_price:,.0f} below "
                f"${rules.min_purchase_price:,.0f} minimum{_unconfirmed(source, 'min_purchase_price')}",
                DeclineReason.OTHER,
            ))

    # 5. LTV (loan / ARV)
    if rules.max_ltv is not None:
        if request.arv is None:
            missing.append("arv")
            unknowns.append(
                f"ARV unknown — cannot verify LTV ≤ {rules.max_ltv:.0%} (capture on call)"
            )
        elif request.arv > 0:
            ltv = request.loan_amount / request.arv
            if ltv > rules.max_ltv:
                declines.append((
                    f"LTV {ltv:.1%} exceeds {rules.max_ltv:.0%} ARV cap "
                    f"(loan ${request.loan_amount:,.0f} / ARV ${request.arv:,.0f})"
                    f"{_unconfirmed(source, 'max_ltv')}",
                    DeclineReason.LTV_OVER_MAX,
                ))

    # 6. LTC (loan / (purchase + rehab))
    if rules.max_ltc is not None:
        purchase = request.purchase_price
        rehab = request.rehab_budget
        if purchase is None or rehab is None:
            if purchase is None:
                missing.append("purchase_price")
            if rehab is None:
                missing.append("rehab_budget")
            unknowns.append("LTC cannot be calculated — purchase price or rehab budget missing (capture on call)")
        else:
            total_cost = purchase + rehab
            if total_cost > 0:
                ltc = request.loan_amount / total_cost
                if ltc > rules.max_ltc:
                    declines.append((
                        f"LTC {ltc:.1%} exceeds {rules.max_ltc:.0%} limit{_unconfirmed(source, 'max_ltc')}",
                        DeclineReason.LTC_OVER_MAX,
                    ))

    # 6b. Property type
    if rules.allowed_property_types:
        if request.property_type is None:
            missing.append("property_type")
            unknowns.append("property type unknown — capture on call")
        elif request.property_type.strip().lower() not in {
            t.lower() for t in rules.allowed_property_types
        }:
            declines.append((
                f"property type {request.property_type!r} not accepted"
                f"{_unconfirmed(source, 'allowed_property_types')}",
                DeclineReason.PROPERTY_TYPE_EXCLUDED,
            ))

    # 7. Credit band (caller-asked, never a pulled score — Josh Oct 1 D2)
    if rules.credit_floor is not None:
        band = profile.credit_band_min_fico
        if band is _CREDIT_UNKNOWN:
            missing.append("credit_band_min_fico")
            unknowns.append(
                f"credit band unknown — caller did not capture above/below "
                f"{rules.credit_floor} (capture on call)"
            )
        elif band >= rules.credit_floor:
            pass
        elif band == _CREDIT_BELOW_640:
            # "below 640" is below every floor.
            declines.append((
                f"credit band below 640 below lender floor {rules.credit_floor}"
                f"{_unconfirmed(source, 'credit_floor')}",
                DeclineReason.CREDIT_BELOW_MIN,
            ))
        elif profile.credit_band_max_fico is not None and profile.credit_band_max_fico < rules.credit_floor:
            # A closed band whose top is below the floor cannot qualify.
            declines.append((
                f"credit band {band}-{profile.credit_band_max_fico} below lender floor {rules.credit_floor}"
                f"{_unconfirmed(source, 'credit_floor')}",
                DeclineReason.CREDIT_BELOW_MIN,
            ))
        else:
            # An open-ended band ("band+") or a closed one that crosses the floor may or may not clear it.
            unknowns.append(
                f"credit band {band}+ straddles the {rules.credit_floor} floor — "
                f"needs confirmation on the call"
            )

    # 8. Borrower experience
    if rules.min_completed_projects > 0:
        projects = profile.completed_projects
        if projects is None:
            missing.append("completed_projects")
            unknowns.append(
                f"experience unknown — lender requires "
                f"{rules.min_completed_projects}+ completed projects (capture on call)"
            )
        elif projects < rules.min_completed_projects:
            declines.append((
                f"{projects} completed project(s) below "
                f"{rules.min_completed_projects}+ minimum"
                f"{_unconfirmed(source, 'min_completed_projects')}",
                DeclineReason.EXPERIENCE_BELOW_MIN,
            ))
        elif rules.requires_ground_up_build:
            # Ground-up build sub-rule (Josh Oct 1 / Consolidated §1.1, Q3)
            ground_up = profile.completed_ground_up_builds
            if ground_up is None:
                missing.append("completed_ground_up_builds")
                unknowns.append(
                    "ground-up build count unknown — lender requires ≥1 "
                    "ground-up build among the 3+ projects (capture on call)"
                )
            elif ground_up < 1:
                declines.append((
                    "no ground-up builds — lender requires ≥1 ground-up "
                    f"build among completed projects{_unconfirmed(source, 'requires_ground_up_build')}",
                    DeclineReason.EXPERIENCE_BELOW_MIN,
                ))

    return declines, unknowns, missing


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

    Returns the proportion of lenders that qualify, scaled 0–100, rounded to
    one decimal place.  Returns None when there are no lenders to count.  The
    caller passes it only once every lender is confirmed (A1).
    """
    if verified_total == 0:
        return None
    return round(
        Decimal(fitting_verified_count) / Decimal(verified_total) * 100,
        1,
    )
