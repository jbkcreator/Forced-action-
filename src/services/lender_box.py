"""
Lender Box eligibility engine.

Evaluates a proposed deal against the active Backflip program rules stored in
lender_box_programs / lender_box_geographies and returns one of three statuses:

  in_box     — at least one active program accepts the deal as-is.
  out_of_box — every program has at least one hard failure.  Routes to EXCEPTIONS.
  uncertain  — at least one required field is missing so a definitive answer
               cannot be given.  Returns the list of missing fields.

All three outcomes are visible to Josh.  Nothing is silently discarded.

Callers:
  - Scenario Builder (WP-8A/8B) — called as part of assembling the deal dossier.
  - Green/yellow/red router (WP-T2-11) — called when a new opportunity is scored.
  - Cora query loop (WP-T2-12) — called when Josh asks "will Backflip do this deal".

Re-evaluation sweep:
  Call evaluate() in a loop over open opportunities after any program rule change.
  The sweep is triggered externally (event-driven or nightly cron); this module
  contains no scheduling logic.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Literal, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Input contract
# ---------------------------------------------------------------------------

@dataclass
class DealInput:
    """
    Everything the eligibility engine needs to evaluate one deal.

    Fields marked optional are derived or may not be known yet.  Missing
    numeric fields cause specific checks to return uncertain rather than fail.
    """
    # Required — cannot evaluate without these.
    property_type: str          # e.g. 'single_family', 'condo', 'duplex'
    state: str                  # 2-letter state code e.g. 'FL'
    proposed_loan_amount: Decimal

    # Optional — used for LTC, LTV, and size checks.
    purchase_price: Optional[Decimal] = None
    rehab_estimate: Optional[Decimal] = None
    arv: Optional[Decimal] = None

    # Optional — used for experience check.  None = unknown.
    borrower_prior_loans: Optional[int] = None

    # Optional — county for fine-grained geography check.
    county: Optional[str] = None

    # Convenience — opportunity or property identifier for logging only.
    ref: Optional[str] = None


# ---------------------------------------------------------------------------
# Output contract
# ---------------------------------------------------------------------------

@dataclass
class EligibilityResult:
    status: Literal["in_box", "out_of_box", "uncertain"]
    matched_program: Optional[str] = None          # program_key when in_box
    matched_program_name: Optional[str] = None
    fail_reasons: list[str] = field(default_factory=list)
    uncertain_flags: list[str] = field(default_factory=list)  # missing fields

    def is_in_box(self) -> bool:
        return self.status == "in_box"

    def summary(self) -> str:
        if self.status == "in_box":
            return f"In-box — {self.matched_program_name}"
        if self.status == "uncertain":
            return f"Uncertain — missing: {', '.join(self.uncertain_flags)}"
        return f"Out-of-box — {'; '.join(self.fail_reasons)}"


# ---------------------------------------------------------------------------
# Core evaluation
# ---------------------------------------------------------------------------

def evaluate(deal: DealInput, db: Session) -> EligibilityResult:
    """
    Evaluate a deal against all active Backflip programs.

    Returns the first in-box program match or the aggregated failure reasons
    across all programs.  Uncertain is returned when a required numeric field
    is missing and the check cannot be completed.
    """
    programs = _load_active_programs(db)

    if not programs:
        logger.warning("lender_box.evaluate: no active programs in lender_box_programs")
        return EligibilityResult(
            status="uncertain",
            uncertain_flags=["no_active_programs"],
        )

    # Derive optional computed fields once — reused across program checks.
    total_cost, ltc, ltv = _derive_financials(deal)

    all_fail_reasons: list[str] = []
    uncertain_flags: list[str] = []

    for program in programs:
        result = _check_program(deal, program, total_cost, ltc, ltv, db)

        if result.status == "in_box":
            logger.info(
                "lender_box.evaluate ref=%s matched program=%s",
                deal.ref, result.matched_program,
            )
            return result

        if result.status == "uncertain":
            # Collect unique uncertain flags across programs.
            for flag in result.uncertain_flags:
                if flag not in uncertain_flags:
                    uncertain_flags.append(flag)
        else:
            all_fail_reasons.extend(result.fail_reasons)

    if uncertain_flags:
        logger.info(
            "lender_box.evaluate ref=%s uncertain flags=%s",
            deal.ref, uncertain_flags,
        )
        return EligibilityResult(status="uncertain", uncertain_flags=uncertain_flags)

    logger.info(
        "lender_box.evaluate ref=%s out_of_box reasons=%s",
        deal.ref, all_fail_reasons,
    )
    return EligibilityResult(status="out_of_box", fail_reasons=all_fail_reasons)


# ---------------------------------------------------------------------------
# Per-program check
# ---------------------------------------------------------------------------

def _check_program(
    deal: DealInput,
    program: dict,
    total_cost: Optional[Decimal],
    ltc: Optional[Decimal],
    ltv: Optional[Decimal],
    db: Session,
) -> EligibilityResult:
    key = program["program_key"]
    name = program["name"]
    failures: list[str] = []
    uncertain: list[str] = []

    # 1. Property type — hard check, no uncertain path.
    prop = deal.property_type.lower().strip()
    allowed = [t.lower() for t in (program["allowed_property_types"] or [])]
    excluded = [t.lower() for t in (program["excluded_property_types"] or [])]

    if prop in excluded:
        failures.append(f"[{key}] property type '{deal.property_type}' is explicitly excluded")
    elif prop not in allowed:
        failures.append(f"[{key}] property type '{deal.property_type}' not in allowed list")

    # 2. Geography — hard check.
    if not _geography_allowed(deal, program["program_key"], db):
        failures.append(f"[{key}] geography {deal.state}/{deal.county or '*'} not permitted")

    # 3. Loan amount — hard check.
    loan = deal.proposed_loan_amount
    min_loan = Decimal(str(program["min_loan_amount"]))
    max_loan = Decimal(str(program["max_loan_amount"]))
    if loan < min_loan:
        failures.append(
            f"[{key}] loan ${loan:,.0f} below minimum ${min_loan:,.0f}"
        )
    elif loan > max_loan:
        failures.append(
            f"[{key}] loan ${loan:,.0f} exceeds maximum ${max_loan:,.0f}"
        )

    # 4. LTC — uncertain if total_cost is missing.
    if program["max_ltc"] is not None:
        if ltc is None:
            uncertain.append("purchase_price_or_rehab_estimate")
        else:
            max_ltc = Decimal(str(program["max_ltc"]))
            if ltc > max_ltc:
                failures.append(
                    f"[{key}] LTC {float(ltc):.1%} exceeds limit {float(max_ltc):.1%}"
                )

    # 5. LTV — uncertain if arv is missing.
    if program["max_ltv"] is not None:
        if ltv is None:
            uncertain.append("arv")
        else:
            max_ltv = Decimal(str(program["max_ltv"]))
            if ltv > max_ltv:
                failures.append(
                    f"[{key}] LTV {float(ltv):.1%} exceeds limit {float(max_ltv):.1%}"
                )

    # 6. Borrower experience — uncertain if unknown, not a hard failure
    #    (Backflip confirmed no experience minimum; better terms for experience).
    min_exp = program["min_borrower_prior_loans"] or 0
    if min_exp > 0:
        if deal.borrower_prior_loans is None:
            uncertain.append("borrower_prior_loans")
        elif deal.borrower_prior_loans < min_exp:
            failures.append(
                f"[{key}] borrower has {deal.borrower_prior_loans} prior loans, "
                f"minimum is {min_exp}"
            )

    if failures:
        return EligibilityResult(status="out_of_box", fail_reasons=failures)

    if uncertain:
        return EligibilityResult(status="uncertain", uncertain_flags=list(set(uncertain)))

    return EligibilityResult(
        status="in_box",
        matched_program=key,
        matched_program_name=name,
    )


# ---------------------------------------------------------------------------
# Geography check
# ---------------------------------------------------------------------------

def _geography_allowed(deal: DealInput, program_key: str, db: Session) -> bool:
    """
    Returns True when the deal's state/county is permitted for the program.

    Logic:
    - If no rows exist for this program + state → not permitted.
    - If a state-wide row exists (county IS NULL, is_excluded = false) → permitted
      unless a county-level exclusion row also exists.
    - If only county-level rows exist → the deal's county must match a non-excluded row.
    """
    rows = db.execute(
        text("""
            SELECT county, is_excluded
            FROM lender_box_geographies
            WHERE program_key = :pk AND state = :state
        """),
        {"pk": program_key, "state": deal.state.upper()},
    ).fetchall()

    if not rows:
        return False

    state_wide_allowed = any(r.county is None and not r.is_excluded for r in rows)

    if deal.county:
        county_lower = deal.county.lower()
        county_excluded = any(
            r.county and r.county.lower() == county_lower and r.is_excluded
            for r in rows
        )
        if county_excluded:
            return False
        county_allowed = any(
            r.county and r.county.lower() == county_lower and not r.is_excluded
            for r in rows
        )
        return state_wide_allowed or county_allowed

    return state_wide_allowed


# ---------------------------------------------------------------------------
# Financial derivations
# ---------------------------------------------------------------------------

def _derive_financials(
    deal: DealInput,
) -> tuple[Optional[Decimal], Optional[Decimal], Optional[Decimal]]:
    """
    Returns (total_cost, ltc, ltv).  Any value is None when the required
    inputs are missing or result in a zero denominator.
    """
    total_cost: Optional[Decimal] = None
    ltc: Optional[Decimal] = None
    ltv: Optional[Decimal] = None

    try:
        if deal.purchase_price is not None and deal.rehab_estimate is not None:
            total_cost = deal.purchase_price + deal.rehab_estimate
            if total_cost > 0:
                ltc = deal.proposed_loan_amount / total_cost

        if deal.arv is not None and deal.arv > 0:
            ltv = deal.proposed_loan_amount / deal.arv

    except (InvalidOperation, ZeroDivisionError) as exc:
        logger.warning("lender_box._derive_financials: %s", exc)

    return total_cost, ltc, ltv


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _load_active_programs(db: Session) -> list[dict]:
    rows = db.execute(
        text("""
            SELECT
                program_key,
                name,
                min_loan_amount,
                max_loan_amount,
                max_ltc,
                max_ltv,
                min_loan_term_months,
                max_loan_term_months,
                allowed_property_types,
                excluded_property_types,
                min_borrower_prior_loans
            FROM lender_box_programs
            WHERE is_active = true
              AND effective_date <= CURRENT_DATE
              AND (expiry_date IS NULL OR expiry_date > CURRENT_DATE)
            ORDER BY program_key
        """)
    ).fetchall()
    return [row._mapping for row in rows]


# ---------------------------------------------------------------------------
# Re-evaluation helper — called by the sweep after a rule change
# ---------------------------------------------------------------------------

def evaluate_batch(
    deals: list[tuple[str, DealInput]],
    db: Session,
) -> list[tuple[str, EligibilityResult]]:
    """
    Evaluate a list of (ref, DealInput) pairs in one call.

    Programs are loaded once and reused across the batch.  Geography checks
    still hit the DB per deal because they are filtered by state; add a
    geography cache here if the batch is large and all deals share a state.
    """
    return [(ref, evaluate(deal, db)) for ref, deal in deals]
