"""Generic multi-lender rules matrix — T-05 (Chunk 1 M1).

This module holds the data-driven lender parameter table consumed by
``src.lending.lender_fit.evaluate_lender_fit``.  All values live here so
Josh can update them without touching business logic.

VERIFICATION STATUS
-------------------
``verified=True`` on a lender means its parameter values have been reviewed
and confirmed by Josh Kantor.  ``verified=False`` means the shell is
structurally complete but the values are placeholders; the evaluator will
route the lender to ``non_fitting`` with reason
"lender parameters not yet verified — awaiting Josh confirmation" rather
than silently treating the lender as eligible.

Current status (2026-10-08):
  generic_backflip_* — CLIENT-STATED, NOT YET CONFIRMED (verified=False):
      values copied by hand from Josh's Oct 1 email ("context on the Backflip
      box") and Consolidated §1.1.  Josh must confirm before they count.  Open
      items: (Q2) confirm 7.75–8% is a rate spread and 12-month hold is the
      correct cost basis; (Q3) confirm whether the 36-month window on the
      ground-up build rule must be tracked separately in the profile.
  rcn_capital       — NOT VERIFIED: shell only, awaiting values from Josh.
  easy_street       — NOT VERIFIED: shell only.
  kiavi_affiliate   — NOT VERIFIED: shell only.
  abl               — NOT VERIFIED: shell only.

OPEN QUESTIONS (block Test 2 sign-off, not the Oct 10 shell)
--------------------------------------------------------------
Q1  Encoding "below 640" as credit_band_min_fico=0 until David's
    LendingFlow schema confirms the vocabulary on Oct 10.
Q2  Total-borrower-cost formula (provisional: points + annual spread × hold).
    Confirm that hold_months=12 is right and that 7.75–8% is a rate spread,
    not an all-in rate.
Q3  Ground-up build rule: confirmed "3+ including ≥1 ground-up in last 36
    months" from Josh Oct 1 / Consolidated §1.1.  The profile carries
    completed_ground_up_builds but no project dates; confirm that a date
    window is not required for the dialer-era use-case.
Q4  lender_fit_score meaning on the contact card — provisional formula is
    the fraction of verified lenders that fit (0–100 integer, None when no
    verified lender is in the matrix).
Q5  Parameter values for rcn_capital, easy_street, kiavi_affiliate, abl.

COPY-SAFETY NOTE
----------------
This result is internal analysis only.  Reason strings must never reference
a specific rate or term being offered to a borrower.  See Consolidated §5.2
"Copy Safety Rules".
"""
from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Optional

from src.lending.contracts import LoanType


# ---------------------------------------------------------------------------
# LenderRules dataclass
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class LenderRules:
    """Parameter set for one generic lender.

    All rate/amount fields are Optional so a missing value means "check not
    enforced" rather than "unknown".  A lender with ``verified=False`` is
    never placed in ``fitting`` — it goes to ``non_fitting`` with a
    "parameters not yet verified" reason regardless of other checks.
    """

    key: str                   # stable identifier used in LenderFit / LenderMiss
    name: str                  # display name on contact card
    verified: bool             # True once Josh has confirmed all values

    # Loan type filter — empty set means "all types accepted"
    loan_types: frozenset[LoanType] = field(default_factory=frozenset)

    # Loan amount bounds (USD)
    min_loan_amount: Optional[Decimal] = None
    max_loan_amount: Optional[Decimal] = None

    # LTC / LTV / ARV cap as decimals (e.g. Decimal("0.85") for 85%)
    max_ltc: Optional[Decimal] = None
    max_ltv: Optional[Decimal] = None
    max_arv_cap: Optional[Decimal] = None  # max ARV as a fraction of purchase+rehab

    # Rehab funding (e.g. Decimal("1.00") = 100% of rehab funded)
    max_rehab_funding_pct: Optional[Decimal] = None

    # Minimum purchase price (separate from loan amount per Josh Oct 1)
    min_purchase_price: Optional[Decimal] = None

    # Credit: caller-asked band lower bound (per contracts.py Q1 encoding)
    #   None = no floor enforced
    #   640  = borrower must answer "above 640"
    credit_floor: Optional[int] = None

    # Borrower experience
    min_completed_projects: int = 0
    # Ground-up build rule: required ≥1 ground-up when min_completed_projects ≥ 3
    requires_ground_up_build: bool = False

    # Geography — empty set means "all approved states"
    approved_states: frozenset[str] = field(default_factory=frozenset)

    # Property types — empty set means "all types accepted"
    allowed_property_types: frozenset[str] = field(default_factory=frozenset)

    # Cost parameters (used for ranking by total borrower cost)
    #   origination_points: decimal fraction (e.g. Decimal("0.02") for 2 points)
    #   rate_spread: annual decimal (e.g. Decimal("0.08") for 8%)
    #   hold_months: assumed loan term for cost calculation (Q2, default 12)
    origination_points: Optional[Decimal] = None
    rate_spread: Optional[Decimal] = None
    hold_months: int = 12          # Q2: confirm with Josh


# ---------------------------------------------------------------------------
# Matrix — 5 lenders
# ---------------------------------------------------------------------------

# Generic Backflip
# Values from Josh Kantor's Oct 1/2 emails (Consolidated §2.3 and Q15 context):
#   Flip:         credit floor 640, min purchase price ~$85K, ARV cap 75%,
#                 100% of rehab funded.
#   Construction: credit floor 680, min loan $500K, 3+ completed projects
#                 including ≥1 ground-up build, spreads 7.75–8.00%.
# Backflip is modelled as TWO rule entries (one per loan-type group) merged
# under the same lender key.  The evaluator evaluates each LenderRules row
# independently and uses the first match for a qualifying result.
#
# NOTE: "Generic Backflip" means these values come from Josh's description
# of Backflip's box, not from any direct Backflip API or sync (SPEC §2).
# Josh places the actual product himself; the evaluator is a decision aid.
_BACKFLIP_FLIP = LenderRules(
    key="generic_backflip_flip",
    name="Backflip (Fix & Flip)",
    verified=False,                     # client-stated (Josh Oct 1); awaiting his confirmation
    loan_types=frozenset({LoanType.FIX_AND_FLIP, LoanType.BRIDGE}),
    min_loan_amount=Decimal("100_000"),  # Consolidated intake diagram: "$100K for flip"
    max_loan_amount=None,               # not stated; no cap modelled
    max_ltv=Decimal("0.75"),            # ARV cap 75% == max LTV of 75% of ARV
    max_rehab_funding_pct=Decimal("1.00"),  # 100% of rehab funded
    min_purchase_price=Decimal("85_000"),   # ~$85K per Josh Oct 1
    credit_floor=640,
    min_completed_projects=0,
    origination_points=None,            # Q2: not confirmed
    rate_spread=None,                   # Q2: Josh to confirm spread for flip product
    hold_months=12,
)

_BACKFLIP_CONSTRUCTION = LenderRules(
    key="generic_backflip_construction",
    name="Backflip (Ground-Up Construction)",
    verified=False,                     # client-stated (Josh Oct 1, Consolidated §1.1); awaiting confirmation
    loan_types=frozenset({LoanType.GROUND_UP_CONSTRUCTION}),
    min_loan_amount=Decimal("500_000"),  # $500K min per Josh Oct 1 / Consolidated §1.1
    max_loan_amount=None,
    credit_floor=680,                   # Josh Oct 1 — "credit floor 680"
    min_completed_projects=3,           # "3+ completed projects"
    requires_ground_up_build=True,      # "including ≥1 ground-up build" per Josh Oct 1
    origination_points=None,
    rate_spread=Decimal("0.0775"),      # 7.75% (lower bound of 7.75–8.00% stated by Consolidated §1.1; Q2)
    hold_months=12,
)

# RCN Capital — NOT VERIFIED, shell only (awaiting values from Josh: Q5)
_RCN_CAPITAL = LenderRules(
    key="rcn_capital",
    name="RCN Capital",
    verified=False,
    loan_types=frozenset({LoanType.FIX_AND_FLIP, LoanType.GROUND_UP_CONSTRUCTION,
                          LoanType.BRIDGE, LoanType.DSCR_RENTAL}),
)

# Easy Street Capital — NOT VERIFIED, shell only (Q5)
_EASY_STREET = LenderRules(
    key="easy_street",
    name="Easy Street Capital",
    verified=False,
    loan_types=frozenset({LoanType.FIX_AND_FLIP, LoanType.BRIDGE}),
)

# Kiavi Affiliate — NOT VERIFIED, shell only (Q5)
_KIAVI_AFFILIATE = LenderRules(
    key="kiavi_affiliate",
    name="Kiavi Affiliate",
    verified=False,
    loan_types=frozenset({LoanType.FIX_AND_FLIP, LoanType.DSCR_RENTAL}),
)

# Asset Based Lending (ABL) — NOT VERIFIED, shell only (Q5)
_ABL = LenderRules(
    key="abl",
    name="Asset Based Lending (ABL)",
    verified=False,
    loan_types=frozenset({LoanType.FIX_AND_FLIP, LoanType.GROUND_UP_CONSTRUCTION,
                          LoanType.BRIDGE}),
)

# Canonical order: used by evaluate_lender_fit when no matrix is passed.
# Backflip entries are split by product but share "generic_backflip" in
# display grouping; the evaluator picks the first qualifying entry.
LENDER_MATRIX: tuple[LenderRules, ...] = (
    _BACKFLIP_FLIP,
    _BACKFLIP_CONSTRUCTION,
    _RCN_CAPITAL,
    _EASY_STREET,
    _KIAVI_AFFILIATE,
    _ABL,
)


def validate_lender_matrix(matrix: tuple[LenderRules, ...] = LENDER_MATRIX) -> None:
    """Raise ``ValueError`` for any structurally invalid LenderRules row.

    Called at import time via the module-level call at the bottom of this file,
    and in tests.  Does NOT validate that values are business-correct — Josh
    controls that via the ``verified`` flag.
    """
    keys_seen: set[str] = set()
    for rules in matrix:
        if not rules.key:
            raise ValueError("LenderRules.key must not be empty")
        if rules.key in keys_seen:
            raise ValueError(f"Duplicate lender key: {rules.key!r}")
        keys_seen.add(rules.key)

        if (rules.min_loan_amount is not None
                and rules.max_loan_amount is not None
                and rules.min_loan_amount > rules.max_loan_amount):
            raise ValueError(
                f"{rules.key}: min_loan_amount {rules.min_loan_amount} "
                f"exceeds max_loan_amount {rules.max_loan_amount}"
            )

        for pct_field in ("max_ltc", "max_ltv", "max_arv_cap",
                          "max_rehab_funding_pct", "origination_points", "rate_spread"):
            val = getattr(rules, pct_field)
            if val is not None and (val < 0 or val > 10):
                raise ValueError(
                    f"{rules.key}.{pct_field} = {val} is outside plausible 0–10 range"
                )

        if rules.hold_months < 1:
            raise ValueError(f"{rules.key}: hold_months must be ≥ 1")


validate_lender_matrix()
