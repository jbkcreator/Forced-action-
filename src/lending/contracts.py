"""T-04 — Lender Box + LendingFlow intake contracts.

Types only — no logic, no I/O, no DB.  Everything in this module is a plain
Pydantic model, enum, or Protocol so that T-05 (engine), T-07/T-08 (PDFs),
T-09–T-12 (intake/enrichment) can all build against a stable interface before
the real evaluator exists.

Open questions pending client answers (do not merge workaround code here):
  Q1 — Credit band vocabulary: field carries Optional[int] lower bound until
       Josh/David confirm value set (Oct 10 LendingFlow schema delivery).
  Q2 — lender_fit_score scale and total-borrower-cost formula: both Optional
       until Josh confirms.  Blocks T-05 sign-off, not T-04.
  Q3 — Ground-up builds as a separate count: field included; confirm with Josh
       whether the construction experience rule (3+ projects including ≥1
       ground-up in last 36 months) requires tracking these separately.

Gap-fix additions (2026-10-13) — all additive, None/empty defaults, no
existing constructor calls may break:
  G1 (A1) — FieldStatus enum + LenderFit.subject_to_confirmation: lender
       parameters are partial until Josh marks them confirmed (Oct 14).
  G2 (A3) — LenderPossible + LenderFitResult.possible: third outcome for
       eligibility fields that are unknown (not a fit, not a decline).
  G3 (B1c) — BindingCap enum + SizingFlag enum for the T-08 calc result;
       DeclineReason enum (Josh's exact list, Oct 8 addendum) on LenderMiss.
  G4 (D-1) — LendingFlowLeadCreated.vendor_lead_id and .needs_review; email
       made Optional (Josh: store-and-flag a missing vendor id; dedupe on
       phone+email; a bad email never costs a paid lead).

Compliance notes:
  - Credit is a caller-asked band only, never a pulled score or exact FICO
    (Josh, Oct 1 answers D2).  No numeric-score field exists here.
  - LenderFitResult is internal analysis only.  It never states a rate, term,
    or commitment to a borrower (SPEC §4.3 non-binding language, playbook rule).
  - phone/email on LendingFlowLeadCreated have repr=False so logging the event
    object cannot leak PII.  Serialised output (model_dump / model_dump_json)
    still includes them — see the class docstring.  The producer (T-11)
    normalises phone via src.services.phone_utils.normalize before
    constructing the event.
"""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Optional, Protocol, runtime_checkable
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


# ---------------------------------------------------------------------------
# Enumerations
# ---------------------------------------------------------------------------

class LoanType(str, Enum):
    """Loan product categories supported by the multi-lender engine."""
    FIX_AND_FLIP = "FIX_AND_FLIP"
    GROUND_UP_CONSTRUCTION = "GROUND_UP_CONSTRUCTION"
    DSCR_RENTAL = "DSCR_RENTAL"
    BRIDGE = "BRIDGE"


class FieldStatus(str, Enum):
    """Data-quality status of a lender-parameter field (G1, A1).

    confirmed          — Josh has verified the value against a real quote.
    placeholder        — a conservative default; used until Josh confirms.
    confirm_at_submission — must be verified with the lender on the actual
                         loan submission; cannot be checked in advance.

    T-05 reads this from the DB row per lender/field.  A lender whose rules
    contain any placeholder or confirm_at_submission field is shown as
    "possible, confirm on the call" rather than a definitive fit or decline.
    """
    CONFIRMED = "confirmed"
    PLACEHOLDER = "placeholder"
    CONFIRM_AT_SUBMISSION = "confirm_at_submission"


class DeclineReason(str, Enum):
    """Machine-readable decline codes — Josh's exact list (Oct 8 addendum, G3).

    Used alongside the human-readable `reasons` strings on LenderMiss.
    T-05 sets these; downstream reporting and the routing engine read them.
    """
    CREDIT_BELOW_MIN = "credit_below_min"
    EXPERIENCE_BELOW_MIN = "experience_below_min"
    LTV_OVER_MAX = "ltv_over_max"
    LTC_OVER_MAX = "ltc_over_max"
    LOAN_TOO_SMALL = "loan_too_small"
    LOAN_TOO_LARGE = "loan_too_large"
    PROPERTY_TYPE_EXCLUDED = "property_type_excluded"
    STATE_NOT_COVERED = "state_not_covered"
    OWNER_OCCUPIED = "owner_occupied"
    SEAT_CAP_REACHED = "seat_cap_reached"
    BORROWER_UNRESPONSIVE = "borrower_unresponsive"
    BORROWER_WITHDREW = "borrower_withdrew"
    DOCS_INCOMPLETE = "docs_incomplete"
    OTHER = "other"


class BindingCap(str, Enum):
    """Which cap reduced the loan amount in the B1(c) sizing formula (G3).

    Set by T-08's calculate_figures; stored alongside the result so the UI
    can explain "the ARV cap cut your loan".  None if the requested amount
    fit within all caps.
    """
    REQUESTED = "requested"
    LTC = "ltc"
    ARV = "arv"
    MAX_LOAN = "max_loan"


class SizingFlag(str, Enum):
    """Non-blocking warnings on a sizing result (G3, B1(c)).

    UPSIDE_DOWN     — ARV < total cost; deal may not pencil.
    ARV_OVER_3X     — ARV > 3 × purchase price; check for a data typo.

    T-08 sets these; they are informational only and do not block PDF
    generation.
    """
    UPSIDE_DOWN = "upside_down"
    ARV_OVER_3X = "arv_over_3x"


class RoutingTag(str, Enum):
    """Pipeline routing assigned after enrichment (SPEC §4.4).

    FULL_MACHINE — address present AND target close date ≤30 days.
                   Routes to closer priority and triggers soft-approval PDF.
    NURTURE      — missing address or close date; enters long-term cadence.

    Routing logic lives in T-12 (enrichment card).  This module only defines
    the canonical values so all tasks share the same strings.
    """
    FULL_MACHINE = "FULL_MACHINE"
    NURTURE = "NURTURE"


# ---------------------------------------------------------------------------
# Input: borrower and loan facts
# ---------------------------------------------------------------------------

class BorrowerProfile(BaseModel):
    """Borrower facts captured by the caller during a live call.

    All fields are optional because the profile is built incrementally — T-05
    must handle any subset gracefully and return missing_fields rather than
    raising.

    credit_band_min_fico: lower bound of the caller-asked credit band.
        "Is your credit generally above 640 or below?" → 640 or None.
        640 = Backflip flip-product floor; 680 = construction floor.
        An exact FICO score is never captured (playbook rule, Josh Oct 1 D2).
        Q1: confirm vocabulary set once David's schema arrives Oct 10.

    completed_projects: total fix-and-flip or construction projects in last
        3 years.  One flip OR one ground-up build counts.  Experience tiers
        in the lender matrix: 0, 1–2, 3+.

    completed_ground_up_builds: subset of completed_projects that are
        ground-up construction.  Needed for the construction floor rule
        (3+ projects including ≥1 ground-up in last 36 months, Josh Oct 1).
        Q3: Josh to confirm this needs a separate count.

    has_live_deal: True = borrower has a specific deal now or is actively in
        the market.  Maps to the "real deal" booking qualifier (D2).

    has_liquidity: True = borrower confirmed reserves to carry a project.
    """
    model_config = ConfigDict(frozen=True)

    credit_band_min_fico: Optional[int] = None
    completed_projects: Optional[int] = None
    completed_ground_up_builds: Optional[int] = None
    has_live_deal: Optional[bool] = None
    has_liquidity: Optional[bool] = None
    state: Optional[str] = None  # 2-letter state code


class LoanRequest(BaseModel):
    """Loan and property facts needed for lender evaluation and PDFs.

    loan_type: required for lender matrix filtering.
    loan_amount: required; Decimal to avoid float rounding.
    state: required for geography checks.

    property_type, purchase_price, rehab_budget, arv, property_address,
    target_close_date: optional — available only after caller captures them
    on the live call (SPEC §4.3, §4.4).

    Routing rule (applied by T-12, not here):
      address present AND target_close_date within 30 days → FULL_MACHINE.
      otherwise → NURTURE.
    """
    model_config = ConfigDict(frozen=True)

    loan_type: LoanType
    loan_amount: Decimal
    state: str  # 2-letter state code

    property_type: Optional[str] = None
    purchase_price: Optional[Decimal] = None
    rehab_budget: Optional[Decimal] = None
    arv: Optional[Decimal] = None
    property_address: Optional[str] = None
    target_close_date: Optional[datetime] = None


# ---------------------------------------------------------------------------
# Output: lender fit result
# ---------------------------------------------------------------------------

class LenderFit(BaseModel):
    """A single lender that qualifies for this borrower+loan combination."""
    model_config = ConfigDict(frozen=True)

    lender_key: str          # e.g. "rcn_capital", "easy_street"
    lender_name: str
    # Total borrower cost (origination points + rate spread expressed as a
    # Decimal over the loan term) used for lowest-cost ranking.
    # Q2: formula not yet specified by Josh — Optional until confirmed.
    total_borrower_cost: Optional[Decimal] = None
    # G1 (A1): whether the lender's parameters are confirmed or placeholder.
    # None means the engine did not populate the field (pre-gap-fix data).
    # A LenderFit with subject_to_confirmation=True should be presented as
    # "likely fits — confirm on the call", not as a definitive qualification.
    subject_to_confirmation: bool = False


class LenderMiss(BaseModel):
    """A single lender that does not qualify, with explicit failure reasons.

    reasons: human-readable strings, e.g. "FICO 620 below 640 floor",
        "Loan amount $350K below $500K construction minimum".  Because
        credit is a band only, reasons reference the band's lower bound, not
        an exact score.

    reason_codes: machine-readable DeclineReason codes alongside reasons
        (G3, B1c).  Empty list when the engine predates the enum.  Callers
        should check both fields: `reasons` is always set; `reason_codes`
        is populated by T-05 from this gap-fix forward.
    """
    model_config = ConfigDict(frozen=True)

    lender_key: str
    lender_name: str
    reasons: list[str]
    reason_codes: list[DeclineReason] = Field(default_factory=list)  # G3


class LenderPossible(BaseModel):
    """A lender whose eligibility cannot be determined from the available data.

    G2 (A3): Josh's answer: unknown state, loan type or credit returns
    "possible, confirm on the call" — not a fit and not a decline.

    why_unknown: human-readable explanation of which field(s) are missing or
        straddle a floor, e.g. "credit band unknown — caller did not capture
        above/below 640" or "state not confirmed".  Always non-empty when
        this object is present.
    """
    model_config = ConfigDict(frozen=True)

    lender_key: str
    lender_name: str
    why_unknown: list[str]


class LenderFitResult(BaseModel):
    """Structured output of evaluate_lender_fit (SPEC §4.3).

    fitting:     lenders that accept the deal, ranked by lowest
                 total_borrower_cost (T-05 is responsible for the sort).
    possible:    lenders whose eligibility could not be determined (G2, A3).
                 Show as "confirm on the call", never as a fit or a decline.
    non_fitting: lenders with at least one disqualifying rule.
    missing_fields: fields the evaluator needed but the profile/request
                 didn't supply — used by T-07/T-08 to decide whether to
                 emit a PDF.

    lender_fit_score: summary score for display on the contact card.
        Q2: scale not yet specified — Optional[Decimal] until Josh confirms.
        When lender rules are partially placeholder, the score is hidden from
        the card (A1: "the lender-fit score on the lead card stays hidden
        until all five are confirmed").

    INTERNAL USE ONLY.  This result is never sent to a borrower as a
    commitment, rate quote, or term offer.  PDF generators must reproduce
    their own non-binding watermark language independently of this field.
    """
    model_config = ConfigDict(frozen=True)

    fitting: list[LenderFit] = Field(default_factory=list)
    possible: list[LenderPossible] = Field(default_factory=list)   # G2
    non_fitting: list[LenderMiss] = Field(default_factory=list)
    missing_fields: list[str] = Field(default_factory=list)
    lender_fit_score: Optional[Decimal] = None  # Q2; hidden when rules are partial


# ---------------------------------------------------------------------------
# Protocol: evaluator interface (satisfied by both Fake and real T-05 engine)
# ---------------------------------------------------------------------------

@runtime_checkable
class LenderFitEvaluator(Protocol):
    """Interface that evaluate_lender_fit and FakeLenderFitEvaluator both satisfy.

    T-07 and T-08 depend on this Protocol so they can be tested with the Fake
    and wired to the real engine (T-05) once it is merged, without changing
    their own code.
    """
    def evaluate(
        self,
        borrower_profile: BorrowerProfile,
        loan_request: LoanRequest,
    ) -> LenderFitResult:
        ...


# ---------------------------------------------------------------------------
# Event: LendingFlow lead created
# ---------------------------------------------------------------------------

class LendingFlowLeadCreated(BaseModel):
    """Internal event emitted by T-11 after a deduplicated LendingFlow lead
    is created in PostgreSQL and GHL.

    Consumed by:
      T-09 — 10-second intro SMS trigger
      T-10 — 2-minute / 5-minute uncalled push alarms
      T-12 — background enrichment card + routing

    phone/email: repr=False only stops them appearing when the event object
        itself is logged or printed.  It does NOT protect serialised output:
        model_dump() and model_dump_json() still include both in plaintext.
        Consumers must use model_dump(exclude={"phone", "email"}) (or log
        lead_id only) for any log or audit line.
        The producer (T-11) must pass phone through phone_utils.normalize
        before constructing this event.

    email: Optional (G4, D-1) — a bad email never costs a paid lead; phone
        is the contact key.  A lead without an email is still valid.

    vendor_lead_id: the opaque ID LendingFlow sends with each lead (G4, D-1).
        Optional because the schema is pending David (Q1).  When None,
        needs_review is True and T-11 must set the DB row's `needs_review`
        flag rather than hard-failing.  Do not dedupe on vendor_lead_id alone
        when it is absent — fall back to phone+email hash.

    needs_review: True when the payload was stored but one or more required
        identifiers (vendor_lead_id) were missing.  T-11 sets this; the ops
        queue must surface it for manual review before the lead enters
        automation.  (G4, D-1 — Josh: "don't flip the flag until the schema
        matches".)

    credit_band_min_fico, loan_amount, state, loan_type: the 4 core pre-qual
        fields from the LendingFlow payload (SPEC §4.3/§4.4).  All Optional
        because LendingFlow raw leads may not carry all four (schema to be
        confirmed by David Oct 10 — Q1).
    """
    model_config = ConfigDict(frozen=True)

    lead_id: UUID
    phone: str = Field(repr=False)
    email: Optional[str] = Field(default=None, repr=False)   # G4: Optional
    created_at: datetime

    # G4 (D-1): vendor identifier and review flag
    vendor_lead_id: Optional[str] = None
    needs_review: bool = False

    # 4 pre-qual core fields — present when LendingFlow delivers them
    credit_band_min_fico: Optional[int] = None   # Q1
    loan_amount: Optional[Decimal] = None
    state: Optional[str] = None
    loan_type: Optional[LoanType] = None
