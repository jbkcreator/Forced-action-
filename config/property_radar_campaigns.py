"""PropertyRadar campaign criteria and lender-name variants.

ENABLED_STATES — states whose campaigns run in production pulls.
  FL and GA are live (Next Deal Lending dials both statewide).

CAMPAIGN_CRITERIA — keyed by (state, campaign_key). Each value is a list of
  PropertyRadar Criteria dicts passed as the ``Criteria`` body parameter.

  ``maturity_target_lender`` is the primary campaign: corporate-owned
  properties where the first loan was recorded 8–15 months ago by one of
  Josh's target hard-money lenders and the property is not listed for sale.

  Lending campaigns, mapped to Josh's source lists (pull order 5, 8, 9, 6):
    maturity_target_lender  List 1 (FL) / List 5 (GA) — named hard-money lenders
    private_maturity        List 8 — first loan coded Private by PropertyRadar
    stalled_flip            List 9 — financed (private) purchase, not resold
    auction_winner          List 6 — trustee-sale buyers
  PropertyRadar does not code the named hard-money lenders as Private, so
  maturity_target_lender and private_maturity never overlap. stalled_flip
  and private_maturity overlap as a loan ages, so they share seen_ids
  (SHARED_SEEN_CAMPAIGNS); stalled_flip also excludes today's private_maturity
  window to save export credits.
  None of these lending campaigns has a handoff entry in
  config/property_radar_handoff.py, so the FA Max handoff skips them.

  NOTE: FirstTermInYears is an EXPORT-ONLY field — it cannot be used as an
  API filter criterion (confirmed by inspection of api-reference/criteria.json
  which has no TermInYears entry). The "20–30 year long-term exclusion"
  (excluding conventional mortgages masquerading as short-term) is therefore
  applied post-fetch inside property_radar_normalizer.py, not here.

LENDER_VARIANTS — empirically tested lender-name spellings that return
  non-zero results from PropertyRadar. Source: saved-criteria/lender_variants.json
  in the team pack. Only variants with FL_rCurrent > 0 are included here
  (zero-result variants are tracked in the source file for auditability but
  excluded from API queries to avoid needless API calls).
"""
from __future__ import annotations

from datetime import date, timedelta
from typing import Optional

ENABLED_STATES: frozenset[str] = frozenset({"FL", "GA"})

# ---------------------------------------------------------------------------
# Lender name variants — only spellings confirmed to return results
# ---------------------------------------------------------------------------

LENDER_VARIANTS: dict[str, list[str]] = {
    "kiavi": ["Kiavi", "LendingHome", "Lending Home", "Kiavi Funding"],
    "rcn": ["RCN"],
    "lima_one": ["Lima One"],
    "easy_street": ["Easy Street"],
    "anchor_loans": ["Anchor Loans", "Anchor Loans LP"],
    "abl": ["ABL", "ABL RPC", "Asset Based"],
    "temple_view": ["TVC", "Temple View", "Temple"],
    "backflip": ["Backflip"],
    "constructive": ["Constructive", "Constructive Loans"],
    "new_silver": ["New Silver"],
    "roc": ["Roc", "ROC", "Roc Funding"],
}

# All lender variant strings flattened — used to build the OR criteria block
_ALL_LENDER_NAMES: list[str] = [
    name for variants in LENDER_VARIANTS.values() for name in variants
]


# Date windows as (older_days, newer_days) before today.
_MATURITY_WINDOW_DAYS: tuple[int, int] = (15 * 30, 8 * 30)  # recorded ~8–15 months ago

# List 8 window per state. FL dials only loans 30–90 days from maturity on a
# 12-month term (recorded 275–335 days ago); GA takes the full 8–15 month
# window because GA starts at zero records.
PRIVATE_MATURITY_WINDOW_DAYS: dict[str, tuple[int, int]] = {
    "FL": (365 - 30, 365 - 90),
    "GA": _MATURITY_WINDOW_DAYS,
}

# List 9: last purchase 90–365 days ago and not resold since.
STALLED_FLIP_PURCHASE_WINDOW_DAYS: tuple[int, int] = (365, 90)

# List 6: trustee-sale purchase in the last 12 months.
AUCTION_WINNER_PURCHASE_WINDOW_DAYS: tuple[int, int] = (365, 0)

# Far-past lower bound for an open-ended "before" date range.
_EARLIEST_DATE = date(1990, 1, 1)


def _window_dates(
    older_days: int, newer_days: int, as_of: Optional[date] = None
) -> tuple[date, date]:
    """Return (start, end) for records dated ``older_days``..``newer_days`` before
    ``as_of`` (default today)."""
    ref = as_of or date.today()
    return ref - timedelta(days=older_days), ref - timedelta(days=newer_days)


def _maturity_window_dates(as_of: Optional[date] = None) -> tuple[date, date]:
    """Return (start, end) dates for loans recorded 8–15 months ago, as of ``as_of``
    (default today). Returns real ``date`` objects — callers format for the API."""
    return _window_dates(*_MATURITY_WINDOW_DAYS, as_of=as_of)


def _fmt(d: date) -> str:
    return d.strftime("%m/%d/%Y")


def build_campaign_criteria(
    state: str,
    campaign: str,
    *,
    daily_since: Optional[date] = None,
) -> list[dict]:
    """Return the Criteria list for a given (state, campaign) pair.

    Raises ValueError for an unknown campaign key. Dates in date-window
    criteria are computed at call time so the window rolls forward each day.

    ``daily_since``: when given, narrows the FirstDate window to only loans
    that newly crossed the 8-month line since that date, instead of the full
    8-15 month backlog window. This is what makes the daily pull cheap
    (§2.3): pass the last successful daily/backlog run's date here, and only
    ~16/week new-to-the-window Florida loans get queried, not the whole ~700+
    record backlog every time. Pass None (the default) for a full backlog pull.
    """
    key = (state.upper(), campaign)
    if key not in _CAMPAIGN_BUILDERS:
        raise ValueError(f"Unknown campaign: state={state!r} campaign={campaign!r}")
    return _CAMPAIGN_BUILDERS[key](daily_since)


def _date_window_criterion(
    name: str, window_days: tuple[int, int], daily_since: Optional[date]
) -> dict:
    if daily_since is None:
        start, end = _window_dates(*window_days)
    else:
        # The window's near edge (`end`) moves forward by 1 day every day.
        # "Newly entered the window since daily_since" = records whose date
        # falls between where that edge was on daily_since and where it is
        # today. +1 day on the lower bound avoids re-matching the boundary
        # date already covered by the previous run.
        _, prev_end = _window_dates(*window_days, as_of=daily_since)
        start = prev_end + timedelta(days=1)
        _, end = _window_dates(*window_days)
        if start > end:
            # daily_since was today or in the future (e.g. two runs same day,
            # or a clock skew) — degenerate to an empty window rather than
            # accidentally widening it.
            start = end
    return {"name": name, "value": [f"from: {_fmt(start)} to: {_fmt(end)}"]}


def _outside_window_criterion(name: str, window_days: tuple[int, int]) -> dict:
    """Dates before or after today's window. PropertyRadar has no NOT, but a
    criterion's value list is OR, so two ranges express "outside"."""
    start, end = _window_dates(*window_days)
    return {"name": name, "value": [
        f"from: {_fmt(_EARLIEST_DATE)} to: {_fmt(start - timedelta(days=1))}",
        f"from: {_fmt(end + timedelta(days=1))} to: {_fmt(date.today())}",
    ]}


def _maturity_date_criterion(daily_since: Optional[date]) -> dict:
    return _date_window_criterion("FirstDate", _MATURITY_WINDOW_DAYS, daily_since)


def _fl_maturity_target_lender(daily_since: Optional[date] = None) -> list[dict]:
    return [
        # Corporate owner (investor-held properties only)
        {"name": "OwnershipType", "value": ["Corporate"]},
        # Not listed for sale
        {"name": "isListedForSale", "value": ["0"]},
        # First loan recorded 8–15 months ago (full backlog window), or only
        # loans newly entering that window since the last run (daily mode)
        _maturity_date_criterion(daily_since),
        # Lender must be one of Josh's target hard-money names (OR semantics
        # within a single criterion's value list per PropertyRadar's API spec)
        {"name": "FirstLenderOriginal", "value": _ALL_LENDER_NAMES},
        # Florida statewide (no county restriction — the pull iterates counties
        # individually using CountyFIPS when per-county counts are needed for
        # the dry-run report; this criteria set is for the bulk pull)
        {"name": "State", "value": ["FL"]},
    ]


def _ga_maturity_target_lender(daily_since: Optional[date] = None) -> list[dict]:
    return [
        {"name": "OwnershipType", "value": ["Corporate"]},
        {"name": "isListedForSale", "value": ["0"]},
        _maturity_date_criterion(daily_since),
        {"name": "FirstLenderOriginal", "value": _ALL_LENDER_NAMES},
        {"name": "State", "value": ["GA"]},
    ]


_CORPORATE = {"name": "OwnershipType", "value": ["Corporate"]}
_NOT_LISTED = {"name": "isListedForSale", "value": ["0"]}
_PRIVATE_FIRST_LOAN = {"name": "FirstLoanType", "value": ["P"]}


def _state(state: str) -> dict:
    return {"name": "State", "value": [state]}


def _private_maturity(state: str, daily_since: Optional[date] = None) -> list[dict]:
    return [
        _CORPORATE,
        _NOT_LISTED,
        _PRIVATE_FIRST_LOAN,
        _date_window_criterion("FirstDate", PRIVATE_MATURITY_WINDOW_DAYS[state], daily_since),
        _state(state),
    ]


def _stalled_flip(state: str, daily_since: Optional[date] = None) -> list[dict]:
    return [
        _CORPORATE,
        _NOT_LISTED,
        _PRIVATE_FIRST_LOAN,
        # The last transfer is the flipper's own purchase: not resold since.
        _date_window_criterion("LastTransferRecDate", STALLED_FLIP_PURCHASE_WINDOW_DAYS, daily_since),
        # Records already covered by private_maturity are not bought again.
        _outside_window_criterion("FirstDate", PRIVATE_MATURITY_WINDOW_DAYS[state]),
        _state(state),
    ]


def _auction_winner(state: str, daily_since: Optional[date] = None) -> list[dict]:
    return [
        {"name": "ForeclosureStage", "value": ["3rd Owned"]},
        _date_window_criterion("LastTransferRecDate", AUCTION_WINNER_PURCHASE_WINDOW_DAYS, daily_since),
        _state(state),
    ]


def _for_state(builder, state: str):
    return lambda daily_since=None: builder(state, daily_since)


# Campaign builder registry — add new campaigns/states here without touching callers
_CAMPAIGN_BUILDERS: dict[tuple[str, str], object] = {
    ("FL", "maturity_target_lender"): _fl_maturity_target_lender,
    ("GA", "maturity_target_lender"): _ga_maturity_target_lender,
    **{
        (state, key): _for_state(builder, state)
        for state in ("FL", "GA")
        for key, builder in (
            ("private_maturity", _private_maturity),
            ("stalled_flip", _stalled_flip),
            ("auction_winner", _auction_winner),
        )
    },
}

# Default campaign run for each state — the daily/backlog pull targets this
DEFAULT_CAMPAIGN: str = "maturity_target_lender"

# Campaigns whose criteria overlap over time: a record either one bought is
# skipped by the other, so it is never billed twice.
SHARED_SEEN_CAMPAIGNS: dict[str, tuple[str, ...]] = {
    "stalled_flip": ("private_maturity",),
    "private_maturity": ("stalled_flip",),
}
