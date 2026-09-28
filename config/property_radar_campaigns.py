"""PropertyRadar campaign criteria and lender-name variants.

ENABLED_STATES — states whose campaigns run in production pulls.
  FL is live. GA is fully defined but disabled; flip ENABLED_STATES to
  activate it with no code changes (state-swap replicability requirement).

CAMPAIGN_CRITERIA — keyed by (state, campaign_key). Each value is a list of
  PropertyRadar Criteria dicts passed as the ``Criteria`` body parameter.

  ``maturity_target_lender`` is the primary campaign: corporate-owned
  properties where the first loan was recorded 8–15 months ago by one of
  Josh's target hard-money lenders and the property is not listed for sale.

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

ENABLED_STATES: frozenset[str] = frozenset({"FL"})

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


def _maturity_window_dates(as_of: Optional[date] = None) -> tuple[date, date]:
    """Return (start, end) dates for loans recorded 8–15 months ago, as of ``as_of``
    (default today). Returns real ``date`` objects — callers format for the API."""
    ref = as_of or date.today()
    end = ref - timedelta(days=8 * 30)    # ~8 months ago
    start = ref - timedelta(days=15 * 30) # ~15 months ago
    return start, end


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


def _maturity_date_criterion(daily_since: Optional[date]) -> dict:
    if daily_since is None:
        start, end = _maturity_window_dates()
    else:
        # The window's near edge (`end`) moves forward by 1 day every day.
        # "Newly entered the window since daily_since" = loans whose FirstDate
        # falls between where that edge was on daily_since and where it is
        # today. +1 day on the lower bound avoids re-matching the boundary
        # date already covered by the previous run.
        _, prev_end = _maturity_window_dates(as_of=daily_since)
        start = prev_end + timedelta(days=1)
        _, end = _maturity_window_dates()
        if start > end:
            # daily_since was today or in the future (e.g. two runs same day,
            # or a clock skew) — degenerate to an empty window rather than
            # accidentally widening it.
            start = end
    return {"name": "FirstDate", "value": [f"from: {_fmt(start)} to: {_fmt(end)}"]}


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


# Campaign builder registry — add new campaigns/states here without touching callers
_CAMPAIGN_BUILDERS: dict[tuple[str, str], object] = {
    ("FL", "maturity_target_lender"): _fl_maturity_target_lender,
    ("GA", "maturity_target_lender"): _ga_maturity_target_lender,
}

# Default campaign run for each state — the daily/backlog pull targets this
DEFAULT_CAMPAIGN: str = "maturity_target_lender"
