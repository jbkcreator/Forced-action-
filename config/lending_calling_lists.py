"""PropertyRadar calling lists for the lending dialer: Verified maturity, Cash buyers, Wholesalers.

All three are Florida statewide and are pulled from PropertyRadar into ``lending.pr_list_records``
(one row per list and property, so a property can sit on several lists without overwriting the
maturity pipeline's ``property_radar_records``).

Verified maturity: investor-owned (non-owner-occupied), not listed, first loan private or from a
  named hard-money lender, maturing 90-180 days out. Every property type, land included (about a
  third of private-loan matches are vacant land, which fits ground-up construction lending);
  ``property_type`` is stored so a type can be left out before tracing. Maturity is not searchable, so each assumed
  term gets its own recording-date window (maturity = recording date + term); the term is checked
  after export. A known term inside the window is "confirmed"; an unknown term is "likely".
Cash buyers: all-cash purchases recorded in the pull window, non-owner-occupied, by owners who
  hold 2-99 properties (100+ are institutional funds).
Wholesalers: properties PropertyRadar classes as flips, resold in the pull window; kept only when
  the resale came within ``WHOLESALER_MAX_HOLD_MONTHS`` of the prior purchase. The seller is the
  wholesaler. PropertyRadar reports the hold in whole months only.
"""
from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

from config.property_radar_campaigns import LENDER_VARIANTS

# Every bought page lands here as-is before the database load (data/ is git-ignored).
LANDING_ROOT = Path(__file__).resolve().parents[1] / "data" / "property_radar_lists"

VERIFIED_MATURITY = "verified_maturity"
CASH_BUYER = "cash_buyer"
WHOLESALER = "wholesaler"
CALLING_LISTS: tuple[str, ...] = (VERIFIED_MATURITY, CASH_BUYER, WHOLESALER)
# Lists whose window is the purchase or resale recording date (given on the command line).
WINDOWED_LISTS: frozenset[str] = frozenset({CASH_BUYER, WHOLESALER})

PULL_STATE = "FL"

# Verified maturity
MATURITY_DAYS_OUT: tuple[int, int] = (90, 180)
MATURITY_ASSUMED_TERMS_YEARS: tuple[int, ...] = (1, 2, 3)
# Original-lender spellings that return results: the same set the daily maturity pull uses.
HARD_MONEY_LENDER_NAMES: tuple[str, ...] = tuple(sorted({
    name for variants in LENDER_VARIANTS.values() for name in variants
}))

# Cash buyers
CASH_BUYER_PORTFOLIO_RANGE: tuple[int, int] = (2, 99)

# Wholesalers
WHOLESALER_FLIP_TYPES: tuple[str, ...] = ("MarketFlip", "ShortSaleFlip", "REOFlip")
# Client rule: "buying and reselling inside 30 days". PropertyRadar gives the hold in whole months only,
# and 1 month already spans roughly 30-60 days, so only 0 months is inside 30 days.
WHOLESALER_MAX_HOLD_MONTHS = 0

# Match strength stored on each qualifying record.
CONFIRMED = "confirmed"
LIKELY = "likely"

# Exclusion reasons (records kept for audit, never loaded).
MATURITY_OUTSIDE_WINDOW = "maturity_outside_window"
RESOLD_AFTER_HOLD_LIMIT = "resold_after_hold_limit"
HOLD_UNKNOWN = "hold_unknown"
SELLER_UNKNOWN = "seller_unknown"

# Fields bought per record (one export each, whatever the field count).
EXPORT_FIELDS: tuple[str, ...] = (
    "RadarID", "APN", "County", "State", "Address", "City", "ZipFive", "PType",
    "Owner", "OwnershipType", "OwnerAddress", "OwnerCity", "OwnerState", "OwnerZipFive",
    "isSameMailingOrExempt", "OwnerExemption", "Persons", "isListedForSale",
    "FirstLoanType", "FirstLenderOriginal", "FirstDate", "FirstAmount", "FirstTermInYears",
    "LastTransferRecDate", "LastTransferValue", "LastTransferSeller", "LastTransferType",
    "LastTransferDownPaymentPercent", "PriorTransferMonthsSince",
)

# A live pull refuses to spend paid exports unless the operator passes an explicit cap.
DEFAULT_MAX_PAID_EXPORTS = 0


def date_range_value(start: date, end: date) -> str:
    """PropertyRadar's custom date-range value (the format the daily maturity pull uses)."""
    return f"from: {start:%m/%d/%Y} to: {end:%m/%d/%Y}"


def maturity_recording_windows(as_of: date) -> dict[int, tuple[date, date]]:
    """Assumed term (years) -> recording-date window whose loans mature 90-180 days after ``as_of``."""
    near, far = (as_of + timedelta(days=d) for d in MATURITY_DAYS_OUT)
    return {term: (_years_before(near, term), _years_before(far, term)) for term in MATURITY_ASSUMED_TERMS_YEARS}


def _years_before(day: date, years: int) -> date:
    try:
        return day.replace(year=day.year - years)
    except ValueError:  # Feb 29 in a non-leap year
        return day.replace(year=day.year - years, day=28)


def validate_calling_list_config() -> None:
    if MATURITY_DAYS_OUT[0] >= MATURITY_DAYS_OUT[1]:
        raise ValueError("MATURITY_DAYS_OUT must be (near, far) with near < far")
    lo, hi = CASH_BUYER_PORTFOLIO_RANGE
    if not 1 <= lo <= hi:
        raise ValueError("CASH_BUYER_PORTFOLIO_RANGE must be 1 <= low <= high")
    if WHOLESALER_MAX_HOLD_MONTHS < 0:
        raise ValueError("WHOLESALER_MAX_HOLD_MONTHS cannot be negative")
    if set(WINDOWED_LISTS) - set(CALLING_LISTS):
        raise ValueError("WINDOWED_LISTS must be calling lists")
