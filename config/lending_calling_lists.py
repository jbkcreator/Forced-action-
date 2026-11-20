"""PropertyRadar calling lists for the lending dialer: Verified maturity, Cash buyers, Investor purchases,
Wholesalers.

Each list is pulled statewide per state (Florida by default, Georgia too) from PropertyRadar into ``lending.pr_list_records``
(one row per list and property, so a property can sit on several lists without overwriting the
maturity pipeline's ``property_radar_records``).

Verified maturity: investor-owned (non-owner-occupied), not listed, first loan private or from a
  named hard-money lender, maturing 90-180 days out. Every property type, land included (about a
  third of private-loan matches are vacant land, which fits ground-up construction lending);
  ``property_type`` is stored so a type can be left out before tracing. Maturity is not searchable, so each assumed
  term gets its own recording-date window (maturity = recording date + term); the term is checked
  after export. A known term inside the window is "confirmed"; an unknown term is "likely".
  The nightly pipeline takes each night only the loans whose maturity newly came within
  ``MATURITY_TRIGGER_DAYS``; the 90-180 day window is the one-time backlog.
Cash buyers: all-cash purchases recorded in the pull window, non-owner-occupied, by owners who
  hold 2-99 properties (100+ are institutional funds).
Investor purchases: any purchase (cash or financed) by the same investor owners. A record the cash
  list matched the same night is left to the cash list, so nothing is bought or called twice.
Wholesalers: properties PropertyRadar classes as flips, resold in the pull window; kept only when
  the resale came within ``WHOLESALER_MAX_HOLD_MONTHS`` of the prior purchase. The seller is the
  wholesaler. PropertyRadar reports the hold in whole months only.
"""
from __future__ import annotations

from datetime import date
from pathlib import Path

from config.property_radar_campaigns import LENDER_VARIANTS

# Every bought page lands here as-is before the database load (data/ is git-ignored).
LANDING_ROOT = Path(__file__).resolve().parents[1] / "data" / "property_radar_lists"

VERIFIED_MATURITY = "verified_maturity"
CASH_BUYER = "cash_buyer"
INVESTOR_PURCHASE = "investor_purchase"
WHOLESALER = "wholesaler"
CALLING_LISTS: tuple[str, ...] = (VERIFIED_MATURITY, CASH_BUYER, INVESTOR_PURCHASE, WHOLESALER)
# Lists whose window is the purchase or resale recording date.
WINDOWED_LISTS: frozenset[str] = frozenset({CASH_BUYER, INVESTOR_PURCHASE, WHOLESALER})
# The nightly pipeline's lists, in buying order: when the nightly export cap runs out, the later lists wait.
NIGHTLY_LISTS: tuple[str, ...] = (VERIFIED_MATURITY, CASH_BUYER, INVESTOR_PURCHASE)

DEFAULT_PULL_STATE = "FL"
PULL_STATES: tuple[str, ...] = ("FL", "GA")
# Georgia may only be cold-called when the owner is a company (src/lending/compliance.py), and its
# owner-occupied flag is unreliable, so its searches use company ownership as the investor signal.
COMPANY_OWNED_ONLY_STATES: frozenset[str] = frozenset({"GA"})

# Verified maturity
MATURITY_DAYS_OUT: tuple[int, int] = (90, 180)
# Nightly trigger: a loan is picked up the night its estimated maturity comes within this many days.
MATURITY_TRIGGER_DAYS = 165
# PropertyRadar shows a recording days or weeks after it happens, so each night searches this many days back
# (listing is free) and buys only what is not already stored: a late arrival is still picked up once.
NIGHTLY_LOOKBACK_DAYS = 30
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


def maturity_recording_windows(maturity_start: date, maturity_end: date) -> dict[int, tuple[date, date]]:
    """Assumed term (years) -> recording-date window whose loans mature between the two dates."""
    return {term: (_years_before(maturity_start, term), _years_before(maturity_end, term))
            for term in MATURITY_ASSUMED_TERMS_YEARS}


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
    if set(WINDOWED_LISTS) - set(CALLING_LISTS) or set(NIGHTLY_LISTS) - set(CALLING_LISTS):
        raise ValueError("WINDOWED_LISTS and NIGHTLY_LISTS must be calling lists")
    if NIGHTLY_LOOKBACK_DAYS < 1:
        raise ValueError("NIGHTLY_LOOKBACK_DAYS must be at least 1")
    earliest = MATURITY_TRIGGER_DAYS - NIGHTLY_LOOKBACK_DAYS + 1
    if not MATURITY_DAYS_OUT[0] <= earliest <= MATURITY_TRIGGER_DAYS <= MATURITY_DAYS_OUT[1]:
        raise ValueError("the nightly maturity window must sit inside MATURITY_DAYS_OUT, or a picked-up loan never "
                         "confirms")
    if DEFAULT_PULL_STATE not in PULL_STATES or set(COMPANY_OWNED_ONLY_STATES) - set(PULL_STATES):
        raise ValueError("DEFAULT_PULL_STATE and COMPANY_OWNED_ONLY_STATES must be pull states")
