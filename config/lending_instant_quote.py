"""Cora P2.2 instant quote: constants and the rule set the engine reads.

Base caps come from the lender matrix (``central_lending``); the sizing shares from the soft-approval params.
Values from claude/Instant-Quote-Rules.md are placeholders until received: a quote that uses one is flagged
``unconfirmed``. Only Central Lending is quoted (NDL lane; Backflip stays off until Josh approves in writing).
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

QUOTE_LENDER_KEY = "central_lending"
DISCLAIMER = "Indicative, subject to underwriting"

# Public rejection codes for a calculator outcome; anything unlisted is QUOTE_UNAVAILABLE.
CALCULATION_REJECTION_CODES: dict[str, str] = {"loan_too_small": "LOAN_TOO_SMALL"}
QUOTE_UNAVAILABLE = "QUOTE_UNAVAILABLE"

# Eligibility rejections (ticket 03).
ENTITY_ONLY_STATES = frozenset({"FL"})
NEEDS_LLC_FORMATION = "NEEDS_LLC_FORMATION"
STATE_NOT_ELIGIBLE = "STATE_NOT_ELIGIBLE"
FICO_BELOW_FLOOR = "FICO_BELOW_FLOOR"
ZIP_STATE_MISMATCH = "ZIP_STATE_MISMATCH"
LEVERAGE_EXHAUSTED = "LEVERAGE_EXHAUSTED"  # stacked cuts took the LTC cap to zero
STATE_FIPS: dict[str, str] = {"FL": "12", "GA": "13", "TX": "48"}  # states the ZIP crosswalk covers

# Interest reserve (ticket 04): a pricing FICO below the threshold requires a full-term reserve.
# Rate and term are unconfirmed (client Q5): while either is None the condition carries no amount.
INTEREST_RESERVE_FICO_THRESHOLD = 660
INTEREST_RESERVE_CONDITION = "INTEREST_RESERVE_FULL_TERM"
INTEREST_RESERVE_RATE: Decimal | None = None  # annual, 0 to 1
INTEREST_RESERVE_TERM_MONTHS: int | None = None

RATE_LIMIT_SCOPE = "lending_instant_quote"
RATE_LIMIT_PER_WINDOW = 30
RATE_LIMIT_WINDOW_SECONDS = 60
API_KEY_HEADER = "X-Api-Key"


# Pricing grid (ticket 05). PLACEHOLDER: the brief gives only the ranges (LTC -2.5% to -15%, fee +0.25% to
# +4.0%); the tier selector and per-tier values await claude/Instant-Quote-Rules.md (client Q1). Tiers are
# picked by pricing FICO here as a stand-in. Any quote priced on this grid is flagged unconfirmed.
@dataclass(frozen=True)
class GridTier:
    min_fico: int
    ltc_cut: Decimal  # negative, 0 to 1 scale
    fee_surcharge: Decimal  # 0 to 1 scale


PRICING_GRID: tuple[GridTier, ...] = (  # highest min_fico first
    GridTier(760, Decimal("-0.025"), Decimal("0.0025")),
    GridTier(740, Decimal("-0.05"), Decimal("0.005")),
    GridTier(720, Decimal("-0.075"), Decimal("0.01")),
    GridTier(700, Decimal("-0.10"), Decimal("0.015")),
    GridTier(680, Decimal("-0.115"), Decimal("0.02")),
    GridTier(660, Decimal("-0.13"), Decimal("0.03")),
    GridTier(0, Decimal("-0.15"), Decimal("0.04")),
)
PRICING_GRID_CONFIRMED = False
GRID_CUT_REASON = "PRICING_GRID"

# CFPB rural + refinance (ticket 07). County list = CFPB rural/underserved 2026 (refresh yearly);
# ZIP -> county = Census 2020 ZCTA-county relationship, largest land share, FL/GA/TX only.
# Cut sizes are PLACEHOLDERS; the refinance cut is flat until the debt-balance rule arrives (client Q8).
# existing_debt is optional and kept on the audit row for that rule.
_DATA_DIR = Path(__file__).resolve().parent / "lending_quote_data"
CFPB_RURAL_COUNTIES_FILE = _DATA_DIR / "cfpb_rural_underserved_2026.csv"
ZIP_COUNTY_FILE = _DATA_DIR / "zip_county_fl_ga_tx.csv"
RURAL_CUT = Decimal("-0.05")
RURAL_CUT_REASON = "CFPB_RURAL"
RURAL_UNKNOWN_NOTE = "RURAL_STATUS_UNKNOWN"
REFINANCE_CUT = Decimal("-0.05")
REFINANCE_CUT_REASON = "REFINANCE"
LEVERAGE_ADJUSTMENTS_CONFIRMED = False

# Experience (ticket 06). Central's 1-2 completed-deal haircut is from the brief; 0 deals has no stated rule
# (client Q11) and takes no cut. Experience = the most experienced guarantor.
EXPERIENCE_HAIRCUT_DEALS = range(1, 3)
EXPERIENCE_HAIRCUT = Decimal("-0.025")
EXPERIENCE_CUT_REASON = "EXPERIENCE"

# Second-program new-construction rule (1 exit required). OFF: that program stays blocked until Josh approves
# in writing (brief: NDL lane only). Neutral code name; the response never names a lender.
NEW_CONSTRUCTION_EXIT_RULE_ENABLED = False
NEW_CONSTRUCTION_MIN_EXITS = 1
NEW_CONSTRUCTION_NEEDS_EXIT = "NEW_CONSTRUCTION_NEEDS_EXIT"
