"""Cora P2.2 instant quote: constants and the rule set the engine reads.

Base caps come from the lender matrix (``central_lending``); the sizing shares from the soft-approval params.
Values from claude/Instant-Quote-Rules.md are placeholders until received: a quote that uses one is flagged
``unconfirmed``. Only Central Lending is quoted (NDL lane; Backflip stays off until Josh approves in writing).
"""
from __future__ import annotations

from decimal import Decimal

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
