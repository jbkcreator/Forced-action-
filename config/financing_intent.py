"""Financing-Intent Scoring Engine configuration.

Weights, thresholds, and keyword sets for the S1 financing-intent lane.
All tuning happens here — no code changes in the engine required.
"""

from __future__ import annotations

SIGNAL_WEIGHTS: dict[str, int] = {
    "fresh_deed_0_30_days":     25,
    "fresh_deed_31_60_days":    15,
    "active_structural_permit": 30,
    "active_roofing_permit":    20,
    "early_lis_pendens":        25,
    "divorce_match":            20,
    "equity_proxy_strong":      25,
    "equity_proxy_medium":      15,
}

SCORE_CAP = 100

# Checked in order; first threshold met wins.
TIER_THRESHOLDS: list[tuple[int, str]] = [
    (70, "high"),
    (45, "medium"),
    (0,  "low"),
]

# ── Fresh Deed ────────────────────────────────────────────────────────────────
FRESH_DEED_STRONG_DAYS = 30
FRESH_DEED_MEDIUM_DAYS = 60
FRESH_DEED_MIN_CONF    = 0.75
FRESH_DEED_EXCLUDE_TYPES: frozenset[str] = frozenset({
    "tax deed", "quit", "certificate",
})

# ── Active Permit ─────────────────────────────────────────────────────────────
PERMIT_EXCLUDE_STATUSES: frozenset[str] = frozenset({
    "complete", "expired", "withdrawn", "revoked",
    "cancel", "cancelled", "awaiting client reply", "waiting on applicant",
})
STRUCTURAL_KEYWORDS: frozenset[str] = frozenset({
    "structural", "building", "addition", "renovation",
    "remodel", "new construction", "demolition",
})
ROOFING_KEYWORDS: frozenset[str] = frozenset({
    "roof", "shingle", "tpo", "tile", "flashing", "underlayment",
})

# ── Early Lis Pendens ─────────────────────────────────────────────────────────
LP_LOOKBACK_DAYS = 90
LP_LATE_STATUSES: frozenset[str] = frozenset({
    "sold", "canceled", "cancelled", "closed",
})

# ── Divorce ───────────────────────────────────────────────────────────────────
DIVORCE_MIN_CONF = 0.75
DIVORCE_EXCLUDE_STATUSES: frozenset[str] = frozenset({
    "closed", "dismissed", "settled",
})

# ── Equity Proxy ──────────────────────────────────────────────────────────────
EQUITY_STRONG_PCT   = 50
EQUITY_MEDIUM_PCT   = 30
EQUITY_TENURE_YEARS = 10

# ── Product Recommendation ────────────────────────────────────────────────────
# First matching active flag wins; insertion order defines priority.
SIGNAL_TO_PRODUCT: dict[str, str] = {
    "fresh_deed_0_30_days":     "bridge",
    "fresh_deed_31_60_days":    "hard_money_purchase",
    "active_structural_permit": "renovation_capital",
    "active_roofing_permit":    "heloc",
    "early_lis_pendens":        "cash_out_refi",
    "divorce_match":            "buyout_refi",
    "equity_proxy_strong":      "cash_out_refi",
    "equity_proxy_medium":      "cash_out_refi",
}
PRODUCT_PRIORITY: list[str] = list(SIGNAL_TO_PRODUCT)

# ── Batch processing ──────────────────────────────────────────────────────────
BATCH_SIZE = 500
