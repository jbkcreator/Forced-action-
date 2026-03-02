"""
CDS Multi-Vertical Scoring Configuration

All scoring weights, multipliers, and routing thresholds live here.
To retune weights: edit this file and run:
    python -m src.services.cds_engine --rescore-all
No code changes required.

SCORING FORMULA (per vertical):
  primary_score  = base_weight[best_signal] + recency_bonus(best_signal_date)
  stacking_bonus = min((signals_within_60_days - 1) * 20, 40)
  final_score    = min(primary_score + stacking_bonus + absentee + contact + equity, 100)

This means:
  • The strongest signal sets the base score (0-100).
  • Each additional concurrent signal (within 60 days) adds +20, capped at +40.
  • Universal bonuses (absentee, contact) push the score higher.
  • Equity bonus applies to wholesalers and fix_flip verticals only.
  • 100 = one dominant signal at max recency with full bonus stack.
"""

# ── 6 Vertical weight maps ────────────────────────────────────────────────────
# Base signal score (0-100) for each signal type per vertical.
# The highest-scoring matched signal becomes the primary score.

VERTICAL_WEIGHTS = {
    "wholesalers": {
        "judgment_liens":    95,
        "tax_delinquencies": 90,
        "probate":           85,
        "foreclosures":      80,
        "bankruptcy":        78,
        "deed_transfers":    65,
        "irs_tax_liens":     65,
        "hoa_liens":         55,
        "evictions":         50,
        "mechanics_liens":   45,
        "tampa_code_liens":  40,
        "county_code_liens": 30,
        "building_permits":  20,
        "code_violations":   20,
    },
    "fix_flip": {
        "foreclosures":      95,
        "judgment_liens":    88,
        "irs_tax_liens":     85,
        "bankruptcy":        82,
        "tax_delinquencies": 80,
        "probate":           78,
        "mechanics_liens":   70,
        "hoa_liens":         60,
        "deed_transfers":    55,
        "evictions":         45,
        "tampa_code_liens":  40,
        "code_violations":   35,
        "building_permits":  25,
        "county_code_liens": 20,
    },
    "restoration": {
        "code_violations":   95,
        "tampa_code_liens":  88,
        "building_permits":  80,
        "evictions":         65,
        "hoa_liens":         55,
        "mechanics_liens":   50,
        "probate":           40,
        "deed_transfers":    35,
        # financial distress sources — lower weight for restoration buyers
        "foreclosures":      15,
        "judgment_liens":    15,
        "bankruptcy":        15,
        "tax_delinquencies": 15,
        "irs_tax_liens":     15,
        "county_code_liens": 15,
    },
    "roofing": {
        "building_permits":  95,
        "code_violations":   90,
        "tampa_code_liens":  80,
        "mechanics_liens":   70,
        "hoa_liens":         60,
        "evictions":         45,
        "probate":           35,
        # financial distress sources
        "foreclosures":      20,
        "judgment_liens":    20,
        "bankruptcy":        20,
        "tax_delinquencies": 20,
        "irs_tax_liens":     20,
        "county_code_liens": 20,
        "deed_transfers":    15,
    },
    "public_adjusters": {
        "code_violations":   95,
        "tampa_code_liens":  88,
        "building_permits":  85,
        "hoa_liens":         70,
        "mechanics_liens":   65,
        "evictions":         50,
        "probate":           40,
        # financial distress sources
        "foreclosures":      20,
        "judgment_liens":    20,
        "bankruptcy":        20,
        "tax_delinquencies": 20,
        "irs_tax_liens":     20,
        "county_code_liens": 20,
        "deed_transfers":    15,
    },
    "attorneys": {
        "judgment_liens":    98,
        "irs_tax_liens":     95,
        "foreclosures":      90,
        "bankruptcy":        88,
        "tampa_code_liens":  75,
        "hoa_liens":         70,
        "mechanics_liens":   65,
        "tax_delinquencies": 60,
        "probate":           55,
        "code_violations":   20,
        "building_permits":  20,
        "evictions":         20,
        "deed_transfers":    20,
        "county_code_liens": 20,
    },
}

# ── Recency decay ─────────────────────────────────────────────────────────────
# List of (max_days, bonus) pairs checked in order.
# First matching threshold wins.
# Signal date used: filing_date / opened_date / issue_date / record_date.

RECENCY_BONUSES = [
    (7,   25),   # under 7 days old:   +25
    (14,  15),   # 7-14 days old:      +15
    (30,   5),   # 14-30 days old:     +5
    (None, 0),   # over 30 days old:   +0
]

# ── Universal multipliers ─────────────────────────────────────────────────────

# Absentee ownership bonus (added to final vertical score)
ABSENTEE_BONUS = {
    "Out-of-State":   15,
    "Out-of-County":   8,
    "In-County":       0,
}

# Contact quality bonus
CONTACT_PHONE_BONUS = 15   # verified phone on Owner record
CONTACT_EMAIL_BONUS = 10   # verified email on Owner record

# Equity bonus — wholesalers and fix_flip ONLY per spec
EQUITY_BONUS_HIGH  = 20   # equity_pct > 50%
EQUITY_BONUS_MID   = 10   # equity_pct 30-50%
EQUITY_HIGH_THRESH = 50   # percent
EQUITY_MID_THRESH  = 30   # percent
EQUITY_VERTICALS   = {"wholesalers", "fix_flip"}

# ── Cross-signal stacking ─────────────────────────────────────────────────────
# Same parcel, 2+ distinct signal types within STACKING_WINDOW_DAYS.
# +20 per additional signal beyond the primary, capped at +40.

STACKING_WINDOW_DAYS      = 60
STACKING_BONUS_PER_SIGNAL = 20
STACKING_BONUS_CAP        = 40

# ── Routing thresholds ────────────────────────────────────────────────────────
# Based on final_cds_score (= max across all 6 verticals).

ROUTING_THRESHOLDS = {
    "immediate": 80,   # Real-time SMS within 15 minutes
    "daily":     60,   # Daily morning briefing email
    "weekly":    40,   # Weekly digest only
    # below weekly threshold → urgency_level = "Low", not routed
}

# ── Lead tier cutoffs ─────────────────────────────────────────────────────────
# Maps final_cds_score ranges to DistressScore.lead_tier values.

LEAD_TIER_THRESHOLDS = [
    (90, "Ultra Platinum"),
    (80, "Platinum"),
    (60, "Gold"),
    (40, "Silver"),
    (0,  "Bronze"),
]

# ── Score cap ─────────────────────────────────────────────────────────────────
SCORE_CAP = 100
