"""
CDS Multi-Vertical Scoring Configuration

All scoring weights, multipliers, and routing thresholds live here.
To retune weights: edit this file and run:
    python -m src.services.cds_engine --rescore-all
No code changes required.

SCORING FORMULA (per vertical):
  primary_score  = base_weight[best_signal] + recency_bonus(best_signal_date) - age_decay
  stacking_bonus = min((signals_within_window - 1) * 20, 40)
  days_open_mod  = +0/+5/+10/+15  (code_violations only, based on how long open)
  fine_mod       = +0/+5/+10/+20  (code_violations only, based on fine_amount)
  prior_viol_mod = +0/+5/+10/+20  (based on total distinct violations count)
  final_score    = min(primary_score + stacking_bonus + days_open + fine + prior_viol
                       + absentee + contact + equity, 100)

This means:
  • The strongest signal sets the base score (0-100).
  • Each additional concurrent signal (within STACKING_WINDOW_DAYS) adds +20, capped at +40.
  • Age decay penalises stale signals (>1 year → -10, >2 years → -20).
  • Code violation signals get extra modifiers: days open, fine amount, prior count.
  • Equity bonus applies to all verticals with per-vertical rates.
  • 100 = one dominant signal at max recency with full bonus stack.
"""

# ── 6 Vertical weight maps ────────────────────────────────────────────────────
# Base signal score (0-100) for each signal type per vertical.
# The highest-scoring matched signal becomes the primary score.

# -- config/scoring.py --

# Revised Weight Table (Targeting <10% Immediate SMS volume) [cite: 30, 33]
VERTICAL_WEIGHTS = {
    "wholesalers": {
        "tax_delinquencies": 70, # Adjusted [cite: 109]
        "probate": 70,           # Adjusted [cite: 109]
        "judgment_liens": 68,    # Adjusted [cite: 109]
        "foreclosures": 68,      # Adjusted [cite: 109]
        "bankruptcy": 55,        # Adjusted [cite: 109]
        "deed_transfers": 50,    # Adjusted [cite: 109]
        "irs_tax_liens": 55,     # Adjusted [cite: 109]
        "hoa_liens": 68,         # Boosted: unpaid dues = financial distress = motivated seller
        "divorce_filings": 60,   # Couple splitting = motivated to sell quickly
        "evictions": 45,         # Adjusted [cite: 109]
        "mechanics_liens": 55,
        "insurance_claim": 10,   # Stacking only
        "Fire": 10,              # Stacking only
        "storm_damage": 10,      # Stacking only
        "flood_damage": 10,      # Stacking only
        "tampa_code_liens": 35,
        "county_code_liens": 30,
        "building_permits": 20,
        "enforcement_permit": 50,  # Stop work/after-the-fact/failed/expired/revoked
        "code_violations": 35,
    },
    "fix_flip": {
        "foreclosures": 75,      # Adjusted [cite: 109]
        "bankruptcy": 68,        # Adjusted [cite: 109]
        "tax_delinquencies": 65, # Adjusted [cite: 109]
        "judgment_liens": 55,    # Adjusted [cite: 109]
        "irs_tax_liens": 50,     # Adjusted [cite: 109]
        "probate": 55,           # Adjusted [cite: 109]
        "mechanics_liens": 60,   # Adjusted [cite: 109]
        "hoa_liens": 65,         # Boosted: financial distress = motivated to sell
        "divorce_filings": 55,   # Motivated seller, property often priced to move
        "deed_transfers": 45,    # Adjusted [cite: 109]
        "evictions": 40,         # Adjusted [cite: 109]
        "insurance_claim": 10,   # Stacking only
        "Fire": 10,              # Stacking only
        "storm_damage": 10,      # Stacking only
        "flood_damage": 10,      # Stacking only
        "tampa_code_liens": 45,
        "code_violations": 50,
        "building_permits": 45,
        "enforcement_permit": 60,  # Stop work/after-the-fact/failed/expired/revoked
        "county_code_liens": 20,
    },
    "restoration": {
        "code_violations": 90,   # Kept high for this vertical [cite: 109]
        "insurance_claim": 10,   # Stacking only
        "Fire": 10,              # Stacking only
        "storm_damage": 10,      # Stacking only
        "flood_damage": 10,      # Stacking only
        "tampa_code_liens": 75,  # Adjusted [cite: 109]
        "building_permits": 10,  # Stacking only — active permit = contractor on job, not a distress signal
        "enforcement_permit": 75,  # Stop work/after-the-fact/failed/expired/revoked
        "evictions": 65,
        "hoa_liens": 50,
        "divorce_filings": 25,
        "mechanics_liens": 55,
        "probate": 20,
        "deed_transfers": 20,
        "foreclosures": 30,
        "judgment_liens": 45,
        "bankruptcy": 30,
        "tax_delinquencies": 30,
        "irs_tax_liens": 40,
        "county_code_liens": 15,
    },
    "roofing": {
        "building_permits": 10,  # Stacking only — active permit = contractor on job, not a lead signal
        "enforcement_permit": 80,  # Stop work/after-the-fact/failed/expired/revoked — real distress
        "insurance_claim": 10,   # Stacking only
        "Fire": 10,              # Stacking only
        "storm_damage": 10,      # Stacking only
        "flood_damage": 10,      # Stacking only
        "code_violations": 68,   # Lowered from 90 [cite: 109]
        "tampa_code_liens": 65,  # Lowered from 80 [cite: 109]
        "mechanics_liens": 60,   # Adjusted [cite: 109]
        "hoa_liens": 60,
        "divorce_filings": 20,
        "evictions": 55,         # Adjusted [cite: 109]
        "probate": 20,
        "foreclosures": 30,
        "judgment_liens": 55,
        "bankruptcy": 30,
        "tax_delinquencies": 30,
        "irs_tax_liens": 50,
        "county_code_liens": 20,
        "deed_transfers": 20,
    },
    "public_adjusters": {        # Activated Vertical
        "code_violations": 85,
        "insurance_claim": 10,   # Stacking only
        "Fire": 10,              # Stacking only
        "storm_damage": 10,      # Stacking only
        "flood_damage": 10,      # Stacking only
        "tampa_code_liens": 70,  # Adjusted from 80
        "building_permits": 55,  # Adjusted [cite: 109]
        "enforcement_permit": 70,  # Stop work/after-the-fact/failed/expired/revoked
        "hoa_liens": 50,         # Adjusted from 55
        "divorce_filings": 20,
        "mechanics_liens": 50,   # Adjusted from 60
        "evictions": 50,
        "probate": 25,
        "foreclosures": 30,
        "judgment_liens": 50,
        "bankruptcy": 30,
        "tax_delinquencies": 30,
        "irs_tax_liens": 45,
        "county_code_liens": 20,
        "deed_transfers": 20,
    },
    "attorneys": {
        "judgment_liens": 72,    # Lowered from 98 [cite: 109]
        "irs_tax_liens": 70,     # Lowered from 95 [cite: 109]
        "divorce_filings": 65,   # Property settlements, title disputes = direct attorney work
        "foreclosures": 55,      # Adjusted [cite: 109]
        "bankruptcy": 55,        # Adjusted [cite: 109]
        "tampa_code_liens": 40,  # Adjusted [cite: 109]
        "hoa_liens": 70,         # Boosted: HOA litigation is common attorney engagement
        "mechanics_liens": 50,   # Adjusted [cite: 109]
        "tax_delinquencies": 50, # Adjusted [cite: 109]
        "probate": 40,           # Adjusted [cite: 109]
        "insurance_claim": 10,   # Stacking only
        "Fire": 10,              # Stacking only
        "storm_damage": 10,      # Stacking only
        "flood_damage": 10,      # Stacking only
        "code_violations": 30,
        "building_permits": 20,
        "enforcement_permit": 40,  # Stop work/after-the-fact/failed/expired/revoked
        "evictions": 25,
        "deed_transfers": 25,
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

# Equity thresholds — shared across all verticals (rates per vertical in EQUITY_BONUS_BY_VERTICAL)
EQUITY_HIGH_THRESH = 50   # percent — equity_pct > 50% → full bonus
EQUITY_MID_THRESH  = 30   # percent — equity_pct 30-50% → half bonus
# Legacy aliases kept for any external references
EQUITY_BONUS_HIGH  = 20
EQUITY_BONUS_MID   = 10
EQUITY_VERTICALS   = {"wholesalers", "fix_flip", "attorneys", "roofing", "restoration", "public_adjusters"}

# ── Cross-signal stacking ─────────────────────────────────────────────────────
# Same parcel, 2+ distinct signal types within STACKING_WINDOW_DAYS.
# +20 per additional signal beyond the primary, capped at +40.

STACKING_WINDOW_DAYS      = 180  # Approved 2026-03-25: expanded from 90 to capture wider stacking signal window
STACKING_BONUS_PER_SIGNAL = 20
STACKING_BONUS_CAP        = 60

# ── Stacking-only signals ─────────────────────────────────────────────────────
# These signals cannot act as the PRIMARY scoring signal on their own.
# A property carrying ONLY these signal types is not scored (returns 0).
# They contribute only as stacking bonuses when a primary signal is also present.
STACKING_ONLY_SIGNALS = {
    "insurance_claim",
    "fire",
    "storm_damage",
    "flood_damage",
    # building_permits (non-enforcement) = contractor on job or active work permit.
    # Not a distress signal on its own — only adds value when stacked with a primary
    # signal (lien, judgment, foreclosure, etc.). Enforcement permits remain a separate
    # high-weight signal type and are NOT stacking-only.
    "building_permits",
}

# ── Age decay ─────────────────────────────────────────────────────────────────
# Applied as a negative modifier to signals that are stale.
# Penalises old records even if their base weight is high (may be settled/resolved).

AGE_DECAY_1Y = -10   # 366–730 days old
AGE_DECAY_2Y = -20   # >730 days old
SIGNAL_HARD_CUTOFF_DAYS = 730  # Signals with a known date older than this are excluded from scoring entirely

# ── Days-open modifier (code_violations only) ─────────────────────────────────
# Measures how long a violation has been open with no resolution.
# Longer open = higher distress urgency.

DAYS_OPEN_MODIFIERS = [
    (30,   0),    # 0–30 days open:   +0
    (90,   5),    # 31–90 days open:  +5
    (180, 10),    # 91–180 days open: +10
    (None, 15),   # 180+ days open:   +15
]

# ── Violation Persistence Score (replaces fine_amount — not captured from Accela) ─
# Measures owner inaction and escalation behavior using fields available from Accela.
# Applied only when code_violations is the primary signal for a vertical.
# Combined with days_open_mod and prior_viol_mod for the complete violation picture.
#
# Component A — Status escalation (max +12):
#   Derived from the Accela 'Status' field on the most recent violation.
#   High escalation:  city enforcement has advanced past initial notice (hearing, abatement).
#   Active/open:      violation issued but owner has not yet resolved.
#   Resolved:         owner complied — distress signal weakens.
#
# Component B — Violation type diversity (max +8):
#   Count of distinct violation_type values across ALL violations for the property.
#   Many single-type violations = localized problem (low diversity).
#   Multiple distinct types = systemic, multi-domain neglect (high diversity).
#
# Total max: 20 — same cap as the former fine_mod for score stability.

# Component A: status keyword sets (matched case-insensitively)
PERSISTENCE_ESCALATION_KEYWORDS = frozenset({
    "hearing", "abatement", "order", "lien", "re-open", "reopen",
})
PERSISTENCE_RESOLVED_KEYWORDS = frozenset({
    "complied", "closed", "resolved", "withdrawn", "expired",
    "cancelled", "void", "dismissed",
})
PERSISTENCE_STATUS_ESCALATED = 12   # status contains escalation keyword
PERSISTENCE_STATUS_ACTIVE    = 6    # status is open/issued but not yet escalated or resolved
PERSISTENCE_STATUS_RESOLVED  = 0    # status indicates compliance / closure

# Component B: distinct violation_type count → bonus
PERSISTENCE_SCOPE_BONUSES = [
    (1,    0),    # 1 distinct type:  +0  (single-issue, focused problem)
    (2,    4),    # 2 distinct types: +4  (multi-issue)
    (None, 8),    # 3+ distinct types:+8  (chronic multi-domain neglect)
]

# ── Prior violations count modifier ──────────────────────────────────────────
# Total distinct violation events for the property across all time.
# Pattern of repeat violations = chronic distress.

PRIOR_VIOLATIONS_MODIFIERS = [
    (1,    0),    # 1 violation:    +0
    (3,    5),    # 2–3 violations: +5
    (5,   10),    # 4–5 violations: +10
    (None, 20),   # 6+ violations:  +20
]

# ── Per-vertical equity bonus ─────────────────────────────────────────────────
# Expanded from wholesalers/fix_flip only to all verticals.
# A property owner with equity can afford to act (repair, sell, settle).

EQUITY_BONUS_BY_VERTICAL = {
    "wholesalers":     20,   # Primary investment signal
    "fix_flip":        20,   # Primary investment signal
    "attorneys":       10,   # Has assets worth collecting on
    "roofing":          8,   # Owner can afford the repair
    "restoration":      8,   # Owner can afford remediation
    "public_adjusters": 5,   # Secondary signal
}

# ── Routing thresholds ────────────────────────────────────────────────────────
# Based on final_cds_score (= max across all 6 verticals).

ROUTING_THRESHOLDS = {
    "immediate": 80,   # Real-time SMS within 15 minutes
    "daily":     57,   # Daily morning briefing email — aligned with Gold cutoff
    "weekly":    40,   # Weekly digest only
    # below weekly threshold → urgency_level = "Low", not routed
}

# ── Lead tier cutoffs ─────────────────────────────────────────────────────────
# Maps final_cds_score ranges to DistressScore.lead_tier values.

LEAD_TIER_THRESHOLDS = [
    (95, "Ultra Platinum"),   # raised from 90 — base weights of 90/85 made UP too accessible
    (83, "Platinum"),          # raised from 80 — wider 83–94 band is defensible at price
    (57, "Gold"),
    (40, "Silver"),
    (0,  "Bronze"),
]

# ── Score cap ─────────────────────────────────────────────────────────────────
SCORE_CAP = 100

# ── HCPA passive signal weights ───────────────────────────────────────────────
# Derived from Property.year_built, Financial.last_sale_date, Financial.value_change_yoy.
# Act as stacking boosters — they add weight only when a primary signal is already present.
# Thresholds:
#   property_age_30plus  → year_built < (current_year - 30)  — aging structure likely needs work
#   long_term_owner      → last_sale_date older than 10 years — owner may be motivated to exit
#   declining_value      → value_change_yoy < 0              — assessed value dropping YoY

HCPA_PASSIVE_WEIGHTS = {
    "property_age_30plus": {
        "roofing":          20,
        "restoration":      18,
        "fix_flip":         12,
        "wholesalers":      10,
        "public_adjusters":  8,
        "attorneys":         5,
    },
    "long_term_owner": {
        "wholesalers":      18,
        "fix_flip":         15,
        "attorneys":        12,
        "restoration":       8,
        "roofing":           5,
        "public_adjusters":  5,
    },
    "declining_value": {
        "wholesalers":      15,
        "fix_flip":         12,
        "attorneys":        10,
        "restoration":       8,
        "roofing":           5,
        "public_adjusters":  5,
    },
}

# Year threshold for property_age_30plus signal (years old)
HCPA_AGE_YEARS = 30
# Year threshold for long_term_owner signal (years since last sale)
HCPA_LONG_TERM_YEARS = 10

# ── Owner-occupied suppression ────────────────────────────────────────────────
# For investment verticals, an owner living at the property is not a motivated seller.
# Zero out these verticals if mailing_address == property address.
# Restoration/roofing/public_adjusters are intentionally excluded — homeowners
# are valid leads for contractors (they need the work done, not to sell).
OWNER_OCCUPIED_EXCLUSION_VERTICALS = frozenset({
    "wholesalers",
    "fix_flip",
    "attorneys",
})

# ── Dead lead gate — recent deed transfer ─────────────────────────────────────
# A property sold within this window is off-market. Distress signals belong to
# the previous owner. Return no signals so the property scores 0 and exits routing.
DEAD_LEAD_DEED_DAYS = 45
