"""
Vertical Fit Rubric — 6-dimension scoring config for REVINT-v2.2 autopilot.

All thresholds are founder-editable defaults.
"""

# ── Dim 1: Volume floor ────────────────────────────────────────────────────────
MIN_MONTHLY_RECORDS = 500  # Dim 1: minimum monthly records to qualify

# ── Packet threshold ───────────────────────────────────────────────────────────
VERTICAL_FIT_THRESHOLD = 5  # ≥5/6 dims → Vertical Candidate Packet

# ── Dim 6: Legal risk ─────────────────────────────────────────────────────────

# Founder-approved low-risk verticals → eligible_for_probe=True, legal_status="approved"
LEGAL_RISK_ALLOWLIST = [
    "tax_lien",
    "lis_pendens",
    "probate",
    "pre_foreclosure",
    "auction",
    # Josh adds more here
]

# Auto-flag → legal_status="blocked", eligible_for_probe=False
LEGAL_RISK_BLOCKLIST_CATEGORIES = [
    "consumer_credit",
    "protected_financial_data",
    "tcpa_sensitive_automated_contact",
    "referral_fees_or_brokerage",
    "record_level_data_licensing_without_dpa",
    "legal_advice",
    "unclear_privacy_licensing",
]

# ── Probe verdict thresholds ───────────────────────────────────────────────────
PROBE_KILL_THRESHOLD = 0.03   # <3% reply rate → killed
PROBE_WIN_THRESHOLD = 0.08    # >8% reply rate → won
# 3–8% → running (below sample floor) or awaiting_ruling (at/above floor)

PROBE_MAX_SENDS_PER_RUN = 30  # blast-radius cap for a single probe execution

# Minimum cumulative sends (across all probes for a packet) before kill/win
# verdicts may fire.  Below this the verdict stays "running".
# Asymmetric: killing is irreversible; winning routes to confirm_presell (manual gate).
PROBE_MIN_SAMPLE_KILL = 200
PROBE_MIN_SAMPLE_WIN = 100

# Hard send ceiling per packet.  run_probe refuses to start a new run once this
# total is reached and packet status is "awaiting_ruling".
# Quoted directly from the client: "off four hundred it's signal".
PROBE_SEND_CEILING = 400

# When False every sub-3% result routes to awaiting_ruling regardless of sample,
# rather than auto-killing.  Flip only after the client ratifies the thresholds.
VERTICAL_AUTO_KILL_ENABLED: bool = False

# ── Probe campaign schedule (mirrors Relay send window) ───────────────────────
PROBE_CAMPAIGN_SEND_FROM = "11:00"
PROBE_CAMPAIGN_SEND_TO   = "18:00"
PROBE_CAMPAIGN_TIMEZONE  = "America/New_York"
PROBE_CAMPAIGN_DAYS: dict = {
    "monday": True, "tuesday": True, "wednesday": True,
    "thursday": True, "friday": True,
    "saturday": False, "sunday": False,
}

# DBPR contractor trades to target per probe vertical
PROBE_DBPR_VERTICAL_MAP: dict[str, list[str]] = {
    "tax_lien":        ["general", "roofing", "hvac"],
    "lis_pendens":     ["general", "roofing"],
    "probate":         ["general"],
    "pre_foreclosure": ["general", "roofing"],
    "auction":         ["general", "roofing", "hvac"],
}

# Probe outreach copy — {first_name} and {vertical} are interpolated at send time
PROBE_EMAIL_SUBJECT = "Distressed property leads — {vertical} contractors in your area"
PROBE_EMAIL_BODY = (
    "Hi {first_name},\n\n"
    "We surface distressed property opportunities in Hillsborough and Pinellas — "
    "homeowners facing {vertical} damage, liens, or pre-foreclosure who need work done fast.\n\n"
    "Testing whether our lead flow fits your business. Would a list of 5–10 "
    "verified distressed properties in your area be useful this week?\n\n"
    "Reply yes or no — takes 5 seconds.\n\n"
    "— Forced Action"
)

# ── Dim 5: Buyer evidence signals ─────────────────────────────────────────────

# Money evidence signals — at least one required for Dim 5 to score
MONEY_EVIDENCE_SIGNALS = [
    "prior_purchases",
    "cash_volume",
    "existing_business_spend",
    "subscription_history",
    "company_size",
]

# Urgency evidence signals — at least one required for Dim 5 to score
URGENCY_EVIDENCE_SIGNALS = [
    "auction_date",
    "storm_event",
    "filing_deadline",
    "license_expiry",
    "regulatory_deadline",
    "fresh_acquisition",
]
