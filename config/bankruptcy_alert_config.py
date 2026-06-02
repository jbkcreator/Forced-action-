"""
Stage 12 — Bankruptcy Filing Alert Product configuration.

A standalone $297/mo subscription product for attorneys, investors, and lenders.
Separate audience from the property-intelligence subscribers (no ZIP territory,
no vertical) — its own subscription table and Stripe webhook.

Source of truth for: pricing, trial, jurisdiction + chapter filters, and the
CourtListener polling knobs (kept rate-limit-friendly).
"""

# ── Pricing ───────────────────────────────────────────────────────────────────

PRICE_MONTHLY_CENTS = 29700          # $297/mo
TRIAL_DAYS = 14                      # optional trial; set 0 to disable
PRODUCT_NAME = "Bankruptcy Filing Alerts"

# ── Subscription statuses ───────────────────────────────────────────────────────

# Statuses that should receive alerts.
ALERT_ELIGIBLE_STATUSES = ("trialing", "active")
# All valid statuses (enforced by DB check constraint).
VALID_STATUSES = ("trialing", "active", "past_due", "canceled")

# ── Filing filters ──────────────────────────────────────────────────────────────

# Bankruptcy chapters relevant to attorneys / investors / lenders.
# Chapter 7 (liquidation), 11 (reorg), 13 (wage-earner). 12 (family farmer) optional.
RELEVANT_CHAPTERS = ("7", "11", "13")

# Jurisdictions to ingest. Keyed by a stable jurisdiction_id used in subscription
# filters; value carries the CourtListener court code + division docket prefix.
# Add rows here to expand coverage without code changes.
JURISDICTIONS = {
    "flmb-tampa": {
        "label": "FL Middle District — Tampa Division",
        "court_code": "flmb",
        "division_prefix": "8:",
    },
    "flmb-orlando": {
        "label": "FL Middle District — Orlando Division",
        "court_code": "flmb",
        "division_prefix": "6:",
    },
}

# Default filter applied to a new subscription when the buyer doesn't choose:
# all jurisdictions, all relevant chapters.
DEFAULT_JURISDICTIONS = list(JURISDICTIONS.keys())
DEFAULT_CHAPTERS = list(RELEVANT_CHAPTERS)

# ── CourtListener polling (rate-limit friendly) ──────────────────────────────────

# CourtListener allows ~per-minute quotas on authenticated tokens. Keep paging
# polite and bounded so a backfill can't hammer the API.
COURTLISTENER_PAGE_DELAY_SECONDS = 1.0   # sleep between paginated requests
COURTLISTENER_MAX_PAGES = 20             # hard cap per ingest run (safety)
COURTLISTENER_PAGE_SIZE = 100            # API max page size
DEFAULT_LOOKBACK_DAYS = 1                # daily incremental window

# ── Signup invite (post-signup outreach) ─────────────────────────────────────────
# When a new property subscriber (free or paid) signs up, a bankruptcy-alert
# invite email is SCHEDULED for T + BANKRUPTCY_INVITE_DELAY_MINUTES and sent by
# the invite-sweep cron. Tracked/deduped via message_outcomes.template_id.
INVITE_TEMPLATE_ID = "bankruptcy_alert_invite"
INVITE_SUBJECT = "New: Daily Bankruptcy Filing Alerts for your market ($297/mo)"
# Subscriber statuses to skip at send time (don't email dead accounts).
INVITE_SKIP_STATUSES = ("churned", "cancelled")
# Give up (mark failed) on a scheduled invite older than this many hours, so a
# persistent send error can't re-mint Stripe sessions forever.
INVITE_GIVE_UP_HOURS = 24
# Max invites a single sweep pass will send.
INVITE_SWEEP_BATCH = 200


# ── Alerting ──────────────────────────────────────────────────────────────────

# Channels available; per-subscription toggles live on the subscription row.
CHANNELS = ("email", "sms")
# Max filings to summarise in a single digest message (avoid SMS truncation spam).
MAX_FILINGS_PER_DIGEST = 25
# Ops alert thresholds for monitoring.
INGEST_FAILURE_ALERT = True               # email ops on ingest API failure
ALERT_DELIVERY_FAILURE_RATE_THRESHOLD = 0.25  # >25% send failures → ops alert


def get_jurisdiction(jurisdiction_id: str) -> dict:
    """Return the jurisdiction config, or raise KeyError."""
    return JURISDICTIONS[jurisdiction_id]


def jurisdiction_for_docket(court_code: str, docket_number: str) -> str | None:
    """Map a (court_code, docket_number) to a jurisdiction_id, or None if no match."""
    for jid, cfg in JURISDICTIONS.items():
        if cfg["court_code"] == court_code and docket_number.startswith(cfg["division_prefix"]):
            return jid
    return None
