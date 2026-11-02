"""T-11 LendingFlow intake: rule values."""
from __future__ import annotations

LEAD_SOURCE = "lendingflow"
MAX_BODY_BYTES = 256 * 1024

GHL_TAG_LENDINGFLOW = "lendingflow"
GHL_TAG_CONSENT_MISSING = "lendingflow-consent-missing"
# Added only for a lead with a consent certificate, in a call of its own after the other tags: it starts the GHL intro-SMS
# workflow (docs/lending/intro-sms.md). The consent decision is made here, never inside GHL.
GHL_TAG_INTRO_READY = "lendingflow-intro-ready"
GHL_SOURCE = "LendingFlow"
# A lead that arrives without a LendingFlow lead id is stored, not rejected (client, Oct 2026): its vendor id is this
# prefix + the phone/email dedupe hash, and GHL gets this tag so the missing id is visible.
MISSING_LEAD_ID_PREFIX = "missing:"


def is_missing_lead_id(vendor_lead_id: str) -> bool:
    """True for a lead stored without a LendingFlow lead id (its vendor id was made up from phone + email)."""
    return vendor_lead_id.startswith(MISSING_LEAD_ID_PREFIX)

GHL_TAG_NO_LEAD_ID = "lendingflow-no-lead-id"

# The intro text greets the borrower with this contact custom field (LENDING_GHL_CF_INTRO_NAME), never with GHL's own
# first-name merge field, which has no fallback: the field holds the lead's first name, or this word when it has none.
INTRO_NAME_FALLBACK = "there"
# A lead held back for the send window is released only while it is still this fresh, so a lead never gets an intro
# text more than a day late. Must outlast the longest stretch the window can stay closed: with the strictest-zone rule
# a Hawaii phone on an Eastern property is open only 14:00-20:00 ET, i.e. closed 18 h
# (tests/lending/test_text_window.py checks every supported zone combination against this value).
INTRO_SMS_HOLD_MAX_HOURS = 24

# Same schedule shape as config.lending_web: minutes to wait after the Nth failure (last value repeats).
GHL_RETRY_BACKOFF_MINUTES = (1, 2, 5, 15, 60)
GHL_GIVE_UP_AFTER_HOURS = 72
GHL_ALERT_AFTER_MINUTES = 15
GHL_CONFIG_ERROR_STATUS_CODES = (401, 403)
SWEEP_BATCH_SIZE = 50
# A synced lead whose follow-ups are still not done after this long was lost to a crash or a failed
# hand-off, not to the background task that is still running; the sweep redoes them.
FOLLOWUP_RECOVERY_AFTER_MINUTES = 5

# --- DNC check before the GHL push (LENDING_LENDINGFLOW_DNC_CHECK_ENABLED) ------------------------------------------
# A Tracerfy batch can poll for up to 10 minutes: a lead left 'checking' longer than this was lost to a crash.
DNC_CHECK_STALE_MINUTES = 15

# --- LendingFlow -> BatchDialer (src/lending/lendingflow_dialer.py) -------------------------------------------------
DIALER_POOL = "lendingflow"  # config.lending_dialer.POOL_CAMPAIGN_TAGS key -> BatchDialer campaign "LendingFlow"
DIALER_REF_PREFIX = "lendingflow:"  # dialer_load_records.source_record_ref = prefix + lead_uuid
# A lead still not loaded this long after arriving is no longer worth an auto-dial (the 14-day follow-up window).
DIALER_MAX_LEAD_AGE_DAYS = 14
# Wait before retrying a lead the loader turned away for a temporary reason (calling hours, cap, scrub, halt).
DIALER_RETRY_MINUTES = 15
# Unexpected push failures (HTTP errors) before a lead stops retrying and is left as 'failed' for a person.
DIALER_MAX_FAILURES = 5
DIALER_BATCH_SIZE = 25
# One dialer step at a time (transaction advisory lock held for the whole run, next to the other lending lock keys).
DIALER_LOCK_KEY = 7_302_020_810
# Loaded leads whose GHL calls are re-read live per sweep (one GHL lookup each), least recently checked first.
DIALER_GHL_RECHECK_BATCH = 50

# Loader reasons (compliance.ReasonCode values + dialer_load reasons) that only mean "not now": the lead stays pending.
# Every other reason blocks the lead for good; an unknown reason is treated as temporary and logged.
DIALER_TEMPORARY_REASONS = frozenset({
    "OUTSIDE_CALL_WINDOW", "ATTEMPT_CAP_REACHED", "NO_FRESH_SCRUB", "SCRUB_FAILED", "NEEDS_SCRUB",
    "BACKFLIP_FEED_STALE", "BACKFLIP_FEED_UNAVAILABLE", "HALTED", "LOAD_REFUSED", "GHL_CHECK_FAILED",
})
DIALER_PERMANENT_REASONS = frozenset({
    "INVALID_PHONE", "NATIONAL_DNC", "STATE_DNC", "LITIGATOR", "SUPPRESSED", "GA_NATURAL_PERSON",
    "HOMESTEAD_OWNER_OCCUPIED", "ATTEMPT_HISTORY_EXCEEDED", "BACKFLIP_CONFLICT", "NO_CONSENT", "HANDLED_IN_GHL",
    "IN_OTHER_CAMPAIGN",
})

# "Handled in GHL" = booked (GHL appointment webhook) or a connected GHL call with at least this much talk time.
# PLACEHOLDER until Josh defines "connected" (plan item 5).
HANDLED_MIN_TALK_SECONDS = 60
GHL_CONNECTED_OUTCOMES = ("ghl:completed", "ghl:answered")  # call_dispositions.disposition_raw of a GHL call

# --- BatchDialer call results on the GHL contact (src/lending/bd_result_to_ghl.py) ----------------------------------
GHL_TAG_CALLED_IN_BATCHDIALER = "called-in-batchdialer"
GHL_TAG_BD_CONNECTED = "bd-connected"
GHL_TAG_BD_CALLBACK = "bd-callback"
GHL_TAG_BD_BOOKED = "bd-booked"
BD_NOTE_LOOKBACK_HOURS = 24  # calls older than this are no longer written to GHL
BD_NOTE_MAX_ATTEMPTS = 5
