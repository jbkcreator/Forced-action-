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

# The intro text greets the borrower with this contact custom field (LENDING_GHL_CF_INTRO_NAME), never with GHL's own
# first-name merge field, which has no fallback: the field holds the lead's first name, or this word when it has none.
INTRO_NAME_FALLBACK = "there"
# A lead held back for the send window is released only while it is still this fresh (a night is at most 12 h), so a
# lead never gets an intro text a day late.
INTRO_SMS_HOLD_MAX_HOURS = 14

# Same schedule shape as config.lending_web: minutes to wait after the Nth failure (last value repeats).
GHL_RETRY_BACKOFF_MINUTES = (1, 2, 5, 15, 60)
GHL_GIVE_UP_AFTER_HOURS = 72
GHL_ALERT_AFTER_MINUTES = 15
GHL_CONFIG_ERROR_STATUS_CODES = (401, 403)
SWEEP_BATCH_SIZE = 50
# A synced lead whose follow-ups are still not done after this long was lost to a crash or a failed
# hand-off, not to the background task that is still running; the sweep redoes them.
FOLLOWUP_RECOVERY_AFTER_MINUTES = 5
