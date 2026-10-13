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
GHL_TAG_NO_LEAD_ID = "lendingflow-no-lead-id"

# Same schedule shape as config.lending_web: minutes to wait after the Nth failure (last value repeats).
GHL_RETRY_BACKOFF_MINUTES = (1, 2, 5, 15, 60)
GHL_GIVE_UP_AFTER_HOURS = 72
GHL_ALERT_AFTER_MINUTES = 15
GHL_CONFIG_ERROR_STATUS_CODES = (401, 403)
SWEEP_BATCH_SIZE = 50
# A synced lead whose follow-ups are still not done after this long was lost to a crash or a failed
# hand-off, not to the background task that is still running; the sweep redoes them.
FOLLOWUP_RECOVERY_AFTER_MINUTES = 5
