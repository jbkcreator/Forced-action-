"""T-11 LendingFlow intake: rule values."""
from __future__ import annotations

LEAD_SOURCE = "lendingflow"
MAX_BODY_BYTES = 256 * 1024

GHL_TAG_LENDINGFLOW = "lendingflow"
GHL_TAG_CONSENT_MISSING = "lendingflow-consent-missing"
GHL_SOURCE = "LendingFlow"

# Same schedule shape as config.lending_web: minutes to wait after the Nth failure (last value repeats).
GHL_RETRY_BACKOFF_MINUTES = (1, 2, 5, 15, 60)
GHL_GIVE_UP_AFTER_HOURS = 72
GHL_ALERT_AFTER_MINUTES = 15
GHL_CONFIG_ERROR_STATUS_CODES = (401, 403)
SWEEP_BATCH_SIZE = 50
