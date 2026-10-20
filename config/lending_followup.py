"""14-day follow-up: call-attempt tracking and the LendingFlow replacement rule (Runbook § 4.1, Item 1).

The cadence itself is a GoHighLevel workflow (docs/lending/followup-14-day.md). The code side records every call
attempt in lending.call_dispositions, including calls made from GoHighLevel, and reports which LendingFlow leads
qualify for replacement credit.
"""
from __future__ import annotations

# lending.call_dispositions.call_source: where a call record came from.
CALL_SOURCE_BATCHDIALER = "batchdialer"
CALL_SOURCE_GHL = "ghl"

# GHL calls are stored with dialer_call_id "ghl:<message id>" so a re-import never duplicates one.
GHL_CALL_ID_PREFIX = "ghl:"
# Shown as the caller and the campaign "hook" on the scoreboard, so GHL calls form their own line.
GHL_CALLER_SEAT = "ghl"
GHL_CAMPAIGN_TAG = "ghl"

# Leads whose GHL calls are imported: received within the 14-day cadence plus a margin for late calls.
IMPORT_WINDOW_DAYS = 16
# Contacts read per run; each costs one search call plus one call per conversation page. With more eligible
# contacts than this, successive runs take successive slices (newest leads first), so every contact is read
# within ceil(eligible / limit) runs.
IMPORT_CONTACT_LIMIT = 200
# The cron interval of src.lending.ghl_call_import; it numbers the runs that pick the slice.
IMPORT_RUN_INTERVAL_MINUTES = 15

# Runbook § 4.1: "leads with 5+ attempts over 5+ days are eligible for replacement credit".
REPLACEMENT_MIN_ATTEMPTS = 5
REPLACEMENT_MIN_CALL_DAYS = 5
# Call days are counted on the Eastern calendar date (the business runs on ET).
REPLACEMENT_DAY_TIMEZONE = "America/New_York"
