"""Call disposition logging — rule values (spec §4.4).

The five dispositions are also the names of the Aircall tags callers apply, so
no tag-to-disposition mapping is needed.
"""
from __future__ import annotations

DISPOSITIONS = ("CONNECTED", "LEFT_VOICEMAIL", "BAD_NUMBER", "DNC_REQUEST", "QUALIFIED_APPOINTMENT")

SHEET_COLUMNS = (
    "Call ID", "Date/Time (ET)", "Caller", "Caller Seat", "Campaign Tag", "Borrower Phone",
    "Disposition", "Talk Duration (sec)", "Qualified (Y/N)", "DNC (Y/N)",
    "Multiple Tags (Y/N)", "Updated At (ET)",
)
SHEET_TIMEZONE = "America/New_York"

DELIVERY_LATENCY_TARGET_SECONDS = 30  # spec §12: disposition → Sheet + Slack
RETRY_MIN_AGE_SECONDS = 60            # let the request's own background task finish first
RETRY_BATCH_LIMIT = 200
