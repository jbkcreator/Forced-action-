"""WP-GL-10 booking confirmation and reminder configuration (values only, no logic).

Wording is verbatim from Josh's Oct 2 answers to B3. Any change needs his re-approval and a
matching update of the 10DLC campaign sample messages.

Open items (not yet answered by the client; defaults are developer choices, not decisions):
  - NIGHT_BEFORE_HOUR_ET: "the evening before" has no clock time.
  - CALLBACK_NUMBER: which number goes in the 90-minute text.

Texts go through GoHighLevel only (src/lending/ghl_sms.py); consent is src/lending/consent.py.
"""
from __future__ import annotations

from zoneinfo import ZoneInfo

from config.lending_text_back import MAX_TEXT_CHARS, QUIET_END_HOUR, QUIET_START_HOUR, STALE_SENDING_SECONDS

# ── Scheduling ──────────────────────────────────────────────────────────────

TIMEZONE = ZoneInfo("America/New_York")  # same convention as WP-GL-9: one Eastern clock

NIGHT_BEFORE_HOUR_ET = 18  # OPEN QUESTION (A1): developer default, 6:00 PM ET the evening before
NIGHT_BEFORE_MINUTE_ET = 0
NINETY_MIN_SECONDS = 90 * 60

# A reminder's wording is a statement about time ("tomorrow", "in about 90 minutes"), so a reminder the
# worker could not send promptly (outage, backlog) is skipped as too_late rather than sent with wrong
# wording: the night-before text is only valid on the Eastern day before the call; the 90-minute text
# only within this many seconds after it came due.
NINETY_MIN_MAX_LATE_SECONDS = 15 * 60

# Texts are only sent inside this ET window (WP-GL-9's safety window, same constants). A confirmation
# or night-before text that comes due outside it waits for the window to open; the 90-minute text is
# never deferred (its wording is a promise about timing) and is recorded as skipped_quiet_hours.
TEXT_WINDOW_START_HOUR = QUIET_START_HOUR
TEXT_WINDOW_END_HOUR = QUIET_END_HOUR

# ── Sending robustness (mirrors WP-GL-9: a text is never blindly re-sent) ─────

MAX_SEND_ATTEMPTS = 3          # only for failures that provably sent nothing
RETRY_DELAY_SECONDS = 60
# A row stuck in 'sending' longer than this is closed as send_unknown and NEVER resent.
STALE_SEND_SECONDS = STALE_SENDING_SECONDS
BATCH_SIZE = 50
POLL_SECONDS = 10

# Final-state vocabulary for lending.booking_messages.status.
STATUS_PENDING, STATUS_SENDING, STATUS_SENT = "pending", "sending", "sent"
STATUS_SEND_UNKNOWN, STATUS_FAILED = "send_unknown", "failed"
STATUS_SKIPPED, STATUS_CANCELLED = "skipped", "cancelled"
ALL_STATUSES = (STATUS_PENDING, STATUS_SENDING, STATUS_SENT, STATUS_SEND_UNKNOWN, STATUS_FAILED,
                STATUS_SKIPPED, STATUS_CANCELLED)

CHANNEL_TEXT, CHANNEL_EMAIL = "text", "email"

# G5: an AI-booked call with no caller on shift is assigned to Josh. Taken from his questionnaire
# answers (E1/G5); change here, not in code.
FALLBACK_ASSIGNEE = "jbkantor@gmail.com"

# ── Text message kinds ───────────────────────────────────────────────────────

KIND_CONFIRMATION = "confirmation"
KIND_NIGHT_BEFORE = "night_before"
KIND_NINETY_MIN = "ninety_min"
ALL_KINDS = (KIND_CONFIRMATION, KIND_NIGHT_BEFORE, KIND_NINETY_MIN)

# ── Approved text templates (B3, Josh Oct 2) ────────────────────────────────

# Placeholders: {first_name}, {date}, {time}, {property_address}, {number}
# The "{property_address}" block (including " about {property_address}") is
# dropped when no address is known (B3: "No address: drop that phrase").

CONFIRMATION_WITH_ADDRESS = (
    "Hi {first_name}, this is Next Deal Lending confirming your call with Josh on "
    "{date} at {time} about {property_address}. "
    "Reply here if you need to reschedule. Reply STOP to opt out."
)

CONFIRMATION_NO_ADDRESS = (
    "Hi {first_name}, this is Next Deal Lending confirming your call with Josh on "
    "{date} at {time}. "
    "Reply here if you need to reschedule. Reply STOP to opt out."
)

NIGHT_BEFORE_WITH_ADDRESS = (
    "Hi {first_name}, reminder from Next Deal Lending. "
    "Your call with Josh is tomorrow at {time} about {property_address}. Talk soon."
)

NIGHT_BEFORE_NO_ADDRESS = (
    "Hi {first_name}, reminder from Next Deal Lending. "
    "Your call with Josh is tomorrow at {time}. Talk soon."
)

# The approved 90-minute wording carries no property address, so there is one variant.
NINETY_MIN_NO_ADDRESS = (
    "Hi {first_name}, your Next Deal Lending call with Josh is in about 90 minutes, "
    "at {time}. "
    "Call us at {number} if anything's come up."
)
NINETY_MIN_WITH_ADDRESS = NINETY_MIN_NO_ADDRESS

# ── Email fallback (B4) ──────────────────────────────────────────────────────

# Contacts without text consent receive the same messages by email instead.
# Email is also used for all contacts if 10DLC is not yet approved.
EMAIL_FROM = "hello@nextdeallending.com"
EMAIL_FROM_NAME = "Next Deal Lending"

EMAIL_SUBJECT_CONFIRMATION = "Your call with Josh is confirmed"
EMAIL_SUBJECT_NIGHT_BEFORE = "Reminder: your call with Josh is tomorrow"
EMAIL_SUBJECT_NINETY_MIN = "Your call with Josh starts in 90 minutes"

# Email bodies mirror the text templates; no STOP language (email uses
# unsubscribe links per CAN-SPAM, handled by the email sender).
EMAIL_CONFIRMATION_WITH_ADDRESS = (
    "Hi {first_name},\n\n"
    "This is Next Deal Lending confirming your call with Josh on {date} at {time} "
    "about {property_address}.\n\n"
    "Reply to this email if you need to reschedule.\n\n"
    "Next Deal Lending\nhello@nextdeallending.com"
)

EMAIL_CONFIRMATION_NO_ADDRESS = (
    "Hi {first_name},\n\n"
    "This is Next Deal Lending confirming your call with Josh on {date} at {time}.\n\n"
    "Reply to this email if you need to reschedule.\n\n"
    "Next Deal Lending\nhello@nextdeallending.com"
)

EMAIL_NIGHT_BEFORE_WITH_ADDRESS = (
    "Hi {first_name},\n\n"
    "Reminder from Next Deal Lending — your call with Josh is tomorrow at {time} "
    "about {property_address}.\n\n"
    "Talk soon,\nNext Deal Lending"
)

EMAIL_NIGHT_BEFORE_NO_ADDRESS = (
    "Hi {first_name},\n\n"
    "Reminder from Next Deal Lending — your call with Josh is tomorrow at {time}.\n\n"
    "Talk soon,\nNext Deal Lending"
)

EMAIL_NINETY_MIN_WITH_ADDRESS = (
    "Hi {first_name},\n\n"
    "Your Next Deal Lending call with Josh is in about 90 minutes, at {time} "
    "about {property_address}.\n\n"
    "Call us at {number} if anything's come up.\n\n"
    "Next Deal Lending"
)

EMAIL_NINETY_MIN_NO_ADDRESS = (
    "Hi {first_name},\n\n"
    "Your Next Deal Lending call with Josh is in about 90 minutes, at {time}.\n\n"
    "Call us at {number} if anything's come up.\n\n"
    "Next Deal Lending"
)

# ── Callback number placeholder ──────────────────────────────────────────────

# OPEN QUESTION (unanswered): Josh was not asked which number goes here.
# Using the batch-dialer main number as a placeholder. Change once confirmed.
CALLBACK_NUMBER_PLACEHOLDER = "(727) 436-9951"  # OPEN QUESTION (A2): the only dialer number that exists today
