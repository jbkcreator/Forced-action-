"""WP-GL-10 booking confirmation and reminder configuration.

Templates, offsets and scheduling rules for:
  - Confirmation text (right after booking)
  - Night-before reminder (default 18:00 ET)
  - 90-minute reminder

Wording is verbatim from Josh's Oct 2 answers to Section B3. Any change to
wording must be re-approved by Josh before the 10DLC campaign registration
is updated with carriers.

Night-before send time (18:00 ET) is the development default. Josh has not
yet confirmed the clock time; 18:00 ET was chosen as a reasonable evening
hour and will be changed if Josh specifies otherwise.

Texts are sent through GoHighLevel under the Next Deal Lending texting
number (G4 confirmed). FA's Telnyx/send_sms path is NOT used here.

10DLC gate: all texts default off (LENDING_TEXT_ENABLED=false). Flip only
after the 10DLC campaign is approved by carriers. Email fallback runs
regardless of the 10DLC state for contacts without text consent (B4).
"""
from __future__ import annotations

from zoneinfo import ZoneInfo

# ── Scheduling ──────────────────────────────────────────────────────────────

TIMEZONE = ZoneInfo("America/New_York")

# Night-before reminder: send at this hour ET on the calendar day before.
# OPEN QUESTION (unanswered): Josh was asked the time but did not specify.
# 18:00 ET is the developer default. Change this constant once confirmed.
NIGHT_BEFORE_HOUR_ET = 18  # 6:00 PM ET
NIGHT_BEFORE_MINUTE_ET = 0

# 90-minute reminder offset in seconds (from the held-call start).
NINETY_MIN_SECONDS = 90 * 60

# Maximum text body length (carrier limit, same as WP-GL-9).
MAX_TEXT_CHARS = 320

# Minimum booking lead time for a reminder to be sent.
# A booking created fewer than NINETY_MIN_SECONDS before the slot start
# skips the 90-minute reminder (it would fire in the past or immediately).
# A booking created after 18:00 ET the day before skips the night-before
# reminder for the same reason.
MIN_LEAD_SECONDS_90MIN = NINETY_MIN_SECONDS

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

NINETY_MIN_WITH_ADDRESS = (
    "Hi {first_name}, your Next Deal Lending call with Josh is in about 90 minutes, "
    "at {time} about {property_address}. "
    "Call us at {number} if anything's come up."
)

NINETY_MIN_NO_ADDRESS = (
    "Hi {first_name}, your Next Deal Lending call with Josh is in about 90 minutes, "
    "at {time}. "
    "Call us at {number} if anything's come up."
)

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
CALLBACK_NUMBER_PLACEHOLDER = "(727) 436-9951"
