"""After-hours conversation: three qualifying questions, then a call slot for the next business morning.

A lead who reaches Next Deal Lending outside the caller shift is asked the questions one at a time over the
channel they can be reached on (text with consent, else email), and is then offered call slots. The state and the
answers live on ``lending.after_hours_conversations`` (``src/lending/after_hours_conversation.py``).

Nothing here is sent while ``LENDING_AFTER_HOURS_CONVERSATION_ENABLED`` is off.
"""
from __future__ import annotations

CHANNEL_SMS = "sms"
CHANNEL_EMAIL = "email"
CHANNELS = (CHANNEL_SMS, CHANNEL_EMAIL)

# One state per question, in the order asked; the wording below is indexed the same way.
QUESTION_STATES = ("question_1", "question_2", "question_3")
SLOT_CHOICE = "slot_choice"  # the slots have been offered; waiting for the lead to pick one
SLOT_CHOSEN = "slot_chosen"  # a slot was picked; waiting for the booking to be made
BOOKED = "booked"
CLOSED = "closed"
OPEN_STATES = (*QUESTION_STATES, SLOT_CHOICE, SLOT_CHOSEN)
STATES = (*OPEN_STATES, BOOKED, CLOSED)

CLOSED_OPTED_OUT = "opted_out"
CLOSED_EXPIRED = "expired"
CLOSED_NO_SLOTS = "no_slots"
CLOSED_UNCLEAR_SLOT = "unclear_slot"
CLOSED_NO_CONSENT = "no_consent"      # consent was withdrawn, or the address was suppressed, before a message went
CLOSED_BLOCKED_LANE = "blocked_lane"  # the message would have carried a Backflip name, number or domain
CLOSED_SEND_REFUSED = "send_refused"  # the sender refused the recipient (cold lead, or do-not-disturb set in GHL)
CLOSED_SEND_FAILED = "send_failed"    # the provider rejected the message MAX_SEND_ATTEMPTS times
CLOSED_BOOKING_REFUSED = "booking_refused"  # the calendar refused the lead (for example a suppressed address)
CLOSED_BOOKING_UNKNOWN = "booking_unknown"  # the booking call gave no clear result; it is never made a second time
CLOSED_REASONS = (CLOSED_OPTED_OUT, CLOSED_EXPIRED, CLOSED_NO_SLOTS, CLOSED_UNCLEAR_SLOT, CLOSED_NO_CONSENT,
                  CLOSED_BLOCKED_LANE, CLOSED_SEND_REFUSED, CLOSED_SEND_FAILED, CLOSED_BOOKING_REFUSED,
                  CLOSED_BOOKING_UNKNOWN)
# The states with a message to send. slot_chosen has none: its pending step is the booking itself.
SENDABLE_STATES = (*QUESTION_STATES, SLOT_CHOICE, BOOKED)

# Interim wording: the client has not confirmed the three questions yet. No figures, rates or approval language.
GREETING = ("Hi {name}, this is Next Deal Lending. Thanks for reaching out. Our team is back in the morning, and "
            "three quick questions now will get your call set up.")
QUESTIONS = (
    "What type of loan are you looking for (for example fix and flip, rental, or new construction)?",
    "Where is the property (address, or city and state)?",
    "About how much do you need to borrow, and when do you need to close?",
)

# Interim wording, like the questions. {slots} is one numbered line per offered time.
_SLOT_LIST = ("Here are the open times for your call with our team:\n{slots}\n"
              "Reply with the number of the time that works.")
SLOT_OFFER = "Thanks, that is everything we need. " + _SLOT_LIST
SLOT_OFFER_AGAIN = "Sorry, I did not catch which time. " + _SLOT_LIST
SLOT_OFFER_AFTER_TAKEN = "Sorry, that time was just taken. " + _SLOT_LIST
BOOKED_CONFIRMATION = "You are booked for {slot}. Our team will call you then."

EMAIL_SUBJECT = "Your Next Deal Lending inquiry"

# The template version stamped on every send's receipt, by state and channel.
TEMPLATE_VERSIONS = {
    **{(state, channel): f"NIGHT-Q{position}-{channel.upper()}-v1"
       for position, state in enumerate(QUESTION_STATES, start=1) for channel in CHANNELS},
    **{(SLOT_CHOICE, channel): f"NIGHT-SLOTS-{channel.upper()}-v1" for channel in CHANNELS},
    **{(BOOKED, channel): f"NIGHT-BOOKED-{channel.upper()}-v1" for channel in CHANNELS},
}

# The call slots offered are on the next business morning, in this window and timezone. Interim hours: the client
# has not confirmed the morning calendar yet.
SLOT_TIMEZONE = "America/New_York"
MORNING_START_HOUR = 9
MORNING_END_HOUR = 12
SLOT_DURATION_MINUTES = 30  # also the grid the slot starts sit on: 9:00, 9:30, ...
# A pass follows a booking with its confirmation, and an answer with the next message, without waiting a minute.
SEND_ROUNDS_PER_PASS = 2

MAX_SEND_ATTEMPTS = 3
# A send claimed this long ago with no recorded result crashed mid-send: it may or may not have gone out, so it is
# recorded as unknown and never sent again (a second copy of a question is worse than a missing one).
STALE_CLAIM_SECONDS = 180
SEND_BATCH_SIZE = 20

ANSWER_MAX_CHARS = 500
OFFERED_SLOT_COUNT = 3
# A reply that names no offered slot gets the offer again this many times; after that the conversation closes and
# the lead stays on the morning call list.
MAX_SLOT_REPROMPTS = 2
# A reply after this long is no longer part of the conversation: the morning shift has taken over by then.
CONVERSATION_TTL_HOURS = 12
# Message ids already applied, kept so a redelivered webhook changes nothing. A conversation has only a handful
# of inbound messages, so the list is capped well above that.
HANDLED_MESSAGE_IDS_KEPT = 50
