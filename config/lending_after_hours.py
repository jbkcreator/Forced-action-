"""After-hours voicemail and answering-service intake (split T-06, Runbook §5.3 Layers 2 and 3).

After-hours is outside the ISA shift, 09:00-19:15 ET (``SHIFT_GROUPS`` in config/lending_compliance.py). GHL's
working-hours routing on (656) 221-1100 applies the same schedule and sends after-hours calls to voicemail; a GHL
workflow posts the transcript to ``POST /webhooks/lending/after-hours-intake``. An outside answering service can post
the same body later. Every intake logs the call, pins the caller first for the next morning and drafts a reply that
waits for Approve in Cora's channel.
"""
from __future__ import annotations

from datetime import timedelta

# Logged on lending.call_dispositions like a dialer call, so the attempt cap and scoreboard see it.
INTAKE_DIRECTION = "inbound"
INTAKE_DISPOSITION = "LEFT_VOICEMAIL"
INTAKE_CAMPAIGN_TAG = "inbound_voicemail"
DEFAULT_SOURCE = "ghl_voicemail"

# A pin ends after the first outbound attempt to the caller, or after this long, whichever comes first.
PIN_DURATION = timedelta(hours=24)

# The "call these first" post goes to Cora's channel at 08:55 ET, before the 09:00 shift starts.
# Cron fires at 12:55 and 13:55 UTC; only the run that is 8:xx ET posts.
MORNING_POST_HOUR_ET = 8

# The drafted reply: the model gets the transcript; this text is used when there is none or the model fails.
DRAFT_MAX_CHARS = 320
DRAFT_TOOL_NAME = "after_hours_reply"
FALLBACK_REPLY = ("Hi {name}, this is Next Deal Lending. Sorry we missed your call last night. "
                  "We got your message and will call you back this morning.")
DRAFT_PROMPT = (
    "You write one short text message from Next Deal Lending to a borrower who called after hours and left a "
    "voicemail. Greet them by first name if one is given, say we got their message, mention what they called about "
    "in a few words, and say we will call them back this morning. Plain words, no emojis, no links. Never quote a "
    "rate, term, point, fee, ratio, payment or dollar amount, and never promise an approval. "
    "Reply with the text message only, at most {max_chars} characters."
)
