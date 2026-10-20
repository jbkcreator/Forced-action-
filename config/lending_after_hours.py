"""After-hours voicemail and answering-service intake (split T-06, Runbook §5.3 Layers 2 and 3).

After-hours is outside the ISA shift, 09:00-19:15 ET (``SHIFT_GROUPS`` in config/lending_compliance.py). GHL's
working-hours routing on (656) 221-1100 applies the same schedule and sends after-hours calls to voicemail; a GHL
workflow posts the transcript to ``POST /webhooks/lending/after-hours-intake``. An outside answering service can post
the same body later. Each intake logs the call and pins the caller; the 08:55 ET job drafts the replies and posts
the "call these first" list.
"""
from __future__ import annotations

from datetime import timedelta

# The intake's call-log row: inbound, no disposition code (it is not a dialer outcome, so the prior-contact
# note keeps the last dialer outcome), and this marker in disposition_raw so the missing-disposition alert skips it.
# The attempt cap and the scoreboard count outbound calls only, so they never see it.
INTAKE_MARKER = "after_hours_voicemail"
DEFAULT_SOURCE = "ghl_voicemail"

# A pin ends after the first outbound attempt to the caller, or after this long, whichever comes first.
PIN_DURATION = timedelta(hours=24)

# 08:55 ET, before the 09:00 shift: draft the replies (Approve cards in Cora's channel, so a draft is never
# approved overnight) and post the "call these first" list. Cron fires at 12:55 and 13:55 UTC; only the 8:xx ET
# run acts. The send itself still waits for the carrier approval and the borrower's 8am-8pm window.
MORNING_POST_HOUR_ET = 8

# The drafted reply: the model gets the transcript; this text is used when there is none or the model's draft
# is unusable.
DRAFT_MAX_CHARS = 320
DRAFT_TOOL_NAME = "after_hours_reply"
FALLBACK_REPLY = ("Hi {name}, this is Next Deal Lending. Sorry we missed your call. "
                  "We got your message and will call you back this morning.")
DRAFT_PROMPT = (
    "You write one short text message from Next Deal Lending to a borrower who called after hours and left a "
    "voicemail. Greet them by first name if one is given, say we got their message, mention what they called about "
    "in a few words, and say we will call them back this morning. Plain words, no emojis, no links. Never quote a "
    "rate, term, point, fee, ratio, payment or dollar amount, and never promise an approval. "
    "Reply with the text message only, at most {max_chars} characters."
)
