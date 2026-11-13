"""P2.1 Speed-to-Lead: the GHL tags our code adds and the timings around the call cascade.

Our code decides; GoHighLevel workflows text and dial (``docs/lending/speed-to-lead.md``). Every gate (consent,
suppression, DNC, calling hours, emergency halt) runs in ``src/lending/speed_to_lead.py`` before a tag is added: GHL
checks none of them, and a tag is the only thing that starts a workflow.
"""
from __future__ import annotations

from datetime import timedelta

from config.lending_lendingflow import INTRO_SMS_HOLD_MAX_HOURS

# Starts the GHL workflow "NDL - Speed to Lead": assign Josh -> Call; no answer or declined -> assign David -> Call;
# nobody connected at 2 minutes and the contact carries GHL_TAG_AI_VOICE_OK -> Voice AI Outbound Call.
GHL_TAG_SPEED_TO_LEAD = "ndl-speed-to-lead"
# Present only while the contact has a recorded, unrevoked call consent (brief section 7: AI voice runs only on
# consented leads). Added or removed on every new inquiry, before GHL_TAG_SPEED_TO_LEAD.
GHL_TAG_AI_VOICE_OK = "ndl-ai-voice-ok"
# Starts the GHL workflow "NDL - Web Intro SMS" (the website twin of LendingFlow's ``lendingflow-intro-ready``).
GHL_TAG_WEB_INTRO_READY = "web-intro-ready"

# Per step: the TARGET the client gave (what we aim for and report against; a miss is logged and shown on the timing
# report) and the LAST CHANCE (the latest point the step still makes sense; after it the step is skipped). The last
# chances are our suggestion, pending Josh's approval (PR Client blockers); the 8am-8pm window applies on top.
# Text: a late "we got your request" text is still useful, so it is sent up to a day late (the same 24 hours the
# held intro texts already use).
TEXT_TARGET = timedelta(seconds=5)
TEXT_LAST_CHANCE = timedelta(hours=INTRO_SMS_HOLD_MAX_HOURS)
# Ring Josh, then David: aimed at catching the borrower still on the form; past this, the lead goes to the normal call
# queue with a "late lead, call now" post in the lead-alerts channel.
CALL_TARGET = timedelta(seconds=60)
CALL_LAST_CHANCE = timedelta(minutes=5)
# Voice AI: the backup when Josh and David miss, so only when the Josh/David step ran, and only when the AI call (GHL
# waits AI_VOICE_AFTER_SECONDS after the cascade starts) still begins inside this limit. Never the first contact.
AI_TARGET = timedelta(minutes=2)
AI_LAST_CHANCE = timedelta(minutes=5)
# The GHL workflow's wait before the Voice AI step, counted from the cascade start (the wait itself is a GHL step).
AI_VOICE_AFTER_SECONDS = 120
# The emergency halt removes the cascade tags from every lead whose cascade started this recently (Josh, David and the
# 2-minute Voice AI step all fall inside it); the GHL workflow re-checks the tag before David's call and the Voice AI.
ACTIVE_CASCADE_MINUTES = 5
# The timing report shows the GHL calls on the lead in this window after it arrived.
REPORT_CALL_WINDOW_MINUTES = 15

# Held website intro texts released per web-lead sweep run.
WEB_INTRO_RELEASE_BATCH = 50

# Website leads need a fresh DNC scrub before any call (LendingFlow leads do not, client decision). One number per lead.
SCRUB_MAX_CREDITS_PER_LEAD = 1
# Tracerfy returns a scrub through a queue polled every 5 seconds (up to 10 minutes for the weekly jobs). Speed-to-lead
# waits at most this many polls (about 30 s), so Josh's phone still rings inside 60 seconds; a scrub not back by then
# leaves the number unscrubbed, so the cascade does not run (the hourly list scrub covers it later).
SCRUB_MAX_POLLS = 6

SOURCE_LENDINGFLOW = "lendingflow"
SOURCE_WEB = "web"
SOURCES = (SOURCE_LENDINGFLOW, SOURCE_WEB)

# lending.speed_to_lead text_status / call_status values.
TEXT_TAGGED = "tagged"
TEXT_HELD = "held_for_window"
TEXT_NOT_ALLOWED = "not_allowed"   # no text consent, suppressed, or halted
TEXT_TOO_LATE = "too_late"         # past TEXT_LAST_CHANCE
TEXT_OFF = "off"                   # LENDING_INTRO_SMS_ENABLED is off
TEXT_BY_LENDINGFLOW = "lendingflow_intro"  # LendingFlow's own T-09 intro text (lendingflow_leads.intro_sms_tagged_at)
CALL_TAGGED = "tagged"
CALL_BLOCKED = "blocked"           # call_block_reason says why (suppressed, DNC, outside hours, attempt cap, halt ...)
CALL_OFF = "off"                   # LENDING_SPEED_TO_LEAD_ENABLED is off
CALL_STALE = "stale"               # past CALL_LAST_CHANCE when it reached GHL: normal call queue + late-lead post
CALL_FAILED = "failed"             # a GHL tag call failed (logged); BatchDialer still picks the lead up
CALL_HALTED = "halted"             # the emergency halt removed the cascade tags while it was running
