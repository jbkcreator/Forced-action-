"""P2.1 Speed-to-Lead: the GHL tags our code adds and the timings around the call cascade.

Our code decides; GoHighLevel workflows text and dial (``docs/lending/speed-to-lead.md``). Every gate (consent,
suppression, DNC, calling hours, emergency halt) runs in ``src/lending/speed_to_lead.py`` before a tag is added: GHL
checks none of them, and a tag is the only thing that starts a workflow.
"""
from __future__ import annotations

from datetime import timedelta

# Starts the GHL workflow "NDL - Speed to Lead": assign Josh -> Call; no answer or declined -> assign David -> Call;
# nobody connected at 2 minutes and the contact carries GHL_TAG_AI_VOICE_OK -> Voice AI Outbound Call.
GHL_TAG_SPEED_TO_LEAD = "ndl-speed-to-lead"
# Present only while the contact has a recorded, unrevoked call consent (brief section 7: AI voice runs only on
# consented leads). Added or removed on every new inquiry, before GHL_TAG_SPEED_TO_LEAD.
GHL_TAG_AI_VOICE_OK = "ndl-ai-voice-ok"
# Starts the GHL workflow "NDL - Web Intro SMS" (the website twin of LendingFlow's ``lendingflow-intro-ready``).
GHL_TAG_WEB_INTRO_READY = "web-intro-ready"

# The brief's "call Josh within 60 seconds", with room for a slow GHL delivery (team decision, Nov 13): a lead that
# reaches GHL more than this long after the form (a retried delivery, a recovered follow-up) gets no call cascade, and
# BatchDialer picks it up as usual. The intro text does not use this limit.
CASCADE_MAX_LEAD_AGE = timedelta(seconds=100)
# Documented for the GHL workflow; the wait itself is a GHL step.
AI_VOICE_AFTER_SECONDS = 120
# The emergency halt removes the cascade tags from every lead whose cascade started this recently (Josh, David and the
# 2-minute Voice AI step all fall inside it); the GHL workflow re-checks the tag before David's call and the Voice AI.
ACTIVE_CASCADE_MINUTES = 5
# The timing report shows the GHL calls on the lead in this window after it arrived.
REPORT_CALL_WINDOW_MINUTES = 15

# A website intro text held for the send window is released by the web-lead sweep once the window opens, while the
# lead is younger than this (the same limit as LendingFlow's INTRO_SMS_HOLD_MAX_HOURS).
WEB_INTRO_HOLD_MAX_HOURS = 24
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
TEXT_OFF = "off"                   # LENDING_INTRO_SMS_ENABLED is off
TEXT_BY_LENDINGFLOW = "lendingflow_intro"  # LendingFlow's own T-09 intro text (lendingflow_leads.intro_sms_tagged_at)
CALL_TAGGED = "tagged"
CALL_BLOCKED = "blocked"           # call_block_reason says why (suppressed, DNC, outside hours, attempt cap, halt ...)
CALL_OFF = "off"                   # LENDING_SPEED_TO_LEAD_ENABLED is off
CALL_STALE = "stale"               # older than CASCADE_MAX_LEAD_AGE when it reached GHL
CALL_FAILED = "failed"             # a GHL tag call failed (logged); BatchDialer still picks the lead up
CALL_HALTED = "halted"             # the emergency halt removed the cascade tags while it was running
