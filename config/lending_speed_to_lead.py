"""P2.1 Speed-to-Lead: the GHL tags our code adds and the timings around the call cascade.

Our code decides; GoHighLevel workflows text and dial (``docs/lending/speed-to-lead.md``). Every gate (consent,
suppression, DNC, calling hours, emergency halt) runs in ``src/lending/speed_to_lead.py`` before a tag is added: GHL
checks none of them, and a tag is the only thing that starts a workflow.
"""
from __future__ import annotations

from datetime import timedelta

from config.lending_lendingflow import DIALER_TEMPORARY_REASONS, GHL_GIVE_UP_AFTER_HOURS, INTRO_SMS_HOLD_MAX_HOURS

# Starts the GHL workflow "NDL - Speed to Lead": assign Josh -> Call; no answer or declined -> assign David -> Call;
# nobody connected at 2 minutes and the contact carries GHL_TAG_AI_VOICE_OK -> Voice AI Outbound Call.
GHL_TAG_SPEED_TO_LEAD = "ndl-speed-to-lead"
# Present only while the contact has a recorded, unrevoked AI-call consent (brief section 7: AI voice runs only on
# consented leads). Added or removed on every cascade start, before GHL_TAG_SPEED_TO_LEAD.
GHL_TAG_AI_VOICE_OK = "ndl-ai-voice-ok"
# Starts the GHL workflow "NDL - Web Intro SMS" (the website twin of LendingFlow's ``lendingflow-intro-ready``).
GHL_TAG_WEB_INTRO_READY = "web-intro-ready"

# Per step: the TARGET the client gave (what we aim for and report against; a miss is logged and shown on the timing
# report). The 8am-8pm window applies on top.
# Text: a late "we got your request" text is still useful, so it is sent up to a day late (the same 24 hours the
# held intro texts already use); past that it is skipped.
TEXT_TARGET = timedelta(seconds=5)
TEXT_LAST_CHANCE = timedelta(hours=INTRO_SMS_HOLD_MAX_HOURS)
# Ring Josh, then David (GHL rolls to David when Josh does not answer, declines or is busy).
CALL_TARGET = timedelta(seconds=60)
# Voice AI: the backup when Josh and David miss, 2 minutes after that lead's cascade starts (queued leads included).
AI_TARGET = timedelta(minutes=2)
# The GHL workflow's wait before the Voice AI step, counted from the cascade start (the wait itself is a GHL step).
AI_VOICE_AFTER_SECONDS = 120

# --- the call queue (src/lending/speed_to_lead.py release_queue, minute job src.tasks.lending_speed_to_lead_queue) ---
# A lead that cannot be called the moment it reaches GHL waits in the queue instead of being skipped: outside the
# callers' hours (LENDING_ALARM_HOURS_START / _END, Eastern, on config.lending_alarms.ALARM_DAYS), delayed (reached GHL
# more than FRESH_LEAD_WINDOW after the form), another cascade still running (waiting its turn), or a temporary gate
# reason (the lead's own 8am-8pm window, the attempt cap, a scrub not back yet). The queue releases one lead at a
# time, fresh leads first, then the oldest; each release re-checks every rule. Numbers below are our suggestion,
# pending Josh's approval (PR Client blockers).
# A lead that reaches GHL within this time of the form is "fresh": called at once when allowed, and ahead of the
# backlog in the queue. Later than this it is "delayed" and joins the queue in arrival order.
FRESH_LEAD_WINDOW = timedelta(minutes=5)
# One cascade at a time: the next one starts at least this long after the last (Josh, then David, then the 2-minute
# Voice AI step).
CASCADE_SPACING = timedelta(minutes=3)
# A lead still queued this long after it arrived leaves the queue (status expired) with a Slack line and stays with
# the normal call paths (BatchDialer, the 08:55 list); the same 72 hours the delivery sweeps give GHL.
QUEUE_MAX_AGE = timedelta(hours=GHL_GIVE_UP_AFTER_HOURS)
# Queued leads checked per minute run (at most one is released).
QUEUE_SCAN_LIMIT = 20
# A lead kept queued by a not-now reason is checked again after this, so the leads behind it get their turn.
QUEUE_RECHECK_AFTER = timedelta(minutes=10)
# A lead whose GHL call read or tag call failed is tried again after this; a run stops after this many failed reads.
QUEUE_GHL_RETRY_AFTER = timedelta(minutes=2)
QUEUE_GHL_FAILURES_PER_RUN = 3
# Any call to the number this recently (GHL or BatchDialer) and the queue waits, so it never rings on top of a call.
QUEUE_RECENT_CALL = timedelta(minutes=10)
# A release claimed but never finished (the job was killed between the claim and the GHL tags) is reported after this.
RELEASE_STUCK_AFTER = timedelta(minutes=10)
# Leads listed per "left the queue" post.
QUEUE_ALERT_BATCH = 20
# Gate reasons that only mean "not now": the lead stays queued. Every other reason ends the cascade (blocked).
QUEUE_TEMPORARY_REASONS = DIALER_TEMPORARY_REASONS
# Backlog alert (once per Eastern day, inside callers' hours): this many leads waiting, or the oldest waiting this
# long since callers' hours opened.
QUEUE_ALERT_DEPTH = 10
QUEUE_ALERT_WAIT = timedelta(minutes=30)
# One advisory lock for the fresh-lead start and the queue worker, so two cascades never start together.
QUEUE_LOCK_KEY = 7_302_020_811
# One queue job at a time (the minute cron can overlap a slow run).
QUEUE_RUN_LOCK_KEY = 7_302_020_812
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
CALL_OFF = "off"                   # LENDING_SPEED_TO_LEAD_ENABLED is off (a queued lead also ends here when it is switched off)
CALL_QUEUED = "queued"             # waiting in the call queue (queue_reason says why)
CALL_RELEASING = "releasing"       # the queue claimed it (committed) and is adding the tags; a crash leaves it here, never re-rung
CALL_EXPIRED = "expired"           # left the queue after QUEUE_MAX_AGE (Slack line; normal call paths)
CALL_INTERRUPTED = "interrupted"   # a release claimed but never finished (Slack line; normal call paths)
CALL_FAILED = "failed"             # a GHL tag call failed (logged); BatchDialer still picks the lead up
CALL_HALTED = "halted"             # the emergency halt removed the cascade tags while it was running

# lending.speed_to_lead queue_reason values (a temporary gate reason is stored as its own code).
QUEUE_OUTSIDE_HOURS = "outside_hours"
QUEUE_DELAYED = "delayed"
QUEUE_WAITING_TURN = "waiting_turn"
# lending.speed_to_lead_notices kinds.
NOTICE_MORNING = "morning"
NOTICE_BACKLOG = "backlog"
