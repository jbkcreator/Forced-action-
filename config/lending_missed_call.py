"""WP-GL-9 missed-call text: rule values and BatchDialer call-record field names.

Call-record field names are provisional until the first real call on the client
account shows the /api/cdrs shape; they live here so confirming them is a config edit.
"""
from __future__ import annotations

POLL_SECONDS = 15                 # /api/cdrs poll interval; well inside the 60 s target
POLL_LOCK_KEY = 7_302_020_803     # one poller cycle at a time (next to the lending compliance lock keys)
MAX_LATE_SECONDS = 300            # a missed call older than this is logged as late, never texted
TIMEZONE = "America/New_York"     # "one text per person per day" is an Eastern calendar day
SMS_CAMPAIGN = "lending_missed_call"
MAX_TEXT_CHARS = 320

# /api/cdrs field names (provisional).
CDR_ID_FIELDS = ("id", "callId", "uuid")
CDR_PHONE_FIELDS = ("phoneNumber", "phone", "to", "destination")
CDR_STATUS_FIELDS = ("status", "result", "disposition")
CDR_DIRECTION_FIELDS = ("direction", "type")
CDR_CALLER_ID_FIELDS = ("callerId", "callerIdNumber", "from")
CDR_ENDED_AT_FIELDS = ("endedAt", "endTime", "ended_at", "end")
NO_ANSWER_STATUSES = frozenset({"no-answer", "no_answer", "noanswer", "no answer", "missed", "unanswered", "busy"})
OUTBOUND_DIRECTIONS = frozenset({"outbound", "out", "outgoing"})

TEMPLATE_WITH_PROPERTY = (
    "Hi, this is Forced Action Capital calling about {property}. Sorry we missed you - "
    "reply here or call back anytime. Reply STOP to opt out."
)
TEMPLATE_NO_PROPERTY = (
    "Hi, this is Forced Action Capital about financing for your project. Sorry we missed you - "
    "reply here or call back anytime. Reply STOP to opt out."
)
