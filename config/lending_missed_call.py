"""WP-GL-9 missed-call text: rule values and BatchDialer call-record field names.

Call-record shape from the BatchDialer public API docs (GET /api/v2/cdrs/last,
"Get Latest CDRs Since Last Poll"); confirmed against a real call once one exists.
"""
from __future__ import annotations

# Paged recent-calls list (cursor next_page). Not /v2/cdrs/last: that keeps one shared
# "last seen" marker per integration, so another reader (e.g. a client Zap) would split
# the feed. Our own marker is the call log: paging stops at a page holding a known call.
CDR_POLL_PATH = "/v2/cdrs"
CDR_PAGE_LENGTH = 100
CDR_MAX_PAGES = 10                # bound per cycle (~1,000 calls)
POLL_SECONDS = 15                 # /api/cdrs poll interval; well inside the 60 s target
POLL_LOCK_KEY = 7_302_020_803     # one poller cycle at a time (next to the lending compliance lock keys)
MAX_LATE_SECONDS = 60             # brief rule: text within 60 s; an older missed call is logged late, never texted
MAX_CALLS_PER_CYCLE = 500         # bound on call records decided per poll cycle
TIMEZONE = "America/New_York"     # "one text per person per day" is an Eastern calendar day
SMS_CAMPAIGN = "lending_missed_call"
MAX_TEXT_CHARS = 320

# Documented CDR fields (first match wins).
CDR_ID_FIELDS = ("id",)
CDR_PHONE_FIELDS = ("customerNumber",)
CDR_STATUS_FIELDS = ("status", "disposition")      # status "NOANSWER" / disposition "No Answer"
CDR_DIRECTION_FIELDS = ("direction",)               # "out" / "in"
CDR_CALLER_ID_FIELDS = ("did",)                     # the caller-ID number the call went out on
CDR_ENDED_AT_FIELDS = ("callEndTime",)
NO_ANSWER_STATUSES = frozenset({"noanswer", "no answer", "no-answer", "no_answer", "busy"})
OUTBOUND_DIRECTIONS = frozenset({"out", "outbound"})
CDR_DISPOSITION_FIELDS = ("disposition",)
# Call results that mean "do not call me": the number is opted out everywhere.
# Final names follow the 13 approved codes once they exist in BatchDialer.
DNC_DISPOSITIONS = frozenset({"dnc_request", "dnc request", "do not call", "dnc"})

TEMPLATE_WITH_PROPERTY = (
    "Hi, this is Next Deal Lending calling about {property}. Sorry we missed you - "
    "reply here or call back anytime. Reply STOP to opt out."
)
TEMPLATE_NO_PROPERTY = (
    "Hi, this is Next Deal Lending about financing for your project. Sorry we missed you - "
    "reply here or call back anytime. Reply STOP to opt out."
)
