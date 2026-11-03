"""Call disposition logging: rule values (spec §4.4, Go Live Brief §2.7, §3).

The codes are the exact names created in the dialer (Call Results), so they must
match what the dialer sends. Changing the list means bumping DISPOSITION_LIST_VERSION.
"""
from __future__ import annotations

DISPOSITION_LIST_VERSION = "2026-11-03"

# The 10 results shared by BatchDialer and GHL (GHL allows at most 10 call dispositions). Client-approved 2026-11-03,
# replacing the 13-code list of 2026-09-30.
DISPOSITIONS = (
    "NO_ANSWER",
    "LEFT_VOICEMAIL",
    "BAD_NUMBER",
    "WRONG_PERSON",
    "REFERRED",
    "DNC_REQUEST",
    "CONNECTED_NOT_INTERESTED",
    "CALLBACK_REQUESTED",
    "NURTURE",
    "BOOKED",
)

# Codes of the 2026-09-30 list merged away. Stored rows are never rewritten: they stay readable and keep counting
# through the sets below, and a dialer still sending one (before its Call Results are switched) is mapped.
# A gate failure is now NURTURE with call_dispositions.gate_failed set.
LEGACY_DISPOSITION_ALIASES: dict[str, str] = {
    "CALL_FAILED": "NO_ANSWER",
    "NOT_DECISION_MAKER": "WRONG_PERSON",
    "DATA_NURTURE_ONLY": "NURTURE",
    "GATE_FAILED_NURTURE": "NURTURE",
}

# Calls nobody answered: these feed the missed-call text (voicemail counts).
UNANSWERED_CODES = frozenset({"NO_ANSWER", "LEFT_VOICEMAIL"})

# The dialer's built-in Call Results, mapped onto our codes (names from BatchDialer's
# published default results; confirm against the account's Call Results screen).
# Matched after upper-casing and turning spaces into underscores.
SYSTEM_DISPOSITION_ALIASES: dict[str, str] = {
    "NO_ANSWER": "NO_ANSWER",
    "BUSY": "NO_ANSWER",
    "ANSWERING_MACHINE": "LEFT_VOICEMAIL",
    "DISCONNECTED_NUMBER": "BAD_NUMBER",
    "DO_NOT_CALL": "DNC_REQUEST",
    "NOT_INTERESTED": "CONNECTED_NOT_INTERESTED",
    "CALL_BACK": "CALLBACK_REQUESTED",
    # A carrier failure is not a bad number (client 2026-11-03): retried like an unanswered call.
    "FAILED": "NO_ANSWER",
    "CONGESTION": "NO_ANSWER",
    # Client-approved 2026-10-02. Abandoned is a multi-line drop (the person picked up, no caller was free):
    # logged as NO_ANSWER and counted as an attempt, but never queued for the missed-call text.
    "ABANDONED": "NO_ANSWER",
    "VOICEMAIL": "LEFT_VOICEMAIL",
}
ABANDONED_RESULT = "ABANDONED"

# Unfunded-cause tags (brief §2.7). Required values for every unfunded outcome.
CAUSE_TAGS = frozenset({
    "contactability", "timing", "fit", "borrower_choice", "lender_execution", "our_execution",
})
# Default cause derived from the call result when the dialer has no second picklist.
# Always stored as provisional; the file owner sets the final value.
DEFAULT_CAUSE: dict[str, str] = {
    "NO_ANSWER": "contactability",
    "LEFT_VOICEMAIL": "contactability",
    "BAD_NUMBER": "contactability",
    "WRONG_PERSON": "contactability",
    "CALLBACK_REQUESTED": "timing",
    "CONNECTED_NOT_INTERESTED": "fit",
    "NURTURE": "fit",
    # legacy codes (stored rows)
    "CALL_FAILED": "contactability",
    "NOT_DECISION_MAKER": "contactability",
    "GATE_FAILED_NURTURE": "fit",
}

BOOKED_CODE = "BOOKED"
DNC_CODE = "DNC_REQUEST"

# Scoreboard definitions (client Part 5.6). Defaults: a client answer is a one-line change here.
# Legacy codes are listed too, so history keeps counting. NURTURE is gated / nurture-sent only with gate_failed set:
# the scoreboard adds that row check (NURTURE_CODE AND gate_failed IS TRUE) on top of these sets.
LIVE_CONVERSATION_CODES = frozenset({
    "CONNECTED_NOT_INTERESTED", "CALLBACK_REQUESTED", "DNC_REQUEST", "NURTURE", "BOOKED",
    "DATA_NURTURE_ONLY", "GATE_FAILED_NURTURE",
})
GATED_CODES = frozenset({"BOOKED", "GATE_FAILED_NURTURE"})  # proxy for "all four captures"
NURTURE_SENT_CODES = frozenset({"GATE_FAILED_NURTURE"})
NURTURE_CODE = "NURTURE"
# A person picked up: everything except no-answer, voicemail and bad-number results (answer rate per caller-ID
# number), plus the answered legacy codes.
ANSWERED_CODES = (frozenset(DISPOSITIONS) | {"NOT_DECISION_MAKER", "DATA_NURTURE_ONLY", "GATE_FAILED_NURTURE"}) \
    - UNANSWERED_CODES - {"BAD_NUMBER"}
# GHL pipeline stage names (lower-cased) that count as "showed": the client moves a lead there once the held call happens.
SHOWED_STAGE_KEYS = frozenset({"held", "showed"})
# GHL pipeline stage names (lower-cased) that count as "booked" for David's numbers and the scoreboard's Booked (stage):
# the Requested Appointment workflow moves the opportunity there. UNVERIFIED spelling: Dev A confirms the stage name.
BOOKED_STAGE_KEYS = frozenset({"booked"})

# GHL call outcomes (G3). GHL's call API carries no disposition (checked 2026-11-03), so a GHL workflow posts the
# chosen outcome to /webhooks/lending/ghl-disposition; it waits in lending.ghl_disposition_events until the GHL call
# import has stored the call, then is matched onto it. GHL label (as the caller sees it, upper-cased, non-alphanumerics
# to "_") -> our code; a body that already sends our code, or a "disp:<CODE>" tag, needs no entry.
GHL_DISPOSITION_LABELS: dict[str, str] = {
    "NO_ANSWER": "NO_ANSWER",
    "VOICEMAIL": "LEFT_VOICEMAIL",
    "FOLLOW_UP": "CALLBACK_REQUESTED",
    "REQUESTED_APPOINTMENT": "BOOKED",
    "NOT_INTERESTED": "CONNECTED_NOT_INTERESTED",
    "INCORRECT_NUMBER": "BAD_NUMBER",
    "WRONG_CONTACT": "WRONG_PERSON",
    "REFERRED": "REFERRED",
    "DNC_REQUEST": "DNC_REQUEST",
    "NURTURE": "NURTURE",
}
# UNVERIFIED webhook body keys (Dev A builds the workflow): candidates tried in order, at the top level or under
# GHL's customData.
GHL_DISPOSITION_FIELD_CANDIDATES: dict[str, tuple[str, ...]] = {
    "contact_id": ("contact_id", "contactId"),
    "phone": ("phone", "contact_phone"),
    "disposition": ("disposition", "lending_disposition", "call_disposition", "call_outcome"),
    "call_id": ("call_id", "call_message_id", "message_id", "callId"),
    "user": ("user", "user_name", "called_by", "caller"),
    "called_at": ("called_at", "call_time", "call_date"),
    "gate_failed": ("gate_failed",),
}
GHL_DISPOSITION_MATCH_WINDOW_MINUTES = 30  # a call started this close to the outcome's time is its call
GHL_DISPOSITION_PENDING_DAYS = 2           # an outcome still unmatched after this is no longer retried
GHL_DISPOSITION_MATCH_BATCH = 200
GHL_DISPOSITION_DEDUPE_MINUTES = 10      # an identical delivery within this is a webhook retry, stored once
GHL_DISPOSITION_UNMATCHED_ALERT_HOURS = 2  # an outcome with no call after this is posted once to #dial-tasks
GHL_MISSING_DISPOSITION_GRACE_MINUTES = 30  # a connected outbound GHL call with no outcome after this is flagged
# GHL call outcomes (ghl_calls.call_state) for David's activity: a voicemail, and a call where someone talked.
GHL_VOICEMAIL_STATES = frozenset({"voicemail"})
GHL_CONNECTED_STATES = frozenset({"completed"})
# A text or email GHL could not send (e.g. "Number not A2P compliant", seen live 2026-10-29) is not counted as sent.
GHL_FAILED_STATUSES = frozenset({"failed", "undelivered"})
DAVID_SUMMARY_HOUR_ET = 19  # posted with the 7:20pm closing board (the dialer stops at 7:15pm)
# Hourly scoreboard (launch runbook Line 11). Defaults: a client answer is a one-line change here.
LONG_CONVERSATION_SECONDS = 120  # "2+ min conversations": talk time of at least this many seconds
# Codes that count as a transfer to Josh. Empty on purpose: the dialer records no transfer yet, so the
# Transfers column shows n/a until a code is added here and to DISPOSITIONS.
TRANSFER_CODES: frozenset[str] = frozenset()
# Hourly posts at 10:00 through 19:00 ET; the 7:20pm closing board follows the 7:00pm one.
HOURLY_POST_HOURS_ET = tuple(range(10, 20))

# Sheet columns. Held / Gate Passed / Packet are added when WP-GL-5/6 write them.
SHEET_COLUMNS = (
    "Call ID", "Date/Time (ET)", "Caller", "Caller Seat", "Queue", "Borrower Phone",
    "Disposition", "Talk Duration (sec)", "Booked (caller-reported)", "DNC (Y/N)",
    "Unfunded Cause (provisional)", "Disposition List Version", "Updated At (ET)",
)
SHEET_TIMEZONE = "America/New_York"

RECORDING_RETRY_MINUTES = 30  # a forbidden/pending recording is rechecked this often, never given up on
RECORDING_BATCH_LIMIT = 50

DELIVERY_LATENCY_TARGET_SECONDS = 30  # spec §12: disposition -> Sheet + Slack
RETRY_MIN_AGE_SECONDS = 60            # let the request's own background task finish first
RETRY_BATCH_LIMIT = 200

# Webhook field map. UNCONFIRMED: candidate key names per field, tried in order
# (dotted paths reach into nested objects). Replace with the real names from the
# BatchDialer docs / a captured payload; parse_event() fails loudly without a call id.
EVENT_FIELD_CANDIDATES: dict[str, tuple[str, ...]] = {
    "call_id": ("id", "call_id", "callId", "uuid"),
    "contact_id": ("contact.id", "contact_id", "contactId"),
    "direction": ("direction", "call_direction"),
    "phone": ("customerNumber", "phone", "phone_number", "to", "dialed_number", "contact.phone"),
    "seat_id": ("agent.id", "user_id", "userId", "agent_id", "agentId", "user.id"),
    "seat_name": ("user_name", "userName", "agent_name", "agentName", "user.name"),
    "caller_id_number": ("did", "caller_id", "callerId", "from", "caller_id_number"),
    "campaign_id": ("campaign.id", "campaign_id", "campaignId"),
    "started_at": ("callStartTime", "started_at", "startTime", "start_time", "start"),
    "ended_at": ("callEndTime", "ended_at", "endTime", "end_time", "end"),
    "duration": ("duration", "talk_duration", "talkTime", "billsec"),
    "disposition": ("disposition", "call_result", "callResult", "result", "disposition_name"),
    "recording": ("callRecordUrl", "recording_url", "recordingUrl", "recording"),
    "disclosure": ("recording_disclosure", "disclosure_played"),
}

DIALER_ORIGIN = "https://app.batchdialer.com"  # callRecordUrl is relative to this

DIRECTION_ALIASES: dict[str, str] = {"out": "outbound", "outbound": "outbound", "in": "inbound", "inbound": "inbound"}

# Telephony statuses on a CDR. UNVERIFIED strings (Task 0 confirms): "ANSWER" means the call
# connected and the caller has not chosen a result yet.
CDR_ANSWERED_STATUSES = frozenset({"ANSWER", "ANSWERED"})

CDR_POLL_SECONDS = 5       # GET /v2/cdrs/last; Runbook § 3 line 10 / § 5.1: 5-second ingestion (T-12)
CDR_RESCAN_SECONDS = 120   # GET /v2/cdrs for today + yesterday (UTC)
CDR_POLL_LOCK_KEY = 7_302_020_803  # one poller at a time: the /last watermark is shared per integration


def validate_dispositions_config() -> None:
    if not set(GHL_DISPOSITION_LABELS.values()) <= set(DISPOSITIONS):
        raise ValueError("GHL_DISPOSITION_LABELS maps to a code missing from DISPOSITIONS")
    current = set(DISPOSITIONS)
    if len(current) != len(DISPOSITIONS):
        raise ValueError("DISPOSITIONS must not repeat a code")
    if set(LEGACY_DISPOSITION_ALIASES) & current:
        raise ValueError("LEGACY_DISPOSITION_ALIASES must not name a current code")
    if not set(LEGACY_DISPOSITION_ALIASES.values()) <= current:
        raise ValueError("LEGACY_DISPOSITION_ALIASES must map onto current codes")
    stored = current | set(LEGACY_DISPOSITION_ALIASES)  # reporting sets may name legacy codes: stored rows still count
    for name, group, allowed in (
            ("UNANSWERED_CODES", UNANSWERED_CODES, current),
            ("SYSTEM_DISPOSITION_ALIASES", set(SYSTEM_DISPOSITION_ALIASES.values()), current),
            ("DEFAULT_CAUSE", set(DEFAULT_CAUSE), stored),
            ("LIVE_CONVERSATION_CODES", LIVE_CONVERSATION_CODES, stored), ("GATED_CODES", GATED_CODES, stored),
            ("NURTURE_SENT_CODES", NURTURE_SENT_CODES, stored), ("ANSWERED_CODES", ANSWERED_CODES, stored),
            ("TRANSFER_CODES", TRANSFER_CODES, current)):
        if not group <= allowed:
            raise ValueError(f"{name} names a code missing from DISPOSITIONS: {sorted(group - allowed)}")
    if not set(DEFAULT_CAUSE.values()) <= CAUSE_TAGS:
        raise ValueError("DEFAULT_CAUSE uses a value outside CAUSE_TAGS")
    if not {BOOKED_CODE, DNC_CODE, NURTURE_CODE} <= current:
        raise ValueError("BOOKED, DNC_REQUEST and NURTURE must be in DISPOSITIONS")


# BatchDialer contact custom field the caller sets during the call ("OK if I text you?" -> yes).
TEXT_CONSENT_FIELD = "text_consent"
TEXT_CONSENT_YES = "yes"
# BatchDialer contact custom field the caller sets when the gate failed (the call is NURTURE + gate_failed).
# UNVERIFIED: field name to be created by Dev A in BatchDialer; a truthy value marks the call.
GATE_FAILED_FIELD = "gate_failed"
GATE_FAILED_TRUE = frozenset({"yes", "true", "y", "1"})

# GHL user id (call message ``userId``) -> caller name (GHL /users API, read 2026-11-03). Outbound GHL call messages
# often carry no userId; the outcome webhook's {{user.name}} fills the name in when it matches the call.
GHL_USER_NAMES: dict[str, str] = {
    "3W4lyuTpPSgBjgiJMzJk": "Josh Kantor",
    "57m9LlZrO1rV2A5cTKxI": "David Willis",
    "YoBwFUwHL1fylqvqjzA1": "Hari K",
    "0mboWYLanQkMz4RG3IbL": "Hari K",
}
GHL_UNKNOWN_CALLER = "GHL (unknown)"


def ghl_caller_name(user_id: str | None) -> str:
    return GHL_USER_NAMES.get(user_id or "", GHL_UNKNOWN_CALLER)


# BatchDialer seats shared by several people: the scoreboard shows this label, never the login owner's name, so no
# one is credited with another person's calls. Stored rows are not changed.
SHARED_SEAT_LABELS: dict[str, str] = {"118131": "BatchDialer (shared seat)"}


# T-12 audio fallback. BatchDialer has no live audio stream; the poller fetches each answered
# call's transcript (GET /cdrs/{id}/transcription) once the call ends and runs the call
# extraction. Off unless LENDING_TRANSCRIPT_INGEST_ENABLED (every extraction is a Claude call).
TRANSCRIPT_DONE = "done"        # transcript fetched and extracted
TRANSCRIPT_MISSING = "missing"  # none arrived within TRANSCRIPT_MAX_WAIT_SECONDS of the call end
TRANSCRIPT_FAILED = "failed"    # invalid extraction output, or out of retries: never retried (each try is a Claude call)
TRANSCRIPT_DEFERRED = "deferred"  # transcript fetched, extraction waiting: AI paused or a temporary API error
TRANSCRIPT_STATUSES = (TRANSCRIPT_DONE, TRANSCRIPT_MISSING, TRANSCRIPT_FAILED, TRANSCRIPT_DEFERRED)  # NULL = still pending; DB CHECK
# Per-tick counter keys for calls left pending (not stored).
TRANSCRIPT_NOT_READY = "not_ready"
TRANSCRIPT_FETCH_FAILED = "fetch_failed"
TRANSCRIPT_RATE_LIMITED = "rate_limited"
TRANSCRIPT_PAUSED = "paused"            # extraction deferred: vendor cost pause active
TRANSCRIPT_EXTRACT_RETRY = "extract_retry"  # extraction deferred: temporary API error
# The one definition of "pending": the poller's queries and the migration's partial index
# both use it, so the index always matches the query.
TRANSCRIPT_PENDING_PREDICATE = "transcript_status IS NULL AND talk_duration_sec > 0"
TRANSCRIPT_BATCH_SIZE = 10
TRANSCRIPT_MAX_WAIT_SECONDS = 30 * 60
TRANSCRIPT_POLL_SECONDS = 5    # transcript worker cycle (own process, never the CDR poller tick)
TRANSCRIPT_WORKER_LOCK_KEY = 7_302_020_804  # one transcript worker at a time
TRANSCRIPT_RETRY_SECONDS = 30   # a not-ready call waits this long before its next fetch
TRANSCRIPT_MAX_EXTRACT_ATTEMPTS = 3  # temporary API errors per call before it is marked failed (pauses do not count)
