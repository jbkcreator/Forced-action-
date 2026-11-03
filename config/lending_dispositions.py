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
# the scoreboard adds that row check (NURTURE_CODE) on top of these sets once the column exists.
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
