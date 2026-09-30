"""Call disposition logging: rule values (spec §4.4, Go Live Brief §2.7, §3).

The codes are the exact names created in the dialer (Call Results), so they must
match what the dialer sends. Changing the list means bumping DISPOSITION_LIST_VERSION.
"""
from __future__ import annotations

DISPOSITION_LIST_VERSION = "2026-10-01"

# Codes marked (proposed) are not worded in the brief and need client approval.
DISPOSITIONS = (
    "NO_ANSWER",
    "LEFT_VOICEMAIL",
    "BAD_NUMBER",
    "CALL_FAILED",              # proposed
    "WRONG_PERSON",             # proposed
    "NOT_DECISION_MAKER",       # proposed
    "REFERRED",                 # proposed
    "DNC_REQUEST",
    "CONNECTED_NOT_INTERESTED",  # proposed
    "CALLBACK_REQUESTED",       # proposed
    "DATA_NURTURE_ONLY",        # proposed
    "GATE_FAILED_NURTURE",
    "BOOKED",
)

# Calls nobody answered: these feed the missed-call text (voicemail counts).
UNANSWERED_CODES = frozenset({"NO_ANSWER", "LEFT_VOICEMAIL", "CALL_FAILED"})

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
}

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
    "CALL_FAILED": "contactability",
    "WRONG_PERSON": "contactability",
    "NOT_DECISION_MAKER": "contactability",
    "CALLBACK_REQUESTED": "timing",
    "CONNECTED_NOT_INTERESTED": "fit",
    "GATE_FAILED_NURTURE": "fit",
}

BOOKED_CODE = "BOOKED"
DNC_CODE = "DNC_REQUEST"

# Sheet columns. Held / Gate Passed / Packet are added when WP-GL-5/6 write them.
SHEET_COLUMNS = (
    "Call ID", "Date/Time (ET)", "Caller", "Caller Seat", "Queue", "Borrower Phone",
    "Disposition", "Talk Duration (sec)", "Booked (caller-reported)", "DNC (Y/N)",
    "Unfunded Cause (provisional)", "Disposition List Version", "Updated At (ET)",
)
SHEET_TIMEZONE = "America/New_York"

DELIVERY_LATENCY_TARGET_SECONDS = 30  # spec §12: disposition -> Sheet + Slack
RETRY_MIN_AGE_SECONDS = 60            # let the request's own background task finish first
RETRY_BATCH_LIMIT = 200

# Webhook field map. UNCONFIRMED: candidate key names per field, tried in order
# (dotted paths reach into nested objects). Replace with the real names from the
# BatchDialer docs / a captured payload; parse_event() fails loudly without a call id.
EVENT_FIELD_CANDIDATES: dict[str, tuple[str, ...]] = {
    "call_id": ("call_id", "callId", "id", "uuid"),
    "contact_id": ("contact_id", "contactId", "contact.id"),
    "direction": ("direction", "call_direction"),
    "phone": ("phone", "phone_number", "to", "dialed_number", "contact.phone"),
    "seat_id": ("user_id", "userId", "agent_id", "agentId", "user.id"),
    "seat_name": ("user_name", "userName", "agent_name", "agentName", "user.name"),
    "caller_id_number": ("caller_id", "callerId", "from", "caller_id_number", "did"),
    "campaign_id": ("campaign_id", "campaignId", "campaign.id"),
    "started_at": ("started_at", "startTime", "start_time", "start"),
    "ended_at": ("ended_at", "endTime", "end_time", "end"),
    "duration": ("talk_duration", "talkTime", "duration", "billsec"),
    "disposition": ("disposition", "call_result", "callResult", "result", "disposition_name"),
    "recording": ("recording_url", "recordingUrl", "recording"),
    "disclosure": ("recording_disclosure", "disclosure_played"),
}


def validate_dispositions_config() -> None:
    codes = set(DISPOSITIONS)
    if len(codes) != len(DISPOSITIONS):
        raise ValueError("DISPOSITIONS must not repeat a code")
    for name, group in (("UNANSWERED_CODES", UNANSWERED_CODES), ("DEFAULT_CAUSE", set(DEFAULT_CAUSE)),
                        ("SYSTEM_DISPOSITION_ALIASES", set(SYSTEM_DISPOSITION_ALIASES.values()))):
        if not group <= codes:
            raise ValueError(f"{name} names a code missing from DISPOSITIONS: {sorted(group - codes)}")
    if not set(DEFAULT_CAUSE.values()) <= CAUSE_TAGS:
        raise ValueError("DEFAULT_CAUSE uses a value outside CAUSE_TAGS")
    if not {BOOKED_CODE, DNC_CODE} <= codes:
        raise ValueError("BOOKED and DNC_REQUEST must be in DISPOSITIONS")
