"""T-10 uncalled-lead alarms: rule values and message wording (no logic).

Spec §4.4 + decisions of 2026-10-09: a LendingFlow or website lead (client Q53) not reached by phone alarms Josh by SMS (GoHighLevel)
at 120 s; at 300 s an URGENT SMS plus a Slack #lendingops post. Only a CONNECTED call stops the alarms; a call
that was tried but not answered sends the "NOT REACHED" variant. Calls are only visible once they END
(BatchDialer CDRs, GHL call messages), so the "no call" texts tell Josh to ignore them if already on the phone.

Wording is approved by the client side (2026-10-09). Plain ASCII only: one non-GSM character halves the SMS
segment size. Slack texts carry no name or phone.
"""
from __future__ import annotations

from config.lending_dispositions import UNANSWERED_CODES

FIRST_ALARM_SECONDS = 120
ESCALATION_SECONDS = 300
POLL_SECONDS = 5
BATCH_SIZE = 50
GHL_TIMEOUT_SECONDS = 5

# Delivery retries. Only a send that provably never reached the provider is retried (GhlSmsError with
# ambiguous=False, a documented Slack rejection); a possible duplicate is never created from an unknown outcome.
# Slack errors that mean the request was REJECTED, so nothing was posted (Slack Web API, chat.postMessage error
# list). Default-deny: any other error, including internal_error / fatal_error ("some aspect of the operation may have
# succeeded"), service_unavailable, request_timeout, an HTTP 5xx and any code not listed here, is an unknown outcome.
SLACK_REJECTED_ERRORS = frozenset({
    "channel_not_found", "not_in_channel", "is_archived", "no_permission", "access_denied", "accesslimited",
    "account_inactive", "invalid_auth", "not_authed", "token_revoked", "token_expired", "missing_scope",
    "not_allowed_token_type", "team_access_not_granted", "org_login_required", "ekm_access_denied",
    "restricted_action", "restricted_action_read_only_channel", "restricted_action_thread_only_channel",
    "restricted_action_non_threadable_channel", "rate_limited", "ratelimited",
})
MAX_SEND_ATTEMPTS = 3
RETRY_DELAYS_SECONDS = (15, 45)   # wait after attempt 1, after attempt 2
RETRY_WINDOW_SECONDS = 15 * 60    # a retry still pending this long after its alarm fired is dropped
STALE_SEND_SECONDS = 120          # a claimed send with no recorded outcome after this long is 'send_unknown'

# A CDR with one of these dispositions is an attempt that did not reach the lead.
NOT_CONNECTED_CODES = tuple(sorted(UNANSWERED_CODES | {"BAD_NUMBER"}))
# GHL call message meta.callStatus values that mean the lead was reached (GHL OpenAPI; unverified live).
GHL_CONNECTED_STATUSES = frozenset({"completed", "answered"})

KIND_NO_CALL = "no_call"
KIND_NOT_REACHED = "not_reached"

SMS_TEXT = {
    (FIRST_ALARM_SECONDS, KIND_NO_CALL):
        "NDL LEAD ALERT: New {source} lead {lead} came in {minutes} min ago and no call is logged yet. "
        "Please call now. Ref {ref}. If you're already on the phone with them, ignore this.",
    (FIRST_ALARM_SECONDS, KIND_NOT_REACHED):
        "NDL LEAD ALERT - NOT REACHED: {source} lead {lead} was called but not reached "
        "({minutes} min since it came in). Please try again now. Ref {ref}.",
    (ESCALATION_SECONDS, KIND_NO_CALL):
        "URGENT - NDL LEAD STILL UNCALLED: {source} lead {lead} came in {minutes} min ago and still has no call "
        "logged. Call now. Ref {ref}. If you're already on the phone with them, ignore this.",
    (ESCALATION_SECONDS, KIND_NOT_REACHED):
        "URGENT - NDL LEAD STILL NOT REACHED: {source} lead {lead} came in {minutes} min ago and has not been "
        "reached yet. Try again now. Ref {ref}.",
}

SLACK_TEXT = {
    KIND_NO_CALL:
        ":rotating_light: *URGENT: {source} lead still uncalled*\n"
        "{who}{state} came in {minutes} min ago and has no call logged. Please call now. "
        "Ignore if a caller is already on the phone with them.",
    KIND_NOT_REACHED:
        ":warning: *URGENT: {source} lead not reached*\n"
        "{who}{state} came in {minutes} min ago and was called but not reached. Please try again now.",
}
