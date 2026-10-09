"""T-10 uncalled-lead alarms: rule values and message wording (no logic).

Spec §4.4 + decisions of 2026-10-09: a LendingFlow lead not reached by phone alarms Josh by SMS (GoHighLevel)
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

# A CDR with one of these dispositions is an attempt that did not reach the lead.
NOT_CONNECTED_CODES = tuple(sorted(UNANSWERED_CODES | {"BAD_NUMBER"}))
# GHL call message meta.callStatus values that mean the lead was reached (GHL OpenAPI; unverified live).
GHL_CONNECTED_STATUSES = frozenset({"completed", "answered"})

KIND_NO_CALL = "no_call"
KIND_NOT_REACHED = "not_reached"

SMS_TEXT = {
    (FIRST_ALARM_SECONDS, KIND_NO_CALL):
        "NDL LEAD ALERT: New LendingFlow lead {lead} came in {minutes} min ago and no call is logged yet. "
        "Please call now. Ref {ref}. If you're already on the phone with them, ignore this.",
    (FIRST_ALARM_SECONDS, KIND_NOT_REACHED):
        "NDL LEAD ALERT - NOT REACHED: LendingFlow lead {lead} was called but not reached "
        "({minutes} min since it came in). Please try again now. Ref {ref}.",
    (ESCALATION_SECONDS, KIND_NO_CALL):
        "URGENT - NDL LEAD STILL UNCALLED: LendingFlow lead {lead} came in {minutes} min ago and still has no call "
        "logged. Call now. Ref {ref}. If you're already on the phone with them, ignore this.",
    (ESCALATION_SECONDS, KIND_NOT_REACHED):
        "URGENT - NDL LEAD STILL NOT REACHED: LendingFlow lead {lead} came in {minutes} min ago and has not been "
        "reached yet. Try again now. Ref {ref}.",
}

SLACK_TEXT = {
    KIND_NO_CALL:
        ":rotating_light: *URGENT: LendingFlow lead still uncalled*\n"
        "Lead ref {ref}{state} came in {minutes} min ago and has no call logged. Please call now. "
        "Ignore if a caller is already on the phone with them.",
    KIND_NOT_REACHED:
        ":warning: *URGENT: LendingFlow lead not reached*\n"
        "Lead ref {ref}{state} came in {minutes} min ago and was called but not reached. Please try again now.",
}
