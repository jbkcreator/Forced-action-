"""Hooks on GHL call outcomes for the code that acts on them (Dev C: DNC, consent, missed-call follow-up).

Two calls, both made inside the caller's database transaction and inside a savepoint of their own:

* :func:`on_ghl_outcome_received`: once per NEW outcome stored by the result webhook (``record_event``), before it is
  matched to a call. A webhook retry stored once is not reported again. Use it to act at once (a DNC_REQUEST blocks
  the number without waiting for the GHL call import).
* :func:`on_ghl_outcome_matched`: once per matching run, with every outcome that run attached to its call (from the
  webhook and from each GHL call import), after the call and event rows are written.

Rules for an implementation: never commit or roll back (the caller commits; a hook error rolls back only the hook's
savepoint and is logged by class, so the outcome and the match are always kept); keep it fast (the webhook waits on
it); never log phone numbers. The matcher looks the functions up on this module at call time, so replacing them here
(or monkeypatching them in tests) is enough.

G4 (act on the result Josh picks in GHL, as for BatchDialer):
* DNC_REQUEST: the number is opted out everywhere (``propagate_opt_out``: suppression list, do-not-contact,
  BatchDialer removal, GHL DND sync) as soon as the outcome arrives; the match stamps the call row so the CDR
  poller's DNC sweep never opts it out a second time. One opt-out per outcome (source_ref ``ghl-disposition:<id>``).
  The stores are written in the caller's transaction, so every send and dial gate blocks the number on commit; the
  BatchDialer removal and the GHL DND follow on the next opt-out poll (<= OPT_OUT_POLL_SECONDS), so the hook makes
  no BatchDialer call and opens no second database connection while the caller's transaction is open.
* Text consent: Text Consent = yes on an answered call is saved as ``on_call_yes`` (the BatchDialer rule: only a
  yes, only when someone picked up).
* No answer / voicemail: the missed-call event is queued for the call (idempotent per call). The text itself comes
  from the immediate missed-call webhook (src/lending/ghl_missed_call.py); an outcome matched later is past the 60 s
  rule, so text_back logs it skipped_late and never sends a late text.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

from sqlalchemy import text

from config.lending_dispositions import ANSWERED_CODES, DNC_CODE, UNANSWERED_CODES
from src.lending.compliance import propagate_opt_out
from src.lending.consent import record_consent
from src.lending.dialer_removal import DialerRemovalUndecided
from src.lending.dispositions import queue_missed_call

GHL_DNC_ACTOR = "ghl"
_CALL_TIMES = text("SELECT id, call_ended_at, caller_id_number FROM lending.call_dispositions WHERE id = ANY(:ids)")
_STAMP_DNC = text("UPDATE lending.call_dispositions SET opt_out_propagated_at = now() "
                  "WHERE id = ANY(:ids) AND opt_out_propagated_at IS NULL")


@dataclass(frozen=True)
class ReceivedOutcome:
    event_id: int                      # lending.ghl_disposition_events.id
    contact_id: Optional[str]          # GHL contact id
    phone: Optional[str]               # E.164 (phone_utils.normalize)
    code: str                          # one of the 10 call result codes, e.g. DNC_REQUEST
    caller_name: Optional[str]         # GHL user who made the call ({{phoneCall.user.name}})
    called_at: Optional[datetime]      # the call's start time as GHL sent it (UTC); None when not sent or unreadable
    received_at: datetime              # when the webhook was stored (UTC, application clock)
    gate_failed: Optional[bool]        # None = not sent
    text_consent: Optional[bool]       # the contact's Text Consent as sent in the webhook; None = not sent


@dataclass(frozen=True)
class MatchedOutcome:
    call_row_id: int                   # lending.call_dispositions.id
    dialer_call_id: str                # ghl:<message id>
    event_id: int                      # lending.ghl_disposition_events.id
    contact_id: Optional[str]
    phone: Optional[str]               # E.164, the call's phone
    code: str
    caller_name: Optional[str]         # the outcome's caller (the call row keeps a name it already had)
    gate_failed: Optional[bool]
    text_consent: Optional[bool]


def dnc_source_ref(event_id: int) -> str:
    return f"ghl-disposition:{event_id}"


def _remove_on_next_poll(phone: str, *, reason: str) -> None:
    """Leaves the dialer removal pending for the opt-out poller's retry."""
    raise DialerRemovalUndecided("removal runs on the next opt-out poll")


def _opt_out(db: Any, phone: str, event_id: int, caller_name: Optional[str]) -> None:
    propagate_opt_out(db, phone=phone, source_ref=dnc_source_ref(event_id), actor=caller_name or GHL_DNC_ACTOR,
                      dialer_remover=_remove_on_next_poll)


def on_ghl_outcome_received(db: Any, event: ReceivedOutcome) -> None:
    """A DNC_REQUEST blocks the number at once, without waiting for the call import."""
    if event.code == DNC_CODE and event.phone:
        _opt_out(db, event.phone, event.event_id, event.caller_name)


def on_ghl_outcome_matched(db: Any, matched: list[MatchedOutcome]) -> None:
    """DNC (an outcome that arrived without a phone is opted out here, by the call's phone), consent and the
    missed-call event, for every outcome this run attached to its call."""
    dnc_rows: list[int] = []
    unanswered: list[MatchedOutcome] = []
    for item in matched:
        if not item.phone:
            continue
        if item.code == DNC_CODE:
            _opt_out(db, item.phone, item.event_id, item.caller_name)
            dnc_rows.append(item.call_row_id)
        elif item.text_consent is True and item.code in ANSWERED_CODES:
            record_consent(db, item.phone, "on_call_yes", call_id=item.dialer_call_id, captured_by=item.caller_name)
        elif item.code in UNANSWERED_CODES:
            unanswered.append(item)
    if dnc_rows:
        db.execute(_STAMP_DNC, {"ids": dnc_rows})
    if unanswered:
        _queue_missed(db, unanswered)


def _queue_missed(db: Any, items: list[MatchedOutcome]) -> None:
    calls = {row[0]: row for row in db.execute(_CALL_TIMES, {"ids": [i.call_row_id for i in items]}).all()}
    for item in items:
        row = calls.get(item.call_row_id)
        if row is not None and row[1] is not None:
            queue_missed_call(db, item.dialer_call_id, item.phone, row[2], None, row[1])
