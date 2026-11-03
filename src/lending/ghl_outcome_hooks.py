"""Hooks on GHL call outcomes for the code that acts on them (Dev C: DNC, consent, missed-call follow-up).

Two calls, both made inside the caller's database transaction and inside a savepoint of their own:

* :func:`on_ghl_outcome_received`: once per NEW outcome stored by the result webhook (``record_event``), before it is
  matched to a call. A webhook retry stored once is not reported again. Use it to act at once (a DNC_REQUEST blocks
  the number without waiting for the GHL call import).
* :func:`on_ghl_outcome_matched`: once per matching run, with every outcome that run attached to its call (from the
  webhook and from each GHL call import), after the call and event rows are written.

Rules for an implementation: never commit or roll back (the caller commits; a hook error rolls back only the hook's
savepoint and is logged by class, so the outcome and the match are always kept); keep it fast (the webhook waits on
it); never log phone numbers. The defaults do nothing. The matcher looks the functions up on this module at call
time, so replacing them here (or monkeypatching them in tests) is enough.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional


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


def on_ghl_outcome_received(db: Any, event: ReceivedOutcome) -> None:
    """A new GHL call outcome was stored (not yet matched to its call). Default: nothing."""
    return None


def on_ghl_outcome_matched(db: Any, matched: list[MatchedOutcome]) -> None:
    """GHL call outcomes were attached to their calls in this run. Default: nothing."""
    return None
