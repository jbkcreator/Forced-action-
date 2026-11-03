"""Cold leads: numbers we reached first from a list (loaded into the dialer), never inbound.

Client rule: a cold lead never gets an email, and gets a text only after giving text consent on a
call. A number counts as cold once it has been loaded into the dialer, unless it also came in
through LendingFlow or the website form (an inbound lead is warm).

A cold lead that later comes in through the form or LendingFlow is warm again: the do-not-disturb the cold push
set in GHL is lifted (SMS only with text consent) and the ndl-cold tag removed, unless the number has opted out,
which always wins (``warm_dnd_reconciliation``, ``reconcile_warm_contact``).

The check runs inside the GHL senders, so every lending text and email passes it. It fails closed:
when the lookup cannot be read, the send is refused.
"""
from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from typing import Any, Optional

from sqlalchemy import text

from config.lending_compliance import (
    COLD_LEAD_DND_CHANNELS,
    COLD_LEAD_DND_MESSAGE,
    GHL_TAG_COLD_LEAD,
    TRACERFY_DNC_SOURCE,
)
from src.lending.consent import has_text_consent
from src.services.phone_utils import normalize

logger = logging.getLogger(__name__)

_IS_COLD = text(
    "SELECT EXISTS (SELECT 1 FROM lending.dialer_load_records WHERE phone = :p) "
    "AND NOT EXISTS (SELECT 1 FROM lending.web_leads WHERE phone = :p) "
    "AND NOT EXISTS (SELECT 1 FROM lending.lendingflow_leads WHERE phone = :p)"
)

# warm_state: one read for a number we first reached cold and that has since come in as an inbound lead.
# 0 = leave its do-not-disturb as it is (never loaded cold, still cold, or opted out: an opt-out always wins),
# 1 = lift every messaging channel except SMS, 2 = lift every messaging channel (recorded text consent).
_WARM_STATE = text("""
    SELECT /* warm_state */ CASE
        WHEN NOT EXISTS (SELECT 1 FROM lending.dialer_load_records WHERE phone = :p)
          OR NOT (EXISTS (SELECT 1 FROM lending.web_leads WHERE phone = :p)
                  OR EXISTS (SELECT 1 FROM lending.lendingflow_leads WHERE phone = :p))
          OR EXISTS (SELECT 1 FROM lending.suppression_list WHERE phone = :p)
          OR EXISTS (SELECT 1 FROM sms_opt_outs WHERE phone = :p AND source <> :tracerfy)
          OR EXISTS (SELECT 1 FROM lending.contacts WHERE phone = :p AND do_not_contact)
        THEN 0
        WHEN EXISTS (SELECT 1 FROM lending.text_consents WHERE phone = :p AND revoked_at IS NULL) THEN 2
        ELSE 1
    END
""")
_SMS = "SMS"


class SendChannel(str, Enum):
    SMS = "sms"
    EMAIL = "email"


class ColdSendBlock(str, Enum):
    """Why a send to a cold lead was refused."""

    COLD_LEAD_NO_TEXT_CONSENT = "cold_lead_no_text_consent"
    COLD_LEAD_NO_EMAIL = "cold_lead_no_email"
    COLD_CHECK_UNAVAILABLE = "cold_check_unavailable"


def _default_session():
    from src.lending.db import lending_session

    return lending_session()


# Read at call time so tests can swap in a fake and never touch the shared database.
_session_factory: Callable[[], Any] = _default_session


def is_cold_lead(db, phone: str) -> bool:
    norm = normalize(phone)
    if not norm:
        return False
    return bool(db.execute(_IS_COLD, {"p": norm}).scalar())


@dataclass(frozen=True)
class ColdSendDecision:
    """``block`` set means refuse the send. ``cold`` is True for a cold lead allowed through on text consent,
    so the SMS sender can lift the SMS do-not-disturb set on cold contacts in GHL."""

    block: Optional[ColdSendBlock] = None
    cold: bool = False
    lift_dnd: tuple[str, ...] = ()  # GHL do-not-disturb channels the sender lifts in its contact upsert


ALLOWED = ColdSendDecision()


def warm_dnd_reconciliation(db, phone: Optional[str], *, text_consent: bool) -> tuple[str, ...]:
    """The messaging channels whose cold-lead do-not-disturb may be lifted for ``phone`` (empty: leave it).
    ``text_consent`` is consent given with the inbound lead itself (the form's text box); recorded consent counts
    too. SMS is lifted only with consent; a number that opted out gets nothing lifted."""
    norm = normalize(phone) if phone else None
    if not norm:
        return ()
    state = db.execute(_WARM_STATE, {"p": norm, "tracerfy": TRACERFY_DNC_SOURCE}).scalar() or 0
    if not state:
        return ()
    sms_ok = state == 2 or text_consent
    return tuple(channel for channel in COLD_LEAD_DND_CHANNELS if channel != _SMS or sms_ok)


def _send_lift(db, phone: str, channel: SendChannel) -> tuple[str, ...]:
    """At send time, for a warm lead first reached cold: the one channel this send needs lifted, if allowed."""
    wanted = _SMS if channel is SendChannel.SMS else "Email"
    return (wanted,) if wanted in warm_dnd_reconciliation(db, phone, text_consent=False) else ()


def cold_send_decision(phone: Optional[str], channel: SendChannel) -> ColdSendDecision:
    """Whether a lending text or email to ``phone`` may go out.

    An email with no phone cannot be matched to a dialer record and is allowed; cold numbers in GHL
    also carry do-not-disturb on email, which covers sends GHL makes on its own. A warm lead first reached
    cold has the do-not-disturb on this send's channel lifted when allowed (``lift_dnd``).
    """
    if not phone or not normalize(phone):
        return ALLOWED
    try:
        with _session_factory() as db:
            if not is_cold_lead(db, phone):
                lift = _send_lift(db, phone, channel)
                return ColdSendDecision(lift_dnd=lift) if lift else ALLOWED
            consented = channel is SendChannel.SMS and has_text_consent(db, phone)
    except Exception as exc:  # class only: SQL errors embed bound parameters
        logger.error("[cold-leads] cold-lead check unreadable (%s); refusing the send", type(exc).__name__)
        return ColdSendDecision(block=ColdSendBlock.COLD_CHECK_UNAVAILABLE)
    if consented:
        return ColdSendDecision(cold=True, lift_dnd=(_SMS,))
    if channel is SendChannel.SMS:
        return ColdSendDecision(block=ColdSendBlock.COLD_LEAD_NO_TEXT_CONSENT, cold=True)
    return ColdSendDecision(block=ColdSendBlock.COLD_LEAD_NO_EMAIL, cold=True)


GhlCall = Callable[..., dict]  # a sink's ``_call(step, method, path, **kwargs)``; raises on an HTTP error


def reconcile_warm_contact(call: GhlCall, location_id: str, contact_id: str, phone: Optional[str], *,
                           text_consent: bool, log_prefix: str) -> None:
    """After an inbound push: lift the cold-lead do-not-disturb the GHL contact still carries and drop the
    ndl-cold tag. Only do-not-disturb set by the cold push (its message) is lifted, so one GHL set for an opt-out
    stays. Best effort and never raises: on a failure messaging stays blocked (the safe side), and the senders lift
    their own channel again at send time."""
    try:
        with _session_factory() as db:
            channels = warm_dnd_reconciliation(db, phone, text_consent=text_consent)
        if not channels:
            return
        contact = (call("read contact", "GET", f"/contacts/{contact_id}") or {}).get("contact") or {}
        current = contact.get("dndSettings") or {}
        ours = [channel for channel in channels
                if (current.get(channel) or {}).get("status") == "active"
                and (current.get(channel) or {}).get("message") == COLD_LEAD_DND_MESSAGE]
        if ours:
            call("lift cold do-not-disturb", "POST", "/contacts/upsert",
                 json={"locationId": location_id, "phone": phone,
                       "dndSettings": {channel: {"status": "inactive"} for channel in ours}})
        call("remove cold tag", "DELETE", f"/contacts/{contact_id}/tags", json={"tags": [GHL_TAG_COLD_LEAD]})
    except Exception as exc:  # class only: the message can carry request detail
        logger.warning("%s cold-lead warm-up skipped: %s", log_prefix, type(exc).__name__)
