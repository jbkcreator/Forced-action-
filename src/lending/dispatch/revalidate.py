"""Pre-dispatch re-validation (R1 item 0.4): right before a send, is the recipient still the one that was approved?

A card freezes the recipient's phone and email when it is drafted. If the lead's record was edited after approval, the
approved number or address may no longer be theirs. The card's ``contact_ref`` names the lead record (``web_lead:12``,
``lendingflow_lead:56``, ``contact:34``, or ``ghl:<contact id>``), so the check compares the frozen value with that
record as it stands now:

* the value changed, or the named lead no longer exists -> the send is held for review (never sent, never retried);
* a ``contact_ref`` that is not one of ours to look up cannot be compared; opt-out and consent checks still run.

A ``ghl:<contact id>`` card (agent jobs draft from the GHL contact) is checked against the GHL contact itself, read at
dispatch through ``ghl_reader``: a changed phone/email or a deleted contact holds the send; a failed read defers it to
the next sweep (never sends unchecked). Only without a reader (GHL not configured) does it fall back to our lead records.

Consent revoked while queued is held the same way (``HOLD_CONSENT_REVOKED``), by the caller's send check.
"""
from __future__ import annotations

import re
from collections.abc import Callable, Collection
from typing import Any, Optional

from sqlalchemy import text

from packages.agent_core.pending_actions import PendingAction
from packages.agent_core.relay import HELD_UNCERTAIN_OUTCOME, SendDeferred, SendHold
from src.services.phone_utils import normalize

HOLD_RECIPIENT_CHANGED = "recipient_changed"
HOLD_LEAD_NOT_FOUND = "lead_not_found"
HOLD_CONSENT_REVOKED = "consent_revoked"
HOLD_UNCERTAIN_OUTCOME = HELD_UNCERTAIN_OUTCOME

_LEAD_REF = re.compile(r"^(web_lead|lendingflow_lead|contact|instantly_reply|doc_chase):(\d+)$")
_GHL_REF = re.compile(r"^ghl:(\S+)$")

_BY_LEAD = {
    "web_lead": text("SELECT phone, email FROM lending.web_leads WHERE id = :id"),
    "lendingflow_lead": text("SELECT phone, email FROM lending.lendingflow_leads WHERE id = :id"),
    "contact": text("SELECT phone, email FROM lending.contacts WHERE id = :id"),
    "instantly_reply": text("SELECT NULL, lead_email FROM lending.instantly_replies WHERE id = :id"),
    "doc_chase": text("SELECT phone, email FROM lending.doc_chases WHERE id = :id"),
}
_BY_GHL_CONTACT = text("""
    SELECT phone, email FROM lending.web_leads WHERE ghl_contact_id = :g
    UNION ALL
    SELECT phone, email FROM lending.lendingflow_leads WHERE ghl_contact_id = :g
""")


def _email(value: Optional[str]) -> Optional[str]:
    return (value or "").strip().lower() or None


def recipient_still_matches(card_phone: Optional[str], card_email: Optional[str], current: Collection[tuple],
                            *, uses_phone: bool, uses_email: bool) -> bool:
    """True when some current record still carries the frozen phone (text) or email (email). Pure."""
    for phone, email in current:
        if uses_phone and normalize(card_phone) != (normalize(phone) if phone else None):
            continue
        if uses_email and _email(card_email) != _email(email):
            continue
        return True
    return False


GhlReader = Callable[[str], Any]  # contact id -> an object with .phone / .email, or None when GHL has no such contact


def changed_recipient(conn, action: PendingAction, *, sms_channels: Collection[str],
                      email_channels: Collection[str], ghl_reader: Optional[GhlReader] = None) -> Optional[str]:
    """A ``SendHold`` when the recipient was edited (or the lead is gone) since approval, a ``SendDeferred`` when the GHL
    contact could not be read, else None. Only text and email channels have a recipient to compare."""
    uses_phone, uses_email = action.channel in sms_channels, action.channel in email_channels
    ref = action.contact_ref or ""
    if not (uses_phone or uses_email) or not ref:
        return None
    lead, ghl = _LEAD_REF.match(ref), _GHL_REF.match(ref)
    if lead:
        rows = conn.execute(_BY_LEAD[lead.group(1)], {"id": int(lead.group(2))}).all()
        if not rows:
            return SendHold(HOLD_LEAD_NOT_FOUND)
    elif ghl and ghl_reader is not None:
        try:
            contact = ghl_reader(ghl.group(1))
        except Exception as exc:  # class only: the message can carry request detail
            return SendDeferred(f"the GHL contact could not be read ({type(exc).__name__}); trying again")
        if contact is None:
            return SendHold(HOLD_LEAD_NOT_FOUND)
        rows = [(contact.phone, contact.email)]
    elif ghl:
        rows = conn.execute(_BY_GHL_CONTACT, {"g": ghl.group(1)}).all()
        if not rows:
            return None  # a GHL contact we hold no lead record for: nothing to compare with
    else:
        return None
    current = [(row[0], row[1]) for row in rows]
    if recipient_still_matches(action.recipient_phone, action.recipient_email, current,
                               uses_phone=uses_phone, uses_email=uses_email):
        return None
    return SendHold(HOLD_RECIPIENT_CHANGED)
