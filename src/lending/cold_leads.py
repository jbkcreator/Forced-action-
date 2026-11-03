"""Cold leads: numbers we reached first from a list (loaded into the dialer), never inbound.

Client rule: a cold lead never gets an email, and gets a text only after giving text consent on a
call. A number counts as cold once it has been loaded into the dialer, unless it also came in
through LendingFlow or the website form (an inbound lead is warm).

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

from src.lending.consent import has_text_consent
from src.services.phone_utils import normalize

logger = logging.getLogger(__name__)

_IS_COLD = text(
    "SELECT EXISTS (SELECT 1 FROM lending.dialer_load_records WHERE phone = :p) "
    "AND NOT EXISTS (SELECT 1 FROM lending.web_leads WHERE phone = :p) "
    "AND NOT EXISTS (SELECT 1 FROM lending.lendingflow_leads WHERE phone = :p)"
)


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


ALLOWED = ColdSendDecision()


def cold_send_decision(phone: Optional[str], channel: SendChannel) -> ColdSendDecision:
    """Whether a lending text or email to ``phone`` may go out.

    An email with no phone cannot be matched to a dialer record and is allowed; cold numbers in GHL
    also carry do-not-disturb on email, which covers sends GHL makes on its own.
    """
    if not phone or not normalize(phone):
        return ALLOWED
    try:
        with _session_factory() as db:
            if not is_cold_lead(db, phone):
                return ALLOWED
            consented = channel is SendChannel.SMS and has_text_consent(db, phone)
    except Exception as exc:  # class only: SQL errors embed bound parameters
        logger.error("[cold-leads] cold-lead check unreadable (%s); refusing the send", type(exc).__name__)
        return ColdSendDecision(block=ColdSendBlock.COLD_CHECK_UNAVAILABLE)
    if consented:
        return ColdSendDecision(cold=True)
    if channel is SendChannel.SMS:
        return ColdSendDecision(block=ColdSendBlock.COLD_LEAD_NO_TEXT_CONSENT, cold=True)
    return ColdSendDecision(block=ColdSendBlock.COLD_LEAD_NO_EMAIL, cold=True)
