"""What a GHL opt-out or inbound-text event does to lending's consent records.

Shared by the GHL webhooks (``src/api/lending_ghl_router.py``) and the retry sweep for webhook calls that
failed (``src/lending/webhook_failures.py``), so a replayed event takes exactly the same path. Nothing here
commits; the caller owns the transaction.
"""
from __future__ import annotations

import re
from typing import Any, Optional

from config.lending_compliance import OptOutChannel
from config.lending_text_back import SOFT_DECLINE_PHRASES, STOP_KEYWORDS, STOP_TOKENS
from src.lending.compliance import phone_hash, propagate_opt_out
from src.lending.consent import record_consent, revoke_consent
from src.services.phone_utils import normalize

_TRUTHY = frozenset({"true", "yes", "y", "1", "on", "checked"})
CONSENT_SOURCES = frozenset({"web_form", "inbound_text"})


class InvalidEvent(ValueError):
    """The payload can never be processed (missing or unusable fields); retrying will not help."""


def field(body: dict[str, Any], name: str) -> Optional[str]:
    contact = body.get("contact") if isinstance(body.get("contact"), dict) else {}
    value = body.get(name) or contact.get(name)
    return str(value) if value else None


def contact_id_of(body: dict[str, Any]) -> Optional[str]:
    return field(body, "contact_id") or field(body, "id")


def message_tokens(body: dict[str, Any]) -> list[str]:
    """Lower-cased alphanumeric tokens of the reply; empty when it is missing or not a string."""
    contact = body.get("contact") if isinstance(body.get("contact"), dict) else {}
    raw = body.get("message") or contact.get("message")
    return re.sub(r"[^a-z0-9]+", " ", raw.lower()).split() if isinstance(raw, str) else []


# Two classes of inbound reply (counsel to confirm the scope and both word lists before go-live):
# - HARD opt-out: the whole reply is one STOP keyword, or it contains stop / stopall / unsubscribe / optout /
#   revoke or the pair "opt out". A missed opt-out is the costly error, so this fails closed and is made
#   durable here (suppression list, dialer removal, GHL DND), not left to GHL's own keyword handling.
# - SOFT decline: a cancel / end / quit word inside a longer reply, or a decline phrase. The contact is
#   unhappy or the message is ambiguous, not necessarily a legal STOP: consent is revoked, nothing is suppressed.
def is_hard_opt_out(tokens: list[str]) -> bool:
    if len(tokens) == 1 and tokens[0] in STOP_KEYWORDS:
        return True
    return any(t in STOP_TOKENS for t in tokens) or any(a == "opt" and b == "out" for a, b in zip(tokens, tokens[1:]))


def is_soft_decline(tokens: list[str]) -> bool:
    if any(t in STOP_KEYWORDS for t in tokens):
        return True
    padded = f" {' '.join(tokens)} "
    return any(f" {phrase} " in padded for phrase in SOFT_DECLINE_PHRASES)


def _make_opt_out_durable(db, phone: str, contact_id: Optional[str]) -> None:
    """Suppression list + dialer removal + GHL DND for a free-text STOP. Channel SMS: it is a text opt-out and,
    unlike OptOutChannel.GHL, leaves ghl_dnd_at unset so the DND sync writes it to GHL (GHL may not have caught
    a free-text STOP). The ref keeps the phone hash so one contact id with two numbers still suppresses both."""
    ref = f"ghl-text:{contact_id or ''}:{phone_hash(phone)[:16]}"
    propagate_opt_out(db, phone=phone, source_ref=ref, channel=OptOutChannel.SMS)


def record_ghl_opt_out(db, body: dict[str, Any]) -> Optional[int]:
    """A contact turned do-not-disturb in GHL: suppress it everywhere. Returns the opt-out event id."""
    phone, email = field(body, "phone"), field(body, "email")
    if not phone and not email:
        raise InvalidEvent("phone or email is required")
    contact_id = contact_id_of(body)
    return propagate_opt_out(db, phone=phone, email=email, source_ref=f"ghl:{contact_id}" if contact_id else None,
                             channel=OptOutChannel.GHL)


def granted_phone(body: dict[str, Any]) -> Optional[str]:
    """The phone a consent event would grant texting permission for, or None when it grants nothing (an
    opt-out, a decline, an unchecked box, an empty reply or an invalid payload). Same rules as
    :func:`record_inbound_consent`, without touching the database."""
    source = field(body, "source")
    phone = normalize(field(body, "phone"))
    if source not in CONSENT_SOURCES or not phone:
        return None
    if source == "inbound_text":
        tokens = message_tokens(body)
        if not tokens or is_hard_opt_out(tokens) or is_soft_decline(tokens):
            return None
        return phone
    return phone if str(field(body, "consent") or "").strip().lower() in _TRUTHY else None


def record_inbound_consent(db, body: dict[str, Any]) -> dict[str, bool]:
    """A GHL workflow reports text consent. ``web_form``: the lead form's consent box (only a checked box
    counts). ``inbound_text``: the contact texted the Next Deal Lending number; a hard opt-out revokes and
    suppresses, a soft decline only revokes; an empty or non-text reply records nothing."""
    source = field(body, "source")
    phone = normalize(field(body, "phone"))
    if source not in CONSENT_SOURCES or not phone:
        raise InvalidEvent("source (web_form or inbound_text) and a valid phone are required")
    contact_id = contact_id_of(body)
    if source == "inbound_text":
        tokens = message_tokens(body)
        if not tokens:
            return {"recorded": False, "revoked": False}
        hard = is_hard_opt_out(tokens)
        if hard or is_soft_decline(tokens):
            revoke_consent(db, phone)
            if hard:
                _make_opt_out_durable(db, phone, contact_id)
            return {"recorded": False, "revoked": True}
    if source == "web_form" and str(field(body, "consent") or "").strip().lower() not in _TRUTHY:
        return {"recorded": False, "revoked": False}
    record_consent(db, phone, source, captured_by=f"ghl:{contact_id}" if contact_id else None)
    return {"recorded": True, "revoked": False}
