"""GoHighLevel -> lending opt-out webhook.

A GHL workflow ("Contact DND changed" -> Webhook) POSTs the contact here; the number is
suppressed in every lending store and removed from the dialer, the same as a dialer
"do not call". Auth: ``X-Webhook-Secret`` must equal LENDING_GHL_WEBHOOK_SECRET; the
endpoint is closed while the secret is unset.

Endpoints: POST /webhooks/lending/ghl-opt-out, POST /webhooks/lending/ghl-text-consent
"""
from __future__ import annotations

import hmac
import logging
import re
from typing import Any, Optional

from fastapi import APIRouter, Depends, Header, HTTPException
from sqlalchemy.orm import Session

from config.lending_compliance import OptOutChannel
from config.lending_text_back import SOFT_DECLINE_PHRASES, STOP_KEYWORDS, STOP_TOKENS
from config.settings import get_settings
from src.api.deps import get_db
from src.lending.compliance import phone_hash, propagate_opt_out
from src.lending.consent import record_consent, revoke_consent
from src.services.phone_utils import normalize

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/webhooks/lending", tags=["lending"])


def _verify_secret(received: Optional[str]) -> None:
    secret = get_settings().lending_ghl_webhook_secret
    if secret is None:
        raise HTTPException(status_code=503, detail="GHL webhook is not configured")
    if not received or not hmac.compare_digest(received, secret.get_secret_value()):
        raise HTTPException(status_code=401, detail="Invalid webhook secret")


def _field(body: dict[str, Any], name: str) -> Optional[str]:
    contact = body.get("contact") if isinstance(body.get("contact"), dict) else {}
    value = body.get(name) or contact.get(name)
    return str(value) if value else None


@router.post("/ghl-opt-out")
def ghl_opt_out(
    body: dict[str, Any],
    x_webhook_secret: Optional[str] = Header(default=None),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    _verify_secret(x_webhook_secret)
    phone, email = _field(body, "phone"), _field(body, "email")
    contact_id = _field(body, "contact_id") or _field(body, "id")
    if not phone and not email:
        raise HTTPException(status_code=422, detail="phone or email is required")
    try:
        event_id = propagate_opt_out(
            db, phone=phone, email=email, source_ref=f"ghl:{contact_id}" if contact_id else None,
            channel=OptOutChannel.GHL,
        )
        db.commit()
    except Exception as exc:
        db.rollback()
        logger.error("[lending-ghl] opt-out webhook failed: %s", type(exc).__name__)
        raise HTTPException(status_code=500, detail="Opt-out could not be recorded") from exc
    return {"recorded": event_id is not None}


_TRUTHY = frozenset({"true", "yes", "y", "1", "on", "checked"})
_CONSENT_SOURCES = frozenset({"web_form", "inbound_text"})


def _message_tokens(body: dict[str, Any]) -> list[str]:
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
def _is_hard_opt_out(tokens: list[str]) -> bool:
    if len(tokens) == 1 and tokens[0] in STOP_KEYWORDS:
        return True
    return any(t in STOP_TOKENS for t in tokens) or any(a == "opt" and b == "out" for a, b in zip(tokens, tokens[1:]))


def _is_soft_decline(tokens: list[str]) -> bool:
    if any(t in STOP_KEYWORDS for t in tokens):
        return True
    padded = f" {' '.join(tokens)} "
    return any(f" {phrase} " in padded for phrase in SOFT_DECLINE_PHRASES)


def _make_opt_out_durable(db: Session, phone: str, contact_id: Optional[str]) -> None:
    """Suppression list + dialer removal + GHL DND for a free-text STOP. Channel SMS: it is a text opt-out and,
    unlike OptOutChannel.GHL, leaves ghl_dnd_at unset so the DND sync writes it to GHL (GHL may not have caught
    a free-text STOP). The ref keeps the phone hash so one contact id with two numbers still suppresses both."""
    ref = f"ghl-text:{contact_id or ''}:{phone_hash(phone)[:16]}"
    propagate_opt_out(db, phone=phone, source_ref=ref, channel=OptOutChannel.SMS)


@router.post("/ghl-text-consent")
def ghl_text_consent(
    body: dict[str, Any],
    x_webhook_secret: Optional[str] = Header(default=None),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    """A GHL workflow reports text consent. ``web_form``: the lead form's consent box (only a checked
    box counts). ``inbound_text``: the contact texted the Next Deal Lending number; a hard opt-out
    revokes and suppresses, a soft decline only revokes; an empty or non-text reply records nothing. Body: source, phone, consent (web_form), message (inbound_text), contact_id."""
    _verify_secret(x_webhook_secret)
    source = _field(body, "source")
    phone = normalize(_field(body, "phone"))
    if source not in _CONSENT_SOURCES or not phone:
        raise HTTPException(status_code=422, detail="source (web_form or inbound_text) and a valid phone are required")
    contact_id = _field(body, "contact_id") or _field(body, "id")
    try:
        if source == "inbound_text":
            tokens = _message_tokens(body)
            if not tokens:
                return {"recorded": False, "revoked": False}
            hard = _is_hard_opt_out(tokens)
            if hard or _is_soft_decline(tokens):
                revoke_consent(db, phone)
                if hard:
                    _make_opt_out_durable(db, phone, contact_id)
                db.commit()
                return {"recorded": False, "revoked": True}
        if source == "web_form" and str(_field(body, "consent") or "").strip().lower() not in _TRUTHY:
            return {"recorded": False, "revoked": False}
        record_consent(db, phone, source, captured_by=f"ghl:{contact_id}" if contact_id else None)
        db.commit()
    except Exception as exc:
        db.rollback()
        logger.error("[lending-ghl] consent webhook failed: %s", type(exc).__name__)
        raise HTTPException(status_code=500, detail="Consent could not be recorded") from exc
    return {"recorded": True, "revoked": False}
