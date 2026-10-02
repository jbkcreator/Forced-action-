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
from config.lending_text_back import STOP_KEYWORDS, STOP_TOKENS
from config.settings import get_settings
from src.api.deps import get_db
from src.lending.compliance import propagate_opt_out
from src.lending.consent import record_consent, revoke_consent
from src.services.phone_utils import normalize

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/webhooks/lending", tags=["lending"])


def _verify_secret(received: Optional[str]) -> None:
    secret = get_settings().lending_ghl_webhook_secret
    if secret is None:
        raise HTTPException(status_code=503, detail="GHL opt-out webhook is not configured")
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


def _is_opt_out(tokens: list[str]) -> bool:
    # Fails closed (counsel to confirm scope): any stop-word in a reply revokes, a missed opt-out is the costly error.
    if len(tokens) == 1 and tokens[0] in STOP_KEYWORDS:
        return True
    return any(t in STOP_TOKENS for t in tokens) or any(a == "opt" and b == "out" for a, b in zip(tokens, tokens[1:]))


@router.post("/ghl-text-consent")
def ghl_text_consent(
    body: dict[str, Any],
    x_webhook_secret: Optional[str] = Header(default=None),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    """A GHL workflow reports text consent. ``web_form``: the lead form's consent box (only a checked
    box counts). ``inbound_text``: the contact texted the Next Deal Lending number; a bare STOP
    phrase revokes instead; an empty or non-text reply records nothing. Body: source, phone, consent (web_form), message (inbound_text), contact_id."""
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
            if _is_opt_out(tokens):
                revoke_consent(db, phone)
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
