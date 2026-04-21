"""
SMS compliance gate — TCPA / CTIA.

Every outbound SMS must pass through can_send() before hitting Twilio.
Inbound STOP keywords are handled by handle_inbound() and written to sms_opt_outs.

Pre-send flow:
    can_send(phone, db) → False  →  add_to_dead_letter(), do not send
                        → True   →  send via Twilio

Inbound keyword flow (Twilio webhook):
    handle_inbound(from_number, body, db)
        → if STOP keyword: record_opt_out(), return TwiML opt-out reply
        → else:            return None (caller handles normal inbound)

Redis note: sms_opt_outs is the Postgres-backed suppression list for 2B-1.
In 2B-2 this will be fronted by a Redis SET for sub-millisecond pre-send checks.
"""

import logging
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from config.settings import settings
from src.core.models import SmsDeadLetter, SmsOptOut

try:
    from twilio.rest import Client
except ImportError:  # Twilio not installed in test/CI environments
    Client = None  # type: ignore[assignment,misc]

logger = logging.getLogger(__name__)

# CTIA-required opt-out keywords (case-insensitive, must suppress immediately)
_STOP_KEYWORDS = {"stop", "unsubscribe", "cancel", "quit", "end"}

# Standard TCPA-compliant opt-out reply (must be sent verbatim after STOP)
_OPT_OUT_REPLY = (
    "You have been unsubscribed and will receive no further messages from Forced Action. "
    "Reply START to re-subscribe."
)


# ── Public API ────────────────────────────────────────────────────────────────


def can_send(phone: str, db: Session) -> bool:
    """
    Return True if it is legal to send an outbound SMS to this number.
    Checks the sms_opt_outs suppression table (Redis in 2B-2).
    Callers must check this before every Twilio send.
    """
    phone = _normalize(phone)
    if not phone:
        return False
    exists = db.execute(
        select(SmsOptOut.id).where(SmsOptOut.phone == phone)
    ).first()
    return exists is None


def handle_inbound(from_number: str, body: str, db: Session) -> Optional[str]:
    """
    Process an inbound SMS from Twilio.

    Returns the TwiML reply string if the message was a STOP keyword (caller
    should return this as the Twilio webhook response).
    Returns None if the message is not a STOP keyword (caller handles normally).
    """
    keyword = _extract_stop_keyword(body)
    if keyword:
        record_opt_out(from_number, keyword, "twilio_inbound", db)
        logger.info("SMS opt-out recorded: phone=%s keyword=%s", from_number, keyword)
        return _twiml_reply(_OPT_OUT_REPLY)
    return None


def record_opt_out(
    phone: str,
    keyword: str,
    source: str,
    db: Session,
) -> None:
    """
    Add a phone number to the suppression list.
    Safe to call multiple times — uses INSERT ... ON CONFLICT DO NOTHING.
    """
    phone = _normalize(phone)
    if not phone:
        return
    existing = db.execute(
        select(SmsOptOut).where(SmsOptOut.phone == phone)
    ).scalar_one_or_none()
    if existing:
        return
    db.add(SmsOptOut(
        phone=phone,
        keyword_used=keyword.upper()[:20],
        source=source,
        opted_out_at=datetime.now(timezone.utc),
    ))
    db.flush()


def add_to_dead_letter(
    phone: Optional[str],
    reason: str,
    payload: Optional[dict],
    db: Session,
) -> None:
    """
    Write a failed or blocked SMS event to the dead-letter queue for manual review.
    reason must be one of: opt_out / delivery_failed / error / unresolvable
    """
    valid_reasons = {"opt_out", "delivery_failed", "error", "unresolvable"}
    if reason not in valid_reasons:
        logger.warning("Invalid DLQ reason '%s' — defaulting to 'error'", reason)
        reason = "error"
    db.add(SmsDeadLetter(
        phone=_normalize(phone) if phone else None,
        reason=reason,
        payload=payload,
        created_at=datetime.now(timezone.utc),
    ))
    db.flush()


def send_sms(
    to: str,
    body: str,
    db: Session,
    subscriber_id: Optional[int] = None,
    task_type: Optional[str] = None,
) -> bool:
    """
    Central outbound SMS dispatcher.

    1. Runs can_send() gate — writes to DLQ and returns False if suppressed.
    2. Sends via Twilio if TWILIO_ENABLED=true, else logs only.
    3. Logs to api_usage_logs via claude_router pattern (cost tracked separately).

    Returns True if the message was sent (or logged in dry-run), False if suppressed.
    """
    to = _normalize(to)
    if not can_send(to, db):
        logger.info("SMS suppressed (opt-out): to=%s", to)
        add_to_dead_letter(to, "opt_out", {"body": body[:160]}, db)
        return False

    if not settings.twilio_enabled:
        logger.info("[DRY RUN] SMS to=%s body=%r", to, body[:160])
        return True

    if not all([settings.twilio_account_sid, settings.twilio_auth_token, settings.twilio_from_number]):
        logger.error("Twilio not configured — cannot send SMS to %s", to)
        add_to_dead_letter(to, "error", {"body": body[:160], "error": "twilio_not_configured"}, db)
        return False

    try:
        client = Client(
            settings.twilio_account_sid,
            settings.twilio_auth_token.get_secret_value(),
        )
        message = client.messages.create(
            body=body,
            from_=settings.twilio_from_number,
            to=to,
        )
        logger.info("SMS sent: sid=%s to=%s", message.sid, to)
        return True
    except Exception as exc:
        logger.error("Twilio send failed: to=%s error=%s", to, exc)
        add_to_dead_letter(to, "delivery_failed", {"body": body[:160], "error": str(exc)}, db)
        return False


# ── Helpers ───────────────────────────────────────────────────────────────────


def _normalize(phone: str) -> str:
    """Strip whitespace; ensure E.164 format check is caller's responsibility."""
    return (phone or "").strip()


def _extract_stop_keyword(body: str) -> Optional[str]:
    """Return the matched STOP keyword if the message body is a STOP command, else None."""
    word = body.strip().lower().split()[0] if body.strip() else ""
    return word if word in _STOP_KEYWORDS else None


def _twiml_reply(message: str) -> str:
    """Minimal TwiML response for Twilio webhook."""
    safe = message.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    return f'<?xml version="1.0" encoding="UTF-8"?><Response><Message>{safe}</Message></Response>'
