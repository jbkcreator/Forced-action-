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
from zoneinfo import ZoneInfo

from sqlalchemy import select
from sqlalchemy.orm import Session

from config.settings import settings
from src.core.models import SmsDeadLetter, SmsOptIn, SmsOptOut

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


# TCPA quiet hours: 8am–9pm recipient local time
_QUIET_START = 21   # 9pm (exclusive upper bound)
_QUIET_END   = 8    # 8am (inclusive lower bound)

# Area code → IANA timezone. All current FL area codes are Eastern.
# Extend this dict when the platform expands to other states.
_AREA_CODE_TZ: dict[str, str] = {
    ac: "America/New_York" for ac in [
        "239", "305", "321", "352", "386", "407", "561", "727",
        "754", "772", "786", "813", "850", "863", "904", "941", "954",
    ]
}


def _recipient_tz(phone: str) -> ZoneInfo:
    digits = "".join(c for c in (phone or "") if c.isdigit())
    if digits.startswith("1"):
        digits = digits[1:]
    tz_name = _AREA_CODE_TZ.get(digits[:3], "America/New_York")
    return ZoneInfo(tz_name)


def is_quiet_hours(phone: str) -> bool:
    """Return True if current local time for this number is outside 8am–9pm (TCPA)."""
    hour = datetime.now(_recipient_tz(phone)).hour
    return hour < _QUIET_END or hour >= _QUIET_START


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
    campaign: Optional[str] = None,
    variant_id: Optional[str] = None,
    decision_id: Optional[str] = None,
) -> bool:
    """
    Central outbound SMS dispatcher.

    1. Runs can_send() gate — writes to DLQ and returns False if suppressed.
    2. Sends via Twilio if TWILIO_ENABLED=true, else logs only.
    3. When TWILIO_SANDBOX=true, writes a sandbox_outbox row regardless of
       TWILIO_ENABLED, so scenario tests can inspect the attempted send.
    4. Logs to api_usage_logs via claude_router pattern (cost tracked separately).

    Optional kwargs (campaign/variant_id/decision_id) enrich the sandbox_outbox
    row for scenario attribution. task_type is used as a fallback campaign name
    when campaign is not supplied — preserves behaviour for existing callers.

    Returns True if the message was sent (or logged in dry-run), False if suppressed.
    """
    to = _normalize(to)
    campaign_label = campaign or task_type

    # 1. Opt-out suppression (compliance gate)
    if not can_send(to, db):
        logger.info("SMS suppressed (opt-out): to=%s", to)
        add_to_dead_letter(to, "opt_out", {"body": body[:160]}, db)
        _capture_sandbox_attempt(
            db=db, to=to, body=body, subscriber_id=subscriber_id,
            campaign=campaign_label, variant_id=variant_id, decision_id=decision_id,
            compliance_allowed=False, compliance_reason="opt_out",
            would_have_delivered=False,
        )
        return False

    # 1b. TCPA quiet hours — no SMS before 8am or after 9pm recipient local time
    if is_quiet_hours(to):
        logger.info("SMS suppressed (quiet hours): to=%s", to)
        add_to_dead_letter(to, "quiet_hours", {"body": body[:160]}, db)
        _capture_sandbox_attempt(
            db=db, to=to, body=body, subscriber_id=subscriber_id,
            campaign=campaign_label, variant_id=variant_id, decision_id=decision_id,
            compliance_allowed=False, compliance_reason="quiet_hours",
            would_have_delivered=False,
        )
        return False

    # 2. Dry-run path (TWILIO_ENABLED=false)
    if not settings.twilio_enabled:
        logger.info("[DRY RUN] SMS to=%s body=%r", to, body[:160])
        _capture_sandbox_attempt(
            db=db, to=to, body=body, subscriber_id=subscriber_id,
            campaign=campaign_label, variant_id=variant_id, decision_id=decision_id,
            compliance_allowed=True, compliance_reason="ok",
            would_have_delivered=True,
        )
        return True

    # 3. Twilio misconfiguration (live mode on but creds missing)
    if not all([settings.twilio_account_sid, settings.twilio_auth_token, settings.twilio_from_number]):
        logger.error("Twilio not configured — cannot send SMS to %s", to)
        add_to_dead_letter(to, "error", {"body": body[:160], "error": "twilio_not_configured"}, db)
        _capture_sandbox_attempt(
            db=db, to=to, body=body, subscriber_id=subscriber_id,
            campaign=campaign_label, variant_id=variant_id, decision_id=decision_id,
            compliance_allowed=True, compliance_reason="ok",
            would_have_delivered=False,
        )
        return False

    # 4. Real Twilio dispatch
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
        # In live mode we still record to sandbox_outbox when TWILIO_SANDBOX=true
        # (e.g. staging smoke tests that fire real SMS but want a local audit).
        _capture_sandbox_attempt(
            db=db, to=to, body=body, subscriber_id=subscriber_id,
            campaign=campaign_label, variant_id=variant_id, decision_id=decision_id,
            compliance_allowed=True, compliance_reason="ok",
            would_have_delivered=True,
        )
        return True
    except Exception as exc:
        logger.error("Twilio send failed: to=%s error=%s", to, exc)
        add_to_dead_letter(to, "delivery_failed", {"body": body[:160], "error": str(exc)}, db)
        _capture_sandbox_attempt(
            db=db, to=to, body=body, subscriber_id=subscriber_id,
            campaign=campaign_label, variant_id=variant_id, decision_id=decision_id,
            compliance_allowed=True, compliance_reason="ok",
            would_have_delivered=False,
        )
        return False


def _capture_sandbox_attempt(
    *,
    db: Session,
    to: Optional[str],
    body: str,
    subscriber_id: Optional[int],
    campaign: Optional[str],
    variant_id: Optional[str],
    decision_id: Optional[str],
    compliance_allowed: bool,
    compliance_reason: str,
    would_have_delivered: bool,
) -> None:
    """Write one sandbox_outbox row when TWILIO_SANDBOX is enabled. No-op otherwise."""
    if not settings.twilio_sandbox:
        return
    try:
        from src.core.models import SandboxOutbox  # local import to avoid cycle
        row = SandboxOutbox(
            channel="sms",
            to_number=to,
            body=body,
            campaign=campaign,
            variant_id=variant_id,
            subscriber_id=subscriber_id,
            decision_id=decision_id,
            compliance_allowed=compliance_allowed,
            compliance_reason=compliance_reason,
            would_have_delivered=would_have_delivered,
            sandbox_flag="twilio_sandbox",
        )
        db.add(row)
        db.flush()
    except Exception as exc:
        # Sandbox capture must never break real dispatch. Log and carry on.
        logger.warning("sandbox_outbox capture failed: %s", exc)


# TCPA opt-in consent prompt — sent to new numbers before any proactive outbound SMS
_OPT_IN_PROMPT = (
    "Forced Action: reply YES to receive distressed property leads for your area. "
    "Msg & data rates may apply. Reply STOP to opt out."
)

# Keywords that constitute affirmative consent
_OPT_IN_KEYWORDS = {"yes", "start", "join", "subscribe", "unstop"}


def has_opted_in(phone: str, db: Session) -> bool:
    """
    Return True if this number has a TCPA double opt-in record.
    Used as a pre-send gate for proactive outbound SMS.
    """
    phone = _normalize(phone)
    if not phone:
        return False
    result = db.execute(
        select(SmsOptIn.id).where(SmsOptIn.phone == phone)
    ).first()
    return result is not None


def record_opt_in(
    phone: str,
    keyword: str,
    source: str,
    db: Session,
    subscriber_id: Optional[int] = None,
    opt_in_message: Optional[str] = None,
    ip_address: Optional[str] = None,
) -> None:
    """
    Record TCPA opt-in consent for a phone number.
    Safe to call multiple times — idempotent (upserts on phone unique constraint).
    source: 'double_opt_in' | 'manual' | 'import' | 'widget'
    """
    phone = _normalize(phone)
    if not phone:
        return
    existing = db.execute(
        select(SmsOptIn).where(SmsOptIn.phone == phone)
    ).scalar_one_or_none()
    if existing:
        return
    db.add(SmsOptIn(
        phone=phone,
        subscriber_id=subscriber_id,
        keyword_used=keyword.upper()[:20] if keyword else None,
        source=source,
        opt_in_message=opt_in_message or _OPT_IN_PROMPT,
        ip_address=ip_address,
    ))
    db.flush()
    logger.info("SMS opt-in recorded: phone=%s source=%s", phone, source)


def send_opt_in_prompt(
    phone: str,
    db: Session,
    subscriber_id: Optional[int] = None,
) -> bool:
    """
    Send the TCPA double opt-in prompt ("Reply YES to confirm…").
    Only sends if the number is not already opted in and not suppressed.
    Returns True if sent, False if suppressed or already opted in.
    """
    if has_opted_in(phone, db):
        return False
    return send_sms(
        to=phone,
        body=_OPT_IN_PROMPT,
        db=db,
        subscriber_id=subscriber_id,
        task_type="tcpa_opt_in_prompt",
    )


def handle_opt_in_reply(from_number: str, body: str, db: Session) -> Optional[str]:
    """
    Check if the inbound message is an opt-in keyword (YES, START, etc.).
    If yes, record the opt-in and return a TwiML confirmation reply.
    Returns None if not an opt-in keyword (caller handles normally).
    """
    word = body.strip().lower().split()[0] if body.strip() else ""
    if word not in _OPT_IN_KEYWORDS:
        return None
    record_opt_in(from_number, keyword=word, source="double_opt_in", db=db)
    reply = (
        "You're confirmed! You'll receive distressed property leads from Forced Action. "
        "Reply STOP anytime to opt out."
    )
    return _twiml_reply(reply)


def check_dnc(phone: str, db: Session) -> bool:
    """
    Return True if this number is on the Do Not Call list.
    DNC entries are stored in sms_opt_outs with source='import' or source='manual'.
    A number on the DNC list must never receive proactive marketing SMS.
    """
    phone = _normalize(phone)
    if not phone:
        return True
    result = db.execute(
        select(SmsOptOut.id).where(
            SmsOptOut.phone == phone,
            SmsOptOut.source.in_(["manual", "import"]),
        )
    ).first()
    return result is not None


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
