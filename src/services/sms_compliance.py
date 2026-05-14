"""
SMS compliance gate — TCPA / CTIA.

Every outbound SMS must pass through can_send() before hitting the vendor.
Inbound STOP keywords are handled by handle_inbound() and written to sms_opt_outs.

Pre-send flow:
    can_send(phone, db) → False  →  add_to_dead_letter(), do not send
                        → True   →  send via Telnyx

Inbound keyword flow (Telnyx webhook):
    handle_inbound(from_number, body, db)
        → if STOP keyword: record_opt_out(), return TeXML opt-out reply
        → else:            return None (caller handles normal inbound)

Vendor: Telnyx Messaging API (replaced Twilio 2026-05-11 — see plan
mellow-strolling-fairy.md). Send mechanics live one layer down in
src/services/telnyx_sms.py; this file owns only the compliance gate
and the dead-letter queue.
"""

import logging
from datetime import datetime, timezone
from typing import Optional
from zoneinfo import ZoneInfo

from sqlalchemy import select
from sqlalchemy.orm import Session

from config.settings import settings
from src.core.models import SmsDeadLetter, SmsOptIn, SmsOptOut
from src.services import phone_utils
from src.services.telnyx_sms import TelnyxSMSError, send_message as telnyx_send_message

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

# Area code → IANA timezone. Most FL area codes are Eastern.
#
# Panhandle exception: 850 spans both ET (Tallahassee) and CST (Pensacola).
# We map it to America/Chicago because the safe direction is to OVER-suppress
# (treat ET-side 850 numbers as if they were CST → quiet hours start an hour
# earlier than necessary). Mapping to ET would UNDER-suppress for CST numbers
# and risk a TCPA violation between 8pm and 9pm CST.
#
# Extend this dict when the platform expands to other states.
_AREA_CODE_TZ: dict[str, str] = {
    "850": "America/Chicago",   # Panhandle (CST + ET) — conservative CST mapping
    **{
        ac: "America/New_York" for ac in [
            "239", "305", "321", "352", "386", "407", "561", "727",
            "754", "772", "786", "813", "863", "904", "941", "954",
        ]
    },
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
    Checks the sms_opt_outs suppression table.
    Callers must check this before every outbound send.
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
    reason must be one of: opt_out / delivery_failed / error / unresolvable / quiet_hours / no_opt_in
    """
    valid_reasons = {"opt_out", "delivery_failed", "error", "unresolvable", "quiet_hours", "no_opt_in"}
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
    *,
    message_type: str = "marketing",
    subscriber_id: Optional[int] = None,
    task_type: Optional[str] = None,
    campaign: Optional[str] = None,
    variant_id: Optional[str] = None,
    decision_id: Optional[str] = None,
) -> bool:
    """
    Central outbound SMS dispatcher.

    Gate order (TCPA/CTIA): opt-out → opt-in (marketing only) → quiet hours → creds → dispatch.
    Every exit writes one SmsSendLog row (V3) for ops auditing.

    message_type:
      "marketing"     — requires SmsOptIn consent record. Default.
      "transactional" — skips opt-in gate (account events, alerts, receipts).
      "opt_in_prompt" — skips opt-in gate; used only by send_opt_in_prompt.

    Returns True if the message was sent (or logged in dry-run), False if suppressed.
    """
    _VALID_MESSAGE_TYPES = {"marketing", "transactional", "opt_in_prompt"}
    if message_type not in _VALID_MESSAGE_TYPES:
        logger.warning("Invalid message_type '%s' — defaulting to 'marketing'", message_type)
        message_type = "marketing"

    to = _normalize(to)
    campaign_label = campaign or task_type

    def _log(outcome: str, suppress_reason: Optional[str] = None, vendor_message_id: Optional[str] = None) -> None:
        from src.services import sms_send_log
        sms_send_log.log_send(
            db=db,
            phone=to or None,
            subscriber_id=subscriber_id,
            task_type=task_type,
            message_type=message_type,
            outcome=outcome,
            suppress_reason=suppress_reason,
            vendor_message_id=vendor_message_id,
            campaign=campaign_label,
            variant_id=variant_id,
            decision_id=decision_id,
            body_preview=body[:160],
        )

    # 1. Opt-out suppression
    if not can_send(to, db):
        logger.info("SMS suppressed (opt-out): to=%s", to)
        add_to_dead_letter(to, "opt_out", {"body": body[:160]}, db)
        _log("suppressed", suppress_reason="opt_out")
        _capture_sandbox_attempt(
            db=db, to=to, body=body, subscriber_id=subscriber_id,
            campaign=campaign_label, variant_id=variant_id, decision_id=decision_id,
            compliance_allowed=False, compliance_reason="opt_out",
            would_have_delivered=False,
        )
        return False

    # 2. Opt-in gate — marketing requires confirmed consent
    if message_type == "marketing" and not has_opted_in(to, db):
        logger.info("SMS suppressed (no opt-in): to=%s", to)
        add_to_dead_letter(to, "no_opt_in", {"body": body[:160]}, db)
        _log("suppressed", suppress_reason="no_opt_in")
        _capture_sandbox_attempt(
            db=db, to=to, body=body, subscriber_id=subscriber_id,
            campaign=campaign_label, variant_id=variant_id, decision_id=decision_id,
            compliance_allowed=False, compliance_reason="no_opt_in",
            would_have_delivered=False,
        )
        return False

    # 3. TCPA quiet hours — no SMS before 8am or after 9pm recipient local time.
    # Gated behind sms_quiet_hours_enabled so QA + local sandbox runs aren't
    # blocked overnight; default ON in production.
    if settings.sms_quiet_hours_enabled and is_quiet_hours(to):
        logger.info("SMS suppressed (quiet hours): to=%s", to)
        add_to_dead_letter(to, "quiet_hours", {"body": body[:160]}, db)
        _log("suppressed", suppress_reason="quiet_hours")
        _capture_sandbox_attempt(
            db=db, to=to, body=body, subscriber_id=subscriber_id,
            campaign=campaign_label, variant_id=variant_id, decision_id=decision_id,
            compliance_allowed=False, compliance_reason="quiet_hours",
            would_have_delivered=False,
        )
        return False

    # 4. Dry-run path (TELNYX_SMS_ENABLED=false)
    if not settings.telnyx_sms_enabled:
        logger.info("[DRY RUN] SMS to=%s body=%r", to, body[:160])
        _log("dry_run")
        _capture_sandbox_attempt(
            db=db, to=to, body=body, subscriber_id=subscriber_id,
            campaign=campaign_label, variant_id=variant_id, decision_id=decision_id,
            compliance_allowed=True, compliance_reason="ok",
            would_have_delivered=True,
        )
        return True

    # 5. Telnyx misconfiguration — live mode but creds missing
    if not all([
        settings.telnyx_sms_api_key,
        settings.telnyx_from_number,
        settings.telnyx_messaging_profile_id,
    ]):
        logger.error("Telnyx not configured — cannot send SMS to %s", to)
        add_to_dead_letter(to, "error", {"body": body[:160], "error": "telnyx_not_configured"}, db)
        _log("failed", suppress_reason="error")
        _capture_sandbox_attempt(
            db=db, to=to, body=body, subscriber_id=subscriber_id,
            campaign=campaign_label, variant_id=variant_id, decision_id=decision_id,
            compliance_allowed=True, compliance_reason="ok",
            would_have_delivered=False,
        )
        return False

    # 6. Real Telnyx dispatch
    try:
        result = telnyx_send_message(to=to, body=body)
        vendor_message_id = result.get("message_id")
        logger.info("SMS sent: id=%s to=%s status=%s", vendor_message_id, to, result.get("status"))
        _log("sent", vendor_message_id=vendor_message_id)
        _capture_sandbox_attempt(
            db=db, to=to, body=body, subscriber_id=subscriber_id,
            campaign=campaign_label, variant_id=variant_id, decision_id=decision_id,
            compliance_allowed=True, compliance_reason="ok",
            would_have_delivered=True,
        )
        return True
    except TelnyxSMSError as exc:
        logger.error("Telnyx send failed: to=%s error=%s", to, exc)
        add_to_dead_letter(to, "delivery_failed", {"body": body[:160], "error": str(exc)}, db)
        _log("failed")
        _capture_sandbox_attempt(
            db=db, to=to, body=body, subscriber_id=subscriber_id,
            campaign=campaign_label, variant_id=variant_id, decision_id=decision_id,
            compliance_allowed=True, compliance_reason="ok",
            would_have_delivered=False,
        )
        return False
    except Exception as exc:
        logger.exception("Unexpected SMS send failure: to=%s error=%s", to, exc)
        add_to_dead_letter(to, "delivery_failed", {"body": body[:160], "error": str(exc)}, db)
        _log("failed")
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
    """Write one sandbox_outbox row when TELNYX_SANDBOX is enabled. No-op otherwise."""
    if not settings.telnyx_sandbox:
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
            sandbox_flag="telnyx_sandbox",
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
    Sets the opt_in_pending Redis sentinel before sending so a subsequent YES
    reply is treated as valid double opt-in consent (V5).
    Returns True if sent, False if suppressed or already opted in.
    """
    if has_opted_in(phone, db):
        return False
    from src.services import opt_in_sentinel
    normalized = _normalize(phone) or phone
    opt_in_sentinel.mark_pending(normalized)
    sent = send_sms(
        to=phone,
        body=_OPT_IN_PROMPT,
        db=db,
        subscriber_id=subscriber_id,
        task_type="tcpa_opt_in_prompt",
        message_type="opt_in_prompt",
    )
    if not sent:
        # Best-effort: clear sentinel so a stale key can't grant consent later
        opt_in_sentinel.consume_pending(normalized)
    return sent


def handle_opt_in_reply(from_number: str, body: str, db: Session) -> Optional[str]:
    """
    Check if the inbound message is an opt-in keyword (YES, START, etc.).
    Only records consent and returns TwiML when the opt_in_pending Redis
    sentinel is present (set by send_opt_in_prompt within the last 15 min).
    Returns None in all other cases so the caller falls through to sms_commands
    (preserving the PAUSE-confirm YES flow and blocking unsolicited consent).
    """
    word = body.strip().lower().split()[0] if body.strip() else ""
    if word not in _OPT_IN_KEYWORDS:
        return None
    from src.services import opt_in_sentinel
    phone = _normalize(from_number) or from_number
    if not opt_in_sentinel.consume_pending(phone):
        return None  # no prompt was sent — fall through to sms_commands
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
    """Normalize to strict E.164 via phone_utils. Returns '' for invalid/unparseable."""
    return phone_utils.normalize(phone) or ""


def _extract_stop_keyword(body: str) -> Optional[str]:
    """Return the matched STOP keyword if the message body is a STOP command, else None."""
    word = body.strip().lower().split()[0] if body.strip() else ""
    return word if word in _STOP_KEYWORDS else None


def _twiml_reply(message: str) -> str:
    """Minimal TwiML response for Twilio webhook."""
    safe = message.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    return f'<?xml version="1.0" encoding="UTF-8"?><Response><Message>{safe}</Message></Response>'
