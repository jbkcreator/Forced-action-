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
from datetime import datetime, timedelta, timezone
from typing import Optional
from zoneinfo import ZoneInfo

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from config.settings import settings
from src.core.models import SmsDeadLetter, SmsOptIn, SmsOptOut, SmsSendLog
from src.services import phone_utils
from src.services.allotment_engine import consume as allotment_consume
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

# Per-subscriber marketing SMS frequency caps
_MARKETING_CAP_24H = 2   # max successful marketing sends per subscriber in a rolling 24-hour window
_MARKETING_CAP_7D  = 5   # max successful marketing sends per subscriber in a rolling 7-day window

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
    _project_sms_reply(from_number, body, db)
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

    fa037 — also fires a Revenue Signal Score update with
    action_type=ACTION_SMS_OPT_OUT so the score reflects the disengagement
    immediately and a clean audit row lands in revenue_signal_score_events.
    Wrapped in try so a score-write failure cannot block the suppression
    write (TCPA compliance must always win).
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

    # fa037 — Revenue Signal Score hook. Resolve the subscriber via the
    # existing SmsOptIn → Subscriber lookup pattern (mirrors
    # sms_commands._find_subscriber). Best-effort: phones imported from
    # external DNC lists may not map to any subscriber, in which case we
    # skip silently.
    try:
        from src.core.models import SmsOptIn, Subscriber
        row = db.execute(
            select(SmsOptIn)
            .where(SmsOptIn.phone == phone)
            .order_by(SmsOptIn.opted_in_at.desc())
            .limit(1)
        ).scalar_one_or_none()
        sub_id: Optional[int] = None
        if row is not None and row.subscriber_id is not None:
            sub_id = row.subscriber_id
        else:
            sub = db.execute(
                select(Subscriber).where(Subscriber.phone == phone)
            ).scalar_one_or_none()
            if sub is not None:
                sub_id = sub.id
        if sub_id is not None:
            from src.services.segmentation_engine import reclassify_safe
            from src.services.revenue_signal import ACTION_SMS_OPT_OUT
            reclassify_safe(
                sub_id, db,
                action_type=ACTION_SMS_OPT_OUT,
                metadata={"source": source, "keyword": keyword.upper()[:20]},
            )
    except Exception:
        logger.warning(
            "record_opt_out: revenue signal update failed for phone=%s",
            phone, exc_info=True,
        )

    try:
        from src.services.subscriber_memory import append_memory_event

        sub_id = _resolve_subscriber_id_for_memory(phone, db)
        if sub_id is not None:
            append_memory_event(
                db,
                subscriber_id=sub_id,
                stream_source="SMS",
                event_type="sms_opt_out",
                source_event_id=f"sms_opt_out:{phone}:{keyword.upper()[:20]}",
                source_event_name="inbound_sms.stop",
                occurred_at=datetime.now(timezone.utc),
                status="opted_out",
                summary="Subscriber opted out of SMS",
                channel="sms",
                actor={"type": "subscriber", "id": phone},
                raw={"keyword": keyword.upper()[:20], "source": source},
            )
    except Exception:
        logger.warning(
            "record_opt_out: subscriber memory projection failed for phone=%s",
            phone, exc_info=True,
        )


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
    valid_reasons = {
        "opt_out", "delivery_failed", "error", "unresolvable",
        "quiet_hours", "no_opt_in", "subscriber_sms_frequency_cap",
        "do_not_text_tag",
        "prospect_not_contactable", "prospect_sms_consent_withdrawn",
        "free_tier_outbound_text_allotment",
    }
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
    prospect_id: Optional[str] = None,
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
            prospect_id=prospect_id,
        )

    # P1. Prospect contactability gate — only when sending in context of a prospect
    if prospect_id:
        from sqlalchemy import text as sa_text
        _state_row = db.execute(
            sa_text("SELECT contactability_state FROM prospects WHERE prospect_id = CAST(:pid AS uuid)"),
            {"pid": prospect_id},
        ).fetchone()
        if not _state_row or _state_row.contactability_state != "contactable":
            logger.info("SMS suppressed (prospect_not_contactable): prospect_id=%s", prospect_id)
            add_to_dead_letter(to, "prospect_not_contactable", {"body": body[:160], "prospect_id": prospect_id}, db)
            _log("suppressed", suppress_reason="prospect_not_contactable")
            return False

    # P2. Prospect channel consent gate — channel_consent.sms must be explicitly True
    if prospect_id:
        from src.services.prospect_service import get_channel_consent
        if get_channel_consent(db, prospect_id, "sms") is not True:
            logger.info("SMS suppressed (prospect_sms_consent_withdrawn): prospect_id=%s", prospect_id)
            add_to_dead_letter(to, "prospect_sms_consent_withdrawn", {"body": body[:160], "prospect_id": prospect_id}, db)
            _log("suppressed", suppress_reason="prospect_sms_consent_withdrawn")
            return False

    # 0. Unified compliance gate — DNC, opt-out, quiet hours (replaces can_send + is_quiet_hours)
    from src.services.compliance_gator import validate_outbound as _compliance_gate
    _result = _compliance_gate(phone=to, channel="sms", db=db)
    if not _result.allowed:
        _dlq_map = {
            "dnc_or_opted_out": "opt_out",
            "dnc_check_required": "opt_out",
            "quiet_hours": "quiet_hours",
            "invalid_phone": "unresolvable",
        }
        _dlq_key: str = _result.reason or ""
        logger.info("SMS suppressed (%s): to=%s", _result.reason, to)
        add_to_dead_letter(to, _dlq_map.get(_dlq_key, "opt_out"), {"body": body[:160]}, db)
        _log("suppressed", suppress_reason=_dlq_key or _dlq_map.get(_dlq_key, "opt_out"))
        return False

    # 2. Opt-in gate — marketing requires confirmed consent (subscribers only; prospects use P2 above)
    if message_type == "marketing" and not prospect_id and not has_opted_in(to, db):
        logger.info("SMS suppressed (no opt-in): to=%s", to)
        add_to_dead_letter(to, "no_opt_in", {"body": body[:160]}, db)
        _log("suppressed", suppress_reason="no_opt_in")
        return False

    # 2b. do_not_text tag — operator-applied marketing-suppression tag (fa045)
    if message_type == "marketing" and subscriber_id is not None:
        from src.core.models import SubscriberTag
        has_dnt = db.query(SubscriberTag).filter(
            SubscriberTag.subscriber_id == subscriber_id,
            SubscriberTag.tag == "do_not_text",
        ).first() is not None
        if has_dnt:
            logger.info("SMS suppressed (do_not_text tag): subscriber_id=%s", subscriber_id)
            add_to_dead_letter(to, "do_not_text_tag", {"body": body[:160]}, db)
            _log("suppressed", suppress_reason="do_not_text_tag")
            return False

    # 3. Per-subscriber marketing frequency cap and free-tier weekly allotment.
    # Marketing without a subscriber_id (and not a prospect-targeted send) would
    # bypass the allotment gate entirely. Fail closed rather than silently skip.
    if message_type == "marketing" and subscriber_id is None and not prospect_id:
        logger.warning("SMS suppressed (marketing_requires_subscriber_id): to=%s", to)
        add_to_dead_letter(to, "error", {"body": body[:160], "error": "marketing_without_subscriber_id"}, db)
        _log("suppressed", suppress_reason="marketing_requires_subscriber_id")
        return False

    if message_type == "marketing" and subscriber_id is not None:
        if _check_marketing_frequency_cap(subscriber_id, db):
            logger.info(
                "SMS suppressed (subscriber_sms_frequency_cap): subscriber_id=%s to=%s",
                subscriber_id, to,
            )
            add_to_dead_letter(to, "subscriber_sms_frequency_cap", {"body": body[:160]}, db)
            _log("suppressed", suppress_reason="subscriber_sms_frequency_cap")
            return False

        if not allotment_consume(subscriber_id, "outbound_text", db):
            logger.info(
                "SMS suppressed (free_tier_outbound_text_allotment): subscriber_id=%s to=%s",
                subscriber_id, to,
            )
            add_to_dead_letter(to, "free_tier_outbound_text_allotment", {"body": body[:160]}, db)
            _log("suppressed", suppress_reason="free_tier_outbound_text_allotment")
            return False

    # 6. Dry-run path (TELNYX_SMS_ENABLED=false)
    if not settings.telnyx_sms_enabled:
        logger.info("[DRY RUN] SMS to=%s body=%r", to, body[:160])
        _log("dry_run")
        return True

    # 7. Telnyx misconfiguration — live mode but creds missing
    if not all([
        settings.telnyx_sms_api_key,
        settings.telnyx_from_number,
        settings.telnyx_messaging_profile_id,
    ]):
        logger.error("Telnyx not configured — cannot send SMS to %s", to)
        add_to_dead_letter(to, "error", {"body": body[:160], "error": "telnyx_not_configured"}, db)
        _log("failed", suppress_reason="error")
        return False

    # 8. Real Telnyx dispatch
    try:
        result = telnyx_send_message(to=to, body=body)
        vendor_message_id = result.get("message_id")
        logger.info("SMS sent: id=%s to=%s status=%s", vendor_message_id, to, result.get("status"))
        _log("sent", vendor_message_id=vendor_message_id)
        return True
    except TelnyxSMSError as exc:
        logger.error("Telnyx send failed: to=%s error=%s", to, exc)
        add_to_dead_letter(to, "delivery_failed", {"body": body[:160], "error": str(exc)}, db)
        _log("failed")
        return False
    except Exception as exc:
        logger.exception("Unexpected SMS send failure: to=%s error=%s", to, exc)
        add_to_dead_letter(to, "delivery_failed", {"body": body[:160], "error": str(exc)}, db)
        _log("failed")
        return False


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


def _resolve_subscriber_id_for_memory(phone: str, db: Session) -> Optional[int]:
    row = db.execute(
        select(SmsOptIn)
        .where(SmsOptIn.phone == phone)
        .order_by(SmsOptIn.opted_in_at.desc())
        .limit(1)
    ).scalar_one_or_none()
    if row is not None and row.subscriber_id is not None:
        return row.subscriber_id

    from src.core.models import Subscriber

    sub = db.execute(
        select(Subscriber).where(Subscriber.phone == phone)
    ).scalar_one_or_none()
    return sub.id if sub is not None else None


def _project_sms_reply(phone: str, body: str, db: Session) -> None:
    phone = _normalize(phone)
    if not phone:
        return

    try:
        from src.services.subscriber_memory import append_memory_event

        sub_id = _resolve_subscriber_id_for_memory(phone, db)
        if sub_id is None:
            return

        append_memory_event(
            db,
            subscriber_id=sub_id,
            stream_source="SMS",
            event_type="sms_replied",
            source_event_id=f"sms_reply:{phone}:{body.strip()[:80]}",
            source_event_name="inbound_sms.reply",
            occurred_at=datetime.now(timezone.utc),
            status="replied",
            summary="Subscriber replied to SMS",
            channel="sms",
            actor={"type": "subscriber", "id": phone},
            raw={"body": body},
        )
    except Exception:
        logger.warning(
            "sms reply memory projection failed for phone=%s",
            phone, exc_info=True,
        )


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


def _check_marketing_frequency_cap(subscriber_id: int, db: Session) -> bool:
    """
    Return True (blocked) if the subscriber has hit the marketing SMS frequency cap.

    Counts rows in SmsSendLog where outcome IN ('sent', 'dry_run') and
    message_type = 'marketing' within the rolling 24-hour and 7-day windows.
    Dry-run sends are counted so the cap holds in staging environments too.

    Caps (per _MARKETING_CAP_24H / _MARKETING_CAP_7D):
        24h window: 2 sends max
        7d window:  5 sends max
    """
    now = datetime.now(timezone.utc)
    cutoff_24h = now - timedelta(hours=24)
    cutoff_7d = now - timedelta(days=7)

    count_24h = db.execute(
        select(func.count()).select_from(SmsSendLog).where(
            SmsSendLog.subscriber_id == subscriber_id,
            SmsSendLog.message_type == "marketing",
            SmsSendLog.outcome.in_(["sent", "dry_run"]),
            SmsSendLog.created_at >= cutoff_24h,
        )
    ).scalar() or 0

    if count_24h >= _MARKETING_CAP_24H:
        logger.debug(
            "marketing_frequency_cap 24h: subscriber_id=%s count=%d cap=%d",
            subscriber_id, count_24h, _MARKETING_CAP_24H,
        )
        return True

    count_7d = db.execute(
        select(func.count()).select_from(SmsSendLog).where(
            SmsSendLog.subscriber_id == subscriber_id,
            SmsSendLog.message_type == "marketing",
            SmsSendLog.outcome.in_(["sent", "dry_run"]),
            SmsSendLog.created_at >= cutoff_7d,
        )
    ).scalar() or 0

    if count_7d >= _MARKETING_CAP_7D:
        logger.debug(
            "marketing_frequency_cap 7d: subscriber_id=%s count=%d cap=%d",
            subscriber_id, count_7d, _MARKETING_CAP_7D,
        )
        return True

    return False
