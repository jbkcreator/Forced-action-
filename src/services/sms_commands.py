"""
SMS product command dispatcher — handles keyword commands from subscribers via SMS.

parse() detects a command from inbound SMS body.
dispatch() routes to the appropriate handler and returns a reply string.
Caller (main.py /webhooks/twilio/inbound) is responsible for sending the reply.
"""

import logging
from typing import Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.core.models import Subscriber, SmsOptIn
from src.services.phone_utils import normalize as _normalize_phone

logger = logging.getLogger(__name__)

COMMANDS = {
    "LOCK", "BOOST", "AUTO ON", "AUTO OFF",
    "PAUSE", "YES", "RESUME", "BALANCE", "TOPUP", "REPORT", "YEARLY", "SAVE CARD",
}

_MAX_SMS_LEN = 160


def parse(body: str) -> Optional[str]:
    normalized = body.strip().upper()
    # Check two-word commands first
    first_two = " ".join(normalized.split()[:2])
    if first_two in COMMANDS:
        return first_two
    # Check single-word commands
    first_word = normalized.split()[0] if normalized.split() else ""
    if first_word in COMMANDS:
        return first_word
    return None


def dispatch(from_number: str, command: str, db: Session) -> str:
    sub = _find_subscriber(from_number, db)
    if not sub:
        return "Reply HELP to get started with Forced Action."

    handlers = {
        "BALANCE": _handle_balance,
        "LOCK": _handle_lock,
        "BOOST": _handle_boost,
        "AUTO ON": _handle_auto_on,
        "AUTO OFF": _handle_auto_off,
        "PAUSE": _handle_pause,
        "YES": _handle_yes,
        "RESUME": _handle_resume,
        "TOPUP": _handle_topup,
        "REPORT": _handle_report,
        "YEARLY": _handle_yearly,
        "SAVE CARD": _handle_save_card,
    }
    handler = handlers.get(command)
    if not handler:
        return "Unknown command. Reply HELP for options."
    return handler(sub, db)


def _find_subscriber(phone: str, db: Session) -> Optional[Subscriber]:
    canonical = _normalize_phone(phone) or phone
    row = db.execute(
        select(SmsOptIn)
        .where(SmsOptIn.phone == canonical)
        .order_by(SmsOptIn.opted_in_at.desc())
        .limit(1)
    ).scalar_one_or_none()
    if row is None or row.subscriber_id is None:
        return None
    return db.execute(
        select(Subscriber).where(Subscriber.id == row.subscriber_id)
    ).scalar_one_or_none()


def _handle_balance(sub: Subscriber, db: Session) -> str:
    from src.services.wallet_engine import get_balance
    balance = get_balance(sub.id, db)
    return f"Wallet balance: {balance} credits. Reply TOPUP to add more."[:_MAX_SMS_LEN]


def _handle_lock(sub: Subscriber, db: Session) -> str:
    from config.settings import settings
    url = f"{settings.app_base_url}/lock?uuid={sub.event_feed_uuid}"
    return f"Lock your ZIP territory: {url}"[:_MAX_SMS_LEN]


def _handle_boost(sub: Subscriber, db: Session) -> str:
    from config.settings import settings
    url = f"{settings.app_base_url}/bundle/zip_booster?uuid={sub.event_feed_uuid}"
    return f"Get 10 bonus leads in your ZIP: {url}"[:_MAX_SMS_LEN]


def _handle_auto_on(sub: Subscriber, db: Session) -> str:
    from src.services.auto_mode import toggle
    toggle(sub.id, True, db)
    return "Auto Mode ON. Cora will act on your behalf within your settings."[:_MAX_SMS_LEN]


def _handle_auto_off(sub: Subscriber, db: Session) -> str:
    from src.services.auto_mode import toggle
    toggle(sub.id, False, db)
    return "Auto Mode OFF. You're back to manual control."[:_MAX_SMS_LEN]


_PAUSE_PENDING_TTL = 300  # 5 minutes


def _handle_pause(sub: Subscriber, db: Session) -> str:
    from src.core.redis_client import redis_available, rget, rset

    key = f"pause_pending:{sub.id}"

    if redis_available() and rget(key):
        return "You already have a pending pause — reply YES to confirm."[:_MAX_SMS_LEN]

    # First PAUSE — set pending state and ask for confirmation
    if redis_available():
        rset(key, "1", ttl_seconds=_PAUSE_PENDING_TTL)
    return (
        "Reply YES to pause your subscription for 60 days. "
        "Leads will stop and billing pauses. Reply NO to cancel."
    )[:_MAX_SMS_LEN]


def _handle_yes(sub: Subscriber, db: Session) -> str:
    """Context-aware YES: routes to pending offer checkout or falls back to pause."""
    import json as _json
    from src.core.redis_client import redis_available, rget as _rget, rdelete as _rdelete

    if redis_available():
        raw = _rget(f"fa:pending_offer:{sub.id}")
        if raw:
            try:
                offer = _json.loads(raw)
            except Exception:
                offer = None
            if offer and offer.get("type") == "lock_close":
                _rdelete(f"fa:pending_offer:{sub.id}")
                return _open_checkout_for_lock(sub, offer.get("zip_code", ""))
    return _handle_pause_yes(sub, db)


def _open_checkout_for_lock(sub: Subscriber, zip_code: str) -> str:
    """Create a Stripe Checkout Session for annual_lock and SMS-reply the URL."""
    try:
        from src.services.stripe_service import create_checkout_session
        result = create_checkout_session(
            subscriber=sub,
            tier="annual_lock",
            lock_zip=zip_code,
        )
        url = result.get("url") or result.get("checkout_url", "")
        if url:
            return f"Your lock checkout: {url}"[:_MAX_SMS_LEN]
    except Exception as exc:
        logger.error("_open_checkout_for_lock failed sub=%s: %s", sub.id, exc)
    from config.settings import settings
    fallback = f"{settings.app_base_url}/checkout?tier=annual_lock&zip={zip_code}&sub={sub.id}"
    return f"Lock {zip_code}: {fallback}"[:_MAX_SMS_LEN]


def _handle_pause_yes(sub: Subscriber, db: Session) -> str:
    from src.core.redis_client import redis_available, rget, rdelete
    from src.services.pause_subscription import pause_subscriber

    key = f"pause_pending:{sub.id}"
    if not (redis_available() and rget(key)):
        return "No pending pause request. Reply PAUSE to start."[:_MAX_SMS_LEN]

    ok = pause_subscriber(db, sub.id)
    if ok:
        rdelete(key)
        resume_str = sub.pause_resume_at.strftime("%B %d, %Y") if sub.pause_resume_at else "60 days"
        return (
            f"Paused. Billing and leads resume {resume_str}. "
            f"Reply RESUME to restart early."
        )[:_MAX_SMS_LEN]
    return "Pause failed. Contact support."[:_MAX_SMS_LEN]


def _handle_resume(sub: Subscriber, db: Session) -> str:
    from src.services.pause_subscription import resume_subscriber

    if sub.status != "paused":
        return "Your subscription is already active."[:_MAX_SMS_LEN]
    ok = resume_subscriber(db, sub.id)
    if ok:
        return "Subscription resumed! Leads will start flowing again."[:_MAX_SMS_LEN]
    return "Resume failed. Contact support."[:_MAX_SMS_LEN]


def _handle_topup(sub: Subscriber, db: Session) -> str:
    from config.settings import settings
    url = f"{settings.app_base_url}/wallet?uuid={sub.event_feed_uuid}"
    return f"Top up your wallet here: {url}"[:_MAX_SMS_LEN]


def _handle_report(sub: Subscriber, db: Session) -> str:
    from config.settings import settings
    url = f"{settings.app_base_url}/deal-capture?uuid={sub.event_feed_uuid}"
    return f"Report a deal win: {url}"[:_MAX_SMS_LEN]


def _handle_yearly(sub: Subscriber, db: Session) -> str:
    from config.settings import settings
    url = f"{settings.app_base_url}/annual?uuid={sub.event_feed_uuid}"
    return f"Lock in your annual rate ($1,970/yr): {url}"[:_MAX_SMS_LEN]


def _handle_save_card(sub: Subscriber, db: Session) -> str:
    from config.settings import settings
    url = f"{settings.app_base_url}/save-card?uuid={sub.event_feed_uuid}"
    return f"Save your card for faster checkout: {url}"[:_MAX_SMS_LEN]
