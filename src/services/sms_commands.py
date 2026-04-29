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

from src.core.models import Subscriber

logger = logging.getLogger(__name__)

COMMANDS = {
    "LOCK", "BOOST", "AUTO ON", "AUTO OFF",
    "PAUSE", "BALANCE", "TOPUP", "REPORT", "YEARLY", "SAVE CARD",
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
    from src.core.models import Owner
    # Try owner lookup first (phone_1 field on Owner)
    owner_match = db.execute(
        select(Owner).where(Owner.phone_1 == phone)
    ).scalar_one_or_none()
    if owner_match:
        # Owner → Property → Subscriber via territory is complex; best effort via email match
        pass
    # Direct subscriber lookup by email not possible without phone field on Subscriber
    # For now return None — will be wired when subscriber.phone field is added in 2B-2
    return None


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


def _handle_pause(sub: Subscriber, db: Session) -> str:
    return "To pause your subscription, visit your dashboard or reply STOP to unsubscribe."[:_MAX_SMS_LEN]


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
