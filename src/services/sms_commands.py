"""
SMS product command dispatcher — handles keyword commands from subscribers via SMS.

parse() detects a command from inbound SMS body.
dispatch() routes to the appropriate handler and returns a reply string.
Caller (main.py /webhooks/twilio/inbound) is responsible for sending the reply.
"""

import json as _json
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
    # fa016 Accelerated Wallet Push
    "WALLET", "NO", "PASS",
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

    from src.services.segmentation_engine import reclassify_safe
    reclassify_safe(sub.id, db)

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
        "WALLET": _handle_wallet,
        "NO": _handle_no,
        "PASS": _handle_no,
    }
    handler = handlers.get(command)
    if not handler:
        return "Unknown command. Reply HELP for options."
    return handler(sub, db)


def _find_subscriber(phone: str, db: Session) -> Optional[Subscriber]:
    """Look up a Subscriber by inbound phone number.

    Resolution order (TCPA-aware):
      1. `SmsOptIn` — canonical opt-in record. Only consenting users return here.
      2. `Subscriber.phone` (fa016) — direct E.164 column; covers subscribers
         created via missed-call / Cora SMS / DBPR email flows that set phone
         on the row but may not have an explicit SmsOptIn yet (transactional).
    """
    if not phone:
        return None
    canonical = _normalize_phone(phone) or phone.strip()

    row = db.execute(
        select(SmsOptIn)
        .where(SmsOptIn.phone == canonical)
        .order_by(SmsOptIn.opted_in_at.desc())
        .limit(1)
    ).scalar_one_or_none()
    if row is not None and row.subscriber_id is not None:
        sub = db.execute(
            select(Subscriber).where(Subscriber.id == row.subscriber_id)
        ).scalar_one_or_none()
        if sub is not None:
            return sub

    return db.execute(
        select(Subscriber).where(Subscriber.phone == canonical)
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
    try:
        toggle(sub.id, True, db)
        return "Auto Mode ON. Cora will act on your behalf within your settings."[:_MAX_SMS_LEN]
    except PermissionError as exc:
        return (
            f"{exc}. Visit your dashboard Settings to upgrade your wallet "
            "or purchase the Auto Mode add-on."
        )[:_MAX_SMS_LEN]


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


def _pending_offer(sub_id: int) -> Optional[dict]:
    from src.core.redis_client import redis_available, rget
    if not redis_available():
        return None
    raw = rget(f"fa:pending_offer:{sub_id}")
    if not raw:
        return None
    try:
        return _json.loads(raw)
    except Exception:
        return None


def _clear_pending_offer(sub_id: int) -> None:
    from src.core.redis_client import redis_available, rdelete
    if redis_available():
        rdelete(f"fa:pending_offer:{sub_id}")


def _handle_yes(sub: Subscriber, db: Session) -> str:
    """Context-aware YES: routes to pending offer checkout or falls back to pause."""
    offer = _pending_offer(sub.id)
    if offer:
        otype = offer.get("type")
        if otype == "lock_close":
            _clear_pending_offer(sub.id)
            return _open_checkout_for_lock(sub, offer.get("zip_code", ""))
        if otype == "wallet_push":
            return _accept_wallet_offer(sub, offer, db)
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
    """TOPUP is context-aware: when a wallet_push offer is pending, treat it
    as acceptance (per spec). Otherwise return the standard top-up URL."""
    offer = _pending_offer(sub.id)
    if offer and offer.get("type") == "wallet_push":
        return _accept_wallet_offer(sub, offer, db)
    from config.settings import settings
    url = f"{settings.app_base_url}/wallet?uuid={sub.event_feed_uuid}"
    return f"Top up your wallet here: {url}"[:_MAX_SMS_LEN]


def _handle_report(sub: Subscriber, db: Session) -> str:
    from config.settings import settings
    url = f"{settings.app_base_url}/deal-capture?uuid={sub.event_feed_uuid}"
    return f"Report a deal win: {url}"[:_MAX_SMS_LEN]


def _handle_yearly(sub: Subscriber, db: Session) -> str:
    """SMS YEARLY now actually performs the switch (Phase 2B v9).

    Old behavior: reply with a dashboard link to self-serve.
    New behavior: treat YEARLY as direct acceptance of the annual offer.
    Maps the three failure modes (already annual / billing blocked / Stripe
    error) to clear SMS replies the user can act on.
    """
    from config.settings import settings
    from src.services.stripe_service import can_switch_subscription
    from src.tasks.annual_push import switch_to_annual

    if sub.tier == "annual_lock":
        return "You're already on the annual plan. Reply BALANCE for credits."[:_MAX_SMS_LEN]

    ok, reason = can_switch_subscription(sub)
    if not ok:
        portal_url = f"{settings.app_base_url}/dashboard/{sub.event_feed_uuid}/settings"
        return (
            f"Annual switch blocked: status={reason}. Update billing first: {portal_url}"
        )[:_MAX_SMS_LEN]

    if switch_to_annual(sub.id, db):
        return (
            "Locked in! You're now on the annual plan ($1,970/yr). "
            "Confirmation email on the way."
        )[:_MAX_SMS_LEN]

    # Stripe call failed — give them the manual route.
    dashboard_url = f"{settings.app_base_url}/dashboard/{sub.event_feed_uuid}/settings"
    return (
        f"Could not switch to annual right now. Try again from your dashboard: {dashboard_url}"
    )[:_MAX_SMS_LEN]


def _handle_save_card(sub: Subscriber, db: Session) -> str:
    from config.settings import settings
    url = f"{settings.app_base_url}/save-card?uuid={sub.event_feed_uuid}"
    return f"Save your card for faster checkout: {url}"[:_MAX_SMS_LEN]


# ─────────────────────────────────────────────────────────────────────────────
# fa016 Accelerated Wallet Push — WALLET / NO / PASS
# ─────────────────────────────────────────────────────────────────────────────

def _handle_wallet(sub: Subscriber, db: Session) -> str:
    offer = _pending_offer(sub.id)
    if offer and offer.get("type") == "wallet_push":
        return _accept_wallet_offer(sub, offer, db)
    return _handle_balance(sub, db)


def _accept_wallet_offer(sub: Subscriber, offer: dict, db: Session) -> str:
    from datetime import datetime, timezone
    from src.core.models import WalletPushOffer
    from src.services import wallet_engine

    tier = offer.get("tier") or "starter_wallet"
    offer_id = offer.get("offer_id")
    row: Optional[WalletPushOffer] = None
    if offer_id:
        try:
            row = db.get(WalletPushOffer, int(offer_id))
        except (TypeError, ValueError):
            row = None

    try:
        result = wallet_engine.activate_via_saved_card(
            subscriber_id=sub.id, tier=tier, db=db, offer_id=int(offer_id or 0)
        )
    except ValueError as exc:
        logger.warning("activate_via_saved_card refused sub=%s: %s", sub.id, exc)
        from config.settings import settings
        url = f"{settings.app_base_url}/save-card?uuid={sub.event_feed_uuid}"
        return f"Couldn't activate wallet. Save a card: {url}"[:_MAX_SMS_LEN]
    except Exception:
        logger.error("activate_via_saved_card error sub=%s", sub.id, exc_info=True)
        from config.settings import settings
        url = f"{settings.app_base_url}/wallet?uuid={sub.event_feed_uuid}"
        return f"Activation issue. Open: {url}"[:_MAX_SMS_LEN]

    requires_action = bool(result.get("requires_action"))
    sub_id = result.get("subscription_id")

    if row is not None:
        row.accepted_at = datetime.now(timezone.utc)
        if sub_id:
            row.stripe_subscription_id = sub_id
        if result.get("status") == "failed":
            row.status = "failed"
        elif row.status == "offered":
            row.status = "accepted"
        db.flush()

    _clear_pending_offer(sub.id)

    if requires_action or not sub_id:
        from config.settings import settings
        url = f"{settings.app_base_url}/dashboard/{sub.event_feed_uuid}?wallet_offer=accept"
        return f"Need to verify card. Open: {url}"[:_MAX_SMS_LEN]

    return "Wallet activating. Credits land in ~1 min."[:_MAX_SMS_LEN]


def _handle_no(sub: Subscriber, db: Session) -> str:
    """NO / PASS — opt out of wallet pushes for this subscriber."""
    from datetime import datetime, timezone
    from src.core.models import WalletPushOffer

    offer = _pending_offer(sub.id)
    if not offer or offer.get("type") != "wallet_push":
        # Fall through to nothing-specific — return a polite default
        return "Got it. Reply HELP for options."[:_MAX_SMS_LEN]

    sub.wallet_opt_out = True
    db.flush()

    offer_id = offer.get("offer_id")
    if offer_id:
        try:
            row = db.get(WalletPushOffer, int(offer_id))
            if row is not None and row.status == "offered":
                row.status = "declined"
                row.declined_at = datetime.now(timezone.utc)
                db.flush()
        except (TypeError, ValueError):
            pass

    _clear_pending_offer(sub.id)
    return "No problem, we won't ask again about Wallet."[:_MAX_SMS_LEN]
