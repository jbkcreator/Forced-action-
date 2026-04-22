"""
Referral engine — credit-based referral program.

Referrer receives 20 credits on referee's first purchase.
Referee receives 10 bonus credits on signup via referral code.
"""

import logging
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.core.models import ReferralEvent, Subscriber

logger = logging.getLogger(__name__)

REFERRER_CREDIT = 20
REFEREE_CREDIT = 10


def ensure_referral_code(subscriber_id: int, db: Session) -> str:
    sub = db.get(Subscriber, subscriber_id)
    if not sub:
        raise ValueError(f"Subscriber {subscriber_id} not found")
    if sub.referral_code:
        return sub.referral_code
    # Base36 encoding of subscriber_id, zero-padded to 8 chars
    digits = "0123456789abcdefghijklmnopqrstuvwxyz"
    n = subscriber_id
    code = ""
    while n:
        code = digits[n % 36] + code
        n //= 36
    code = code.zfill(8)
    sub.referral_code = code
    db.flush()
    return code


def process_signup(referee_id: int, referral_code: str, db: Session) -> Optional[ReferralEvent]:
    referrer = db.execute(
        select(Subscriber).where(Subscriber.referral_code == referral_code)
    ).scalar_one_or_none()
    if not referrer:
        logger.warning("process_signup: no subscriber with referral_code=%s", referral_code)
        return None
    if referrer.id == referee_id:
        return None  # can't refer yourself

    event = ReferralEvent(
        referrer_subscriber_id=referrer.id,
        referee_subscriber_id=referee_id,
        referral_code=referral_code,
        status="pending",
        reward_type="credits",
        reward_value=str(REFERRER_CREDIT),
    )
    db.add(event)
    db.flush()

    # Credit referee bonus immediately
    from src.services.wallet_engine import add_bonus
    add_bonus(referee_id, REFEREE_CREDIT, "referral_signup_bonus", db)

    return event


def confirm_purchase(referee_id: int, db: Session) -> Optional[ReferralEvent]:
    event = db.execute(
        select(ReferralEvent).where(
            ReferralEvent.referee_subscriber_id == referee_id,
            ReferralEvent.status == "pending",
        )
    ).scalar_one_or_none()
    if not event:
        return None
    event.status = "confirmed"
    event.confirmed_at = datetime.now(timezone.utc)
    db.flush()
    return event


def reward_referrer(referral_event_id: int, db: Session) -> ReferralEvent:
    event = db.get(ReferralEvent, referral_event_id)
    if not event:
        raise ValueError(f"ReferralEvent {referral_event_id} not found")

    # Use credit() directly — referral reward is a pre-approved amount, not a per-event bonus
    from src.services.wallet_engine import credit
    credit(event.referrer_subscriber_id, REFERRER_CREDIT, "referral_reward", db)

    event.status = "rewarded"
    db.flush()

    # Notify referrer via SMS
    referrer = db.get(Subscriber, event.referrer_subscriber_id)
    if referrer:
        _notify_referrer(referrer, db)

    return event


def _notify_referrer(referrer: Subscriber, db: Session) -> None:
    from src.services.sms_compliance import can_send, send_sms
    phone = getattr(referrer, "phone", None) or getattr(referrer, "email", None)
    if not phone:
        return
    if not can_send(phone, db):
        return
    msg = f"Your referral bonus: {REFERRER_CREDIT} credits added to your wallet. Keep sharing!"
    send_sms(phone, msg, db, subscriber_id=referrer.id, task_type="sms_copy")
