"""
Referral engine — credit-based referral program.

Referrer receives 20 credits on referee's first purchase.
Referee receives 10 bonus credits on signup via referral code.

Stage 5 adds team-unlock logic: when a referrer accumulates 3 confirmed
referrals where every member is in the same county + vertical, a
ReferralTeam row is created so all three members get a Shared ZIP View.
"""

import logging
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.core.models import ReferralEvent, ReferralTeam, Subscriber, ZipTerritory

logger = logging.getLogger(__name__)

REFERRER_CREDIT = 20
REFEREE_CREDIT = 10
TEAM_UNLOCK_THRESHOLD = 3   # confirmed referrals in same county + vertical


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

    # Stage 5 — check whether this confirmation completes a 3-person team
    try:
        _check_team_unlock(event.referrer_subscriber_id, db)
    except Exception as exc:
        logger.warning("[Referral] team unlock check failed: %s", exc)

    return event


def _check_team_unlock(referrer_id: int, db: Session) -> Optional[ReferralTeam]:
    """
    If the referrer + 2 confirmed referees all share the same county AND vertical,
    and no active team exists for them yet, create a ReferralTeam row and return it.
    """
    referrer = db.get(Subscriber, referrer_id)
    if not referrer:
        return None

    # Find confirmed referees that match the referrer's county+vertical
    same_cohort = db.execute(
        select(ReferralEvent, Subscriber)
        .join(Subscriber, Subscriber.id == ReferralEvent.referee_subscriber_id)
        .where(
            ReferralEvent.referrer_subscriber_id == referrer_id,
            ReferralEvent.status.in_(("confirmed", "rewarded")),
            Subscriber.county_id == referrer.county_id,
            Subscriber.vertical == referrer.vertical,
            Subscriber.status == "active",
        )
    ).all()

    if len(same_cohort) < (TEAM_UNLOCK_THRESHOLD - 1):
        return None   # need 2+ referees plus the referrer = 3 members

    # Skip if an active team already exists for this referrer
    existing = db.execute(
        select(ReferralTeam).where(
            ReferralTeam.lead_subscriber_id == referrer_id,
            ReferralTeam.county_id == referrer.county_id,
            ReferralTeam.vertical == referrer.vertical,
            ReferralTeam.status == "active",
        )
    ).scalar_one_or_none()
    if existing:
        return existing

    referee_ids = [row.Subscriber.id for row in same_cohort[:TEAM_UNLOCK_THRESHOLD - 1]]
    member_ids = [referrer_id] + referee_ids

    # Build the union of locked ZIPs across the trio
    zips = db.execute(
        select(ZipTerritory.zip_code).where(
            ZipTerritory.subscriber_id.in_(member_ids),
            ZipTerritory.status == "locked",
        )
    ).scalars().all()
    shared_zips = sorted(set(zips))

    team = ReferralTeam(
        lead_subscriber_id=referrer_id,
        county_id=referrer.county_id,
        vertical=referrer.vertical,
        member_subscriber_ids=member_ids,
        shared_zips=shared_zips,
        status="active",
    )
    db.add(team)
    db.flush()
    logger.info(
        "[Referral] team unlocked: id=%d members=%s county=%s vertical=%s",
        team.id, member_ids, referrer.county_id, referrer.vertical,
    )

    # Notify all three members via SMS (best-effort; falls back silently
    # until subscriber.phone column ships)
    try:
        from src.services.sms_compliance import send_sms
        for mid in member_ids:
            sub = db.get(Subscriber, mid)
            phone = getattr(sub, "phone", None) if sub else None
            if not phone:
                continue
            body = (
                f"Team unlocked! 3 of you in {referrer.county_id.title()} "
                f"{referrer.vertical} now share a live ZIP heat map. Open your dashboard."
            )
            send_sms(phone, body, db, subscriber_id=mid, task_type="referral_team_unlock")
    except Exception as exc:
        logger.warning("[Referral] team unlock SMS failed: %s", exc)

    return team


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
