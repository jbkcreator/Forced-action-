"""
Referral engine — credit-based referral program.

Reward structure:
  - Referee receives 10 bonus credits on signup via referral code.
  - Referrer receives 5 credits per confirmed referral (every referral).
  - Milestone at 3 confirmed referrals: one-time Stripe free-month coupon.
  - Milestone at 5 confirmed referrals: one-time +1 bonus ZIP lock slot.

Milestones are tracked in referral_milestone_awards (UNIQUE per referrer+milestone)
so all grants are idempotent. Notifications are published async via Redis Pub/Sub
by referral_notifier — no SMS in the webhook hot path.
"""

import logging
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.core.models import ReferralEvent, ReferralTeam, Subscriber, ZipTerritory

logger = logging.getLogger(__name__)

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
        reward_value="5",
    )
    db.add(event)
    db.flush()

    # Credit referee bonus immediately
    from src.services.wallet_engine import add_bonus
    add_bonus(referee_id, REFEREE_CREDIT, "referral_signup_bonus", db)

    return event


def confirm_purchase(referee_id: int, db: Session) -> Optional[ReferralEvent]:
    """
    Called when a referee's first purchase is confirmed (Stripe webhook).

    Orchestrates the full reward flow:
      1. Transition event pending → confirmed.
      2. Grant 5 credits to the referrer.
      3. Evaluate and apply any newly-crossed milestones (free_month, lock_slot).
      4. Publish async notifications via Redis Pub/Sub (best-effort).
      5. Check team-unlock (Stage 5).
    """
    from config.settings import get_settings

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

    referrer_id = event.referrer_subscriber_id

    # Grant per-referral credits
    from src.services.milestone_grants import (
        grant_free_month,
        grant_lock_slot,
        grant_per_referral_credits,
    )
    from src.services.milestone_evaluator import Milestone, evaluate
    from src.services.referral_notifier import publish

    grant_per_referral_credits(referrer_id, event.id, db)

    # Evaluate and fire milestone grants
    settings = get_settings()
    base_url = getattr(settings, "base_url", "")
    referrer = db.get(Subscriber, referrer_id)
    share_url = f"{base_url}/share/{referrer.referral_code}" if referrer and referrer.referral_code else ""

    newly_crossed = evaluate(referrer_id, db)
    for milestone in newly_crossed:
        if milestone == Milestone.FREE_MONTH_3:
            grant_free_month(referrer_id, event.id, db)
        elif milestone == Milestone.LOCK_SLOT_5:
            grant_lock_slot(referrer_id, event.id, db)

    # Count current confirmed referrals for notification copy
    n_total = db.execute(
        select(ReferralEvent).where(
            ReferralEvent.referrer_subscriber_id == referrer_id,
            ReferralEvent.status.in_(("confirmed", "rewarded")),
        )
    ).scalars().all()

    # Publish async notifications (best-effort; never block on failure)
    try:
        publish({
            "type": "per_referral",
            "event_id": event.id,
            "referrer_id": referrer_id,
            "n_total": len(n_total),
            "share_url": share_url,
        })
        for milestone in newly_crossed:
            publish({
                "type": milestone.value,
                "event_id": event.id,
                "referrer_id": referrer_id,
                "n_total": len(n_total),
                "share_url": share_url,
            })
    except Exception as exc:
        logger.warning("[Referral] notification publish failed: %s", exc)

    # Stage 5 — check whether this confirmation completes a 3-person team
    try:
        _check_team_unlock(referrer_id, db)
    except Exception as exc:
        logger.warning("[Referral] team unlock check failed: %s", exc)

    return event


def revoke_referral_event(event_id: int, reason: str, db: Session) -> Optional[ReferralEvent]:
    """
    Mark a referral event as revoked (referee refunded / disputed).
    The referrer keeps any already-granted milestones and per-referral credits.
    The revoked event no longer counts toward milestone thresholds.
    Logs to fraud review via standard logger (future: dedicated table).
    """
    event = db.get(ReferralEvent, event_id)
    if not event:
        return None
    if event.status == "revoked":
        return event  # idempotent

    prior_status = event.status
    event.status = "revoked"
    db.flush()

    logger.warning(
        "[Referral][FRAUD_REVIEW] event=%d revoked from status=%s reason=%s "
        "referrer=%d referee=%d",
        event_id, prior_status, reason,
        event.referrer_subscriber_id, event.referee_subscriber_id,
    )
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
            send_sms(phone, body, db, message_type="transactional", subscriber_id=mid, task_type="referral_team_unlock")
    except Exception as exc:
        logger.warning("[Referral] team unlock SMS failed: %s", exc)

    return team


def revoke_team_for_subscriber(subscriber_id: int, reason: str, db: Session) -> int:
    """
    Break every active ReferralTeam in which subscriber_id appears as lead or member.

    Called when a subscriber refunds, disputes, or confirms churn (NOT during grace).
    Idempotent: already-broken teams are skipped.
    Returns the number of teams actually broken.
    """
    from sqlalchemy import text as _text

    now = datetime.now(timezone.utc)
    reason = reason[:32]

    # Teams where the subscriber is the lead
    lead_teams = db.execute(
        select(ReferralTeam).where(
            ReferralTeam.lead_subscriber_id == subscriber_id,
            ReferralTeam.status == "active",
        )
    ).scalars().all()

    # Teams where the subscriber is a member (ARRAY @> ARRAY[id])
    member_teams = db.execute(
        select(ReferralTeam).where(
            ReferralTeam.status == "active",
            ReferralTeam.lead_subscriber_id != subscriber_id,
            _text(f"member_subscriber_ids @> ARRAY[{subscriber_id}]::integer[]"),
        )
    ).scalars().all()

    all_teams = {t.id: t for t in lead_teams + member_teams}
    for team in all_teams.values():
        team.status = "broken"
        team.broken_at = now
        team.broken_reason = reason

    if all_teams:
        db.flush()
        logger.info(
            "[Referral] revoked %d team(s) for subscriber=%d reason=%s",
            len(all_teams), subscriber_id, reason,
        )

    return len(all_teams)
