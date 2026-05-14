"""
Referral notification worker — async Pub/Sub notifications for the Referral Core Loop.

publish(payload)        — call from the Stripe webhook hot path to fire-and-forget.
subscribe_and_send()    — long-running worker loop; run via `python -m src.services.referral_notifier`.

Delivery is best-effort: if no subscriber is connected at publish time the
message is lost (raw Pub/Sub semantics). A SETNX dedup lock prevents
double-delivery when multiple worker replicas are running.

SMS templates are static constants; no Claude in the notification path.
"""

import json
import logging
import sys
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

CHANNEL = "referral.notifications"
DEDUP_LOCK_TTL = 60  # seconds

# Static SMS templates
_TEMPLATES = {
    "per_referral":   "Referral confirmed! +5 credits. {n_total} total referral{s}.",
    "free_month_3":   "3 referrals = free month! Next invoice on us.",
    "lock_slot_5":    "5 referrals = bonus ZIP slot unlocked. Claim it: {share_url}",
}


def publish(payload: dict) -> None:
    """
    Publish a notification event to the Redis referral.notifications channel.
    Silently no-ops when Redis is unavailable (best-effort delivery).
    """
    from src.core.redis_client import redis_available, _get_client
    if not redis_available():
        logger.debug("[ReferralNotifier] Redis unavailable; skipping publish for %s", payload.get("type"))
        return
    client = _get_client()
    try:
        client.publish(CHANNEL, json.dumps(payload))
    except Exception as exc:
        logger.warning("[ReferralNotifier] publish failed: %s", exc)


def _render_sms(payload: dict) -> Optional[str]:
    msg_type = payload.get("type")
    template = _TEMPLATES.get(msg_type)
    if not template:
        return None
    n = payload.get("n_total", 0)
    return template.format(
        n_total=n,
        s="" if n == 1 else "s",
        share_url=payload.get("share_url", ""),
    )


def _acquire_dedup_lock(client, payload: dict) -> bool:
    """Return True if this worker wins the dedup lock for this event."""
    msg_type = payload.get("type", "unknown")
    event_id = payload.get("event_id", "0")
    lock_key = f"referral:notif:{msg_type}:{event_id}"
    return bool(client.set(lock_key, "1", nx=True, ex=DEDUP_LOCK_TTL))


def _stamp_notified_at(payload: dict, db: Session) -> None:
    """Stamp notified_at on the milestone-award row for milestone notifications."""
    msg_type = payload.get("type")
    if msg_type not in ("free_month_3", "lock_slot_5"):
        return
    referrer_id = payload.get("referrer_id")
    if not referrer_id:
        return
    try:
        from sqlalchemy import select, update
        from src.core.models import ReferralMilestoneAward
        db.execute(
            update(ReferralMilestoneAward)
            .where(
                ReferralMilestoneAward.referrer_subscriber_id == referrer_id,
                ReferralMilestoneAward.milestone == msg_type,
                ReferralMilestoneAward.notified_at.is_(None),
            )
            .values(notified_at=datetime.now(timezone.utc))
        )
        db.flush()
    except Exception as exc:
        logger.warning("[ReferralNotifier] failed to stamp notified_at: %s", exc)


def subscribe_and_send() -> None:
    """
    Long-running worker. Subscribes to CHANNEL and sends SMS for each message.
    Run as: python -m src.services.referral_notifier
    """
    from src.core.redis_client import _get_client, redis_available
    from src.core.database import Database
    from src.services.sms_compliance import can_send, send_sms
    from src.core.models import Subscriber

    if not redis_available():
        logger.error("[ReferralNotifier] Redis is not available; worker cannot start.")
        return

    client = _get_client()
    pubsub = client.pubsub()
    pubsub.subscribe(CHANNEL)
    logger.info("[ReferralNotifier] Subscribed to channel '%s'", CHANNEL)

    db_factory = Database()

    for raw in pubsub.listen():
        if raw["type"] != "message":
            continue
        try:
            payload = json.loads(raw["data"])
        except (json.JSONDecodeError, TypeError) as exc:
            logger.warning("[ReferralNotifier] bad message: %s", exc)
            continue

        if not _acquire_dedup_lock(client, payload):
            logger.debug("[ReferralNotifier] dedup lock lost for %s:%s — skipping",
                         payload.get("type"), payload.get("event_id"))
            continue

        sms_body = _render_sms(payload)
        if not sms_body:
            continue

        referrer_id = payload.get("referrer_id")
        if not referrer_id:
            continue

        try:
            with db_factory.session_scope() as db:
                sub = db.get(Subscriber, referrer_id)
                phone = getattr(sub, "phone", None) if sub else None
                if phone and can_send(phone, db):
                    send_sms(phone, sms_body, db, message_type="transactional", subscriber_id=referrer_id, task_type="referral_notify")
                _stamp_notified_at(payload, db)
        except Exception as exc:
            logger.error("[ReferralNotifier] SMS dispatch failed for referrer=%s: %s", referrer_id, exc)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    subscribe_and_send()
