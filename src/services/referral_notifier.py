"""
Referral notification worker — async Pub/Sub notifications for the Referral Core Loop.

publish(payload)        — call from the Stripe webhook hot path to fire-and-forget.
subscribe_and_send()    — long-running worker loop; run via `python -m src.services.referral_notifier`.

Delivery is best-effort: if no subscriber is connected at publish time the
message is lost (raw Pub/Sub semantics). A SETNX dedup lock prevents
double-delivery when multiple worker replicas are running.

Both SMS and email are composed by Lifecycle (Claude) for each milestone event.
Static templates are kept as fallbacks if Claude is unavailable.
"""

import json
import logging
import sys
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy.orm import Session

from src.core.database import Database
from src.core.models import Subscriber
from src.core.redis_client import _get_client, redis_available
from src.services.claude_router import call_claude_with_usage
from src.services.email import send_email
from src.services.sms_compliance import can_send, send_sms
from src.utils.prompt_loader import get_prompt

logger = logging.getLogger(__name__)

CHANNEL = "referral.notifications"
DEDUP_LOCK_TTL = 60  # seconds

# Static SMS fallbacks — used when Claude composition fails
_SMS_FALLBACKS = {
    "per_referral":   "Referral confirmed! +5 credits. {n_total} total referral{s}.",
    "free_month_3":   "3 referrals = free month! Next invoice on us.",
    "lock_slot_5":    "5 referrals = bonus ZIP slot unlocked. Claim it: {share_url}",
}

# Static email fallbacks — used when Claude composition fails
_EMAIL_FALLBACKS = {
    "per_referral": (
        "Referral confirmed — +5 credits",
        "Hi {name},\n\nYour referral was confirmed. +5 lead credits have been added to your account.\n\nKeep sharing — every referral counts.\n\n— Forced Action Team",
    ),
    "free_month_3": (
        "Your next month is on us",
        "Hi {name},\n\n3 referrals confirmed. Your next billing cycle is free — no charge.\n\nYou're 2 referrals away from unlocking a bonus ZIP territory slot.\n\n— Forced Action Team",
    ),
    "lock_slot_5": (
        "Bonus ZIP slot unlocked — claim it",
        "Hi {name},\n\n5 referrals confirmed. You've unlocked a bonus ZIP territory slot.\n\nClaim it here:\n{share_url}\n\n— Forced Action Team",
    ),
}


def publish(payload: dict) -> None:
    """
    Publish a notification event to the Redis referral.notifications channel.
    Silently no-ops when Redis is unavailable (best-effort delivery).
    """
    if not redis_available():
        logger.debug("[ReferralNotifier] Redis unavailable; skipping publish for %s", payload.get("type"))
        return
    client = _get_client()
    try:
        client.publish(CHANNEL, json.dumps(payload))
    except Exception as exc:
        logger.warning("[ReferralNotifier] publish failed: %s", exc)


def _compose_sms(msg_type: str, payload: dict, name: str, db=None) -> str:
    """Return Lifecycle-composed SMS copy, falling back to static template on any failure."""
    n = payload.get("n_total", 0)
    share_url = payload.get("share_url", "")

    try:
        system_prompt = get_prompt("referral_milestone_sms.yaml", f"{msg_type}.system")
        user_prompt = get_prompt(
            "referral_milestone_sms.yaml", f"{msg_type}.user",
            name=name, n_total=n, share_url=share_url,
        )
        result = call_claude_with_usage(
            task_type="referral_milestone_sms",
            messages=[{"role": "user", "content": user_prompt}],
            system=system_prompt,
            max_tokens=100,
            db=db,
        )
        text = result["text"].strip()
        if text and len(text) <= 160:
            return text
        logger.warning("[ReferralNotifier] SMS too long (%d chars), using fallback", len(text))
    except Exception as exc:
        logger.warning("[ReferralNotifier] SMS composition failed for %s: %s", msg_type, exc)

    template = _SMS_FALLBACKS.get(msg_type, "")
    return template.format(n_total=n, s="" if n == 1 else "s", share_url=share_url)


def _compose_email(msg_type: str, payload: dict, name: str, db=None) -> tuple[str, str]:
    """
    Return (subject, body) Lifecycle-composed email copy.
    Falls back to static template on any failure. Returns ('', '') if msg_type unknown.
    """
    if msg_type not in _EMAIL_FALLBACKS:
        return "", ""

    n = payload.get("n_total", 0)
    share_url = payload.get("share_url", "")

    try:
        system_prompt = get_prompt("emails/referral_milestone_email.yaml", f"{msg_type}.system")
        user_prompt = get_prompt(
            "emails/referral_milestone_email.yaml", f"{msg_type}.user",
            name=name, n_total=n, share_url=share_url,
        )
        result = call_claude_with_usage(
            task_type="referral_milestone_email",
            messages=[{"role": "user", "content": user_prompt}],
            system=system_prompt,
            max_tokens=500,
            db=db,
        )
        subject, body = _parse_email(result["text"])
        if subject and body:
            return subject, body
        logger.warning("[ReferralNotifier] Email parse failed for %s, using fallback", msg_type)
    except Exception as exc:
        logger.warning("[ReferralNotifier] Email composition failed for %s: %s", msg_type, exc)

    fallback_subject, fallback_body_tpl = _EMAIL_FALLBACKS[msg_type]
    return fallback_subject, fallback_body_tpl.format(name=name, share_url=share_url)


def _parse_email(text: str) -> tuple[str, str]:
    lines = text.strip().splitlines()
    subject = next((l.replace("SUBJECT:", "").strip() for l in lines if l.startswith("SUBJECT:")), "")
    body_start = next((i for i, l in enumerate(lines) if l.startswith("BODY:")), None)
    body = "\n".join(lines[body_start + 1:]).strip() if body_start is not None else ""
    if not subject or len(subject) > 60 or not body:
        return "", ""
    return subject, body


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
        from sqlalchemy import update
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
    Long-running worker. Subscribes to CHANNEL and sends SMS + email for each message.
    Run as: python -m src.services.referral_notifier
    """
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

        msg_type = payload.get("type")
        if msg_type not in _SMS_FALLBACKS:
            continue

        if not _acquire_dedup_lock(client, payload):
            logger.debug("[ReferralNotifier] dedup lock lost for %s:%s — skipping",
                         msg_type, payload.get("event_id"))
            continue

        referrer_id = payload.get("referrer_id")
        if not referrer_id:
            continue

        try:
            with db_factory.session_scope() as db:
                sub = db.get(Subscriber, referrer_id)
                if not sub:
                    logger.warning("[ReferralNotifier] subscriber %s not found", referrer_id)
                    continue

                name = sub.name or "there"

                # SMS
                phone = getattr(sub, "phone", None)
                if phone and can_send(phone, db):
                    sms_body = _compose_sms(msg_type, payload, name, db=db)
                    send_sms(
                        phone, sms_body, db,
                        message_type="transactional",
                        subscriber_id=referrer_id,
                        task_type="referral_notify",
                    )

                # Email
                if sub.email:
                    subject, body = _compose_email(msg_type, payload, name, db=db)
                    if subject and body:
                        send_email(to=sub.email, subject=subject, body_text=body)
                        logger.info("[ReferralNotifier] email sent type=%s sub=%s", msg_type, referrer_id)

                _stamp_notified_at(payload, db)

        except Exception as exc:
            logger.error("[ReferralNotifier] dispatch failed for referrer=%s: %s", referrer_id, exc)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    subscribe_and_send()
