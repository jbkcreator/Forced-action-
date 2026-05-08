"""
Save / Pause 60-day flow — Phase B.

pause_subscriber()  — sets Stripe pause_collection + updates DB status
resume_subscriber() — clears pause, restores billing
"""
import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy.orm import Session

from src.core.models import Subscriber

logger = logging.getLogger(__name__)

_PAUSE_DAYS = 60


def pause_subscriber(db: Session, subscriber_id: int, days: int = _PAUSE_DAYS) -> bool:
    """
    Pause an active subscriber for `days` days.
    Sets Stripe pause_collection with behavior=void and auto-resume timestamp.
    Returns True on success, False if subscriber not eligible.
    """
    sub = db.get(Subscriber, subscriber_id)
    if not sub:
        logger.error("pause_subscriber: subscriber %d not found", subscriber_id)
        return False
    if sub.status != "active":
        logger.info("pause_subscriber: sub=%d status=%s — not pausable", subscriber_id, sub.status)
        return False
    if not sub.stripe_subscription_id:
        logger.warning("pause_subscriber: sub=%d has no stripe_subscription_id", subscriber_id)
        return False

    resume_at = datetime.now(timezone.utc) + timedelta(days=days)

    try:
        import stripe as _stripe
        from config.settings import settings as _settings
        _stripe.api_key = _settings.active_stripe_secret_key.get_secret_value()
        _stripe.Subscription.modify(
            sub.stripe_subscription_id,
            pause_collection={"behavior": "void", "resumes_at": int(resume_at.timestamp())},
        )
    except Exception as exc:
        logger.error("pause_subscriber: Stripe call failed sub=%d: %s", subscriber_id, exc)
        return False

    sub.status = "paused"
    sub.paused_at = datetime.now(timezone.utc)
    sub.pause_resume_at = resume_at
    db.flush()

    logger.info("pause_subscriber: sub=%d paused until %s", subscriber_id, resume_at.isoformat())
    return True


def resume_subscriber(db: Session, subscriber_id: int) -> bool:
    """
    Resume a paused subscriber immediately.
    Clears Stripe pause_collection and resets DB status to active.
    Returns True on success.
    """
    sub = db.get(Subscriber, subscriber_id)
    if not sub:
        logger.error("resume_subscriber: subscriber %d not found", subscriber_id)
        return False

    if sub.status != "paused":
        logger.info("resume_subscriber: sub=%d status=%s — not paused", subscriber_id, sub.status)
        return False

    if sub.stripe_subscription_id:
        try:
            import stripe as _stripe
            from config.settings import settings as _settings
            _stripe.api_key = _settings.active_stripe_secret_key.get_secret_value()
            _stripe.Subscription.modify(sub.stripe_subscription_id, pause_collection="")
        except Exception as exc:
            logger.error("resume_subscriber: Stripe call failed sub=%d: %s", subscriber_id, exc)
            return False

    sub.status = "active"
    sub.paused_at = None
    sub.pause_resume_at = None
    db.flush()

    logger.info("resume_subscriber: sub=%d resumed", subscriber_id)
    return True
