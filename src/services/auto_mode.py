"""
Auto Mode — functional stub.

Full implementation requires LangGraph (Phase 2B-2).
queue_action() logs intent only; no actual automation runs locally.
"""

import logging

from sqlalchemy.orm import Session

from src.core.models import Subscriber

logger = logging.getLogger(__name__)


def is_enabled(subscriber_id: int, db: Session) -> bool:
    sub = db.get(Subscriber, subscriber_id)
    return bool(sub and sub.auto_mode_enabled)


def toggle(subscriber_id: int, enabled: bool, db: Session) -> bool:
    sub = db.get(Subscriber, subscriber_id)
    if not sub:
        return False
    sub.auto_mode_enabled = enabled
    db.flush()
    logger.info("Auto mode %s for subscriber %d", "enabled" if enabled else "disabled", subscriber_id)
    return enabled


def queue_action(subscriber_id: int, action_type: str, lead_id: int, db: Session) -> None:
    logger.info(
        "Auto mode action queued (stub): subscriber=%d action=%s lead=%d — full impl in 2B-2",
        subscriber_id, action_type, lead_id,
    )
