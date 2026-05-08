"""
Retention event producer.

Emits retention_summary_due events for inactive subscribers based on
tier-specific cadences defined in config/retention.py.

Cadences (days of inactivity before event fires):
  wallet:         3 days
  annual_lock:    5 days
  autopilot_lite: 5 days
  autopilot_pro:  7 days

Inactivity = no MessageOutcome.sent_at in the cadence window.

Cron: 0 16 * * * (4 PM UTC daily)

Usage:
    python -m src.tasks.retention_event_producer [--dry-run]
"""
import logging
import sys
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from config.retention import (
    RETENTION_CADENCE_DAYS,
    RETENTION_EXCLUDED_TIERS,
    RETENTION_IDEMPOTENCY_WINDOW,
)
from src.core.database import get_db_context
from src.core.models import MessageOutcome, Subscriber
from src.core.redis_client import redis_available, rget, rset

logger = logging.getLogger(__name__)

# TTL for idempotency key: 25 hours (slightly over 1 day to tolerate cron drift)
_IDEM_TTL_SECONDS = 25 * 3600


def _last_engagement(db: Session, subscriber_id: int) -> Optional[datetime]:
    """Return sent_at of most recent Cora message for this subscriber."""
    row = db.execute(
        select(MessageOutcome.sent_at)
        .where(MessageOutcome.subscriber_id == subscriber_id)
        .order_by(MessageOutcome.sent_at.desc())
        .limit(1)
    ).scalar_one_or_none()
    return row


def _is_deduplicated(subscriber_id: int) -> bool:
    if not redis_available():
        return False
    key = f"retention_idem:{subscriber_id}:{date.today().strftime(RETENTION_IDEMPOTENCY_WINDOW)}"
    return bool(rget(key))


def _mark_deduplicated(subscriber_id: int) -> None:
    if not redis_available():
        return
    key = f"retention_idem:{subscriber_id}:{date.today().strftime(RETENTION_IDEMPOTENCY_WINDOW)}"
    rset(key, "1", ttl_seconds=_IDEM_TTL_SECONDS)


def _emit_event(subscriber_id: int, tier: str, window_days: int) -> None:
    from src.agents.events.types import Event
    from src.agents.supervisor import dispatch_event

    evt = Event(
        event_type="retention_summary_due",
        subscriber_id=subscriber_id,
        payload={"tier": tier, "window_days": window_days},
        source="cron",
        decision_id=str(uuid.uuid4()),
        idempotency_key=f"retention:{subscriber_id}:{date.today().strftime(RETENTION_IDEMPOTENCY_WINDOW)}",
    )
    dispatch_event(evt.to_dispatch_dict())


def run(dry_run: bool = False) -> dict:
    results = {
        "checked": 0,
        "inactive_found": 0,
        "events_emitted": 0,
        "deduped": 0,
        "errors": 0,
    }
    now = datetime.now(timezone.utc)

    with get_db_context() as db:
        for tier, days in RETENTION_CADENCE_DAYS.items():
            subs = db.execute(
                select(Subscriber).where(
                    Subscriber.tier == tier,
                    Subscriber.status == "active",
                )
            ).scalars().all()

            cutoff = now - timedelta(days=days)

            for sub in subs:
                results["checked"] += 1
                try:
                    if _is_deduplicated(sub.id):
                        results["deduped"] += 1
                        continue

                    last = _last_engagement(db, sub.id)
                    ref = last if last else (
                        sub.created_at.replace(tzinfo=timezone.utc)
                        if sub.created_at and sub.created_at.tzinfo is None
                        else sub.created_at
                    )

                    if not ref or ref >= cutoff:
                        continue

                    results["inactive_found"] += 1
                    logger.info(
                        "retention: inactive sub=%s tier=%s last_engagement=%s",
                        sub.id, tier, ref.isoformat() if ref else "never",
                    )

                    if not dry_run:
                        _emit_event(sub.id, tier, days)
                        _mark_deduplicated(sub.id)
                        results["events_emitted"] += 1

                except Exception as exc:
                    logger.error("retention_producer error sub=%s: %s", sub.id, exc)
                    results["errors"] += 1

    logger.info(
        "[RetentionProducer] checked=%d inactive=%d emitted=%d deduped=%d errors=%d dry_run=%s",
        results["checked"],
        results["inactive_found"],
        results["events_emitted"],
        results["deduped"],
        results["errors"],
        dry_run,
    )
    return results


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    dry = "--dry-run" in sys.argv
    print(run(dry_run=dry))
