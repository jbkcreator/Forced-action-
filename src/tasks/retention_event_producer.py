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
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from config.churn import FIRE_BANDS, HORIZON_DAYS
from config.retention import (
    RETENTION_CADENCE_DAYS,
    RETENTION_EXCLUDED_TIERS,
    RETENTION_IDEMPOTENCY_WINDOW,
)
from src.core.database import get_db_context
from src.core.models import ChurnPrediction, MessageOutcome, Subscriber, WalletBalance
from src.services.vendor_cost_pause_service import get_active_pause
from src.core.redis_client import redis_available, rget, rset

logger = logging.getLogger(__name__)

# TTL for idempotency key: 25 hours (slightly over 1 day to tolerate cron drift)
_IDEM_TTL_SECONDS = 25 * 3600


def _is_high_churn_risk(db: Session, subscriber_id: int) -> bool:
    """Return True if the latest churn prediction is high/very_high and within horizon.

    When True the churn_scoring job has already flagged this subscriber for a
    proactive save offer — emitting a retention_summary_due would duplicate the
    outreach on the same day. Suppress and count as deferred_to_save.

    Reads the latest churn_predictions row (written for every scored subscriber),
    NOT the user_segments churn columns: those are an UPDATE-only display mirror
    and are absent for subscribers with no segment row (~90%), which would make
    this silently return False and skip suppression.
    """
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)

    pred = db.execute(
        select(ChurnPrediction)
        .where(ChurnPrediction.subscriber_id == subscriber_id)
        .order_by(ChurnPrediction.predicted_at.desc())
        .limit(1)
    ).scalar_one_or_none()

    if pred is None:
        return False
    if pred.churn_risk_band not in FIRE_BANDS:
        return False
    if pred.predicted_inactivity_at is None:
        return False

    predicted = pred.predicted_inactivity_at
    if predicted.tzinfo is None:
        predicted = predicted.replace(tzinfo=timezone.utc)
    days_out = (predicted - now).total_seconds() / 86400
    return days_out <= HORIZON_DAYS


def _last_engagement(db: Session, subscriber_id: int) -> Optional[datetime]:
    """Return sent_at of most recent Lifecycle message for this subscriber."""
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
    key = f"retention_idem:{subscriber_id}:{datetime.now(timezone.utc).date().strftime(RETENTION_IDEMPOTENCY_WINDOW)}"
    return bool(rget(key))


def _mark_deduplicated(subscriber_id: int) -> None:
    if not redis_available():
        return
    key = f"retention_idem:{subscriber_id}:{datetime.now(timezone.utc).date().strftime(RETENTION_IDEMPOTENCY_WINDOW)}"
    rset(key, "1", ttl_seconds=_IDEM_TTL_SECONDS)


def _emit_event(subscriber_id: int, tier: str, window_days: int) -> None:
    from src.agents.events.ingestion import publish_lifecycle_event
    from src.agents.events.types import Event

    evt = Event(
        event_type="retention_summary_due",
        subscriber_id=subscriber_id,
        payload={"tier": tier, "window_days": window_days},
        source="cron",
        decision_id=str(uuid.uuid4()),
        idempotency_key=f"retention:{subscriber_id}:{datetime.now(timezone.utc).date().strftime(RETENTION_IDEMPOTENCY_WINDOW)}",
    )
    try:
        publish_lifecycle_event(evt.to_dispatch_dict())
    except Exception as exc:
        logger.error("retention_event_producer emit failed sub=%s: %s", subscriber_id, exc)


def run(dry_run: bool = False) -> dict:
    results = {
        "checked": 0,
        "inactive_found": 0,
        "events_emitted": 0,
        "deduped": 0,
        "deferred_to_save": 0,
        "errors": 0,
        "skipped_by_pause": False,
    }
    now = datetime.now(timezone.utc)

    with get_db_context() as db:
        if get_active_pause(db, "claude", "retention_event_producer"):
            logger.warning("[RetentionProducer] active vendor cost pause — skipping run")
            results["skipped_by_pause"] = True
            return results

        for tier, days in RETENTION_CADENCE_DAYS.items():
            # "wallet" is not a DB tier — resolve via WalletBalance membership
            if tier == "wallet":
                subs = db.execute(
                    select(Subscriber).where(
                        Subscriber.id.in_(select(WalletBalance.subscriber_id)),
                        Subscriber.status.in_(("active", "past_due")),
                    )
                ).scalars().all()
            else:
                subs = db.execute(
                    select(Subscriber).where(
                        Subscriber.tier == tier,
                        Subscriber.status.in_(("active", "past_due")),
                    )
                ).scalars().all()

            cutoff = now - timedelta(days=days)

            for sub in subs:
                results["checked"] += 1
                try:
                    if _is_deduplicated(sub.id):
                        results["deduped"] += 1
                        continue

                    # Churn suppression guard: save offer takes precedence (ADR 0008)
                    if _is_high_churn_risk(db, sub.id):
                        results["deferred_to_save"] += 1
                        logger.debug(
                            "retention: sub=%d deferred to save offer (high churn risk)", sub.id
                        )
                        continue

                    last = _last_engagement(db, sub.id)
                    # Normalize to UTC-aware to avoid TypeError when comparing
                    # with aware cutoff (MessageOutcome.sent_at has no timezone column).
                    def _to_utc(dt: Optional[datetime]) -> Optional[datetime]:
                        if dt is None:
                            return None
                        return dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)

                    ref = _to_utc(last) or _to_utc(sub.created_at)

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
        "[RetentionProducer] checked=%d inactive=%d emitted=%d deduped=%d deferred=%d errors=%d dry_run=%s",
        results["checked"],
        results["inactive_found"],
        results["events_emitted"],
        results["deduped"],
        results["deferred_to_save"],
        results["errors"],
        dry_run,
    )
    return results


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    dry = "--dry-run" in sys.argv
    print(run(dry_run=dry))
