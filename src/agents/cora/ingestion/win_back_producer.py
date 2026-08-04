"""
win_back cell producer — the one Cora cell sourced from subscribers, not
buyer_entities.

Population: lapsed subscribers (churned_at IS NOT NULL), reusing the SAME
eligibility logic the already-live Lifecycle reactivation system uses for
its own tier3_winback cohort — src.services.reactivation_eligibility.
check_tier3_winback_eligibility and src.tasks.reactivation_scheduler's
_lapsed_subscriber_ids/_fetch_subscribers. Both are read-only imports from
src/services and src/tasks, never from src/agents/graphs — nothing here
imports or triggers src.agents.graphs.reactivation.py (forbidden on this
branch) or duplicates its query logic.

Cross-system double-messaging guard: src.agents.graphs.reactivation.py
stamps subscribers.last_reactivation_attempt_at the moment it actually
attempts contact (see that file's "finalize" step). This producer treats
that column as the coordination signal — the *unique identifier* being
checked isn't just subscriber.id, it's subscriber.id joined against that
timestamp: any subscriber the live system attempted within
CROSS_SYSTEM_SAFETY_WINDOW_DAYS is skipped outright, deliberately wider
than that system's own 3-day cooldown (src.services.reactivation_eligibility.
REACTIVATION_COOLDOWN_DAYS) since a Cora draft can sit pending human
approval for a while after being produced, so the exclusion window has to
cover that lag too, not just the instant of production.

Each subscriber gets a synthetic opportunity_thread_id ("SUB-{subscriber_id}")
so it fits Cora's buyer_entity-shaped schema/dedup — subscriber.id (already
globally unique) is what makes that thread id stable across sweeps.
confidence_score is set to 100, not inferred: a subscriber row is a known,
already-verified identity (they paid us before), not a Hunter-style
unverified public-record match, so Hunter's is_citable floor is trivially
satisfied on purpose rather than bypassed.
"""
from __future__ import annotations

import hashlib
import logging
from datetime import date, datetime, timezone
from typing import Any, Dict, List

from sqlalchemy.orm import Session

from src.agents.cora import queue, store

logger = logging.getLogger(__name__)

WIN_BACK_CELL_ID = "win_back"
CROSS_SYSTEM_SAFETY_WINDOW_DAYS = 14


def _throttled_limit(db: Session, venture_key: str, limit: int) -> int:
    """Scale `limit` down to the throttle floor when the win_back cell is
    throttled. Fails open (returns the unscaled limit) on any error — a missing
    cell_allocation table must never block win-back production."""
    from config.cell_allocation import THROTTLE_FLOOR_PCT
    from src.services.cell_allocation import cell_is_throttled

    try:
        if cell_is_throttled(db, venture_key, WIN_BACK_CELL_ID):
            floored = max(int(limit * THROTTLE_FLOOR_PCT / 100.0), 1)
            logger.info(
                "win_back_producer: cell throttled for venture=%s — limit %d -> %d",
                venture_key, limit, floored,
            )
            return floored
    except Exception:
        logger.warning(
            "win_back_producer: throttle lookup failed for venture=%s — using unscaled limit",
            venture_key, exc_info=True,
        )
    return limit


def _subscriber_thread_id(subscriber_id: int) -> str:
    return f"SUB-{subscriber_id}"


def _recently_attempted_by_live_reactivation_system(subscriber: Any, window_days: int = CROSS_SYSTEM_SAFETY_WINDOW_DAYS) -> bool:
    last = getattr(subscriber, "last_reactivation_attempt_at", None)
    if last is None:
        return False
    if last.tzinfo is None:
        last = last.replace(tzinfo=timezone.utc)
    from datetime import timedelta
    return last > datetime.now(timezone.utc) - timedelta(days=window_days)


def _facts_for(subscriber: Any, branch: str) -> List[Dict[str, Any]]:
    observed_at = store.now().isoformat()
    return [
        {
            "fact_key": "churned_at",
            "value": str(subscriber.churned_at),
            "source_ref": "subscribers",
            "observed_at": observed_at,
            "freshness_class": "subscriber_snapshot",
        },
        {
            "fact_key": "winback_branch",
            "value": branch,
            "source_ref": "reactivation_eligibility",
            "observed_at": observed_at,
            "freshness_class": "subscriber_snapshot",
        },
    ]


def _idempotency_key(thread_id: str) -> str:
    content_hash = hashlib.sha256(f"{thread_id}:{WIN_BACK_CELL_ID}:{date.today().isoformat()}".encode()).hexdigest()[:16]
    return queue.make_idempotency_key("target.ready", thread_id, content_hash)


def produce_win_back_targets(db: Session, limit: int = 25) -> List[str]:
    from src.services.reactivation_eligibility import check_tier3_winback_eligibility
    from src.tasks.reactivation_scheduler import _fetch_subscribers, _lapsed_subscriber_ids

    from config.venture_template import DEFAULT_VENTURE_KEY

    limit = _throttled_limit(db, DEFAULT_VENTURE_KEY, limit)

    sub_ids = _lapsed_subscriber_ids(db)
    subs = _fetch_subscribers(sub_ids, db)

    produced: List[str] = []
    checked = 0
    for sub in subs:
        if checked >= limit:
            break
        checked += 1

        eligible, reason, branch = check_tier3_winback_eligibility(sub, db)
        if not eligible:
            logger.debug("win_back_producer: sub_id=%s not eligible reason=%s", sub.id, reason)
            continue

        if _recently_attempted_by_live_reactivation_system(sub):
            logger.info(
                "win_back_producer: sub_id=%s attempted by the live reactivation system within %dd — "
                "skipping to avoid double-messaging",
                sub.id, CROSS_SYSTEM_SAFETY_WINDOW_DAYS,
            )
            continue

        thread_id = _subscriber_thread_id(sub.id)
        if store.has_duplicate_actionable_draft(db, thread_id, WIN_BACK_CELL_ID):
            continue

        buyer_entity = {
            "id": sub.id,
            "canonical_name": sub.name or sub.email or thread_id,
            "opportunity_thread_id": thread_id,
            "confidence_score": 100,
        }
        payload = {
            "buyer_entity": buyer_entity,
            "cell_id": WIN_BACK_CELL_ID,
            "facts_used": _facts_for(sub, branch),
            "contact_email": sub.email,
            "contact_phone": sub.phone,
        }
        message_id = queue.publish("target.ready", payload, idempotency_key=_idempotency_key(thread_id))
        if message_id is not None:
            produced.append(thread_id)

    logger.info("win_back_producer: produced %d target.ready event(s) out of %d checked", len(produced), checked)
    return produced
