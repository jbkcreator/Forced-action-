"""
Bundle dispatcher — Stage 5.

Hourly cron that proactively offers the four bundles to wallet subscribers
based on contextual triggers in BUNDLE_TRIGGERS:

  - weekend       : Fridays 4pm subscriber-local through Sunday — wallet users
  - storm         : Active NWS alert for subscriber's locked ZIPs
  - zip_booster   : Subscriber has 5+ delivered leads in same ZIP / 7d
  - monthly_reload: Wallet balance below threshold AND auto_reload_enabled=False

Per offer:
  1. Check cooldown (no duplicate offers within `cooldown_hours`).
  2. Assign A/B variant via ab_engine. Variants are pricing tweaks within
     ±25% of base price — variants outside the band are rejected by the
     `bundle_pricing_within_guardrail` check at seed time.
  3. Build a deep link of form `/dashboard/{feed_uuid}?bundle={type}&variant={a|b}`
     and dispatch via sms_compliance.send_sms (TCPA/quiet-hours/opt-out
     enforced inside that helper).
  4. Log a MessageOutcome row with template_id="bundle_<type>_offer".

Run via `python -m src.tasks.bundle_dispatcher` from cron.
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import and_, func, select
from sqlalchemy.orm import Session

from config.revenue_ladder import (
    BUNDLES,
    BUNDLE_TRIGGERS,
    bundle_pricing_within_guardrail,
)
from config.settings import settings
from src.core.database import get_db_context
from src.core.models import (
    BundlePurchase,
    MessageOutcome,
    SentLead,
    Subscriber,
    WalletBalance,
    ZipTerritory,
)

logger = logging.getLogger(__name__)


# ── Audience selectors ───────────────────────────────────────────────────────

def _candidates_weekend(db: Session) -> list[Subscriber]:
    """All wallet subscribers (Friday 4pm guard handled by `_should_dispatch`)."""
    rows = db.execute(
        select(Subscriber)
        .join(WalletBalance, WalletBalance.subscriber_id == Subscriber.id)
        .where(Subscriber.status == "active")
    ).scalars().all()
    return list(rows)


def _candidates_storm(db: Session) -> list[Subscriber]:
    """Wallet subscribers in counties with an active NWS storm alert."""
    from src.core.redis_client import redis_available, rget
    if not redis_available():
        return []
    rows = db.execute(
        select(Subscriber, ZipTerritory)
        .join(ZipTerritory, ZipTerritory.subscriber_id == Subscriber.id)
        .join(WalletBalance, WalletBalance.subscriber_id == Subscriber.id)
        .where(
            Subscriber.status == "active",
            ZipTerritory.status == "locked",
        )
    ).all()
    affected: dict[int, Subscriber] = {}
    for sub, terr in rows:
        if rget(f"storm_active:{terr.zip_code}"):
            affected.setdefault(sub.id, sub)
    return list(affected.values())


def _candidates_zip_booster(db: Session) -> list[Subscriber]:
    """Wallet subs with 5+ delivered leads in same ZIP within last 7 days."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=7)
    rows = db.execute(
        select(SentLead.subscriber_id, func.count())
        .where(SentLead.sent_at >= cutoff)
        .group_by(SentLead.subscriber_id)
        .having(func.count() >= 5)
    ).all()
    sub_ids = [r[0] for r in rows]
    if not sub_ids:
        return []
    subs = db.execute(
        select(Subscriber)
        .join(WalletBalance, WalletBalance.subscriber_id == Subscriber.id)
        .where(Subscriber.id.in_(sub_ids), Subscriber.status == "active")
    ).scalars().all()
    return list(subs)


def _candidates_monthly_reload(db: Session) -> list[Subscriber]:
    """Wallet subs at low balance with auto-reload OFF — push the bundle alternative."""
    rows = db.execute(
        select(Subscriber)
        .join(WalletBalance, WalletBalance.subscriber_id == Subscriber.id)
        .where(
            Subscriber.status == "active",
            WalletBalance.credits_remaining < 5,
            WalletBalance.auto_reload_enabled.is_(False),
        )
    ).scalars().all()
    return list(rows)


_AUDIENCE_FNS = {
    "wallet_active": _candidates_weekend,
    "wallet_active_in_affected_county": _candidates_storm,
    "wallet_5plus_leads_same_zip_7d": _candidates_zip_booster,
    "wallet_low_balance_no_auto_reload": _candidates_monthly_reload,
}


# ── Schedule gates ───────────────────────────────────────────────────────────

def _should_dispatch(bundle_type: str, now_utc: datetime) -> bool:
    schedule = BUNDLE_TRIGGERS.get(bundle_type, {}).get("schedule")
    if schedule == "fri_after_4pm_local":
        # Approximate — Friday 4pm ET == Friday 20:00 UTC. Run window: Fri 20:00 → Sun 23:59 UTC.
        weekday = now_utc.weekday()  # Mon=0 ... Sun=6
        if weekday == 4 and now_utc.hour >= 20:
            return True
        if weekday in (5, 6):
            return True
        return False
    if schedule == "on_nws_alert":
        return True   # storm candidates filter by Redis active-alert flag
    if schedule == "hourly":
        return True
    return False


# ── Cooldown ────────────────────────────────────────────────────────────────

def _in_cooldown(subscriber_id: int, bundle_type: str, hours: int, db: Session) -> bool:
    template_id = f"bundle_{bundle_type}_offer"
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    last = db.execute(
        select(MessageOutcome.id).where(
            MessageOutcome.subscriber_id == subscriber_id,
            MessageOutcome.template_id == template_id,
            MessageOutcome.sent_at >= cutoff,
        ).limit(1)
    ).scalar_one_or_none()
    return last is not None


# ── Dispatch ────────────────────────────────────────────────────────────────

def _dispatch_offer(sub: Subscriber, bundle_type: str, db: Session) -> bool:
    """Send one bundle offer SMS to a subscriber. Returns True if sent."""
    cfg = BUNDLES.get(bundle_type, {})
    trigger = BUNDLE_TRIGGERS.get(bundle_type, {})

    # A/B variant assignment
    from src.services.ab_engine import assign_variant
    test_name = trigger.get("ab_test_name", f"bundle_{bundle_type}_pricing")
    variant = assign_variant(sub.id, test_name, db) or "a"   # default to baseline if no test

    base_price = cfg.get("price_cents", 0)

    deep_link = (
        f"{settings.app_base_url}/dashboard/{sub.event_feed_uuid}"
        f"?bundle={bundle_type}&variant={variant}"
        if sub.event_feed_uuid else f"{settings.app_base_url}"
    )

    label = cfg.get("label", bundle_type.title())
    price_str = f"${base_price / 100:.0f}" if base_price else ""

    body = f"{label}{(' — ' + price_str) if price_str else ''}. {cfg.get('description', '')} {deep_link}"

    phone = getattr(sub, "phone", None)
    if not phone:
        logger.debug("[BundleDispatcher] no phone for subscriber=%d — offer queued only (%s)", sub.id, bundle_type)
        # Still log the intent so cooldown/attribution work once phone column ships
        _log_outcome(sub.id, bundle_type, variant, db, sent=False)
        return False

    from src.services.sms_compliance import send_sms
    sent = send_sms(
        to=phone,
        body=body,
        db=db,
        subscriber_id=sub.id,
        task_type="bundle_offer",
        campaign=f"bundle_{bundle_type}",
        variant_id=variant,
    )
    _log_outcome(sub.id, bundle_type, variant, db, sent=sent)
    return sent


def _log_outcome(subscriber_id: int, bundle_type: str, variant: str, db: Session, sent: bool) -> None:
    db.add(MessageOutcome(
        subscriber_id=subscriber_id,
        message_type="sms",
        template_id=f"bundle_{bundle_type}_offer",
        variant_id=variant,
        channel="twilio",
        sent_at=datetime.now(timezone.utc),
    ))
    db.flush()


# ── Main loop ────────────────────────────────────────────────────────────────

def run(dry_run: bool = False) -> dict:
    stats = {"checked": 0, "dispatched": 0, "skipped_cooldown": 0, "skipped_schedule": 0}
    now = datetime.now(timezone.utc)

    with get_db_context() as db:
        for bundle_type, trigger in BUNDLE_TRIGGERS.items():
            if not _should_dispatch(bundle_type, now):
                stats["skipped_schedule"] += 1
                continue

            audience_fn = _AUDIENCE_FNS.get(trigger["audience"])
            if not audience_fn:
                logger.error("[BundleDispatcher] unknown audience %s", trigger["audience"])
                continue

            candidates = audience_fn(db)
            cooldown = trigger.get("cooldown_hours", 24)

            for sub in candidates:
                stats["checked"] += 1
                if _in_cooldown(sub.id, bundle_type, cooldown, db):
                    stats["skipped_cooldown"] += 1
                    continue
                if dry_run:
                    logger.info("[BundleDispatcher] DRY-RUN would offer %s to subscriber=%d", bundle_type, sub.id)
                    continue
                if _dispatch_offer(sub, bundle_type, db):
                    stats["dispatched"] += 1

    logger.info("[BundleDispatcher] %s", stats)
    return stats


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    dry = "--dry-run" in sys.argv
    print(run(dry_run=dry))
