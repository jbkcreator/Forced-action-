"""
Kill-switch metric ingest.

Computes observable metrics from the DB that feed into the Cora decision
hierarchy kill-switch gate. Results are cached in Redis (25hr TTL) so agents
can call kill_switch_status(feature, observed_value) with live data.

Metrics computed:
  first_payment_rate  — % of subscribers created in last 30d who are active
  saved_card_rate     — % of active subscribers with has_saved_card=True
  wallet_adoption     — % of active subscribers with a WalletBalance record
  lock_conversion     — % of wallet subscribers who upgraded to annual_lock in last 30d
  retention_30d       — % of subscribers active 30d ago who are still active today
  sms_reply_rate      — skipped (no reply tracking table yet — returns None)
  cac_paid_channels   — skipped (no ad spend tracking yet — returns None)
  free_tier_cost_ratio — % of compute cost attributable to free-tier subscribers (proxy)
  twilio_cost_per_signup — avg Twilio cost per new subscriber (proxy from agent_decisions)

Cron: 0 6 * * * (6:00 UTC daily, before retention cron at 16:00)

Usage:
    python -m src.tasks.kill_switch_metric_ingest [--dry-run]
"""
import json
import logging
import sys
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from src.core.database import get_db_context
from src.core.models import AgentDecision, Subscriber, WalletBalance

logger = logging.getLogger(__name__)

_REDIS_TTL = 25 * 3600  # 25 hours — survives one missed cron run
_REDIS_PREFIX = "fa:ks_metric:"


def _get_redis():
    """Return a Redis client or None if not configured."""
    try:
        from config.settings import settings
        if not settings.redis_url:
            return None
        import redis as redis_lib
        return redis_lib.from_url(settings.redis_url, decode_responses=True)
    except Exception:
        return None


def _cache_metric(r, feature: str, value: Optional[float]) -> None:
    if r is None or value is None:
        return
    try:
        r.setex(f"{_REDIS_PREFIX}{feature}", _REDIS_TTL, str(value))
    except Exception as exc:
        logger.warning("kill_switch_metric_ingest: Redis write failed for %s: %s", feature, exc)


def get_cached_metric(feature: str) -> Optional[float]:
    """Return the last-cached metric value for a feature, or None."""
    r = _get_redis()
    if r is None:
        return None
    try:
        val = r.get(f"{_REDIS_PREFIX}{feature}")
        return float(val) if val is not None else None
    except Exception:
        return None


def _compute_metrics(db: Session) -> dict:
    now = datetime.now(timezone.utc)
    ago_30 = now - timedelta(days=30)

    metrics = {}

    # first_payment_rate — active subs created in last 30d / total created last 30d
    total_new = db.execute(
        select(func.count(Subscriber.id)).where(Subscriber.created_at >= ago_30)
    ).scalar() or 0
    active_new = db.execute(
        select(func.count(Subscriber.id)).where(
            Subscriber.created_at >= ago_30,
            Subscriber.status == "active",
        )
    ).scalar() or 0
    metrics["first_payment_rate"] = round((active_new / total_new * 100), 1) if total_new > 0 else None

    # saved_card_rate — active subs with has_saved_card=True / total active
    total_active = db.execute(
        select(func.count(Subscriber.id)).where(Subscriber.status == "active")
    ).scalar() or 0
    saved_card = db.execute(
        select(func.count(Subscriber.id)).where(
            Subscriber.status == "active",
            Subscriber.has_saved_card.is_(True),
        )
    ).scalar() or 0
    metrics["saved_card_rate"] = round((saved_card / total_active * 100), 1) if total_active > 0 else None

    # wallet_adoption — active subs with WalletBalance / total active
    with_wallet = db.execute(
        select(func.count(WalletBalance.id)).where(
            WalletBalance.subscriber_id.in_(
                select(Subscriber.id).where(Subscriber.status == "active")
            )
        )
    ).scalar() or 0
    metrics["wallet_adoption"] = round((with_wallet / total_active * 100), 1) if total_active > 0 else None

    # lock_conversion — active annual_lock subs created last 30d / wallet subs last 30d
    wallet_new = db.execute(
        select(func.count(Subscriber.id)).where(
            Subscriber.created_at >= ago_30,
            Subscriber.tier == "wallet",
        )
    ).scalar() or 0
    lock_new = db.execute(
        select(func.count(Subscriber.id)).where(
            Subscriber.created_at >= ago_30,
            Subscriber.tier == "annual_lock",
            Subscriber.status == "active",
        )
    ).scalar() or 0
    base = (wallet_new + lock_new)
    metrics["lock_conversion"] = round((lock_new / base * 100), 1) if base > 0 else None

    # retention_30d — subs active 30d ago (created before ago_30) still active today
    cohort_total = db.execute(
        select(func.count(Subscriber.id)).where(
            Subscriber.created_at <= ago_30,
            Subscriber.tier.notin_(["free", "data_only"]),
        )
    ).scalar() or 0
    cohort_still_active = db.execute(
        select(func.count(Subscriber.id)).where(
            Subscriber.created_at <= ago_30,
            Subscriber.tier.notin_(["free", "data_only"]),
            Subscriber.status == "active",
        )
    ).scalar() or 0
    metrics["retention_30d"] = round((cohort_still_active / cohort_total * 100), 1) if cohort_total > 0 else None

    # claude_cost_per_decision — avg Claude API cost_usd per completed agent decision (scaled ×100 for Redis precision)
    avg_cost = db.execute(
        select(func.avg(AgentDecision.cost_usd)).where(
            AgentDecision.started_at >= ago_30,
            AgentDecision.terminal_status == "completed",
        )
    ).scalar()
    metrics["claude_cost_per_decision"] = round(float(avg_cost or 0) * 100, 4) if avg_cost else None

    # Skipped — no data source yet
    metrics["sms_reply_rate"] = None
    metrics["cac_paid_channels"] = None
    metrics["free_tier_cost_ratio"] = None

    return metrics


def run_kill_switch_metric_ingest(dry_run: bool = False) -> dict:
    """Compute kill-switch metrics and cache in Redis. Returns computed values."""
    r = _get_redis()

    with get_db_context() as db:
        metrics = _compute_metrics(db)

    if not dry_run:
        for feature, value in metrics.items():
            _cache_metric(r, feature, value)
        logger.info("[KillSwitchMetricIngest] cached %d metrics", sum(1 for v in metrics.values() if v is not None))
    else:
        logger.info("[KillSwitchMetricIngest] dry_run — computed: %s", json.dumps(metrics, indent=2))

    return metrics


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    dry = "--dry-run" in sys.argv
    result = run_kill_switch_metric_ingest(dry_run=dry)
    print(json.dumps(result, indent=2))
