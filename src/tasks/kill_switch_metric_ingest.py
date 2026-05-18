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
  sms_cost_per_signup — avg SMS vendor cost per new subscriber (proxy from agent_decisions; Telnyx)
  county_profitability — 1.0 if any active paying subscribers in county, else 0.0 (v1 proxy)

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

from config.settings import settings
from src.core.database import get_db_context
from src.core.models import AgentDecision, Subscriber, WalletBalance, WalletPushOffer
from src.core.redis_client import redis_available, rget, rset

logger = logging.getLogger(__name__)

_REDIS_TTL = 25 * 3600  # 25 hours — survives one missed cron run
_REDIS_PREFIX = "fa:ks_metric:"


def _cache_metric(feature: str, value: Optional[float], county_id: Optional[str] = None) -> None:
    if value is None or not redis_available():
        return
    if county_id is not None:
        key = f"{_REDIS_PREFIX}{county_id}:{feature}"
    else:
        key = f"{_REDIS_PREFIX}{feature}"
    rset(key, str(value), ttl_seconds=_REDIS_TTL)


def get_cached_metric(feature: str, county_id: Optional[str] = None) -> Optional[float]:
    """Return the last-cached metric value for a feature, or None."""
    if not redis_available():
        return None
    if county_id is not None:
        key = f"{_REDIS_PREFIX}{county_id}:{feature}"
    else:
        key = f"{_REDIS_PREFIX}{feature}"
    val = rget(key)
    try:
        return float(val) if val is not None else None
    except (TypeError, ValueError):
        return None


def _compute_metrics(db: Session, county_id: str) -> dict:
    now = datetime.now(timezone.utc)
    ago_30 = now - timedelta(days=30)

    metrics = {}

    # first_payment_rate — active subs created in last 30d / total created last 30d
    total_new = db.execute(
        select(func.count(Subscriber.id)).where(
            Subscriber.county_id == county_id,
            Subscriber.created_at >= ago_30,
        )
    ).scalar() or 0
    active_new = db.execute(
        select(func.count(Subscriber.id)).where(
            Subscriber.county_id == county_id,
            Subscriber.created_at >= ago_30,
            Subscriber.status == "active",
        )
    ).scalar() or 0
    metrics["first_payment_rate"] = round((active_new / total_new * 100), 1) if total_new > 0 else None

    # saved_card_rate — active subs with has_saved_card=True / total active
    total_active = db.execute(
        select(func.count(Subscriber.id)).where(
            Subscriber.county_id == county_id,
            Subscriber.status == "active",
        )
    ).scalar() or 0
    saved_card = db.execute(
        select(func.count(Subscriber.id)).where(
            Subscriber.county_id == county_id,
            Subscriber.status == "active",
            Subscriber.has_saved_card.is_(True),
        )
    ).scalar() or 0
    metrics["saved_card_rate"] = round((saved_card / total_active * 100), 1) if total_active > 0 else None

    # wallet_adoption — % of saved-card active subs with a WalletBalance
    # (fa016: denominator changed from total_active to saved_card_count so the
    # metric matches the Accelerated Wallet Push spec — "≥X% of saved-card users").
    with_wallet_saved_card = db.execute(
        select(func.count(WalletBalance.id)).where(
            WalletBalance.subscriber_id.in_(
                select(Subscriber.id).where(
                    Subscriber.county_id == county_id,
                    Subscriber.status == "active",
                    Subscriber.has_saved_card.is_(True),
                )
            )
        )
    ).scalar() or 0
    metrics["wallet_adoption"] = (
        round((with_wallet_saved_card / saved_card * 100), 1)
        if saved_card > 0 else None
    )

    # fa016: accelerated_wallet_push_take_rate = activated offers / offered offers
    # in the last 30 days (rolling window so growth still moves the needle).
    offered_n = db.execute(
        select(func.count(WalletPushOffer.id)).where(
            WalletPushOffer.offered_at >= ago_30,
        )
    ).scalar() or 0
    activated_n = db.execute(
        select(func.count(WalletPushOffer.id)).where(
            WalletPushOffer.offered_at >= ago_30,
            WalletPushOffer.status == "activated",
        )
    ).scalar() or 0
    metrics["accelerated_wallet_push_take_rate"] = (
        round((activated_n / offered_n * 100), 1) if offered_n > 0 else None
    )

    # lock_conversion — active annual_lock subs created last 30d / wallet subs last 30d
    # "wallet" is not a DB tier; resolve via WalletBalance membership.
    wallet_new = db.execute(
        select(func.count(Subscriber.id)).where(
            Subscriber.county_id == county_id,
            Subscriber.created_at >= ago_30,
            Subscriber.id.in_(select(WalletBalance.subscriber_id)),
        )
    ).scalar() or 0
    lock_new = db.execute(
        select(func.count(Subscriber.id)).where(
            Subscriber.county_id == county_id,
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
            Subscriber.county_id == county_id,
            Subscriber.created_at <= ago_30,
            Subscriber.tier.notin_(["free", "data_only"]),
        )
    ).scalar() or 0
    cohort_still_active = db.execute(
        select(func.count(Subscriber.id)).where(
            Subscriber.county_id == county_id,
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

    # county_profitability — v1 proxy: any active paying subscribers in county
    # See docs/adr/0001-county-profitability-gate.md for rationale and v2 plan.
    paying_subs = db.execute(
        select(func.count(Subscriber.id)).where(
            Subscriber.county_id == county_id,
            Subscriber.status == "active",
            Subscriber.tier.notin_(["free", "data_only"]),
        )
    ).scalar() or 0
    metrics["county_profitability"] = 1.0 if paying_subs > 0 else 0.0

    return metrics


def _check_accelerated_wallet_push_floor(db, take_rate: Optional[float]) -> Optional[str]:
    """fa016: at Day 35+ of the feature being enabled, if take_rate is below
    the wallet_adoption.floor_pct (12%) for accelerated_wallet_push offers,
    flip Redis kill_switch:accelerated_wallet_push=red so the graph aborts.

    Returns the color set ('red' | 'green'), or None if the check did not run
    (no offers yet or feature not enabled).
    """
    from config.cora_guardrails import KILL_SWITCH

    cfg = KILL_SWITCH.get("wallet_adoption", {})
    floor_pct = cfg.get("floor_pct", 12)
    after_days = cfg.get("floor_check_after_days", 35)

    earliest = db.execute(
        select(func.min(WalletPushOffer.offered_at))
    ).scalar()
    if not earliest:
        return None

    age_days = (datetime.now(timezone.utc) - earliest.replace(tzinfo=timezone.utc)).days
    if age_days < after_days:
        return None

    if take_rate is None:
        return None

    color = "red" if take_rate < floor_pct else "green"
    if redis_available():
        rset("kill_switch:accelerated_wallet_push", color, ttl_seconds=_REDIS_TTL)
    return color


def run_kill_switch_metric_ingest(dry_run: bool = False) -> dict:
    """Compute kill-switch metrics and cache in Redis. Returns computed values."""
    county_id = settings.county_launch_source_county

    with get_db_context() as db:
        metrics = _compute_metrics(db, county_id=county_id)
        floor_color = _check_accelerated_wallet_push_floor(
            db, metrics.get("accelerated_wallet_push_take_rate"),
        )

    if not dry_run:
        for feature, value in metrics.items():
            # Write county-scoped key (new)
            _cache_metric(feature, value, county_id=county_id)
            # Write legacy key (no county prefix) for backward compat
            _cache_metric(feature, value)
        logger.info(
            "[KillSwitchMetricIngest] cached %d metrics county=%s aw_push_floor=%s",
            sum(1 for v in metrics.values() if v is not None), county_id, floor_color,
        )
    else:
        logger.info(
            "[KillSwitchMetricIngest] dry_run county=%s — metrics=%s aw_push_floor=%s",
            county_id, json.dumps(metrics, indent=2), floor_color,
        )

    metrics["_accelerated_wallet_push_floor"] = floor_color
    return metrics


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    dry = "--dry-run" in sys.argv
    result = run_kill_switch_metric_ingest(dry_run=dry)
    print(json.dumps(result, indent=2))
