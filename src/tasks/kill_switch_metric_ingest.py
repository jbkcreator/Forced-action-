"""
Kill-switch metric ingest.

Computes observable metrics from the DB that feed into the Cora decision
hierarchy kill-switch gate. Results are cached in Redis (25hr TTL) so agents
can call kill_switch_status(feature, observed_value) with live data.

Metrics computed:
  first_payment_rate    — % of subscribers created in last 30d who are active
  saved_card_rate       — % of active subscribers with has_saved_card=True
  wallet_adoption       — % of active subscribers with a WalletBalance record
  lock_conversion       — % of wallet subscribers who upgraded to annual_lock in last 30d
  retention_30d         — % of subscribers active 30d ago who are still active today
  sms_reply_rate        — % of marketing SMS in last 7d with a non-null replied_at (fa034)
  offer_acceptance_rate — % of wallet_push_offers accepted/activated in last 7d (fa034)
  county_profitability  — 1.0 if any active paying subscribers in county, else 0.0 (v1 proxy)
  claude_cost_per_decision — avg AgentDecision.cost_usd × 100

Known gaps (kept as None so cora_self_healing treats them fail-safe):
  cac_paid_channels    — no ad-spend ledger yet
  free_tier_cost_ratio — no per-sub cost allocation
  sms_cost_per_signup  — Telnyx doesn't expose per-send cost on MessageOutcome

fa034: After computing the dict, this task also writes a row to
platform_daily_stats (one row per (run_date, county_id)) so the
src/tasks/cora_self_healing.py task can compute a 7-day rolling baseline
per metric via the compute_baseline() helper exported from this module.

Cron: 0 6 * * * (6:00 UTC daily, before retention cron at 16:00)

Usage:
    python -m src.tasks.kill_switch_metric_ingest [--dry-run]
"""
import json
import logging
import sys
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import func, select, text as sa_text
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

    # ── sms_reply_rate ─────────────────────────────────────────────────────
    # Raw SQL on message_outcomes (per repo convention — see CDS engine).
    # 7-day window so we have a meaningful sample on rolling cadence.
    sms_row = db.execute(sa_text("""
        SELECT
            COUNT(*) FILTER (WHERE replied_at IS NOT NULL) AS replied,
            COUNT(*)                                       AS total
        FROM message_outcomes
        WHERE message_type = 'sms'
          AND sent_at >= NOW() - INTERVAL '7 days'
    """)).first()
    if sms_row and sms_row.total and sms_row.total > 0:
        metrics["sms_reply_rate"] = round(sms_row.replied / sms_row.total * 100, 1)
    else:
        metrics["sms_reply_rate"] = None

    # ── offer_acceptance_rate ──────────────────────────────────────────────
    # Wallet push offers, 7-day window. Counts both 'accepted' and 'activated'
    # statuses as accepted (activated implies user accepted then subscription
    # went through). Per spec example: 35% → 12% drop is the canonical breach.
    offer_row = db.execute(sa_text("""
        SELECT
            COUNT(*) FILTER (WHERE status IN ('accepted','activated')) AS accepted,
            COUNT(*)                                                    AS total
        FROM wallet_push_offers
        WHERE offered_at >= NOW() - INTERVAL '7 days'
    """)).first()
    if offer_row and offer_row.total and offer_row.total > 0:
        metrics["offer_acceptance_rate"] = round(offer_row.accepted / offer_row.total * 100, 1)
    else:
        metrics["offer_acceptance_rate"] = None

    # ── Known gaps — documented, kept as None so the self-healing job
    #    consistently sees "unknown" (treated fail-safe) instead of zero.
    # cac_paid_channels: requires ad-spend tracking (utm_source exists on
    #   subscribers but no spend ledger). Add when marketing budgets land
    #   in the DB.
    metrics["cac_paid_channels"] = None
    # free_tier_cost_ratio: no per-subscriber compute-cost allocation. Would
    #   need a Stripe invoice JOIN + free-tier cost imputation.
    metrics["free_tier_cost_ratio"] = None
    # sms_cost_per_signup: project moved to Telnyx (May 2026) and MessageOutcome
    #   doesn't track per-send cost. Add when Telnyx billing API is wired.
    metrics["sms_cost_per_signup"] = None

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


# Columns on platform_daily_stats that the self-healing job will read back
# as 7-day baselines. Maps metric_name (from KILL_SWITCH) to the column on
# platform_daily_stats. Only metrics with a column here participate in
# baseline-aware grading; the rest use threshold-only grading.
_BASELINE_COLUMNS = {
    "sms_reply_rate":         "sms_reply_rate",
    "offer_acceptance_rate":  "offer_acceptance_rate",
    "first_payment_rate":     "first_payment_rate",
    "saved_card_rate":        "saved_card_rate",
    "wallet_adoption":        "wallet_adoption",
    "lock_conversion":        "lock_conversion",
    "retention_30d":          "retention_30d",
    "cac_paid_channels":      "cac_paid_channels",
}


def _write_platform_daily_stats_row(db: Session, county_id: str, metrics: dict) -> None:
    """Upsert today's metric values into platform_daily_stats for the
    baseline rolling window. Raw SQL — uses the existing (run_date, county_id)
    unique constraint so re-running on the same day is idempotent.

    The non-metric columns (signals_*, properties_*, leads_*, tier_*) are
    NOT NULL on the table and owned by the CDS engine. When the engine has
    already written today's row, we hit the ON CONFLICT branch and update
    only the metric columns. When this task runs first on a fresh day
    (e.g. before 07:00 UTC scoring), the INSERT supplies zeros for the
    required columns — the CDS engine will overwrite them later through
    its own upsert path.
    """
    # Build the per-column update fragment from metrics that have a value
    # AND a known column mapping.
    updatable = {
        col_name: metrics[metric_name]
        for metric_name, col_name in _BASELINE_COLUMNS.items()
        if metric_name in metrics and metrics[metric_name] is not None
    }
    if not updatable:
        return

    # NOT NULL columns on platform_daily_stats — must be supplied with
    # something on a fresh INSERT (zeros are the sane default; the CDS
    # engine overwrites with real values on its next run).
    _NOT_NULL_DEFAULTS = (
        "signals_scraped", "signals_matched", "signals_skipped",
        "properties_scored", "properties_with_signals", "score_runs_total",
        "leads_new", "leads_updated", "leads_unchanged",
        "leads_qualified", "leads_upgraded",
        "tier_ultra_platinum", "tier_platinum", "tier_gold",
        "tier_silver", "tier_bronze",
    )
    not_null_cols = ", ".join(_NOT_NULL_DEFAULTS)
    not_null_zeros = ", ".join("0" for _ in _NOT_NULL_DEFAULTS)

    update_only_clause = ", ".join(f"{col} = EXCLUDED.{col}" for col in updatable)
    insert_cols = ", ".join(updatable.keys())
    insert_vals = ", ".join(f":{col}" for col in updatable.keys())

    params = {**updatable, "county_id": county_id}

    db.execute(sa_text(f"""
        INSERT INTO platform_daily_stats (
            run_date, county_id,
            {not_null_cols},
            created_at, updated_at,
            {insert_cols}
        )
        VALUES (
            CURRENT_DATE, :county_id,
            {not_null_zeros},
            NOW(), NOW(),
            {insert_vals}
        )
        ON CONFLICT (run_date, county_id) DO UPDATE
        SET {update_only_clause},
            updated_at = NOW()
    """), params)


def compute_baseline(
    db: Session,
    metric_name: str,
    county_id: str,
    window_days: int = 7,
) -> Optional[float]:
    """Return the rolling mean of `metric_name` over the last `window_days`
    rows in platform_daily_stats for the given county. None if no data.

    Used by src/tasks/cora_self_healing.py to compare current observed
    values against trend, not just absolute thresholds. Raw SQL only.
    """
    col = _BASELINE_COLUMNS.get(metric_name)
    if col is None:
        return None
    row = db.execute(sa_text(f"""
        SELECT AVG({col}) AS baseline
        FROM platform_daily_stats
        WHERE county_id = :county_id
          AND run_date >= CURRENT_DATE - :days
          AND {col} IS NOT NULL
    """), {"county_id": county_id, "days": window_days}).first()
    if row is None or row.baseline is None:
        return None
    return float(row.baseline)


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
            # fa034: snapshot today's values into platform_daily_stats so
            # cora_self_healing can compute a 7-day rolling baseline.
            _write_platform_daily_stats_row(db, county_id, metrics)

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
