"""
Stage 10 — Cron-based alert rules evaluator.

Replaces n8n as the alert evaluation layer. Runs every 15 minutes and
evaluates the same conditions that Prometheus alert_rules.yml watches —
so self-healing fires from cron even when Prometheus/Alertmanager are not
running (e.g. local dev, single-server deploys).

Actions taken when a rule fires:
  LifecycleFirstPaymentRateLow  → variant_engine.promote_winner("wallet_push_v1")
  LifecycleVariantSigmaRollback → variant_engine.check_sigma_rollback per active sequence
  LifecycleLockConversionLow    → lifecycle_self_healing step for lock_conversion
  LifecycleWalletAdoptionLow    → lifecycle_self_healing step for wallet_adoption
  LifecycleSMSReplyRateLow      → lifecycle_self_healing step for sms_reply_rate

Idempotency: every action delegates to variant_engine or lifecycle_self_healing,
both of which use Postgres-backed idempotency keys or open-incident guards.
Re-running within the same breach window is always a no-op.

Gate: LIFECYCLE_SELF_HEALING_ENABLED env var (same as self-healing). No-op when
disabled.

Usage:
    python -m src.tasks.alert_rules_check
    python -m src.tasks.alert_rules_check --dry-run
"""

from __future__ import annotations

import json
import logging
import sys
from typing import Optional

from sqlalchemy import text as sa_text

from config.lifecycle_guardrails import KILL_SWITCH, get_effective_kill_switch
from config.settings import get_settings
from src.core.database import get_db_context
from src.tasks.kill_switch_metric_ingest import get_cached_metric

logger = logging.getLogger(__name__)

# ── Thresholds mirroring alert_rules.yml ─────────────────────────────────────

ALERT_RULES = [
    {
        "name": "LifecycleFirstPaymentRateLow",
        "metric": "first_payment_rate",
        "threshold": 25,
        "duration_hours": 48,
        "action": "variant_promote",
    },
    {
        "name": "LifecycleLockConversionLow",
        "metric": "lock_conversion",
        "threshold": 3,
        "duration_hours": 48,
        "action": "self_healing",
    },
    {
        "name": "LifecycleWalletAdoptionLow",
        "metric": "wallet_adoption",
        "threshold": 10,
        "duration_hours": 48,
        "action": "self_healing",
    },
    {
        "name": "LifecycleSMSReplyRateLow",
        "metric": "sms_reply_rate",
        "threshold": 5,
        "duration_hours": 48,
        "action": "self_healing",
    },
    {
        "name": "LifecycleOfferAcceptanceRateLow",
        "metric": "offer_acceptance_rate",
        "threshold": 10,
        "duration_hours": 48,
        "action": "self_healing",
    },
]


def _breach_open_longer_than(db, metric_name: str, duration_hours: float) -> bool:
    """Return True if an open lifecycle_incident for metric_name is older than duration_hours."""
    row = db.execute(sa_text("""
        SELECT breach_started FROM lifecycle_incident
        WHERE metric_name = :metric
          AND breach_resolved IS NULL
        ORDER BY breach_started DESC
        LIMIT 1
    """), {"metric": metric_name}).first()

    if not row:
        return False
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)
    bs = row.breach_started
    if bs.tzinfo is None:
        bs = bs.replace(tzinfo=timezone.utc)
    return (now - bs).total_seconds() / 3600 >= duration_hours


def _run_alert_rule(rule: dict, db, dry_run: bool) -> dict:
    metric = rule["metric"]
    name = rule["name"]
    threshold = rule["threshold"]
    duration_hours = rule["duration_hours"]
    action_type = rule["action"]

    val = get_cached_metric(metric)
    if val is None:
        return {"rule": name, "status": "no_data"}

    if val >= threshold:
        return {"rule": name, "status": "ok", "value": val}

    # Metric is below threshold — check if breach has been open long enough.
    if not _breach_open_longer_than(db, metric, duration_hours):
        return {
            "rule": name,
            "status": "observing",
            "value": val,
            "required_hours": duration_hours,
        }

    if dry_run:
        return {"rule": name, "status": f"would_fire_{action_type}", "value": val}

    # Fire the action.
    if action_type == "variant_promote":
        return _fire_variant_promote(name, db)

    if action_type == "self_healing":
        return _fire_self_healing(name, metric, db)

    return {"rule": name, "status": "unknown_action"}


def _fire_variant_promote(rule_name: str, db) -> dict:
    from config.stage10_config import STAGE10_KILL_SWITCH_OVERRIDES
    from src.services.variant_engine import promote_winner

    overrides = STAGE10_KILL_SWITCH_OVERRIDES.get("first_payment_rate", {})
    sequence = overrides.get("variant_sequence_name", "wallet_push_v1")
    result = promote_winner(sequence, db)
    logger.info("[alert-rules] %s → promote_winner: %s", rule_name, result)
    return {"rule": rule_name, "action": "variant_promoted", "detail": result}


def _fire_self_healing(rule_name: str, metric_name: str, db) -> dict:
    from src.tasks.lifecycle_self_healing import (
        _process_metric, _Counters, _count_today_kill_recommendations,
    )
    settings = get_settings()
    county_id = getattr(settings, "county_launch_source_county", None) or "hillsborough"
    counters = _Counters(kill_recs_today=_count_today_kill_recommendations(db))
    result = _process_metric(
        db,
        metric_name=metric_name,
        county_id=county_id,
        feature_name=None,
        counters=counters,
        dry_run=False,
    )
    logger.info("[alert-rules] %s → self_healing: %s", rule_name, result)
    return {"rule": rule_name, "action": "self_healing_step", "detail": result}


def _run_sigma_rollback_check(db, dry_run: bool) -> list[dict]:
    """Check sigma rollback for all active sequences."""
    from src.services.variant_engine import check_sigma_rollback

    rows = db.execute(sa_text("""
        SELECT sequence_name FROM message_variant_tests WHERE status = 'active'
    """)).fetchall()

    results = []
    for row in rows:
        if dry_run:
            results.append({"sequence": row.sequence_name, "status": "would_check"})
            continue
        result = check_sigma_rollback(row.sequence_name, db)
        results.append({"sequence": row.sequence_name, **result})
    return results


def run_alert_rules_check(dry_run: bool = False) -> dict:
    """Evaluate all alert rules and dispatch actions. Idempotent."""
    rule_results = []
    sigma_results = []

    with get_db_context() as db:
        for rule in ALERT_RULES:
            try:
                result = _run_alert_rule(rule, db, dry_run)
                rule_results.append(result)
            except Exception:
                logger.exception("[alert-rules] rule %s failed", rule["name"])

        try:
            sigma_results = _run_sigma_rollback_check(db, dry_run)
        except Exception:
            logger.exception("[alert-rules] sigma rollback check failed")

    summary = {
        "dry_run": dry_run,
        "rules_evaluated": len(rule_results),
        "rule_results": rule_results,
        "sigma_rollback_results": sigma_results,
    }
    logger.info("[alert-rules] %s", json.dumps(summary, default=str))
    return summary


def main(argv: Optional[list[str]] = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )
    args = set(argv or sys.argv[1:])
    dry_run = "--dry-run" in args

    settings = get_settings()
    if not settings.lifecycle_self_healing_enabled:
        logger.info("[alert-rules] disabled via LIFECYCLE_SELF_HEALING_ENABLED — exiting")
        return 0

    summary = run_alert_rules_check(dry_run=dry_run)
    if dry_run:
        print(json.dumps(summary, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
