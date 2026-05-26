"""
Cora self-healing loop (fa034).

Reads kill-switch metrics + 7-day baselines, runs a state machine over the
cora_incident table, and dispatches safe corrective actions within
guardrails. Escalates to Slack/email + Revenue Pulse when human review is
needed. NEVER takes autonomous actions outside the guardrails defined in
`config/cora_guardrails.py`.

Source of truth:
  FA-2B-v9-FINAL — Self-Healing Ops, Kill-Switch Discipline, Q44, Q52.
  Plan: ~/.claude/plans/the-bronze-at-2-34-vs-ultra-platinum-at-happy-pretzel.md.

Schedule: hourly via cron (half-hour offset).

Master switch:
  CORA_SELF_HEALING_ENABLED env var. Default `false`. When false, this
  task returns 0 without touching DB, Redis, or any subsystem.

State machine (per metric × county tuple):

    read current_value (Redis cache)
    read baseline_value (compute_baseline — 7d rolling avg)
    read open_incident (raw SQL)
    severity = grade(current_value, threshold)

    green AND no open       → no-op
    green AND open          → close incident (action='resolved')
    yellow/red AND no open  → INSERT incident + post_to_slack("new")
    yellow/red AND open:
        duration < threshold  → no-op (still observing)
        duration ≥ threshold AND auto_action_type == "fallback_enabled":
            Redis SET kill_switch:{feature_flag}=red
            UPDATE incident.action_taken='fallback_enabled'
            post_to_slack("action_taken")
        duration ≥ threshold AND auto_action_type == "auto_paused" AND A/B:
            ab_engine.complete_test(test_name, winner=control)
            UPDATE incident.action_taken='auto_paused'
            post_to_slack("action_taken")
        duration ≥ threshold AND (requires_approval OR human_escalated):
            UPDATE incident.action_taken='human_escalated'
            post_to_slack("human_required")
        red AND days_red ≥ kill_after_red_days:
            UPDATE incident.action_taken='feature_killed'  # recommendation only
            post_to_slack("kill_recommended")

Rate limits (from CORA_SELF_HEALING in cora_guardrails):
  - max_actions_per_run: 3
  - max_feature_kill_recommendations_per_day: 1
  - max_new_incidents_per_hour: 5

All DB I/O is raw SQL via sa_text (no ORM filter chains).

Usage:
    python -m src.tasks.cora_self_healing
    python -m src.tasks.cora_self_healing --dry-run
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any, Optional

from sqlalchemy import text as sa_text
from sqlalchemy.orm import Session

from config.cora_guardrails import CORA_SELF_HEALING, KILL_SWITCH
from config.settings import get_settings
from src.core.database import get_db_context
from src.core.redis_client import redis_available, rset
from src.services.cora_slack import post_incident_alert
from src.tasks.kill_switch_metric_ingest import compute_baseline, get_cached_metric

logger = logging.getLogger(__name__)


# Severity grading -----------------------------------------------------------

def _grade(metric_name: str, observed: Optional[float]) -> str:
    """Return 'green' | 'yellow' | 'red' | 'unknown' for `observed`.

    Uses the KILL_SWITCH thresholds for `metric_name`. Respects the
    `direction` field: higher_is_better or lower_is_better. Unknown
    metrics or None observed → 'unknown' (caller treats as no-op).
    """
    cfg = KILL_SWITCH.get(metric_name)
    if cfg is None or observed is None:
        return "unknown"
    green = cfg["green"]
    red = cfg["red"]
    direction = cfg.get("direction", "higher_is_better")

    if direction == "higher_is_better":
        if observed >= green:
            return "green"
        if observed < red:
            return "red"
        return "yellow"
    # lower_is_better
    if observed <= green:
        return "green"
    if observed > red:
        return "red"
    return "yellow"


# Raw SQL helpers ------------------------------------------------------------

def _find_open_incident(
    db: Session, metric_name: str, county_id: Optional[str], feature_name: Optional[str],
) -> Optional[Any]:
    return db.execute(sa_text("""
        SELECT id, metric_name, county_id, feature_name, severity,
               observed_value, threshold_value, baseline_value,
               breach_started, action_taken
        FROM cora_incident
        WHERE metric_name = :metric
          AND (county_id     = :county    OR (:county    IS NULL AND county_id    IS NULL))
          AND (feature_name  = :feature   OR (:feature   IS NULL AND feature_name IS NULL))
          AND breach_resolved IS NULL
        ORDER BY breach_started DESC
        LIMIT 1
    """), {"metric": metric_name, "county": county_id, "feature": feature_name}).first()


def _open_incident(
    db: Session,
    *,
    metric_name: str,
    county_id: Optional[str],
    feature_name: Optional[str],
    severity: str,
    observed_value: float,
    threshold_value: float,
    baseline_value: Optional[float],
    details: Optional[dict] = None,
) -> int:
    row = db.execute(sa_text("""
        INSERT INTO cora_incident (
            metric_name, county_id, feature_name, severity,
            observed_value, threshold_value, baseline_value,
            breach_started, action_taken, action_details, created_at, updated_at
        ) VALUES (
            :metric, :county, :feature, :severity,
            :observed, :threshold, :baseline,
            NOW(), 'no_op', CAST(:details AS jsonb), NOW(), NOW()
        )
        RETURNING id
    """), {
        "metric":   metric_name,
        "county":   county_id,
        "feature":  feature_name,
        "severity": severity,
        "observed": observed_value,
        "threshold": threshold_value,
        "baseline": baseline_value,
        "details":  json.dumps(details or {}),
    }).first()
    return int(row.id)


def _close_incident(db: Session, incident_id: int) -> None:
    db.execute(sa_text("""
        UPDATE cora_incident
        SET breach_resolved = NOW(),
            duration_hours  = GREATEST(0, EXTRACT(EPOCH FROM (NOW() - breach_started))::INTEGER / 3600),
            action_taken    = 'resolved',
            updated_at      = NOW()
        WHERE id = :id
    """), {"id": incident_id})


def _record_action(
    db: Session, incident_id: int, action: str, details: Optional[dict] = None,
) -> None:
    db.execute(sa_text("""
        UPDATE cora_incident
        SET action_taken   = :action,
            action_details = CAST(:details AS jsonb),
            updated_at     = NOW()
        WHERE id = :id
    """), {"id": incident_id, "action": action, "details": json.dumps(details or {})})


def _incident_age_hours(breach_started: datetime) -> float:
    """Wall-clock duration the breach has been open. Robust to naive
    timestamps coming back from psycopg2."""
    now = datetime.now(timezone.utc)
    if breach_started.tzinfo is None:
        breach_started = breach_started.replace(tzinfo=timezone.utc)
    return (now - breach_started).total_seconds() / 3600


# Rate-limit helpers (raw SQL) -----------------------------------------------

def _count_today_kill_recommendations(db: Session) -> int:
    row = db.execute(sa_text("""
        SELECT COUNT(*) AS c FROM cora_incident
        WHERE action_taken = 'feature_killed'
          AND created_at >= DATE_TRUNC('day', NOW())
    """)).first()
    return int(row.c) if row else 0


def _count_new_incidents_last_hour(db: Session) -> int:
    row = db.execute(sa_text("""
        SELECT COUNT(*) AS c FROM cora_incident
        WHERE breach_started >= NOW() - INTERVAL '1 hour'
    """)).first()
    return int(row.c) if row else 0


# Action dispatch ------------------------------------------------------------

def _apply_fallback(feature_flag: str) -> None:
    """Flip the Redis kill_switch:{feature_flag} key to 'red' with a
    24h TTL. The Cora graph decision_hierarchy reads this on the next
    decision and routes to fallback template copy / disabled path.
    """
    if not redis_available():
        logger.warning("[cora-self-heal] redis unavailable — cannot apply fallback %s", feature_flag)
        return
    rset(f"kill_switch:{feature_flag}", "red", ttl_seconds=24 * 3600)


def _apply_ab_pause(db: Session, metric_name: str) -> Optional[dict]:
    """If `metric_name` maps to an active A/B test that should be rolled
    back, call ab_engine.complete_test(winner=control). Returns a details
    dict describing the action, or None when no eligible test exists.

    Looking up the test name is a thin convention: KILL_SWITCH[metric].fallback_feature_flag
    can encode the A/B test name when auto_action_type == "auto_paused".
    For Stage 1, no metrics are wired to "auto_paused" — the live A/B
    rollback runs in ab_rollback_check.py daily. This is the integration
    point if a future metric flips to "auto_paused".
    """
    try:
        from src.services.ab_engine import should_rollback, complete_test
    except ImportError:
        return None

    cfg = KILL_SWITCH.get(metric_name, {})
    test_name = cfg.get("fallback_feature_flag")
    if not test_name:
        return None

    if not should_rollback(test_name, db):
        return {"test_name": test_name, "skipped": "should_rollback returned False"}

    complete_test(test_name, winner="a", db=db)
    return {"test_name": test_name, "promoted": "a"}


# Per-metric step ------------------------------------------------------------

class _Counters:
    __slots__ = ("actions_taken", "incidents_opened", "kill_recs_today")

    def __init__(self, kill_recs_today: int) -> None:
        self.actions_taken = 0
        self.incidents_opened = 0
        self.kill_recs_today = kill_recs_today


def _process_metric(
    db: Session,
    *,
    metric_name: str,
    county_id: Optional[str],
    feature_name: Optional[str],
    counters: _Counters,
    dry_run: bool,
) -> dict:
    """Run the state machine for a single (metric, county, feature). Returns
    a dict describing what happened — used for the run summary log line."""
    cfg = KILL_SWITCH.get(metric_name)
    if cfg is None:
        return {"metric": metric_name, "skipped": "unknown_metric"}

    observed = get_cached_metric(metric_name, county_id=county_id)
    baseline = compute_baseline(db, metric_name, county_id) if county_id else None
    severity = _grade(metric_name, observed)

    open_incident = _find_open_incident(db, metric_name, county_id, feature_name)

    if severity == "green":
        if open_incident is not None:
            if not dry_run:
                _close_incident(db, open_incident.id)
                post_incident_alert(open_incident, kind="resolved", action_summary="metric back in green")
            return {"metric": metric_name, "result": "closed_resolved"}
        return {"metric": metric_name, "result": "green_no_op"}

    if severity == "unknown":
        return {"metric": metric_name, "result": "skipped_unknown",
                "observed": observed, "baseline": baseline}

    # severity is 'yellow' or 'red'.
    threshold = cfg["red"] if severity == "red" else cfg["green"]
    duration_for_action = cfg.get("duration_hours_for_action", 48)
    kill_after_red_days = cfg.get("kill_after_red_days", 7)
    auto_action_type = cfg.get("auto_action_type", "human_escalated")
    fallback_feature_flag = cfg.get("fallback_feature_flag")
    requires_approval = cfg.get("requires_approval", True)

    if open_incident is None:
        # Honor hourly new-incident rate limit.
        if counters.incidents_opened + _count_new_incidents_last_hour(db) >= CORA_SELF_HEALING["max_new_incidents_per_hour"]:
            return {"metric": metric_name, "result": "skipped_rate_limit_new_incidents"}
        if dry_run:
            return {"metric": metric_name, "result": "would_open", "severity": severity}
        new_id = _open_incident(
            db,
            metric_name=metric_name,
            county_id=county_id,
            feature_name=feature_name,
            severity=severity,
            observed_value=float(observed),
            threshold_value=float(threshold),
            baseline_value=baseline,
        )
        counters.incidents_opened += 1
        # Re-fetch so the post-to-slack call sees the canonical row.
        fresh = _find_open_incident(db, metric_name, county_id, feature_name)
        post_incident_alert(
            fresh, kind="new",
            action_summary=f"observed={observed}, threshold={threshold}, baseline={baseline}",
        )
        return {"metric": metric_name, "result": "opened", "id": new_id, "severity": severity}

    # An incident is already open for this (metric, county, feature).
    age_h = _incident_age_hours(open_incident.breach_started)

    # Already past 7-day red trigger?
    age_days = age_h / 24
    if severity == "red" and age_days >= kill_after_red_days:
        if open_incident.action_taken == "feature_killed":
            return {"metric": metric_name, "result": "already_kill_recommended"}
        if counters.kill_recs_today >= CORA_SELF_HEALING["max_feature_kill_recommendations_per_day"]:
            return {"metric": metric_name, "result": "skipped_rate_limit_kill"}
        if counters.actions_taken >= CORA_SELF_HEALING["max_actions_per_run"]:
            return {"metric": metric_name, "result": "skipped_rate_limit_actions"}
        if dry_run:
            return {"metric": metric_name, "result": "would_recommend_kill"}
        _record_action(db, open_incident.id, "feature_killed",
                       {"reason": "red for >= kill_after_red_days",
                        "age_days": round(age_days, 1)})

        # fa036 — write a `cora_playbook` recommendation row so the kill
        # appears in Metric 5 ("net new playbooks Cora authored") and is
        # discoverable through the admin endpoint for human approval.
        # Idempotent via source_key dedupe — re-running the loop doesn't
        # create duplicate rows for the same metric breach.
        try:
            from src.services.playbook_writer import upsert_recommendation
            upsert_recommendation(
                db,
                name=f"kill_recommendation:{metric_name}",
                description=(
                    f"Metric {metric_name} red for {age_days:.1f} days — "
                    f"recommend feature kill"
                ),
                pattern={
                    "metric": metric_name,
                    "feature_flag": cfg.get("fallback_feature_flag"),
                    "age_days": round(age_days, 1),
                },
                source_type="self_healing_kill",
                source_id=metric_name,
                authored_by="cora",
            )
        except Exception:
            # Playbook write is auxiliary — never fail the self-healing
            # loop because the recommendation log had an issue.
            logger.warning(
                "[cora-self-heal] playbook write failed for %s — incident still recorded",
                metric_name, exc_info=True,
            )

        counters.actions_taken += 1
        counters.kill_recs_today += 1
        post_incident_alert(
            open_incident, kind="kill_recommended",
            action_summary=f"metric red for {age_days:.1f} days — recommending feature kill, awaits human approval",
        )
        return {"metric": metric_name, "result": "kill_recommended"}

    # Inside the action duration threshold?
    if age_h < duration_for_action:
        return {"metric": metric_name, "result": "observing",
                "age_hours": round(age_h, 1), "needs_hours": duration_for_action}

    # Threshold met — already actioned?
    if open_incident.action_taken not in ("no_op",):
        return {"metric": metric_name, "result": "already_actioned",
                "action": open_incident.action_taken}

    if counters.actions_taken >= CORA_SELF_HEALING["max_actions_per_run"]:
        return {"metric": metric_name, "result": "skipped_rate_limit_actions"}

    # Pick the action.
    if auto_action_type == "fallback_enabled" and fallback_feature_flag and not requires_approval:
        if dry_run:
            return {"metric": metric_name, "result": "would_apply_fallback",
                    "flag": fallback_feature_flag}
        _apply_fallback(fallback_feature_flag)
        _record_action(db, open_incident.id, "fallback_enabled",
                       {"flag": fallback_feature_flag, "age_hours": round(age_h, 1)})
        counters.actions_taken += 1
        post_incident_alert(
            open_incident, kind="action_taken",
            action_summary=f"applied fallback: kill_switch:{fallback_feature_flag}=red",
        )
        return {"metric": metric_name, "result": "fallback_enabled",
                "flag": fallback_feature_flag}

    if auto_action_type == "auto_paused":
        if dry_run:
            return {"metric": metric_name, "result": "would_apply_ab_pause"}
        ab_details = _apply_ab_pause(db, metric_name)
        if ab_details and "promoted" in ab_details:
            _record_action(db, open_incident.id, "auto_paused", ab_details)
            counters.actions_taken += 1
            post_incident_alert(
                open_incident, kind="action_taken",
                action_summary=f"A/B variant paused via ab_engine: {ab_details}",
            )
            return {"metric": metric_name, "result": "ab_paused", **ab_details}
        # Fell through (no eligible test) — fall back to human escalation.

    # Default safe action: escalate to human.
    if dry_run:
        return {"metric": metric_name, "result": "would_escalate_human"}
    _record_action(db, open_incident.id, "human_escalated",
                   {"reason": "duration threshold met; requires_approval or no auto-action available",
                    "age_hours": round(age_h, 1)})
    counters.actions_taken += 1
    post_incident_alert(
        open_incident, kind="human_required",
        action_summary=f"metric breach {age_h:.1f}h old — requires human review (auto_action={auto_action_type}, requires_approval={requires_approval})",
    )
    return {"metric": metric_name, "result": "human_escalated", "age_hours": round(age_h, 1)}


# Orchestrator ---------------------------------------------------------------

def run_self_healing(dry_run: bool = False) -> dict:
    """Run one pass of the self-healing loop. Returns a dict summarising
    actions per metric. Idempotent — re-running with no metric change is
    a no-op (the rate-limit + open-incident lookup keep things stable)."""
    settings = get_settings()
    county_id = settings.county_launch_source_county
    if not county_id:
        county_id = "hillsborough"

    results = []
    with get_db_context() as db:
        kill_recs_today = _count_today_kill_recommendations(db)
        counters = _Counters(kill_recs_today=kill_recs_today)
        for metric_name in KILL_SWITCH.keys():
            results.append(
                _process_metric(
                    db,
                    metric_name=metric_name,
                    county_id=county_id,
                    feature_name=None,
                    counters=counters,
                    dry_run=dry_run,
                )
            )

    summary = {
        "county_id": county_id,
        "actions_taken": counters.actions_taken,
        "incidents_opened": counters.incidents_opened,
        "kill_recommendations_today": counters.kill_recs_today,
        "dry_run": dry_run,
        "per_metric": results,
    }
    logger.info("[cora-self-heal] %s", json.dumps(summary, default=str))
    return summary


def main(argv: Optional[list[str]] = None) -> int:
    """Entry point.

    Gated by `CORA_SELF_HEALING_ENABLED` env var, default `false`. When
    disabled, returns 0 without reading any DB or Redis state.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )
    args = set(argv or sys.argv[1:])
    dry_run = "--dry-run" in args

    settings = get_settings()
    if not settings.cora_self_healing_enabled:
        logger.info(
            "[cora-self-heal] disabled via CORA_SELF_HEALING_ENABLED — exiting cleanly"
        )
        return 0

    summary = run_self_healing(dry_run=dry_run)
    if dry_run:
        # Pretty-print for operator visibility.
        print(json.dumps(summary, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
