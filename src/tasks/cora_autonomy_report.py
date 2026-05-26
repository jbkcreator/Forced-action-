"""
Weekly Cora Autonomy Scorecard (fa036).

Computes five metrics over the last 7 days and writes a single
`learning_cards` row with `card_type='autonomy_summary'`. The row is
keyed by Monday-of-the-week, so re-runs within the same week overwrite
idempotently via the existing `(card_date, card_type)` unique constraint.
The Monday-09:00 Revenue Pulse picks up the latest row and appends a
one-line summary to the founder SMS.

Metrics (per FA-2B-v9-FINAL §Weekly Cora Autonomy Scorecard):

  1. autonomous_pct          — % decisions Cora made without human approval
                                = count(autonomy_class='autonomous')
                                / count(autonomy_class IS NOT NULL)

  2. overridden_pct          — % autonomous decisions later rejected/overridden
                                Denominator uses the sticky `was_autonomous`
                                flag so rows that flipped to 'overridden'
                                still count. Numerator counts
                                overridden_at IS NOT NULL OR
                                autonomy_class IN ('rejected','overridden').

  3. recommended_adoptions   — count(cora_playbook adopted in window)

  4. approval_latency_seconds — median(first_autonomous_apply.completed_at -
                                playbook.adopted_at), per playbook adopted
                                in window. Returns null when there are no
                                paired (playbook, autonomous decision) rows
                                — honest "not enough lifecycle data yet."

  5. net_new_playbooks       — Cora-authored playbooks in window - retired
                                playbooks in window.

The task is honest: when a metric can't be computed (no classified rows,
no adopted playbooks, no approval cycles yet), it returns None with a
clear note. Revenue Pulse renders these as "n/a", not "0".

Schedule: Monday 08:45 UTC (cron entry in scripts/cron/crontab.txt) —
15 minutes before the weekly Revenue Pulse so the autonomy card is
fresh when the pulse reads it.

Usage:
    python -m src.tasks.cora_autonomy_report
    python -m src.tasks.cora_autonomy_report --dry-run
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import text as sa_text
from sqlalchemy.orm import Session

from src.core.database import get_db_context

logger = logging.getLogger(__name__)


WINDOW_DAYS = 7


# ── Metric computations (all raw SQL) ─────────────────────────────────────


def _metric_1_autonomous_pct(db: Session, since: datetime) -> Optional[float]:
    """% of CLASSIFIED decisions that were autonomous. None when no
    classified rows in the window (the scorecard says "n/a" honestly).
    """
    row = db.execute(sa_text("""
        SELECT
            COUNT(*) FILTER (WHERE autonomy_class = 'autonomous') AS autonomous,
            COUNT(*) FILTER (WHERE autonomy_class IS NOT NULL)    AS classified
        FROM agent_decisions
        WHERE started_at >= :since
    """), {"since": since}).first()
    if row is None or not row.classified:
        return None
    return round(row.autonomous / row.classified * 100, 1)


def _metric_2_overridden_pct(db: Session, since: datetime) -> Optional[float]:
    """% of originally-autonomous decisions later rejected or overridden.

    Denominator uses the sticky `was_autonomous=TRUE` flag so a row that
    flipped to autonomy_class='overridden' still counts — without this
    the metric would exclude exactly the overrides it's supposed to
    measure (correction #3 from the plan review).
    """
    row = db.execute(sa_text("""
        SELECT
            COUNT(*) AS denom,
            COUNT(*) FILTER (
                WHERE overridden_at IS NOT NULL
                   OR autonomy_class IN ('rejected', 'overridden')
            ) AS reversed
        FROM agent_decisions
        WHERE started_at >= :since
          AND was_autonomous = TRUE
    """), {"since": since}).first()
    if row is None or not row.denom:
        return None
    return round(row.reversed / row.denom * 100, 1)


def _metric_3_adoptions(db: Session, since: datetime) -> int:
    """Count of playbooks adopted in the window."""
    return db.execute(sa_text("""
        SELECT COUNT(*) FROM cora_playbook
        WHERE status = 'adopted' AND adopted_at >= :since
    """), {"since": since}).scalar() or 0


def _metric_4_approval_latency(db: Session, since: datetime) -> tuple[Optional[float], Optional[str]]:
    """Median time (seconds) from playbook.adopted_at to first autonomous
    decision linked to that playbook. None + note string when no lifecycle
    data exists.

    Uses agent_decisions.playbook_id FK (correction #4 from plan review)
    rather than the decision-level approved_at column — the spec metric is
    about a PATTERN's adopted_at → first autonomous APPLICATION OF THE
    PATTERN, not per-decision approval timing.
    """
    row = db.execute(sa_text("""
        WITH adopted_playbooks AS (
            SELECT id, adopted_at
            FROM cora_playbook
            WHERE adopted_at >= :since
              AND status = 'adopted'
        ),
        first_autonomous_apply AS (
            SELECT
                ap.id          AS playbook_id,
                ap.adopted_at  AS adopted_at,
                MIN(ad.completed_at) AS first_applied_at
            FROM adopted_playbooks ap
            JOIN agent_decisions ad ON ad.playbook_id = ap.id
            WHERE ad.autonomy_class = 'autonomous'
              AND ad.completed_at IS NOT NULL
              AND ad.completed_at > ap.adopted_at
            GROUP BY ap.id, ap.adopted_at
        )
        SELECT
            PERCENTILE_CONT(0.5) WITHIN GROUP (
                ORDER BY EXTRACT(EPOCH FROM (first_applied_at - adopted_at))
            ) AS median_seconds,
            COUNT(*) AS sample_size
        FROM first_autonomous_apply
    """), {"since": since}).first()

    if row is None or row.median_seconds is None or not (row.sample_size or 0):
        return None, "not enough approval lifecycle data yet"
    return float(row.median_seconds), None


def _metric_5_net_new_playbooks(db: Session, since: datetime) -> int:
    """Cora-authored playbooks created in window MINUS retired in window."""
    row = db.execute(sa_text("""
        SELECT
            COUNT(*) FILTER (
                WHERE authored_by = 'cora' AND authored_at >= :since
            ) AS authored,
            COUNT(*) FILTER (
                WHERE status = 'retired' AND retired_at >= :since
            ) AS retired
        FROM cora_playbook
    """), {"since": since}).first()
    if row is None:
        return 0
    return int((row.authored or 0) - (row.retired or 0))


# ── Composition + persistence ─────────────────────────────────────────────


def compute_autonomy_metrics(db: Session) -> dict:
    """Pure function — computes the five-metric dict over the last 7 days."""
    now = datetime.now(timezone.utc)
    since = now - timedelta(days=WINDOW_DAYS)

    autonomous_pct = _metric_1_autonomous_pct(db, since)
    overridden_pct = _metric_2_overridden_pct(db, since)
    adoptions      = _metric_3_adoptions(db, since)
    latency_sec, latency_note = _metric_4_approval_latency(db, since)
    net_new        = _metric_5_net_new_playbooks(db, since)

    return {
        "window_start":             since.isoformat(),
        "window_end":                now.isoformat(),
        "window_days":               WINDOW_DAYS,
        "autonomous_pct":            autonomous_pct,
        "overridden_pct":            overridden_pct,
        "recommended_adoptions":     int(adoptions),
        "approval_latency_seconds":  latency_sec,
        "approval_latency_note":     latency_note,
        "net_new_playbooks":         int(net_new),
    }


def format_summary_text(m: dict) -> str:
    """One-line human-readable summary for the learning_cards.summary_text
    column. Honest about missing values."""
    def pct(v):
        return f"{v}%" if v is not None else "n/a"
    parts = [
        f"autonomous {pct(m.get('autonomous_pct'))}",
        f"overridden {pct(m.get('overridden_pct'))}",
        f"adoptions {m.get('recommended_adoptions', 0)}",
        f"net playbooks {m.get('net_new_playbooks', 0):+d}",
    ]
    latency = m.get("approval_latency_seconds")
    if latency is not None:
        # Show in hours for readability.
        hours = latency / 3600
        parts.append(f"approval→auto latency {hours:.1f}h")
    else:
        parts.append("approval→auto latency n/a")
    return "Cora autonomy: " + ", ".join(parts)


def _write_learning_card(db: Session, metrics: dict, summary: str) -> None:
    """Upsert one learning_cards row keyed by Monday-of-the-week.

    `card_date` is set to the most recent Monday (DOW=1) at the time the
    task runs. Re-runs within the same week overwrite. New weeks create
    a new row.
    """
    db.execute(sa_text("""
        INSERT INTO learning_cards (card_date, card_type, summary_text, data_json, created_at)
        VALUES (
            (CURRENT_DATE - ((EXTRACT(DOW FROM CURRENT_DATE)::INT + 6) % 7))::date,
            'autonomy_summary',
            :summary,
            CAST(:data AS jsonb),
            NOW()
        )
        ON CONFLICT (card_date, card_type)
        DO UPDATE SET summary_text = EXCLUDED.summary_text,
                      data_json    = EXCLUDED.data_json
    """), {"summary": summary, "data": json.dumps(metrics)})


def _post_slack_summary(metrics: dict, summary: str) -> None:
    """Post the autonomy summary to Slack via the existing cora_slack
    incident-helper. Reuses the WebClient/email-fallback path so we don't
    duplicate infrastructure. On any failure, log + continue — never
    let an alert path block the metric write.
    """
    try:
        from types import SimpleNamespace
        from src.services.cora_slack import post_incident_alert
        # Shape a row-like for the existing helper. Severity is informational
        # (no actual incident severity — the autonomy summary is a weekly
        # heads-up).
        fake_row = SimpleNamespace(
            metric_name="cora_autonomy",
            severity="yellow",   # neutral — neither green nor red
            observed_value=metrics.get("autonomous_pct"),
            threshold_value=None,
            baseline_value=None,
            county_id=None,
            feature_name=None,
            action_taken="weekly_summary",
            duration_hours=metrics.get("window_days", WINDOW_DAYS) * 24,
            breach_started=metrics.get("window_start"),
        )
        post_incident_alert(fake_row, kind="action_taken", action_summary=summary)
    except Exception:
        logger.warning(
            "[cora-autonomy] Slack post failed; metric still persisted",
            exc_info=True,
        )


def run_autonomy_report(dry_run: bool = False) -> dict:
    """End-to-end: compute → write learning_cards row → post to Slack."""
    with get_db_context() as db:
        metrics = compute_autonomy_metrics(db)
        summary = format_summary_text(metrics)

        logger.info("[cora-autonomy] %s", summary)
        logger.info("[cora-autonomy] raw metrics: %s", json.dumps(metrics, default=str))

        if dry_run:
            return {"dry_run": True, "summary": summary, "metrics": metrics}

        _write_learning_card(db, metrics, summary)

    # Slack post is outside the session — runs even if DB commit succeeded.
    _post_slack_summary(metrics, summary)
    return {"dry_run": False, "summary": summary, "metrics": metrics}


def main(argv: Optional[list[str]] = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )
    args = set(argv or sys.argv[1:])
    dry_run = "--dry-run" in args
    result = run_autonomy_report(dry_run=dry_run)
    if dry_run:
        print(json.dumps(result, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
