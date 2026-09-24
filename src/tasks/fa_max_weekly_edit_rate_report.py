"""FA Max weekly edit-rate report (WP-T2-2 item — Friday operations report).

WP-T3-2 extension: now renders a per-agent edit log (edit rate, trend,
categories, biggest edit) using fa_max_edit_log.build_rollup. The cron,
delivery path, rule name, and "no approvals" message are unchanged.

Delivery via durable exceptions_alert_queue (crash-safe, dedup-by-rule).

Run (Friday only, per crontab.txt):
    python -m src.tasks.fa_max_weekly_edit_rate_report
"""
from __future__ import annotations

import argparse
import logging

from src.core.database import get_db_context
from src.services.fa_max_autonomy import FA_MAX_VENTURE
from src.services.fa_max_edit_log import AgentRollup, build_rollup, count_uncaptured
from src.services.relay import exceptions_alert_queue

logger = logging.getLogger(__name__)

_RULE = "fa_max_weekly_edit_rate_report"


def _format_trend(rollup: AgentRollup) -> str:
    """Arrow + prior-4w rate, or '—' when no prior data."""
    if not rollup.rate_prior_4w_has_data:
        return "—"
    delta = rollup.rate_this_week - rollup.rate_prior_4w
    arrow = "↑" if delta > 0.01 else ("↓" if delta < -0.01 else "→")
    return f"prior 4w {rollup.rate_prior_4w:.1%} {arrow}"


def _format_rollup_line(rollup: AgentRollup) -> list[str]:
    """Return 1–3 lines for one (agent, tier) entry."""
    gate_flag = " ⚠" if rollup.over_gate else ""
    trend = _format_trend(rollup)
    header = (
        f"• `{rollup.agent_name}` (tier {rollup.tier}) — "
        f"{rollup.rate_this_week:.1%}{gate_flag} ({trend}) · "
        f"{rollup.n_material} material / {rollup.n_edited} edited / {rollup.n_decided} decided"
    )
    lines = [header]

    if rollup.top_categories:
        cats = ", ".join(rollup.top_categories)
        lines.append(f"    top: {cats}")

    if rollup.biggest_edit:
        e = rollup.biggest_edit
        snippet = e.diff.replace("\n", " ")
        if len(snippet) > 300:
            snippet = snippet[:297] + "…"
        pct = int(e.change_ratio * 100)
        lines.append(f"    biggest: #{e.item_id} ({pct}% changed) \"{snippet}\"")

    return lines


def build_report(*, dry_run: bool = False) -> str:
    """Compute the report body. Exposed for tests."""
    with get_db_context() as session:
        rollups = build_rollup(session)
        if not rollups:
            return "No FA Max human approvals recorded yet this week."

        from datetime import datetime, timezone, timedelta
        from zoneinfo import ZoneInfo
        eastern = ZoneInfo("America/New_York")
        now_et = datetime.now(eastern)
        week_start_et = (now_et - timedelta(days=now_et.weekday())).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        week_start_utc = week_start_et.astimezone(timezone.utc)
        week_end_utc = week_start_utc + timedelta(days=7)
        uncaptured = count_uncaptured(session, week_start=week_start_utc, week_end=week_end_utc)

        # strftime "%-d" is Linux-only; use lstrip("0") for cross-platform day
        week_label = week_start_et.strftime("%b ") + str(week_start_et.day)
        header = f"*FA Max weekly edit log*" + (f" — week of {week_label}" if week_label else "")
        lines = [header]

        for rollup in rollups:
            lines.extend(_format_rollup_line(rollup))

        if uncaptured:
            lines.append(
                f"_{uncaptured} revised draft{'s' if uncaptured != 1 else ''} this week "
                f"had no captured original (pre-fix rows)._"
            )

        return "\n".join(lines)


def run(*, dry_run: bool = False) -> bool:
    message = build_report(dry_run=dry_run)
    if dry_run:
        logger.info("[fa_max_weekly_edit_rate_report][DRY]\n%s", message)
        return True
    return exceptions_alert_queue.enqueue_and_attempt(
        venture_key=FA_MAX_VENTURE, rule=_RULE, message=message,
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(description="FA Max weekly edit-rate report")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    delivered = run(dry_run=args.dry_run)
    logger.info("fa_max_weekly_edit_rate_report: delivered=%s", delivered)
