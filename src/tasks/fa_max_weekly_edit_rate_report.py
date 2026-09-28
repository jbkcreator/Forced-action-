"""FA Max weekly edit-rate report (WP-T2-2 item — Friday operations report).

Posts each (agent_name, autonomy_tier_at_send) pair's current-ISO-week edit
rate to Slack every Friday. This is the Done-When line's "Friday edit-rate
report" requirement — distinct from WP-T2-12's Command Center Friday
scorecard (a different, adjacent report; this one is not superseded by or
folded into that one, per the WP-T2-2 scope table).

WP-T3-2 extends the body into the weekly edit log: per pair, this week's rate
against the prior 4 weeks, material / edited / decided counts, the most
common edit categories and the biggest edit of the week, with ⚠ when the
rate is at or over the Tier B gate. Rates come from
fa_max_edit_log.build_rollup, which reads the same population and
material_edit count as the graduation gate (fa_max_autonomy).

Discovers which (agent_name, tier) pairs to report on from relay_approval_queue
itself rather than a hardcoded agent list -- there is no canonical fixed
roster of FA Max agent names anywhere in this codebase (agent_name is a
free-text column, see src/services/relay/queue.py's own comment). A pair with
zero human approvals this week is not reported.

Delivery reuses src.services.relay.exceptions_alert_queue.enqueue_and_attempt
-- the same durable-alert pattern src.tasks.fa_max_send_health_monitor
already uses: the report is committed to fa_max_exceptions_alert_queue
BEFORE Slack is contacted, so a Slack outage or a mid-attempt crash leaves it
recoverable by the existing drain worker
(src.tasks.fa_max_exceptions_alert_drain) rather than silently lost. Reusing
this queue also gets audience/channel routing (EXCEPTIONS lane) and the
existing dedup-by-rule mechanism for free, rather than building a second
Slack-posting path for a report.

Run (Friday only, per crontab.txt):
    python -m src.tasks.fa_max_weekly_edit_rate_report
"""
from __future__ import annotations

import argparse
import logging
from datetime import datetime, timezone
from typing import Optional
from zoneinfo import ZoneInfo

from src.core.database import get_db_context
from src.services.fa_max_autonomy import FA_MAX_VENTURE, iso_week_bounds
from src.services.fa_max_edit_log import AgentRollup, build_rollup, count_uncaptured
from src.services.relay import exceptions_alert_queue

logger = logging.getLogger(__name__)

_RULE = "fa_max_weekly_edit_rate_report"
_EASTERN = ZoneInfo("America/New_York")
_SNIPPET_MAX = 300
_TREND_DEADBAND = 0.01


def _trend(rollup: AgentRollup) -> str:
    prior = rollup.rate_prior_4w
    if prior is None:
        return "prior 4w — "
    delta = rollup.rate_this_week - prior
    arrow = "↑" if delta > _TREND_DEADBAND else "↓" if delta < -_TREND_DEADBAND else "→"
    return f"prior 4w {prior:.1%} {arrow}"


def _snippet(diff: str) -> str:
    flat = " ".join(diff.split())
    return flat if len(flat) <= _SNIPPET_MAX else flat[: _SNIPPET_MAX - 1] + "…"


def _rollup_lines(rollup: AgentRollup) -> list[str]:
    counts = rollup.this_week
    gate = " ⚠" if rollup.over_gate else ""
    lines = [
        f"• `{rollup.agent_name}` (tier {rollup.tier}) — {rollup.rate_this_week:.1%}{gate} "
        f"({_trend(rollup)}) · {counts.material} material / {counts.revised} edited / "
        f"{counts.decided} decided"
    ]
    if rollup.top_categories:
        lines.append("    top: " + ", ".join(f"{c} ×{n}" for c, n in rollup.top_categories))
    if rollup.biggest_edit:
        edit = rollup.biggest_edit
        lines.append(
            f"    biggest: #{edit.item_id} ({edit.change_ratio:.0%} changed) \"{_snippet(edit.diff)}\""
        )
    return lines


def build_report(*, now: Optional[datetime] = None) -> str:
    """Compute the report body. Exposed separately from run() so tests can
    assert on content without touching the alert queue."""
    now = now or datetime.now(timezone.utc)
    week_start, week_end = iso_week_bounds(now)
    with get_db_context() as session:
        rollups = build_rollup(session, now=now)
        if not rollups:
            return "No FA Max human approvals recorded yet this week."
        uncaptured = count_uncaptured(session, window_start=week_start, window_end=week_end)

    week_start_et = week_start.astimezone(_EASTERN)
    lines = [f"*FA Max weekly edit log* — week of {week_start_et:%b} {week_start_et.day}"]
    for rollup in rollups:
        lines.extend(_rollup_lines(rollup))
    if uncaptured:
        noun = "draft" if uncaptured == 1 else "drafts"
        lines.append(f"_{uncaptured} revised {noun} this week had no captured original (pre-fix rows)._")
    return "\n".join(lines)


def run(*, dry_run: bool = False) -> bool:
    message = build_report()
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
