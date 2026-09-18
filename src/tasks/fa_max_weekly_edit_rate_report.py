"""FA Max weekly edit-rate report (WP-T2-2 item — Friday operations report).

Posts each (agent_name, autonomy_tier_at_send) pair's current-ISO-week edit
rate (fa_max_autonomy.get_weekly_edit_rate) to Slack every Friday. This is
the Done-When line's "Friday edit-rate report" requirement — distinct from
WP-T2-12's Command Center Friday scorecard (a different, adjacent report;
this one is not superseded by or folded into that one, per the WP-T2-2
scope table).

Discovers which (agent_name, tier) pairs to report on by querying
relay_approval_queue directly for any pair with at least one 'sent' row in
the current week, rather than a hardcoded agent list -- there is no
canonical fixed roster of FA Max agent names anywhere in this codebase
(agent_name is a free-text column, see src/services/relay/queue.py's own
comment). A pair with zero sends this week is not reported (nothing to say).

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

from sqlalchemy import text

from src.core.database import get_db_context
from src.services.fa_max_autonomy import FA_MAX_VENTURE, get_weekly_edit_rate
from src.services.relay import exceptions_alert_queue

logger = logging.getLogger(__name__)

_RULE = "fa_max_weekly_edit_rate_report"


def _agent_tier_pairs_with_sends_this_week(session) -> list[tuple[str, str]]:
    """(agent_name, autonomy_tier_at_send) pairs with >=1 'sent' row so far
    in the current ISO week (America/New_York), matching get_weekly_edit_
    rate()'s own week-start computation so the roster and the rate it reports
    agree on what "this week" means."""
    from datetime import timedelta
    from zoneinfo import ZoneInfo

    eastern = ZoneInfo("America/New_York")
    now_eastern = datetime.now(eastern)
    week_start_eastern = (now_eastern - timedelta(days=now_eastern.weekday())).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    week_start_utc = week_start_eastern.astimezone(timezone.utc)

    rows = session.execute(
        text(
            "SELECT DISTINCT agent_name, autonomy_tier_at_send "
            "FROM relay_approval_queue "
            "WHERE venture_key = :v AND status = 'sent' "
            "AND agent_name IS NOT NULL AND autonomy_tier_at_send IS NOT NULL "
            "AND dispatched_at >= :week_start "
            "ORDER BY agent_name, autonomy_tier_at_send"
        ),
        {"v": FA_MAX_VENTURE, "week_start": week_start_utc},
    ).all()
    return [(row.agent_name, row.autonomy_tier_at_send) for row in rows]


def build_report(*, dry_run: bool = False) -> str:
    """Compute the report body. Exposed separately from run() so tests can
    assert on content without touching the alert queue."""
    with get_db_context() as session:
        pairs = _agent_tier_pairs_with_sends_this_week(session)
        if not pairs:
            return "No FA Max approved sends recorded yet this week."

        lines = ["*FA Max weekly edit-rate report*"]
        for agent_name, tier in pairs:
            rate = get_weekly_edit_rate(agent_name, tier, session)
            lines.append(f"  • `{agent_name}` (tier {tier}): {rate:.1%} edited before approval this week")
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
