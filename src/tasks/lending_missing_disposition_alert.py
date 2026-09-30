"""Warn #dial-tasks about connected calls that never got a disposition (brief §3:
"a skipped disposition is a missed shift"). One warning per call, cron every 5 minutes.

    python -m src.tasks.lending_missing_disposition_alert
"""
from __future__ import annotations

import logging
from typing import Any, Optional

from sqlalchemy import text

from config.settings import get_settings
from src.lending.db import lending_session
from src.lending.disposition_delivery import _configured_slack, _slack_client

logger = logging.getLogger(__name__)


def run(slack_client: Any = None, minutes: Optional[int] = None) -> int:
    """Post one warning per seat for calls past the grace period; returns calls flagged."""
    settings = get_settings()
    minutes = minutes if minutes is not None else settings.lending_disposition_missing_alert_minutes
    with lending_session() as db:
        rows = db.execute(
            text(
                "UPDATE lending.call_dispositions SET disposition_missing_alerted_at = now() "
                "WHERE id IN (SELECT id FROM lending.call_dispositions "
                "  WHERE disposition IS NULL AND disposition_raw IS NULL AND disposition_missing_alerted_at IS NULL "
                "  AND call_ended_at IS NOT NULL AND coalesce(talk_duration_sec, 0) > 0 "
                "  AND call_ended_at < now() - make_interval(mins => :m) FOR UPDATE SKIP LOCKED) "
                "RETURNING dialer_call_id, caller_seat, caller_name"
            ),
            {"m": minutes},
        ).all()
        db.commit()
    if not rows:
        return 0
    by_seat: dict[str, list[str]] = {}
    for call_id, seat, name in rows:
        by_seat.setdefault(name or seat or "unknown seat", []).append(call_id)
    if not (slack_client or _configured_slack()):
        logger.error("[lending] %d call(s) missing a disposition and Slack is not configured", len(rows))
        return len(rows)
    client = slack_client or _slack_client()
    for seat, ids in by_seat.items():
        try:
            client.chat_postMessage(
                channel=settings.lending_dial_tasks_channel,
                text=f":warning: {seat}: {len(ids)} connected call(s) have no disposition after {minutes} min "
                     f"({', '.join(ids[:5])}{'…' if len(ids) > 5 else ''}). A skipped disposition is a missed shift.",
            )
        except Exception as exc:
            logger.error("[lending] missing-disposition alert failed: %s", type(exc).__name__)
    return len(rows)


if __name__ == "__main__":
    run()
