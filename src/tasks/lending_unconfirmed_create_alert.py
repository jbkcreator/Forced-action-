"""Remind #dial-tasks about dialer creates that failed ambiguously and are still unresolved.

While a phone has an open row in lending.dialer_unconfirmed_creates its opt-out stays pending
(a contact may exist that we hold no id for), so an unnoticed row means someone who opted out may
still be callable. Cron hourly; silent when nothing is open.

    python -m src.tasks.lending_unconfirmed_create_alert

Runbook: docs/lending/unconfirmed-creates-runbook.md
"""
from __future__ import annotations

import logging
from typing import Any

from sqlalchemy import text

from config.settings import get_settings
from src.lending.db import lending_session
from src.lending.disposition_delivery import _configured_slack, _slack_client

logger = logging.getLogger(__name__)


def run(slack_client: Any = None) -> int:
    """Post one summary of the open rows; returns how many are open."""
    with lending_session() as db:
        rows = db.execute(
            text(
                "SELECT phone_hash, run_id, attempted_at FROM lending.dialer_unconfirmed_creates "
                "WHERE resolved_at IS NULL ORDER BY attempted_at"
            )
        ).all()
    if not rows:
        return 0
    logger.error("[lending] %d unresolved unconfirmed dialer create(s); their opt-outs stay pending", len(rows))
    if not (slack_client or _configured_slack()):
        logger.error("[lending] unconfirmed-create alert not posted: Slack is not configured")
        return len(rows)
    refs = ", ".join(f"{hash_[:8]} (run {run_id})" for hash_, run_id, _ in rows[:10])
    try:
        (slack_client or _slack_client()).chat_postMessage(
            channel=get_settings().lending_dial_tasks_channel,
            text=f":warning: {len(rows)} dialer create(s) failed ambiguously and are unresolved, oldest "
                 f"{rows[0].attempted_at:%Y-%m-%d %H:%M} UTC: {refs}{'…' if len(rows) > 10 else ''}. "
                 "Opt-outs for these numbers stay pending until each is checked in the dialer and closed "
                 "(docs/lending/unconfirmed-creates-runbook.md).",
        )
    except Exception as exc:
        logger.error("[lending] unconfirmed-create alert failed: %s", type(exc).__name__)
    return len(rows)


if __name__ == "__main__":
    run()
