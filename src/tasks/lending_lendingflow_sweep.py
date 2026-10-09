"""Retry delivery of LendingFlow leads that are not yet in GoHighLevel (T-11).

Same shape as lending_web_lead_sweep: retries pending/failed leads with growing waits
(config.lending_lendingflow.GHL_RETRY_BACKOFF_MINUTES) until a lead is GHL_GIVE_UP_AFTER_HOURS old,
and posts a lead still not in GHL GHL_ALERT_AFTER_MINUTES after arriving to Slack once (ids and last
error only). The event and pre-qual hand-off fire from here too when this path is the one that
gets the lead into GHL. Off with LENDING_LENDINGFLOW_ENABLED=false. Cron: every 5 minutes.

    python -m src.tasks.lending_lendingflow_sweep
"""
from __future__ import annotations

import logging
from typing import Any, Optional

from sqlalchemy import text

from config.lending_lendingflow import GHL_ALERT_AFTER_MINUTES, GHL_GIVE_UP_AFTER_HOURS
from config.settings import get_settings
from src.lending.db import lending_session
from src.lending.disposition_delivery import _configured_slack, _slack_client
from src.lending.lendingflow import deliver_and_follow_up
from src.lending.lendingflow_ghl import get_live_sink

logger = logging.getLogger(__name__)


def alert_undelivered(slack_client: Any = None, minutes: Optional[int] = None) -> int:
    """Post one Slack warning for leads still not in GHL after ``minutes``; each lead is flagged once.
    Never raises: an alert problem must not stop the sweep."""
    minutes = GHL_ALERT_AFTER_MINUTES if minutes is None else minutes
    if not (slack_client or _configured_slack()):
        logger.error("[lendingflow] undelivered-lead alert skipped: Slack is not configured")
        return 0
    try:
        with lending_session() as db:
            rows = db.execute(
                text("UPDATE lending.lendingflow_leads SET ghl_alerted_at = now() WHERE id IN ("
                     "SELECT id FROM lending.lendingflow_leads WHERE ghl_status IN ('pending', 'failed') "
                     "AND NOT suppressed AND ghl_alerted_at IS NULL AND received_at < now() - make_interval(mins => :m) "
                     "ORDER BY received_at LIMIT 50 FOR UPDATE SKIP LOCKED) RETURNING id, ghl_last_error"),
                {"m": minutes},
            ).all()
        if not rows:
            return 0
        ids = [r[0] for r in rows]
        reasons = sorted({r[1] for r in rows if r[1]})
        client = slack_client or _slack_client()
        try:
            client.chat_postMessage(
                channel=get_settings().lending_dial_tasks_channel,
                text=(f":warning: {len(ids)} LendingFlow lead(s) are not in GoHighLevel {minutes} min after arriving "
                      f"(lead ids {', '.join(map(str, ids[:10]))}{'…' if len(ids) > 10 else ''}). They are saved in "
                      f"lending.lendingflow_leads and keep retrying for {GHL_GIVE_UP_AFTER_HOURS}h. "
                      f"Last error: {'; '.join(reasons) or 'none (GHL not configured?)'}."),
            )
        except Exception as exc:
            logger.error("[lendingflow] undelivered-lead alert failed: %s", type(exc).__name__)
            with lending_session() as db:  # un-flag so the next sweep retries the post
                db.execute(text("UPDATE lending.lendingflow_leads SET ghl_alerted_at = NULL WHERE id = ANY(:ids)"), {"ids": ids})
            return 0
        return len(ids)
    except Exception as exc:
        logger.error("[lendingflow] undelivered-lead alert crashed: %s", type(exc).__name__)
        return 0


def run() -> int:
    if not get_settings().lending_lendingflow_enabled:
        return 0
    delivered = deliver_and_follow_up(get_live_sink())
    logger.info("[lendingflow] sweep delivered=%d", delivered)
    alert_undelivered()
    return delivered


if __name__ == "__main__":
    run()
