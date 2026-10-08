"""Retry delivery of website leads that are not yet in GoHighLevel (WP-GL-11).

Picks up leads still pending (GHL was down, or not configured when they arrived) or failed, waiting
longer between tries as failures pile up (config.lending_web.GHL_RETRY_BACKOFF_MINUTES) and giving up
only when a lead is GHL_GIVE_UP_AFTER_HOURS old. A rejected key (HTTP 401/403) does not use up
attempts. A lead still not in GHL GHL_ALERT_AFTER_MINUTES after it arrived is posted to Slack once,
with ids and the last error only (no names or numbers). Cron: every 5 minutes.

    python -m src.tasks.lending_web_lead_sweep
"""
from __future__ import annotations

import logging
from typing import Any, Optional

from sqlalchemy import text

from config.lending_web import GHL_ALERT_AFTER_MINUTES, GHL_GIVE_UP_AFTER_HOURS
from config.settings import get_settings
from src.lending.db import lending_session
from src.lending.disposition_delivery import _configured_slack, _slack_client
from src.lending.web_lead_ghl import get_live_sink
from src.lending.web_leads import deliver_pending

logger = logging.getLogger(__name__)


def alert_undelivered(slack_client: Any = None, minutes: Optional[int] = None) -> int:
    """Post one Slack warning for leads still not in GHL after ``minutes``; each lead is flagged once.
    Returns how many leads were flagged. Never raises: an alert problem must not stop the sweep."""
    minutes = GHL_ALERT_AFTER_MINUTES if minutes is None else minutes
    if not (slack_client or _configured_slack()):
        logger.error("[lending-web] undelivered-lead alert skipped: Slack is not configured")
        return 0
    try:
        with lending_session() as db:
            rows = db.execute(
                text("UPDATE lending.web_leads SET ghl_alerted_at = now() WHERE id IN ("
                     "SELECT id FROM lending.web_leads WHERE ghl_status IN ('pending', 'failed') "
                     "AND ghl_alerted_at IS NULL AND received_at < now() - make_interval(mins => :m) "
                     "ORDER BY received_at LIMIT 50 FOR UPDATE SKIP LOCKED) "
                     "RETURNING id, ghl_last_error"),
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
                text=(f":warning: {len(ids)} website lead(s) are not in GoHighLevel {minutes} min after arriving "
                      f"(lead ids {', '.join(map(str, ids[:10]))}{'…' if len(ids) > 10 else ''}). They are saved in "
                      f"lending.web_leads and keep retrying for {GHL_GIVE_UP_AFTER_HOURS}h. "
                      f"Last error: {'; '.join(reasons) or 'none (GHL not configured?)'}."),
            )
        except Exception as exc:
            logger.error("[lending-web] undelivered-lead alert failed: %s", type(exc).__name__)
            with lending_session() as db:  # un-flag so the next sweep retries the post
                db.execute(text("UPDATE lending.web_leads SET ghl_alerted_at = NULL WHERE id = ANY(:ids)"), {"ids": ids})
            return 0
        logger.error("[lending-web] %d lead(s) undelivered to GHL after %d min, alert posted", len(ids), minutes)
        return len(ids)
    except Exception as exc:
        logger.error("[lending-web] undelivered-lead alert crashed: %s", type(exc).__name__)
        return 0


def run() -> int:
    with lending_session() as db:
        delivered = deliver_pending(db, get_live_sink())
    logger.info("[lending-web] sweep delivered=%d", delivered)
    alert_undelivered()
    return delivered


if __name__ == "__main__":
    run()
