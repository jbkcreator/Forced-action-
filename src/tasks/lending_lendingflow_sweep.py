"""Retry delivery of LendingFlow leads that are not yet in GoHighLevel (T-11).

Same shape as lending_web_lead_sweep: retries pending/failed leads with growing waits
(config.lending_lendingflow.GHL_RETRY_BACKOFF_MINUTES) until a lead is GHL_GIVE_UP_AFTER_HOURS old,
and posts a lead still not in GHL GHL_ALERT_AFTER_MINUTES after arriving to Slack once (full name + lead id,
and the last error). The event and pre-qual hand-off fire from here too when this path is the one that
gets the lead into GHL. Each run also tags delivered leads whose intro text was held for the borrower's 8am-8pm window
(T-09, needs LENDING_INTRO_SMS_ENABLED). Off with LENDING_LENDINGFLOW_ENABLED=false. Cron: every 5 minutes.

    python -m src.tasks.lending_lendingflow_sweep
"""
from __future__ import annotations

import logging
from typing import Any, Optional

from sqlalchemy import text

from config.lending_lendingflow import GHL_ALERT_AFTER_MINUTES, GHL_GIVE_UP_AFTER_HOURS, SWEEP_BATCH_SIZE
from config.settings import get_settings
from src.lending import lendingflow_events
from src.lending.db import lending_session
from src.lending.lead_arrival_alert import alert_josh
from src.lending.disposition_delivery import _configured_slack, _slack_client
from src.lending.lendingflow import deliver_and_follow_up, release_held_and_commit
from src.lending.lendingflow_ghl import get_live_sink
from src.lending.slack_names import name_and_ref
from src.lending.slack_test_filter import log_skipped, hidden_lendingflow_ids

logger = logging.getLogger(__name__)

# T-05: a lead this sweep gets into GHL alerts Josh too (the event fires in this process, not lending-api).
lendingflow_events.subscribe(alert_josh)


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
                     "ORDER BY received_at LIMIT :limit FOR UPDATE SKIP LOCKED) RETURNING id, ghl_last_error, "
                     "NULLIF(btrim(concat_ws(' ', first_name, last_name)), '')"),
                {"m": minutes, "limit": SWEEP_BATCH_SIZE},
            ).all()
            test_ids = hidden_lendingflow_ids(db, [r[0] for r in rows])
        for lead_id in sorted(test_ids):  # flagged with no post, so they are not picked up again
            log_skipped("undelivered LendingFlow lead alert", f"lead {lead_id}")
        rows = [r for r in rows if r[0] not in test_ids]
        if not rows:
            return 0
        ids = [r[0] for r in rows]
        reasons = sorted({r[1] for r in rows if r[1]})
        who = ", ".join(name_and_ref(r[2], f"lead {r[0]}") for r in rows[:10])
        client = slack_client or _slack_client()
        try:
            client.chat_postMessage(
                channel=get_settings().lending_dial_tasks_channel,
                text=(f":warning: {len(ids)} LendingFlow lead(s) are not in GoHighLevel {minutes} min after arriving "
                      f"({who}{'…' if len(ids) > 10 else ''}). They are saved in "
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
    sink = get_live_sink()
    delivered = deliver_and_follow_up(sink)
    released = release_held_and_commit(sink)
    logger.info("[lendingflow] sweep delivered=%d intro_released=%d", delivered, released)
    alert_undelivered()
    return delivered


if __name__ == "__main__":
    run()
