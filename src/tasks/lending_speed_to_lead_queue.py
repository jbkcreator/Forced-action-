"""P2.1 speed-to-lead call queue, every minute (``src.lending.speed_to_lead.release_queue``).

Inside the callers' hours, starts the GHL call cascade for at most one queued lead (fresh leads first, then the
oldest), after re-checking every rule and a live read of the lead's GHL calls; expires leads queued for 72 hours and
reports interrupted releases in one Slack post; posts the once-a-day morning summary and backlog alert to the
lead-alerts channel (while ``LENDING_OPS_CHANNEL`` or the lending bot token is unset, each skipped post is
reported once through ``slack_channels.report_unset`` and not retried). With LENDING_SPEED_TO_LEAD_ENABLED off it ends
every queued lead, so nothing waits on a switched-off queue. One run at a time: a run that finds the previous one
still going does nothing.

    python -m src.tasks.lending_speed_to_lead_queue
"""
from __future__ import annotations

import logging

from sqlalchemy import text

from config.lending_speed_to_lead import QUEUE_RUN_LOCK_KEY
from config.settings import get_settings
from src.lending import speed_to_lead
from src.lending.db import lending_session
from src.lending.ops_alerts import post_ops_alert
from src.lending.slack_channels import BOT_TOKEN, report_unset

logger = logging.getLogger(__name__)

_TRY_RUN_LOCK = text("SELECT pg_try_advisory_xact_lock(:key)")


def _report_unset(post: str) -> None:
    report_unset(post, ("LENDING_OPS_CHANNEL", BOT_TOKEN))


def _release(db) -> speed_to_lead.QueueRun:
    settings = get_settings()
    configured = bool(settings.lending_ops_channel and settings.lending_slack_bot_token)
    return speed_to_lead.release_queue(
        db, tags=speed_to_lead.live_tag_client(), scrubber=speed_to_lead.live_scrubber(),
        read_calls=speed_to_lead.live_call_reader(), notify=post_ops_alert if configured else None,
        contact_url=speed_to_lead.live_contact_url(), unset=_report_unset)


def run() -> speed_to_lead.QueueRun:
    """One pass with the live GHL account and Tracerfy. Never raises."""
    try:
        # The run lock lives in its own never-committed transaction, so the work session's commits keep it held.
        with lending_session() as lock_db:
            if not lock_db.execute(_TRY_RUN_LOCK, {"key": QUEUE_RUN_LOCK_KEY}).scalar():
                logger.info("[speed-to-lead] queue: the previous run is still going, skipped")
                return speed_to_lead.QueueRun()
            try:
                with lending_session() as db:
                    result = _release(db)
            finally:
                lock_db.rollback()
    except Exception as exc:  # class only: SQL / HTTP errors can carry phone numbers
        logger.error("[speed-to-lead] queue run crashed: %s", type(exc).__name__)
        return speed_to_lead.QueueRun()
    logger.info("[speed-to-lead] queue released=%d expired=%d interrupted=%d switched_off=%d ended=%d "
                "rechecked_later=%d ghl_check_failed=%d", result.released, result.expired, result.interrupted,
                result.switched_off, result.blocked, result.waiting, result.ghl_check_failed)
    return result


if __name__ == "__main__":
    run()
