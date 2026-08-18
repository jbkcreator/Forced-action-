"""
Vertical Autopilot probe reply poller (REVINT-v2.2 I4).

Polls Instantly for every probe in 'completed' status that has an
instantly_campaign_id, completed ≥ MIN_HOURS_AFTER_SEND hours ago, and does
not yet have a terminal verdict.  Updates reply_count + reply_rate and
re-evaluates the verdict.

run_probe() marks a probe 'completed' the moment its sends go out — the probe
is not "running" during the wait for replies — so the poll window is keyed off
completed_at, and terminal probes (won/killed/awaiting_ruling) are excluded so
a settled vertical is never re-polled.

Run via cron every 6 hours:
    0 */6 * * * $PROJECT/scripts/cron/run.sh src.tasks.probe_reply_poll

Settings required: RELAY_INSTANTLY_SENDER_EMAIL (reused for API auth),
                   INSTANTLY_API_KEY
"""

import logging
from datetime import datetime, timezone, timedelta

from sqlalchemy import text

from src.core.database import get_db_context
from src.services.vertical_autopilot import refresh_probe_replies

logger = logging.getLogger(__name__)

MIN_HOURS_AFTER_SEND = 48


def run() -> None:
    with get_db_context() as db:
        rows = db.execute(
            text(
                "SELECT p.id FROM vertical_probes p "
                "WHERE p.status = 'completed' "
                "  AND p.instantly_campaign_id IS NOT NULL "
                "  AND p.completed_at <= :cutoff "
                "  AND NOT EXISTS ("
                "      SELECT 1 FROM vertical_verdicts v "
                "      WHERE v.vertical_probe_id = p.id "
                "        AND v.verdict IN ('won','killed','awaiting_ruling')"
                "  ) "
                "ORDER BY p.id"
            ),
            {"cutoff": datetime.now(timezone.utc) - timedelta(hours=MIN_HOURS_AFTER_SEND)},
        ).fetchall()

    if not rows:
        logger.info("probe_reply_poll: no probes ready for poll")
        return

    logger.info("probe_reply_poll: polling %d probe(s)", len(rows))
    errors = 0
    for (probe_id,) in rows:
        try:
            with get_db_context() as db:
                probe = refresh_probe_replies(probe_id, db)
                logger.info(
                    "probe_reply_poll: probe=%d reply_rate=%.3f verdict=%s",
                    probe_id,
                    probe.reply_rate or 0.0,
                    probe.status,
                )
        except Exception:
            logger.exception("probe_reply_poll: probe=%d failed", probe_id)
            errors += 1

    if errors:
        logger.warning("probe_reply_poll: %d/%d probes errored", errors, len(rows))


if __name__ == "__main__":
    import src.utils.logger as _l
    _l.setup_logging()
    run()
