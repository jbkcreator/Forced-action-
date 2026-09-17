"""
FA Max EXCEPTIONS alert drain worker (WP-T2-1 go-live review, 2026-09).

Retries every still-'pending' row in fa_max_exceptions_alert_queue on a
short cron cycle (*/5 min — mirrors relay's own
--post-pending-fa-max card-retry cadence in scripts/cron/crontab.txt).
Both fa_max_send_health_monitor.py and relay/sweep.py's suppression-sync-
failure path commit a row here before ever attempting Slack; this worker is
what recovers a row left 'pending' after a failed attempt, a Slack outage,
or a process crash mid-attempt. See exceptions_alert_queue.py and the
FaMaxExceptionsAlertQueue model docstring (src/core/models.py) for the full
design and the documented, accepted duplicate-post risk on an ambiguous
Slack result.

Run:
  python -m src.tasks.fa_max_exceptions_alert_drain
"""
from __future__ import annotations

import logging

from src.services.relay import exceptions_alert_queue

logger = logging.getLogger(__name__)


def run() -> int:
    attempted = exceptions_alert_queue.drain_pending()
    logger.info("fa_max_exceptions_alert_drain: %d pending alert(s) attempted", attempted)
    return attempted


if __name__ == "__main__":
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
        stream=sys.stdout,
    )
    run()
