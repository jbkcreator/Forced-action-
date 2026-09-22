"""scripts/run_backflip_mailbox_poller.py

WP-T2-6 Addendum 2 -- per-run cron worker for Josh's own Backflip
notification mailbox (IMAP + app password). Mirrors
scripts/run_stage_monitor_worker.py's shape.

Usage:
    PYTHONPATH=. python scripts/run_backflip_mailbox_poller.py

Cron (every 15 minutes -- Backflip status changes aren't second-critical;
matches stage_monitor_worker's own cadence):
    */15 * * * * cd $PROJECT && PYTHONPATH=. python scripts/run_backflip_mailbox_poller.py >> /var/log/fa/backflip_mailbox_poller.log 2>&1
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from config.settings import get_settings

logger = logging.getLogger(__name__)


def main() -> None:
    from src.agents.reply_concierge.backflip_mailbox_poller import poll_backflip_mailbox

    engine = create_engine(get_settings().database_url)
    Session_ = sessionmaker(bind=engine)

    with Session_() as session:
        applied = poll_backflip_mailbox(session)
        logger.info("backflip_mailbox_poller_worker: applied=%d", applied)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    main()
