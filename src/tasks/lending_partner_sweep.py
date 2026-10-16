"""Rerun partner files whose after-submit processing never finished (P-1).

A partner file is scored, routed and confirmed in a FastAPI background task after it commits; a
crash or restart in between leaves it 'received'. This runs the same steps again (the Slack note and
the partner confirmation are stamped, so neither is sent twice). It also retries the Slack note and the
partner confirmation for files already routed whose notices failed (for up to NOTICE_RETRY_HOURS).
Cron: every 5 minutes.

    python -m src.tasks.lending_partner_sweep
"""
from __future__ import annotations

import logging

from config.lending_partners import NOTICE_RETRY_HOURS, SWEEP_AFTER_MINUTES, SWEEP_BATCH_SIZE
from src.lending.db import lending_session
from src.lending.partner_pipeline import process_new_file, send_pending_notices, unnotified_file_ids, unprocessed_file_ids

logger = logging.getLogger(__name__)


def run() -> int:
    with lending_session() as db:
        file_ids = unprocessed_file_ids(db, SWEEP_AFTER_MINUTES, SWEEP_BATCH_SIZE)
        notice_ids = unnotified_file_ids(db, SWEEP_AFTER_MINUTES, NOTICE_RETRY_HOURS, SWEEP_BATCH_SIZE)
    for file_id in file_ids:
        process_new_file(file_id)
    for file_id in notice_ids:
        send_pending_notices(file_id)
    if file_ids or notice_ids:
        logger.info("[partner-door] sweep reran %s file(s), retried notices for %s", len(file_ids), len(notice_ids))
    return len(file_ids) + len(notice_ids)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    run()
