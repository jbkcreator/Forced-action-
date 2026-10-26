"""Rerun partner files whose after-submit processing never finished (P-1).

A partner file is scored, routed and confirmed in a FastAPI background task after it commits; a
crash or restart in between leaves it 'received'. This runs the same steps again (the Slack note and
the partner confirmation are stamped, so neither is sent twice). It also retries the Slack note, the partner confirmation (each channel once) and the pre-qual letter for files already routed whose step
failed, and status notices (routed / declined / approved / funded) that did not fully go out (up to NOTICE_RETRY_HOURS).
A partner notice still unsent after NOTICE_RETRY_HOURS is stamped failed once and posted to Slack
(``partner_pipeline.flag_lapsed_notices``), so it is followed up by hand instead of dropping silently.
It also re-applies an approval or funding whose partner-file status update failed after the outcome committed
(``partner_link.resync_outcomes``), until the file shows it.
Cron: every 5 minutes.

    python -m src.tasks.lending_partner_sweep
"""
from __future__ import annotations

import logging

from config.lending_partners import NOTICE_RETRY_HOURS, SWEEP_AFTER_MINUTES, SWEEP_BATCH_SIZE
from src.lending.db import lending_session
from src.lending.partner_ghl import get_partner_letter_sink
from src.lending.routing.partner_link import lagging_outcomes, resync_outcomes
from src.lending.partner_pipeline import (
    flag_lapsed_notices,
    letters_enabled,
    pending_status_notice_ids,
    process_new_file,
    retry_status_notice,
    send_pending_notices,
    unnotified_file_ids,
    unprocessed_file_ids,
)

logger = logging.getLogger(__name__)


def run() -> int:
    with lending_session() as db:
        file_ids = unprocessed_file_ids(db, SWEEP_AFTER_MINUTES, SWEEP_BATCH_SIZE)
        notice_ids = unnotified_file_ids(db, SWEEP_AFTER_MINUTES, NOTICE_RETRY_HOURS, SWEEP_BATCH_SIZE,
                                         letters=letters_enabled() and get_partner_letter_sink() is not None)
        status_ids = pending_status_notice_ids(db, SWEEP_AFTER_MINUTES, NOTICE_RETRY_HOURS, SWEEP_BATCH_SIZE)
        outcomes = lagging_outcomes(db, older_than_minutes=SWEEP_AFTER_MINUTES, limit=SWEEP_BATCH_SIZE)
    for file_id in file_ids:
        process_new_file(file_id)
    for file_id in notice_ids:
        send_pending_notices(file_id)
    for file_id in status_ids:
        retry_status_notice(file_id)
    resync_outcomes(outcomes)
    lapsed = flag_lapsed_notices(NOTICE_RETRY_HOURS, limit=SWEEP_BATCH_SIZE)
    if file_ids or notice_ids or status_ids or outcomes or lapsed:
        logger.info("[partner-door] sweep reran %s file(s), retried notices for %s, status notices for %s, "
                    "outcome status for %s, flagged %s lapsed notice(s)", len(file_ids), len(notice_ids),
                    len(status_ids), len(outcomes), len(lapsed))
    return len(file_ids) + len(notice_ids) + len(status_ids) + len(outcomes) + len(lapsed)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    run()
