"""Retry Sheet / Slack delivery for logged dispositions that are still behind.

A row is behind while its synced disposition differs from its disposition
(first delivery failed, or the caller changed the result). Cron: every 5 minutes.

    python -m src.tasks.lending_disposition_delivery_retry
"""
from __future__ import annotations

import logging

from sqlalchemy import text

from config.lending_dispositions import RETRY_BATCH_LIMIT, RETRY_MIN_AGE_SECONDS
from src.lending.db import lending_session
from src.lending.disposition_delivery import deliver_disposition

logger = logging.getLogger(__name__)


def behind_row_ids() -> list[int]:
    with lending_session() as db:
        rows = db.execute(
            text(
                "SELECT id FROM lending.call_dispositions "
                "WHERE (sheet_synced_disposition IS DISTINCT FROM disposition "
                "       OR slack_posted_disposition IS DISTINCT FROM disposition) "
                "AND disposition_at < now() - make_interval(secs => :age) "
                "ORDER BY disposition_at LIMIT :limit"
            ),
            {"age": RETRY_MIN_AGE_SECONDS, "limit": RETRY_BATCH_LIMIT},
        ).all()
    return [r[0] for r in rows]


def run() -> int:
    ids = behind_row_ids()
    for row_id in ids:
        deliver_disposition(row_id)
    if ids:
        logger.info("[lending] delivery retry attempted %d call(s)", len(ids))
    return len(ids)


if __name__ == "__main__":
    run()
