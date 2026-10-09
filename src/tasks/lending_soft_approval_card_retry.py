"""Post the soft approval card for finished calls that never got one.

``post_soft_approval_card`` runs once from the CDR poller. If Slack was down it gives up and releases its claim,
and the poller never revisits an unchanged call, so this sweep is the only second attempt. A call is picked up
while it has no row in ``lending.soft_approval_cards``; ``post_soft_approval_card`` still decides eligibility
and keeps the one-card-per-call claim, so a call that already has a card is never posted twice.
Cron: every 5 minutes.

    python -m src.tasks.lending_soft_approval_card_retry
"""
from __future__ import annotations

import logging

from sqlalchemy import text

from config.lending_dispositions import DNC_CODE
from config.lending_soft_approval import (
    CARD_RETRY_BATCH_LIMIT,
    CARD_RETRY_MIN_AGE_SECONDS,
    CARD_RETRY_WINDOW_HOURS,
)
from config.settings import get_settings
from src.lending.db import lending_session
from src.lending.soft_approval.slack_card import post_soft_approval_card

logger = logging.getLogger(__name__)


def cards_missing_row_ids() -> list[int]:
    with lending_session() as db:
        rows = db.execute(
            text(
                "SELECT d.id FROM lending.call_dispositions d "
                "WHERE d.call_ended_at IS NOT NULL "
                "AND d.call_ended_at > now() - make_interval(hours => :window) "
                "AND d.call_ended_at < now() - make_interval(secs => :age) "
                "AND d.phone IS NOT NULL AND COALESCE(d.talk_duration_sec, 0) > 0 "
                "AND d.disposition IS DISTINCT FROM :dnc "
                "AND NOT EXISTS (SELECT 1 FROM lending.soft_approval_cards c WHERE c.dialer_call_id = d.dialer_call_id) "
                "ORDER BY d.call_ended_at LIMIT :limit"
            ),
            {"window": CARD_RETRY_WINDOW_HOURS, "age": CARD_RETRY_MIN_AGE_SECONDS,
             "dnc": DNC_CODE, "limit": CARD_RETRY_BATCH_LIMIT},
        ).all()
    return [r[0] for r in rows]


def run() -> int:
    if not get_settings().lending_soft_approval_enabled:
        return 0
    ids = cards_missing_row_ids()
    for row_id in ids:
        post_soft_approval_card(row_id)
    if ids:
        logger.info("[soft-approval] card retry attempted %d call(s)", len(ids))
    return len(ids)


if __name__ == "__main__":
    run()
