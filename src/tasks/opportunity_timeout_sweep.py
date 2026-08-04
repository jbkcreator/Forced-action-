"""
LEARN-v2.2 T-LEARN-03 — daily opportunity outcome sweep.

One daily job that codes both terminal outcomes it can infer without a human:

  1. Wins from payment.received fleet events (sweep_payment_wins) —
     NULL-tolerant; codes nothing until a payment.received carries a thread_id.
  2. no_response losses by timeout — a thread that had >=1 outbound draft, has
     never received a reply.received fleet event, has no outcome row yet, and
     whose most recent draft is older than NO_RESPONSE_DAYS (30) is terminal
     lost/no_response.

Loss reasons other than no_response are supplied by a human via the admin tap
endpoint — never inferred here.

Usage:
    python -m src.tasks.opportunity_timeout_sweep [--dry-run]

Options:
    --dry-run   Report what would be coded without writing any rows.
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timezone

try:
    from slack_sdk import WebClient as _SlackWebClient
except ImportError:
    _SlackWebClient = None  # type: ignore[assignment,misc]

from sqlalchemy import text

from config.settings import get_settings
from src.core.database import get_db_context
from src.services.opportunity_outcome import insert_outcomes_bulk, sweep_payment_wins
from src.utils.logger import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

NO_RESPONSE_DAYS = 30


def _find_no_response_threads(db) -> list[str]:
    """Threads that timed out with no reply: >=1 draft, no reply.received, no
    outcome row, newest draft older than NO_RESPONSE_DAYS."""
    rows = db.execute(text("""
        SELECT d.opportunity_thread_id
        FROM outbound_drafts d
        WHERE d.opportunity_thread_id IS NOT NULL
        GROUP BY d.opportunity_thread_id
        HAVING MAX(d.created_at) < NOW() - (:days || ' days')::interval
           AND NOT EXISTS (
               SELECT 1 FROM fleet_events fe
               WHERE fe.opportunity_thread_id = d.opportunity_thread_id
                 AND fe.event_type = 'reply.received'
           )
           AND NOT EXISTS (
               SELECT 1 FROM agent_lane_opportunity_outcomes o
               WHERE o.opportunity_thread_id = d.opportunity_thread_id
           )
    """), {"days": NO_RESPONSE_DAYS}).fetchall()
    return [r.opportunity_thread_id for r in rows]


def _post_slack_digest(wins: int, no_response: int, dry_run: bool) -> None:
    settings = get_settings()
    bot_token = getattr(settings, "slack_bot_token", None)
    channel = getattr(settings, "relay_slack_channel", None)

    if not bot_token or not channel or _SlackWebClient is None:
        logger.info(
            "[OpportunityTimeoutSweep] Slack not configured — skipping digest post."
        )
        return

    prefix = "[DRY-RUN] " if dry_run else ""
    msg = (
        f"{prefix}:checkered_flag: *Opportunity Outcome Sweep* complete\n"
        f"• Wins coded (payment.received): {wins}\n"
        f"• Losses coded (no_response timeout {NO_RESPONSE_DAYS}d): {no_response}"
    )
    try:
        token = bot_token.get_secret_value() if hasattr(bot_token, "get_secret_value") else bot_token
        client = _SlackWebClient(token=token)
        client.chat_postMessage(channel=channel, text=msg)
    except Exception as exc:
        logger.warning("[OpportunityTimeoutSweep] Slack digest failed: %s", exc)


def _run(db, dry_run: bool) -> tuple[int, int]:
    wins = sweep_payment_wins(db)

    threads = _find_no_response_threads(db)
    no_response_rows = [
        {
            "opportunity_thread_id": thread,
            "outcome": "lost",
            "reason_code": "no_response",
            "coded_by": "opportunity_timeout_sweep",
            "source_ref": "timeout_30d",
        }
        for thread in threads
    ]
    no_response = insert_outcomes_bulk(db, no_response_rows)
    if not dry_run:
        db.commit()
    return wins, no_response


def main() -> int:
    parser = argparse.ArgumentParser(
        description="LEARN-v2.2 T-LEARN-03 opportunity outcome sweep"
    )
    parser.add_argument("--dry-run", action="store_true", help="Report without writing rows")
    args = parser.parse_args()

    now = datetime.now(tz=timezone.utc)
    logger.info(
        "[OpportunityTimeoutSweep] starting dry_run=%s now=%s",
        args.dry_run, now.isoformat(),
    )

    if args.dry_run:
        with get_db_context() as db:
            sp = db.begin_nested()
            try:
                wins, no_response = _run(db, dry_run=True)
            finally:
                sp.rollback()
    else:
        with get_db_context() as db:
            wins, no_response = _run(db, dry_run=False)

    logger.info(
        "[OpportunityTimeoutSweep] done wins=%d no_response_losses=%d",
        wins, no_response,
    )

    _post_slack_digest(wins, no_response, dry_run=args.dry_run)
    return 0


if __name__ == "__main__":
    sys.exit(main())
