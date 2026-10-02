"""BatchDialer CDR poller: fast poll for new calls, periodic day rescan.

Usage:
    python -m src.lending.cdr_poller            # loop
    python -m src.lending.cdr_poller --once     # single fast poll + rescan (smoke test)

One instance only: GET /v2/cdrs/last keeps a server-side watermark for the API key, so a
second poller would steal calls from the first. A Postgres advisory lock enforces it.
"""
from __future__ import annotations

import argparse
import logging
import time
from typing import Optional

from sqlalchemy import text

from config.lending_dispositions import CDR_POLL_LOCK_KEY, CDR_POLL_SECONDS, CDR_RESCAN_SECONDS
from src.lending.call_pipeline import lending_campaign_ids, retry_unpropagated_dnc
from src.lending.cdr_poll import IngestStats, poll_new, rescan_today
from src.lending.db import lending_session
from src.lending.dialer_port import get_http

logger = logging.getLogger(__name__)


def run_cycle(db, http, *, rescan: bool) -> IngestStats:
    retry_unpropagated_dnc(db)
    stats = poll_new(db, http)
    if rescan:
        stats = stats + rescan_today(db, http)
    return stats


def text_back_step() -> None:
    """WP-GL-9: send the missed-call texts queued by the cycle that just ran. Never blocks ingestion."""
    from src.lending.text_back import run_text_back_cycle

    try:
        counts = run_text_back_cycle()
        if counts:
            logger.info("[lending-cdr-poller] text-back %s", counts)
    except Exception as exc:  # class only: bodies carry phone numbers
        logger.error("[lending-cdr-poller] text-back cycle failed (%s); retrying next interval", type(exc).__name__)


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--once", action="store_true", help="run a single cycle (with a rescan) and exit")
    parser.add_argument("--backfill-days", type=int, default=0, metavar="N",
                        help="first rescan the last N days (outage recovery), then continue normally")
    args = parser.parse_args(argv)

    http = get_http()
    if http is None:
        logger.error("[lending-cdr-poller] BATCHDIALER_API_KEY is not set; nothing to poll")
        return 2

    if not lending_campaign_ids():
        logger.error("[lending-cdr-poller] LENDING_DIALER_CAMPAIGN_IDS is empty: every CDR, DNC requests included, "
                     "would be ignored; refusing to start")
        return 2

    with lending_session() as lock_db:
        if not lock_db.execute(text("SELECT pg_try_advisory_lock(:k)"), {"k": CDR_POLL_LOCK_KEY}).scalar():
            logger.error("[lending-cdr-poller] another poller holds the lock; exiting")
            return 3
        if args.backfill_days > 0:
            with lending_session() as db:
                logger.info("[lending-cdr-poller] backfill over %d days: %s", args.backfill_days,
                            rescan_today(db, http, days=args.backfill_days))
            if args.once:
                return 0
        next_rescan = 0.0
        while True:
            due = time.monotonic() >= next_rescan
            try:
                with lending_session() as db:
                    stats = run_cycle(db, http, rescan=due or args.once)
                if due:
                    next_rescan = time.monotonic() + CDR_RESCAN_SECONDS
                if stats.processed or stats.failed:
                    logger.info("[lending-cdr-poller] processed=%d skipped=%d failed=%d",
                                stats.processed, stats.skipped, stats.failed)
            except Exception as exc:  # class only: bodies carry phone numbers
                logger.error("[lending-cdr-poller] cycle failed (%s); retrying next interval", type(exc).__name__)
                if args.once:
                    raise
            text_back_step()
            if args.once:
                return 0
            time.sleep(CDR_POLL_SECONDS)


if __name__ == "__main__":
    raise SystemExit(main())
