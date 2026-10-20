"""T-12 audio fallback worker: fetch and extract transcripts of finished calls.

    python -m src.lending.transcript_worker            # loop
    python -m src.lending.transcript_worker --once     # one cycle and exit

Runs apart from the CDR poller so slow transcript fetches and Claude extractions never delay
CDR ingestion or missed-call texts. Idles while LENDING_TRANSCRIPT_INGEST_ENABLED is false
(every extraction is a Claude call). One instance at a time, enforced by an advisory lock.
"""
from __future__ import annotations

import argparse
import logging
import signal
import time
from collections import Counter
from contextlib import ExitStack
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import text

from config.lending_dispositions import TRANSCRIPT_BATCH_SIZE, TRANSCRIPT_POLL_SECONDS, TRANSCRIPT_WORKER_LOCK_KEY
from config.settings import get_settings
from src.lending.db import lending_session
from src.lending.dialer_port import get_dialer
from src.lending.transcript_ingest import run_transcript_ingest

logger = logging.getLogger(__name__)

_running = True
_lock_stack = ExitStack()


def _stop(signum, _frame) -> None:
    global _running
    _running = False


def _acquire_lock() -> bool:
    """Hold the advisory lock on a session kept open for the life of the process."""
    lock_db = _lock_stack.enter_context(lending_session())
    return bool(lock_db.execute(text("SELECT pg_try_advisory_lock(:k)"), {"k": TRANSCRIPT_WORKER_LOCK_KEY}).scalar())


def run_cycle() -> Counter:
    if not get_settings().lending_transcript_ingest_enabled:
        return Counter()
    dialer = get_dialer()
    if dialer is None:
        return Counter()
    with lending_session() as db:
        counts = run_transcript_ingest(db, dialer, now=datetime.now(timezone.utc), limit=TRANSCRIPT_BATCH_SIZE)
        db.commit()
    return counts


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--once", action="store_true", help="run a single cycle and exit")
    args = parser.parse_args(argv)
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)
    if not _acquire_lock():
        logger.error("[lending-transcript] another worker holds the lock; exiting")
        return 3
    logger.info("[lending-transcript] starting (enabled=%s)", get_settings().lending_transcript_ingest_enabled)
    while _running:
        try:
            counts = run_cycle()
            if counts:
                logger.info("[lending-transcript] %s", dict(counts))
        except Exception as exc:  # class only: transcripts carry borrower speech
            logger.error("[lending-transcript] cycle failed (%s); retrying next interval", type(exc).__name__)
        if args.once:
            break
        time.sleep(TRANSCRIPT_POLL_SECONDS)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
