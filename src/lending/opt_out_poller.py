"""Lending stop-propagation poller (WP-W0-8).

Mirrors FA opt-outs (SMS STOP, email UNSUBSCRIBE) into the lending stores and the
dialer, and retries pending dialer removals. One transaction per cycle.

Usage:
    python -m src.lending.opt_out_poller            # loop every OPT_OUT_POLL_SECONDS
    python -m src.lending.opt_out_poller --once     # single cycle (cron / smoke test)
"""
from __future__ import annotations

import argparse
import time

from config.lending_compliance import OPT_OUT_POLL_SECONDS
from src.core.database import get_db_context
from src.lending.compliance import PollResult, poll_fa_opt_outs, sync_ghl_dnd
from src.utils.logger import get_logger

logger = get_logger(__name__)


def run_once() -> PollResult:
    with get_db_context() as db:  # session_scope commits on exit
        result = poll_fa_opt_outs(db, sync_ghl=False)
    if not result.skipped_locked:
        # Own transaction: up to a batch of sequential GHL calls must not hold the poll's
        # advisory lock, or delay (or, on error, roll back) the opt-outs mirrored above.
        try:
            with get_db_context() as db:
                sync_ghl_dnd(db)
        except Exception as exc:
            logger.error("[lending-opt-out-poller] GHL DND sync failed (%s); retrying next interval",
                         type(exc).__name__)
    return result


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--once", action="store_true", help="run a single cycle and exit")
    args = parser.parse_args(argv)

    while True:
        try:
            run_once()
        except Exception as exc:
            # Class only: SQL errors embed bound params (phones/emails).
            logger.error("[lending-opt-out-poller] cycle failed (%s); retrying next interval", type(exc).__name__)
            if args.once:
                raise
        if args.once:
            return
        time.sleep(OPT_OUT_POLL_SECONDS)


if __name__ == "__main__":
    main()
