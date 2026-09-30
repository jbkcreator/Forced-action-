"""Lending dialer enforcement sweep (WP-W0-3).

Pulls dialer contacts outside 09:00–19:15 ET / 8–20 recipient local time or at 3 attempts /
rolling 24 h, and restores them when allowed. One transaction per cycle.

Usage:
    python -m src.lending.dialer_sweep            # loop every DIALER_SWEEP_SECONDS
    python -m src.lending.dialer_sweep --once     # single cycle (cron / smoke test)
"""
from __future__ import annotations

import argparse
import time

from config.lending_compliance import DIALER_SWEEP_SECONDS
from src.core.database import get_db_context
from src.lending.compliance import SweepResult, sweep_dialer_pool
from src.utils.logger import get_logger

logger = get_logger(__name__)


def run_once() -> SweepResult:
    with get_db_context() as db:  # session_scope commits on exit
        return sweep_dialer_pool(db)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--once", action="store_true", help="run a single cycle and exit")
    args = parser.parse_args(argv)

    while True:
        try:
            run_once()
        except Exception as exc:
            # Class only: SQL errors embed bound params (phones).
            logger.error("[lending-dialer-sweep] cycle failed (%s); retrying next interval", type(exc).__name__)
            if args.once:
                raise
        if args.once:
            return
        time.sleep(DIALER_SWEEP_SECONDS)


if __name__ == "__main__":
    main()
