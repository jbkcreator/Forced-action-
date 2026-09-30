"""Count report for the three launch queues. Dry run only: spends no Tracerfy credits.

Usage:
    python -m src.tasks.lending_queue_report --input pools.json [--balance]
    python -m src.tasks.lending_queue_report --from-staging [--staging-run-id ID] [--balance]

The input is a JSON list of pool records, each with a ``source_tag`` (list_1..list_9).
``--balance`` reads the Tracerfy credit balance (a read-only account call).
"""
from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

from src.core.database import get_db_context
from src.lending.pool_source import staged_pool_records
from src.lending.queues import queue_count_report

logger = logging.getLogger(__name__)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Per-queue count report (no credits spent).")
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--input", type=Path)
    source.add_argument("--from-staging", action="store_true", help="read the calling-pool staging table")
    parser.add_argument("--staging-run-id", help="staging run to read (default: newest)")
    parser.add_argument("--balance", action="store_true", help="include the Tracerfy credit balance")
    args = parser.parse_args(argv)

    if args.from_staging:
        with get_db_context() as session:
            records = staged_pool_records(session, run_id=args.staging_run_id)
    else:
        records = json.loads(args.input.read_text(encoding="utf-8"))
    balance = None
    if args.balance:
        from src.services.tracerfy_batch import get_tracerfy_balance

        balance = get_tracerfy_balance().get("balance")
    with get_db_context() as session:
        report = queue_count_report(records, session, tracerfy_balance=balance)
        session.rollback()
    logger.info("[queue-report] %s", json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
