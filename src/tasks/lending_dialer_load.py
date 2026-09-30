"""Load a lending pool export into the dialer (BatchDialer).

Dry run by default: every gate runs and the report is logged, but nothing is
sent to the dialer or Tracerfy and the transaction is rolled back. ``--live``
scrubs stale numbers with Tracerfy, loads the dialer and commits.

Usage:
    python -m src.tasks.lending_dialer_load --input pools.json
    python -m src.tasks.lending_dialer_load --input pools.json --live
    python -m src.tasks.lending_dialer_load --from-staging [--staging-run-id ID]

``--from-staging`` reads one calling-pool staging run (default: newest) and keeps
only records in a launch queue; nurture-only and untagged records are never loaded.

The input is a JSON list of pool records (source_record_ref, pool, phone,
email, borrower_name, entity_name, property_address, estimated_loan_value,
recent_permit_details, parcel_id, state, entity_status).
"""
from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from src.core.database import get_db_context
from src.lending.compliance import tracerfy_scrub
from src.lending.dialer_load import LoadRefused, run_dialer_load
from src.lending import dialer_port
from src.lending.pool_source import staged_pool_records
from src.lending.queues import assign_queue

logger = logging.getLogger(__name__)


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load a lending pool export into the dialer.")
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--input", type=Path, help="JSON list of pool records")
    source.add_argument("--from-staging", action="store_true", help="read the calling-pool staging table")
    parser.add_argument("--staging-run-id", help="staging run to read (default: newest)")
    parser.add_argument("--live", action="store_true", help="load the dialer and commit (default: dry run)")
    parser.add_argument("--run-id", help="identifier for this run (default: timestamped)")
    return parser.parse_args(argv)


def launch_queue_records(records: list[dict]) -> list[dict]:
    """Records assigned to a launch queue, with ``pool`` set to that queue."""
    return [r for r in (assign_queue(rec) for rec in records) if r["queue"]]


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = _parse_args(argv)
    if args.from_staging:
        with get_db_context() as session:
            records = launch_queue_records(staged_pool_records(session, run_id=args.staging_run_id))
    else:
        records = json.loads(args.input.read_text(encoding="utf-8"))
    if not isinstance(records, list):
        logger.error("[dialer-load] input must be a JSON list of records")
        return 2
    run_id = args.run_id or f"dialer-load-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}"

    dialer = dialer_port.get_dialer() if args.live else None
    if args.live and dialer is None:
        logger.error("[dialer-load] refused: no dialer configured (BATCHDIALER_API_KEY)")
        return 3
    missing = dialer.missing_for_load() if args.live and hasattr(dialer, "missing_for_load") else []
    if missing:
        logger.error("[dialer-load] refused: unconfirmed dialer endpoint(s): %s", ", ".join(missing))
        return 3

    with get_db_context() as session:
        try:
            if args.live:
                report = run_dialer_load(records, session, run_id=run_id, dry_run=False,
                                         scrubber=tracerfy_scrub, dialer=dialer,
                                         commit=session.commit)
            else:
                report = run_dialer_load(records, session, run_id=run_id, dry_run=True)
                session.rollback()
        except LoadRefused as exc:
            session.rollback()
            logger.error("[dialer-load] refused: %s", exc)
            return 3
    logger.info("[dialer-load] report: %s", json.dumps(report.as_dict(), indent=2))
    return 1 if report.failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
