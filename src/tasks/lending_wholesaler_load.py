"""Split T-11: load FL wholesaler lists into the Partners queue and tag them in GHL.

Dry run by default: the CSV is parsed and every dialer-load gate runs, but nothing is
sent to Tracerfy, BatchDialer or GHL and the transaction is rolled back. A live run
needs ``--confirm-scrub N`` where N is the number of phones the run would send to the
Tracerfy DNC scrub (the dry run prints it), so no credits are spent unconfirmed.
Numbers failing the DNC or suppression scrub are never loaded or pushed to GHL.

Usage:
    python -m src.tasks.lending_wholesaler_load --input wholesalers.csv
    python -m src.tasks.lending_wholesaler_load --input wholesalers.csv --live --confirm-scrub 1830

CSV columns: source (sunbiz|hunter), phone, state; optional name, entity_name, email.

Exit codes: 0 ok, 1 some pushes failed, 2 bad CSV, 3 refused (no dialer/GHL, load refused),
4 scrub count not confirmed, 6 loaded fine but wholesaler total still below the 1,500 target.
"""
from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import text

from config.lending_wholesalers import DIALABLE_TARGET
from src.core.database import get_db_context
from src.lending import dialer_port
from src.lending.compliance import tracerfy_scrub
from src.lending.dial_order import sort_for_load
from src.lending.dialer_load import LoadRefused, run_dialer_load
from src.lending.wholesaler_lists import ghl_sink, parse_wholesaler_csv, push_to_ghl

logger = logging.getLogger(__name__)

EXIT_OK = 0
EXIT_PUSH_FAILED = 1
EXIT_BAD_INPUT = 2
EXIT_REFUSED = 3
EXIT_SCRUB_NOT_CONFIRMED = 4
EXIT_BELOW_TARGET = 6


def active_wholesaler_total(db) -> int:
    """Wholesaler numbers live in the dialer across every run (Section 0 counts the total)."""
    return int(db.execute(text(
        "SELECT count(*) FROM lending.dialer_load_records "
        "WHERE active AND source_record_ref LIKE 'wholesaler:%'"
    )).scalar() or 0)


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load FL wholesaler lists into the Partners queue.")
    parser.add_argument("--input", type=Path, required=True, help="wholesaler CSV")
    parser.add_argument("--live", action="store_true", help="scrub, load the dialer, push to GHL (default: dry run)")
    parser.add_argument("--confirm-scrub", type=int, help="phones you approve sending to the Tracerfy DNC scrub")
    parser.add_argument("--run-id", help="identifier for this run (default: timestamped)")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = _parse_args(argv)
    try:
        parsed = parse_wholesaler_csv(args.input.read_text(encoding="utf-8"))
    except ValueError as exc:
        logger.error("[wholesaler-load] %s", exc)
        return EXIT_BAD_INPUT
    records = sort_for_load(parsed.records)
    logger.info("[wholesaler-load] parsed %d FL records; dropped %s", len(records), dict(parsed.dropped))
    run_id = args.run_id or f"wholesaler-load-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}"

    # Dry pass first, always: its needs_scrub is the number of phones a live run sends to Tracerfy.
    with get_db_context() as session:
        dry = run_dialer_load(records, session, run_id=run_id, dry_run=True)
        session.rollback()
    scrub_count = dry.needs_scrub
    logger.info("[wholesaler-load] %d phone(s) would go to the Tracerfy scrub", scrub_count)
    if not args.live:
        logger.info("[wholesaler-load] dry run: %s", json.dumps(dry.as_dict(), indent=2))
        return EXIT_OK

    if args.confirm_scrub != scrub_count:
        logger.error("[wholesaler-load] refused: this run sends %d phone(s) to the Tracerfy scrub; "
                     "re-run with --confirm-scrub %d once approved", scrub_count, scrub_count)
        return EXIT_SCRUB_NOT_CONFIRMED
    dialer = dialer_port.get_dialer()
    sink = ghl_sink()
    if dialer is None or sink is None:
        logger.error("[wholesaler-load] refused: dialer (BATCHDIALER_API_KEY) or lending GHL account not configured")
        return EXIT_REFUSED

    with get_db_context() as session:
        try:
            report = run_dialer_load(records, session, run_id=run_id, dry_run=False,
                                     scrubber=tracerfy_scrub, dialer=dialer, commit=session.commit)
        except LoadRefused as exc:
            session.rollback()
            logger.error("[wholesaler-load] refused: %s", exc)
            return EXIT_REFUSED
        total = active_wholesaler_total(session)
    loaded = set(report.loaded_refs)
    ghl = push_to_ghl([r for r in records if r["source_record_ref"] in loaded], sink)
    logger.info("[wholesaler-load] report: %s", json.dumps(report.as_dict(), indent=2))
    logger.info("[wholesaler-load] dialer loaded %d this run; GHL tagged %d, failed %d; wholesaler total live %d",
                report.loaded, ghl.pushed, len(ghl.failed), total)
    if report.failed or ghl.failed:
        return EXIT_PUSH_FAILED
    if total < DIALABLE_TARGET:
        logger.warning("[wholesaler-load] %d wholesaler numbers live, below the %d target (Section 0)",
                       total, DIALABLE_TARGET)
        return EXIT_BELOW_TARGET
    return EXIT_OK

if __name__ == "__main__":
    raise SystemExit(main())
