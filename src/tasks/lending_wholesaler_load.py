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
"""
from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from config.lending_wholesalers import DIALABLE_TARGET
from src.core.database import get_db_context
from src.lending import dialer_port
from src.lending.compliance import tracerfy_scrub
from src.lending.dial_order import sort_for_load
from src.lending.dialer_load import LoadRefused, run_dialer_load
from src.lending.wholesaler_lists import ghl_sink, parse_wholesaler_csv, push_to_ghl

logger = logging.getLogger(__name__)


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
        return 2
    records = sort_for_load(parsed.records)
    scrub_count = len(records)
    logger.info("[wholesaler-load] parsed %d FL records; dropped %s; %d phone(s) would go to the Tracerfy scrub",
                scrub_count, dict(parsed.dropped), scrub_count)
    run_id = args.run_id or f"wholesaler-load-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}"

    if not args.live:
        with get_db_context() as session:
            report = run_dialer_load(records, session, run_id=run_id, dry_run=True)
            session.rollback()
        logger.info("[wholesaler-load] dry run: %s", json.dumps(report.as_dict(), indent=2))
        return 0

    if args.confirm_scrub != scrub_count:
        logger.error("[wholesaler-load] refused: this run sends %d phone(s) to the Tracerfy scrub; "
                     "re-run with --confirm-scrub %d once approved", scrub_count, scrub_count)
        return 4
    dialer = dialer_port.get_dialer()
    sink = ghl_sink()
    if dialer is None or sink is None:
        logger.error("[wholesaler-load] refused: dialer (BATCHDIALER_API_KEY) or lending GHL account not configured")
        return 3

    with get_db_context() as session:
        try:
            report = run_dialer_load(records, session, run_id=run_id, dry_run=False,
                                     scrubber=tracerfy_scrub, dialer=dialer, commit=session.commit)
        except LoadRefused as exc:
            session.rollback()
            logger.error("[wholesaler-load] refused: %s", exc)
            return 3
    loaded = set(report.loaded_refs)
    ghl = push_to_ghl([r for r in records if r["source_record_ref"] in loaded], sink)
    logger.info("[wholesaler-load] report: %s", json.dumps(report.as_dict(), indent=2))
    logger.info("[wholesaler-load] GHL tagged %d, failed %d; dialable loaded %d of %d target",
                ghl.pushed, len(ghl.failed), report.loaded, DIALABLE_TARGET)
    return 1 if report.failed or ghl.failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
