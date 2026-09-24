"""Replace the active Backflip campaign-contact snapshot from a CSV export.

Expected columns: email and/or phone (case insensitive). An empty snapshot
requires --allow-empty so a malformed export cannot silently clear protection.
The exact feed mapping remains pending Bailey's response. This importer never
writes permanent opt-out tables.
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from src.services.fa_max_backflip_feed import (
    CsvBackflipFeedPort,
    parse_backflip_csv,
    replace_backflip_snapshot,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

def run(csv_path: Path, *, dry_run: bool = False, allow_empty: bool = False) -> int:
    identifiers = parse_backflip_csv(csv_path)
    if not identifiers and not allow_empty:
        raise ValueError("empty campaign snapshot; pass --allow-empty after verifying the export")
    if dry_run:
        logger.info("DRY RUN: %d identifiers parsed; database unchanged", len(identifiers))
        return len(identifiers)

    # Do not remove rows written by the old importer from permanent opt-outs:
    # an address may also have a genuine unsubscribe or DNC reason.
    replace_backflip_snapshot(identifiers, allow_empty=allow_empty)
    logger.info("Backflip campaign snapshot replaced: %d identifiers", len(identifiers))
    return len(identifiers)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--file", required=True, help="Path to suppression CSV")
    parser.add_argument("--dry-run", action="store_true", help="Parse and log but do not write")
    parser.add_argument("--allow-empty", action="store_true", help="Permit a verified empty campaign snapshot")
    args = parser.parse_args()

    csv_path = Path(args.file)
    if not csv_path.exists():
        logger.error("File not found: %s", csv_path)
        sys.exit(1)

    if args.dry_run:
        run(csv_path, dry_run=True, allow_empty=args.allow_empty)
    else:
        CsvBackflipFeedPort(csv_path, allow_empty=args.allow_empty).import_snapshot()


if __name__ == "__main__":
    main()
