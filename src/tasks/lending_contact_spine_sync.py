"""R1 1.1 contact spine sync (cron */15, flock): resolve every lending source row to one contact per person.

Off until ``LENDING_CONTACT_SPINE_ENABLED``; ``--dry-run`` reads and logs the counts even while off and writes
nothing. Exit 1 when a source could not be read (nothing is written that run; the next run is the retry).

Usage:
    python -m src.tasks.lending_contact_spine_sync [--dry-run]
"""
from __future__ import annotations

import argparse
import logging
from datetime import datetime, timezone
from typing import Optional, Sequence

from config.settings import get_settings
from src.lending.db import lending_session
from src.lending.spine.contact_spine import sync_contact_spine

logger = logging.getLogger(__name__)


def main(argv: Optional[Sequence[str]] = None, *, now: Optional[datetime] = None) -> int:
    parser = argparse.ArgumentParser(description="R1 1.1 contact spine sync")
    parser.add_argument("--dry-run", action="store_true", help="read and count; write nothing")
    args = parser.parse_args(argv)
    if not args.dry_run and not get_settings().lending_contact_spine_enabled:
        logger.info("contact spine sync: off (LENDING_CONTACT_SPINE_ENABLED is false)")
        return 0
    now = now or datetime.now(timezone.utc)
    try:
        with lending_session() as db:
            result = sync_contact_spine(db, now=now, dry_run=args.dry_run)
    except Exception as exc:
        logger.error("contact spine sync failed (%s)", type(exc).__name__)
        return 1
    logger.info("contact spine sync%s: %s", " (dry run)" if args.dry_run else "", result)
    return 1 if result.failed_sources else 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    raise SystemExit(main())
