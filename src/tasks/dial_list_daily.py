"""WP-9 Dial List — daily generation cron driver.

Assembles the day's financing-intent opportunities, ranks them by expected
revenue, and posts the top-N digest to the MONEY Slack channel — so the list
is waiting before Josh's morning calling block.

    python -m src.tasks.dial_list_daily                 # generate + post (live)
    python -m src.tasks.dial_list_daily --dry-run       # generate + log, no post
    python -m src.tasks.dial_list_daily --as-of 2026-09-16
    python -m src.tasks.dial_list_daily --county-id hillsborough

Cron: 45 10 * * *  (10:45 UTC — after the 04:00–08:15 scrape/CDS/skip-trace/
enrichment chain so scores + contacts + financing-intent are fresh, and ahead
of Josh's 7 AM Eastern calling block, mirroring new_distress_digest's morning
delivery.)
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date
from typing import Optional

from src.core.database import get_db_context
from src.services.dial_list.delivery import generate_and_deliver
from src.services.dial_list.repository import generate_dial_list

logger = logging.getLogger(__name__)


def run(
    *,
    as_of: Optional[date] = None,
    county_id: Optional[str] = None,
    dry_run: bool = False,
) -> int:
    """Generate the dial list and (unless dry-run) post it. Returns entry count."""
    effective_as_of = as_of or date.today()

    with get_db_context() as db:
        if dry_run:
            dial_list = generate_dial_list(
                db, as_of=effective_as_of, county_id=county_id
            )
            logger.info(
                "[DialList] DRY RUN %s — %d entries from %d candidates (not posted)",
                effective_as_of, len(dial_list.entries), dial_list.candidate_count,
            )
            for entry in dial_list.entries:
                logger.info(
                    "[DialList]   #%d property=%s triggers=%s est=%s",
                    entry.rank, entry.property_id,
                    ",".join(entry.triggers), entry.expected_loan,
                )
        else:
            dial_list, ts = generate_and_deliver(
                db, as_of=effective_as_of, county_id=county_id, interactive=True
            )
            logger.info(
                "[DialList] %s — %d entries posted (ts=%s)",
                effective_as_of, len(dial_list.entries), ts,
            )

    return len(dial_list.entries)


def _parse_as_of(value: Optional[str]) -> Optional[date]:
    if value is None:
        return None
    return date.fromisoformat(value)


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Generate and deliver the daily dial list.",
    )
    parser.add_argument(
        "--as-of", type=str, default=None,
        help="generation date (YYYY-MM-DD); defaults to today.",
    )
    parser.add_argument(
        "--county-id", type=str, default=None,
        help="restrict to one county; omitted = all counties.",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="generate and log the ranked list but do NOT post to Slack.",
    )
    args = parser.parse_args(argv)

    try:
        run(
            as_of=_parse_as_of(args.as_of),
            county_id=args.county_id,
            dry_run=args.dry_run,
        )
    except Exception:
        logger.error("[DialList] daily run failed", exc_info=True)
        return 1
    return 0


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
    )
    sys.exit(main())
