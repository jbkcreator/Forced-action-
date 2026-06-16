"""Daily financing-intent scoring sweep (Sprint S1).

Scores properties for hard-money / bridge / renovation / refinance / buyout
financing intent and writes results to financing_intent_scores.

Usage:
    python -m src.tasks.financing_intent_sweep [options]

Options:
    --dry-run       Score but do not write to DB (prints summary)
    --county-id     Restrict sweep to one county
    --limit N       Stop after scoring N properties
    --rescore-all   Re-score properties already scored today
"""

from __future__ import annotations

import argparse
import json
import logging
import sys

from src.core.database import get_db_context
from src.services.financing_intent_engine import score_properties_for_financing
from src.utils.logger import setup_logging

setup_logging()
logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Financing-intent scoring sweep",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--dry-run",     action="store_true", help="Score without writing to DB")
    parser.add_argument("--county-id",   default=None,        help="Restrict to one county")
    parser.add_argument("--limit",       type=int, default=None, help="Max properties to score")
    parser.add_argument("--rescore-all", action="store_true", help="Re-score today's rows")
    args = parser.parse_args()

    logger.info(
        "Starting financing_intent_sweep county=%s limit=%s dry_run=%s rescore_all=%s",
        args.county_id, args.limit, args.dry_run, args.rescore_all,
    )

    with get_db_context() as session:
        result = score_properties_for_financing(
            session,
            county_id=args.county_id,
            limit=args.limit,
            dry_run=args.dry_run,
            rescore_all=args.rescore_all,
        )

    logger.info("Sweep complete: %s", result)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
    sys.exit(0)
