"""
Backfill severity_tier on all existing code_violations records.

Usage:
    python scripts/backfill_violation_severity.py           # dry run (preview only)
    python scripts/backfill_violation_severity.py --commit  # write to DB
"""

import argparse
import logging
import sys
from collections import Counter

from src.core.database import Database
from src.core.models import CodeViolation
from src.loaders.violations import classify_severity

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def run(commit: bool) -> None:
    db = Database()
    tier_counts: Counter = Counter()
    updated = 0
    skipped = 0

    with db.session_scope() as session:
        records = session.query(CodeViolation).all()
        total = len(records)
        logger.info(f"Found {total} code_violation records to process")

        for rec in records:
            tier = classify_severity(
                violation_type=rec.violation_type,
                description=rec.description,
                fine_amount=float(rec.fine_amount) if rec.fine_amount is not None else None,
                is_lien=rec.is_lien,
                status=rec.status,
            )

            tier_counts[tier] += 1

            if rec.severity_tier == tier:
                skipped += 1
                continue

            if commit:
                rec.severity_tier = tier
            updated += 1

        if commit:
            # session_scope auto-commits on exit
            logger.info(f"Committing {updated} updates...")
        else:
            # Roll back so nothing is written in dry-run mode
            session.expunge_all()

    mode = "COMMITTED" if commit else "DRY RUN"
    logger.info(f"\n{'=' * 50}")
    logger.info(f"  {mode} — {total} records processed")
    logger.info(f"  Updated : {updated}")
    logger.info(f"  Already correct / skipped: {skipped}")
    logger.info(f"\n  Severity breakdown:")
    for tier in ("Critical", "Major", "Minor"):
        count = tier_counts[tier]
        pct = count / total * 100 if total else 0
        logger.info(f"    {tier:10s}: {count:>5}  ({pct:.1f}%)")

    if not commit:
        logger.info("\n  Re-run with --commit to write changes to the database.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill severity_tier on code_violations")
    parser.add_argument(
        "--commit",
        action="store_true",
        help="Write changes to the database (default is dry run)",
    )
    args = parser.parse_args()
    run(commit=args.commit)
