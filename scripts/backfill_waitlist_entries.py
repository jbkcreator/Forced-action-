"""
Backfill legacy ZipTerritory.waitlist_emails → waitlist_entries.

Every email in `ZipTerritory.waitlist_emails` was collected via the old
ZipChecker flow (ZIP already taken). These are all sold_out signups —
no phone, no sms_opt_in, waitlist_type='sold_out'.

The unique constraint uq_waitlist_zip_vert_county_email means re-running
is safe (conflicts are skipped).

Usage:
    python scripts/backfill_waitlist_entries.py [--dry-run]
"""

import argparse
import logging
import sys
from datetime import datetime, timezone

sys.path.insert(0, ".")

from sqlalchemy import select, text  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def run(dry_run: bool = False) -> None:
    from src.core.database import get_db_context
    from src.core.models import ZipTerritory, WaitlistEntry

    inserted = 0
    skipped = 0

    with get_db_context() as db:
        rows = db.execute(
            select(ZipTerritory).where(
                ZipTerritory.waitlist_emails.isnot(None),
            )
        ).scalars().all()

        for zt in rows:
            emails = zt.waitlist_emails or []
            for email in emails:
                email = email.strip().lower()
                if not email:
                    continue
                existing = db.execute(
                    select(WaitlistEntry).where(
                        WaitlistEntry.zip_code == zt.zip_code,
                        WaitlistEntry.vertical == zt.vertical,
                        WaitlistEntry.county_id == zt.county_id,
                        WaitlistEntry.email == email,
                    )
                ).scalar_one_or_none()

                if existing:
                    skipped += 1
                    continue

                if dry_run:
                    logger.info("[dry-run] would insert: %s / %s / %s / %s",
                                zt.zip_code, zt.vertical, zt.county_id, email)
                    inserted += 1
                    continue

                entry = WaitlistEntry(
                    zip_code=zt.zip_code,
                    vertical=zt.vertical,
                    county_id=zt.county_id,
                    name="",
                    email=email,
                    phone_e164=None,
                    sms_opt_in=False,
                    waitlist_type="sold_out",
                    status="waiting",
                )
                db.add(entry)
                inserted += 1

        if not dry_run:
            db.commit()

    logger.info("Backfill complete. inserted=%d skipped=%d dry_run=%s",
                inserted, skipped, dry_run)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    run(dry_run=args.dry_run)
