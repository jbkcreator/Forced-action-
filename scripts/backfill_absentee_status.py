"""Backfill Owner.absentee_status using the corrected mailing-vs-situs comparison.

The original _determine_absentee_status() compared the full mailing address blob
("ADDR_1, CITY, STATE, ZIP") directly against the bare situs street ("SITE_ADDR"),
causing an exact-string comparison that could never match for full-address rows and
was fragile for street-only rows. As a result nearly every property was stamped
Out-of-County, and Out-of-State was never produced.

This script recomputes absentee_status for all existing Owner rows using the fixed
logic from MasterPropertyLoader._determine_absentee_status():
  1. Parse mailing_address blob → (mailing_street, mailing_state)
  2. If mailing_state != Property.state → Out-of-State
  3. If normalize(mailing_street) == normalize(Property.address) → In-County
  4. Fallback → Out-of-County

Only Owner.absentee_status is written. No other column or table is touched.

Known limitation: rows where mailing_address contains only a street (no city/state/zip
was stored at load time) cannot have Out-of-State detected — state data was simply
never captured for those rows. In-County detection still works via street comparison.

Usage:
    python scripts/backfill_absentee_status.py [--dry-run] [--county-id ID]
        [--batch-size N] [--limit N]

Examples:
    # Smoke test — preview 200 rows, no writes
    python scripts/backfill_absentee_status.py --dry-run --county-id hillsborough --limit 200

    # Production run for one county
    python scripts/backfill_absentee_status.py --county-id hillsborough

    # All counties
    python scripts/backfill_absentee_status.py
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from typing import Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import select
from sqlalchemy.orm import joinedload

from src.core.database import get_db_context
from src.core.models import Owner
from src.loaders.master import MasterPropertyLoader

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def _county_list(db, county_id: Optional[str]) -> list[str]:
    if county_id:
        return [county_id]
    rows = db.execute(
        select(Owner.county_id).distinct().where(Owner.county_id.is_not(None))
    ).scalars().all()
    return sorted(rows)


def run(dry_run: bool, county_id: Optional[str], batch_size: int, limit: Optional[int]) -> dict:
    counts = {"scanned": 0, "changed": 0, "current": 0, "skipped_no_property": 0}
    samples: list[tuple] = []  # (parcel_id, old_status, new_status) — first 10 changes

    with get_db_context() as db:
        counties = _county_list(db, county_id)
        logger.info("Counties to process: %s", counties)

        for county in counties:
            last_id = 0
            county_changed = 0
            county_scanned = 0

            while True:
                if limit is not None and counts["scanned"] >= limit:
                    break

                batch = db.execute(
                    select(Owner)
                    .options(joinedload(Owner.property))
                    .where(
                        Owner.county_id == county,
                        Owner.id > last_id,
                        Owner.mailing_address.is_not(None),
                    )
                    .order_by(Owner.id)
                    .limit(batch_size)
                ).scalars().all()

                if not batch:
                    break

                for owner in batch:
                    counts["scanned"] += 1
                    county_scanned += 1

                    prop = owner.property
                    if prop is None:
                        counts["skipped_no_property"] += 1
                        continue

                    # mailing_address is guaranteed non-None by the query filter above.
                    # Delegate to the loader's method so logic stays in one place.
                    new_status = MasterPropertyLoader._determine_absentee_status(
                        property_address=prop.address,
                        property_state=prop.state,
                        mailing_address=owner.mailing_address,
                    )

                    if new_status == owner.absentee_status:
                        counts["current"] += 1
                    else:
                        counts["changed"] += 1
                        county_changed += 1
                        if len(samples) < 10:
                            samples.append((
                                prop.parcel_id,
                                owner.absentee_status,
                                new_status,
                            ))
                        if not dry_run:
                            owner.absentee_status = new_status

                    if limit is not None and counts["scanned"] >= limit:
                        break

                last_id = batch[-1].id
                if not dry_run:
                    db.commit()
                logger.info(
                    "county=%s scanned=%d changed=%d last_id=%d",
                    county, county_scanned, county_changed, last_id,
                )

            logger.info(
                "county=%s DONE scanned=%d changed=%d",
                county, county_scanned, county_changed,
            )

    if samples:
        logger.info("Sample changes (parcel_id | old → new):")
        for parcel_id, old, new in samples:
            logger.info("  %s  %r → %r", parcel_id, old, new)

    return counts


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--dry-run", action="store_true",
                        help="Print sample output, no DB writes")
    parser.add_argument("--county-id", default=None,
                        help="Limit to one county (default: all)")
    parser.add_argument("--batch-size", type=int, default=1000,
                        help="Rows per commit (default: 1000)")
    parser.add_argument("--limit", type=int, default=None,
                        help="Cap total rows processed (smoke-test mode)")
    args = parser.parse_args()

    try:
        result = run(
            dry_run=args.dry_run,
            county_id=args.county_id,
            batch_size=args.batch_size,
            limit=args.limit,
        )
    except Exception as exc:
        logger.error("[backfill_absentee_status] FAILED: %s", exc, exc_info=True)
        sys.exit(1)

    logger.info(
        "[backfill_absentee_status] %s%s",
        result,
        " (dry-run)" if args.dry_run else "",
    )
    print(result)
