"""fa029 — backfill Property.normalized_address from raw Property.address.

Walks the properties table in keyset-paginated batches and populates the
new `normalized_address` column using the canonical
`src.utils.address_normalize.normalize_street_address` function. The raw
`address` column is never written.

Idempotent: rows whose `normalized_address` already matches the canonical
form are skipped, so re-running is safe.

Usage:
    python scripts/renormalize_addresses.py [--dry-run] [--county-id ID]
        [--batch-size N] [--limit N] [--only-null]

Examples:
    # Smoke test — preview 200 rows, no writes
    python scripts/renormalize_addresses.py --dry-run --county-id hillsborough --limit 200

    # Production run for one county
    python scripts/renormalize_addresses.py --county-id hillsborough

    # Fast re-run: only touch rows that never got a normalized value
    python scripts/renormalize_addresses.py --county-id hillsborough --only-null

    # All counties
    python scripts/renormalize_addresses.py
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from typing import Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import select, update

from src.core.database import get_db_context
from src.core.models import Property
from src.utils.address_normalize import normalize_street_address

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def _county_list(db, county_id: Optional[str]) -> list[str]:
    if county_id:
        return [county_id]
    rows = db.execute(
        select(Property.county_id).distinct().where(Property.county_id.is_not(None))
    ).scalars().all()
    return sorted(rows)


def run(
    dry_run: bool,
    county_id: Optional[str],
    batch_size: int,
    limit: Optional[int],
    only_null: bool,
) -> dict:
    counts = {"scanned": 0, "changed": 0, "current": 0, "nulled": 0}
    samples: list[tuple[str, str]] = []  # (address, normalized) — first 10 changes

    with get_db_context() as db:
        counties = _county_list(db, county_id)
        logger.info(
            "Counties to process: %s (only_null=%s, batch_size=%d)",
            counties, only_null, batch_size,
        )

        for county in counties:
            last_id = 0
            county_changed = 0
            county_scanned = 0

            while True:
                if limit is not None and counts["scanned"] >= limit:
                    break

                remaining = (limit - counts["scanned"]) if limit is not None else None
                fetch_n = min(batch_size, remaining) if remaining is not None else batch_size

                stmt = (
                    select(Property.id, Property.address, Property.normalized_address)
                    .where(
                        Property.county_id == county,
                        Property.id > last_id,
                    )
                    .order_by(Property.id)
                    .limit(fetch_n)
                )
                if only_null:
                    stmt = stmt.where(Property.normalized_address.is_(None))

                batch = db.execute(stmt).all()
                if not batch:
                    break

                updates: list[dict] = []
                for row in batch:
                    counts["scanned"] += 1
                    county_scanned += 1
                    raw = row.address or ""
                    new_norm = normalize_street_address(raw) or None
                    current = row.normalized_address or None

                    if new_norm == current:
                        counts["current"] += 1
                        continue

                    if new_norm is None and row.address:
                        counts["nulled"] += 1
                    counts["changed"] += 1
                    county_changed += 1
                    if len(samples) < 10:
                        samples.append((raw, new_norm or "<NULL>"))
                    updates.append({"id": row.id, "normalized_address": new_norm})

                last_id = batch[-1].id

                if updates and not dry_run:
                    # ORM-enabled bulk UPDATE by primary key — one round-trip
                    # per batch instead of one UPDATE per dirty row.
                    db.execute(update(Property), updates)
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
        logger.info("Sample changes (raw -> normalized):")
        for raw, norm in samples:
            logger.info("  %r -> %r", raw, norm)

    return counts


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--dry-run", action="store_true",
                        help="Print sample output, no DB writes")
    parser.add_argument("--county-id", default=None,
                        help="Limit to one county (default: all)")
    parser.add_argument("--batch-size", type=int, default=5000,
                        help="Rows per commit (default: 5000)")
    parser.add_argument("--limit", type=int, default=None,
                        help="Cap total rows processed (smoke-test mode)")
    parser.add_argument("--only-null", action="store_true",
                        help="Skip rows that already have a normalized_address "
                             "(fast re-run mode; will not detect drift in the "
                             "normalizer output for already-populated rows)")
    args = parser.parse_args()

    try:
        result = run(
            dry_run=args.dry_run,
            county_id=args.county_id,
            batch_size=args.batch_size,
            limit=args.limit,
            only_null=args.only_null,
        )
    except Exception as exc:
        logger.error("[renormalize_addresses] FAILED: %s", exc, exc_info=True)
        sys.exit(1)

    logger.info("[renormalize_addresses] %s%s", result, " (dry-run)" if args.dry_run else "")
    print(result)
