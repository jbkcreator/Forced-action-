"""Wave 0 calling-pool extraction task — WP-W0-1.

Extracts three Aircall calling pools from the FA database for Hillsborough and
Pinellas counties and writes results to ``lending_calling_pool_staging``.

Trigger: manual CLI run for Wave 0.  A nightly scheduler is Wave 1 (O21).

Usage:
    python -m src.tasks.lending_pool_extract [options]

Options:
    --dry-run           Extract and print counts; do not write to DB.
    --counties          Comma-separated county names (default: Hillsborough,Pinellas).
    --export-csv DIR    After the run, write one CSV per pool into DIR (the FULL
                        export for Dev 2's A2 coverage check — all rows, including
                        phone_available=false). CSVs hold real phone numbers —
                        keep DIR local and OUT OF GIT.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import sys
from pathlib import Path

from sqlalchemy import text

from src.core.database import get_db_context
from src.services.lending.pool_extraction import (
    WAVE0_COUNTY_NAMES,
    extract_calling_pools,
)
from src.utils.logger import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

# Full staging column set, in a stable order, for the compliance export.
_EXPORT_COLUMNS = [
    "run_id", "pool_name", "county_id", "county_name",
    "borrower_name", "entity_name", "entity_status",
    "parcel_id", "target_property_address", "zip", "state",
    "estimated_loan_value", "recent_permit_details",
    "normalized_phone", "phone_available", "email",
    "financing_intent_score", "intent_tier", "recommended_product",
    "aircall_campaign_tag",
    "buyer_entity_id", "permit_number", "source_property_id", "source_table",
]


def _export_run_to_csv(session, run_id: str, out_dir: Path) -> dict[str, dict[str, int]]:
    """Write one CSV per pool for a run, reading back from the staging table.

    Returns {pool_name: {"total": N, "phone_available": M}}.  Exports the FULL
    set (both phone_available true and false) so Dev 2 has the A2 coverage
    denominator; phone_available=false rows are their O15 call (INVALID_PHONE).
    """
    rows = session.execute(
        text(f"""
            SELECT {", ".join(_EXPORT_COLUMNS)}
            FROM lending_calling_pool_staging
            WHERE run_id = :run_id
            ORDER BY pool_name, county_id
        """),
        {"run_id": run_id},
    ).mappings().all()

    out_dir.mkdir(parents=True, exist_ok=True)
    by_pool: dict[str, list[dict]] = {}
    for row in rows:
        by_pool.setdefault(row["pool_name"], []).append(dict(row))

    counts: dict[str, dict[str, int]] = {}
    for pool_name, pool_rows in by_pool.items():
        path = out_dir / f"wave0_{pool_name}_{run_id[:8]}.csv"
        with path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=_EXPORT_COLUMNS, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(pool_rows)
        counts[pool_name] = {
            "total": len(pool_rows),
            "phone_available": sum(1 for r in pool_rows if r["phone_available"]),
        }
        logger.info("Exported %d rows (%d phone-available) to %s",
                    counts[pool_name]["total"], counts[pool_name]["phone_available"], path)
    return counts


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Wave 0 calling-pool extraction (WP-W0-1)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--dry-run", action="store_true", help="Extract without writing to DB")
    parser.add_argument(
        "--counties",
        default=",".join(WAVE0_COUNTY_NAMES),
        help="Comma-separated county names",
    )
    parser.add_argument(
        "--export-csv",
        default=None,
        metavar="DIR",
        help="Directory for per-pool CSVs (real phone data — keep local, out of git)",
    )
    args = parser.parse_args()

    county_names = tuple(c.strip() for c in args.counties.split(",") if c.strip())

    logger.info(
        "Starting lending_pool_extract county_names=%s dry_run=%s export_csv=%s",
        county_names, args.dry_run, args.export_csv,
    )

    with get_db_context() as session:
        summary = extract_calling_pools(
            session,
            dry_run=args.dry_run,
            county_names=county_names,
        )

        if args.export_csv and not args.dry_run and summary.get("run_id"):
            export_counts = _export_run_to_csv(session, summary["run_id"], Path(args.export_csv))
            summary["csv_export"] = {"dir": args.export_csv, "pools": export_counts}
        elif args.export_csv and args.dry_run:
            logger.warning("--export-csv ignored under --dry-run (nothing written to staging).")

    print(json.dumps(summary, indent=2, default=str))
    logger.info("lending_pool_extract complete run_id=%s", summary.get("run_id"))


if __name__ == "__main__":
    main()
    sys.exit(0)
