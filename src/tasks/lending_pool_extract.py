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
    "normalized_phone", "phone_available", "line_type", "email",
    "financing_intent_score", "intent_tier", "recommended_product",
    "aircall_campaign_tag", "campaign_list",
    "buyer_entity_id", "permit_number", "source_property_id", "source_table",
]


def _export_run_to_csv(session, run_id: str, out_dir: Path) -> dict[str, dict[str, int]]:
    """Write one CSV per campaign (Josh's List 1-9 taxonomy) for a run.

    Split by (pool_name, campaign_list) rather than pool_name alone: Builders
    spans two lists (List 3 DBPR, List 7 NOC/permits) under one pool_name, and
    Josh's requested reporting table (client_commnets_answers.md Section 2) is
    organized per campaign/list, not per internal pool. A record with no
    campaign_list (e.g. a not-yet-confirmed mapping) groups under "unassigned"
    rather than being silently dropped from the export.

    Returns {file_key: {"total": N, "phone_available": M}}.  Exports the FULL
    set (both phone_available true and false) so Dev 2 has the A2 coverage
    denominator; phone_available=false rows are their O15 call (INVALID_PHONE).
    """
    rows = session.execute(
        text(f"""
            SELECT {", ".join(_EXPORT_COLUMNS)}
            FROM lending_calling_pool_staging
            WHERE run_id = :run_id
            ORDER BY pool_name, campaign_list, county_id
        """),
        {"run_id": run_id},
    ).mappings().all()

    out_dir.mkdir(parents=True, exist_ok=True)
    by_campaign: dict[str, list[dict]] = {}
    for row in rows:
        list_slug = (row["campaign_list"] or "unassigned").lower().replace(" ", "_")
        key = f"{row['pool_name']}__{list_slug}"
        by_campaign.setdefault(key, []).append(dict(row))

    counts: dict[str, dict[str, int]] = {}
    for file_key, campaign_rows in by_campaign.items():
        path = out_dir / f"wave0_{file_key}_{run_id[:8]}.csv"
        with path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=_EXPORT_COLUMNS, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(campaign_rows)
        counts[file_key] = {
            "total": len(campaign_rows),
            "phone_available": sum(1 for r in campaign_rows if r["phone_available"]),
        }
        logger.info("Exported %d rows (%d phone-available) to %s",
                    counts[file_key]["total"], counts[file_key]["phone_available"], path)
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

    _print_campaign_table(summary)
    print(json.dumps(summary, indent=2, default=str))
    logger.info("lending_pool_extract complete run_id=%s", summary.get("run_id"))


def _print_campaign_table(summary: dict) -> None:
    """Human-readable version of the per-campaign counts (client_commnets_answers.md
    Section 2's requested shape: campaign, raw records, records with a phone).

    NOTE: these are raw/phone-available counts only — NOT "dialable after DNC and
    suppression" (that needs WP-W0-2's compliance pass, a separate step/owner).
    """
    campaign_lists = summary.get("campaign_lists")
    if not campaign_lists:
        return
    print("\n=== Per-campaign counts (raw / with phone) — NOT post-compliance 'dialable' ===")
    print(f"{'Campaign':<14} {'Pools':<30} {'Total':>8} {'With Phone':>12} {'Hit %':>7}")
    for list_name in sorted(campaign_lists.keys()):
        bucket = campaign_lists[list_name]
        total = bucket["total"]
        with_phone = bucket["phone_available"]
        pct = f"{(with_phone / total * 100):.1f}%" if total else "0.0%"
        pools = ",".join(bucket["pool_names"])
        print(f"{list_name:<14} {pools:<30} {total:>8} {with_phone:>12} {pct:>7}")
    print(f"{'TOTAL':<14} {'':<30} {summary['total_records']:>8} {summary['total_phone_available']:>12}")
    print()


if __name__ == "__main__":
    main()
    sys.exit(0)
