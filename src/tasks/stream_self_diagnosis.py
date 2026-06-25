"""Stream self-diagnosis cron task (Sprint 4.2).

Computes stream metrics per county, upserts into platform_daily_stats, and
opens/updates/resolves stream_diagnostics episodes on 3-day breaches.

Usage:
    python -m src.tasks.stream_self_diagnosis [options]

Options:
    --county-id   County to run (default: all configured counties)
    --dry-run     Compute + classify but write nothing; prints JSON result
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import date

from sqlalchemy import text

from config.stream_diagnostics import STREAM_METRICS
from src.core.database import get_db_context
from src.services import stream_metrics as sm
from src.services.stream_diagnosis import run_metric_lifecycle
from src.utils.logger import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

_COMPUTE_FN = {
    "compute_enrichment_rate": sm.compute_enrichment_rate,
    "compute_dialable_rate": sm.compute_dialable_rate,
    "compute_sms_delivery_rate": sm.compute_sms_delivery_rate,
    "compute_closer_conv_rate": sm.compute_closer_conv_rate,
}

_DEFAULT_COUNTIES = ["hillsborough", "pinellas"]


def run(county_id: str, *, dry_run: bool = False) -> dict:
    today = date.today()
    results = []

    with get_db_context() as db:
        for metric_name, cfg in STREAM_METRICS.items():
            compute_fn = _COMPUTE_FN[cfg["compute"]]
            value = compute_fn(db, county_id)

            if value is not None and not dry_run:
                col = cfg["column"]
                db.execute(text(f"""
                    INSERT INTO platform_daily_stats
                        (run_date, county_id, {col},
                         signals_scraped, signals_matched, signals_skipped,
                         properties_scored, properties_with_signals, score_runs_total,
                         leads_new, leads_updated, leads_unchanged, leads_qualified,
                         leads_upgraded, tier_ultra_platinum, tier_platinum, tier_gold,
                         tier_silver, tier_bronze, created_at, updated_at)
                    VALUES (:today, :cid, :val,
                            0,0,0, 0,0,0, 0,0,0,0, 0,0,0,0, 0,0, NOW(), NOW())
                    ON CONFLICT (run_date, county_id) DO UPDATE SET {col}=:val
                """), {"today": today, "cid": county_id, "val": value})

            action = None
            if not dry_run:
                result = run_metric_lifecycle(db, county_id, metric_name, today)
                action = result["action"]

            results.append({
                "metric_name": metric_name,
                "county_id": county_id,
                "value": value,
                "action": action,
                "dry_run": dry_run,
            })
            logger.info(
                "county=%s metric=%s value=%s action=%s dry_run=%s",
                county_id, metric_name, value, action, dry_run,
            )

    return {"county_id": county_id, "date": str(today), "metrics": results}


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Stream self-diagnosis sweep",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--county-id", default=None, help="County to diagnose (default: all)")
    parser.add_argument("--dry-run", action="store_true", help="Compute without writing")
    args = parser.parse_args()

    counties = [args.county_id] if args.county_id else _DEFAULT_COUNTIES
    all_results = []
    for county in counties:
        logger.info("Starting stream_self_diagnosis county=%s dry_run=%s", county, args.dry_run)
        result = run(county, dry_run=args.dry_run)
        all_results.append(result)
        logger.info("Done county=%s", county)

    print(json.dumps(all_results, indent=2))


if __name__ == "__main__":
    main()
    sys.exit(0)
