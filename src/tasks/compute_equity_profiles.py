"""
Nightly batch sweep to compute property equity profiles (Sprint 4.4).

Pipeline:
  1. Roll latest deed mortgage_amount → financials.est_mortgage_bal
  2. Aggregate lien amounts from spoke tables → financials.total_lien_amount
  3. Compute equity fields (est_equity, equity_pct, total_debt) from
     assessed_value_mkt - (est_mortgage_bal + total_lien_amount)

Runs at ~03:00 UTC after the deed loader, HCPA refresh, and all
lien/scraper pipelines have completed for the day.

Usage:
    python -m src.tasks.compute_equity_profiles
    python -m src.tasks.compute_equity_profiles --county-id hillsborough
    python -m src.tasks.compute_equity_profiles --county-id pinellas
"""
import argparse
import logging
import sys
import time
from typing import Optional

from src.core.database import Database
from src.services.equity_compute import compute_equity_profiles
from src.services.lien_aggregator import aggregate_lien_amounts
from src.services.mortgage_aggregator import aggregate_mortgage_balances

logger = logging.getLogger(__name__)


def run_equity_pipeline(county_id: Optional[str] = None) -> int:
    """Run the full equity pipeline: mortgage rollup → lien aggregation → equity compute.

    Returns total affected rows across the three steps.
    """
    t0 = time.monotonic()
    db = Database()

    # Step 1: roll each property's latest deed mortgage_amount into est_mortgage_bal
    mortgage_affected = aggregate_mortgage_balances(db=db, county_id=county_id)
    logger.info(
        "Step 1/3 — Mortgage rollup: %d financials rows affected in %.1fs",
        mortgage_affected, time.monotonic() - t0,
    )

    # Step 2: aggregate all lien amounts into financials.total_lien_amount
    t1 = time.monotonic()
    lien_affected = aggregate_lien_amounts(db=db, county_id=county_id)
    logger.info(
        "Step 2/3 — Lien aggregation: %d financials rows affected in %.1fs",
        lien_affected, time.monotonic() - t1,
    )

    # Step 3: compute equity from assessed_value_mkt - total_debt
    t2 = time.monotonic()
    equity_affected = compute_equity_profiles(db=db, county_id=county_id)
    logger.info(
        "Step 3/3 — Equity compute: %d financials rows affected in %.1fs",
        equity_affected, time.monotonic() - t2,
    )

    total = mortgage_affected + lien_affected + equity_affected
    elapsed = time.monotonic() - t0
    logger.info(
        "Equity pipeline complete: %d total rows affected in %.1fs",
        total, elapsed,
    )
    return total


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compute property equity profiles (Sprint 4.4)"
    )
    parser.add_argument(
        "--county-id", default=None,
        help="Limit to one county (default: all counties)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )

    try:
        total = run_equity_pipeline(county_id=args.county_id)
        logger.info("SUCCESS: %d rows affected", total)
    except Exception:
        logger.exception("Equity pipeline failed")
        sys.exit(1)


if __name__ == "__main__":
    main()