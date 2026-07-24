"""
Hunter nightly sweep (HUNTER-01, H3 wrapper).

Wires H2's incremental resolver together with portfolio aggregation and
whale-flag scoring into one nightly cron entry, per the dev-split plan's H3
row ("Nightly enrichment sweep wiring — portfolio size & purchase cadence").
Runs in this order because each step depends on the previous one's writes:
resolution creates/links buyer_entities first, portfolio aggregation sums
each entity's linked deeds next, and whale scoring reads that freshly-summed
total_cash_volume last.

Portfolio aggregation and whale scoring are deliberately NOT county-scoped
(no --county-id filter) even though resolution is — a buyer entity isn't
bound to one county, and both are cheap set-based SQL sweeps over the whole
`buyer_entities` table already, per their own docstrings. Safe to run once
per county's cron invocation; the second run is a no-op recompute, not a
correctness risk.

Does NOT write to /shared/facts/enriched/ yet — per plan §6b, that schema
doesn't exist and is being defined via a Vera/Hunter sync (Phase C in
Hunter's pending-tasks plan). A stub writer here risks exactly the "Vera and
Hunter each invent an incompatible fact record shape" failure §6b warns
about, so this sweep stops at the DB; the facts-directory write is a
separate, explicitly deferred follow-up once that sync happens.

Usage (cron):
    PYTHONPATH=. python -m src.tasks.hunter_nightly_sweep --county-id hillsborough
"""
from __future__ import annotations

import argparse

from src.core.database import get_db_context
from src.services.buyer_entity_resolution import refresh_portfolio_aggregates, run_incremental
from src.services.whale_detection import refresh_whale_flags
from src.utils.logger import get_logger, setup_logging

setup_logging()
logger = get_logger(__name__)


def run_sweep(county_id: str = "hillsborough") -> dict:
    with get_db_context() as session:
        resolution_stats = run_incremental(session, county_id=county_id)
        portfolio_entities_refreshed = refresh_portfolio_aggregates(session)
        whale_entities_rescored = refresh_whale_flags(session)

    stats = {
        "county_id": county_id,
        "resolution": resolution_stats,
        "portfolio_entities_refreshed": portfolio_entities_refreshed,
        "whale_entities_rescored": whale_entities_rescored,
    }
    logger.info("[HunterNightlySweep] %s", stats)
    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description="Hunter nightly buyer-entity resolution + whale sweep.")
    parser.add_argument("--county-id", default="hillsborough")
    args = parser.parse_args()

    stats = run_sweep(county_id=args.county_id)
    print(stats)


if __name__ == "__main__":
    main()
