"""
Hunter nightly sweep (HUNTER-01, H3 wrapper; HUNTER-03/04 incremental wiring).

Wires H2's incremental resolver together with portfolio aggregation,
whale-flag scoring, portfolio profiling (HUNTER-04), and buyer-type
classification (HUNTER-03) into one nightly cron entry, per the dev-split
plan's H3 row ("Nightly enrichment sweep wiring — portfolio size & purchase
cadence"). Runs in this order because each step depends on the previous
one's writes: resolution creates/links buyer_entities first, portfolio
aggregation sums each entity's linked deeds next, whale scoring reads that
freshly-summed total_cash_volume, portfolio profiling (cadence/capacity/
hold-time) needs the same freshly-linked deeds, and buyer-type classification
reads portfolio profiling's just-persisted evidence last.

Scoped to changed_entity_ids from this run's resolution step, not the whole
table -- run_incremental returns exactly which entities it touched.
refresh_portfolio_aggregates/refresh_whale_flags already accepted an optional
entity_ids param before this changed; they just weren't given one. An empty
changed_entity_ids list (a quiet run, nothing new resolved) must SKIP these
steps entirely, not fall through to a full-table rescan -- `x or None` would
be wrong here, since an empty list is falsy and `or` would silently promote
it to None, and None means "scope to everything" to every one of these
functions. entity_ids=None is reserved for scripts/backfill_hunter_profiling.py
(the one-time initial population) -- no code path here ever passes it
deliberately.

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

from src.agents.hunter.buyer_type_classification import classify_buyer_types
from src.agents.hunter.kill_switch import hunter_halted
from src.agents.hunter.portfolio_profiling import refresh_portfolio_profiling
from src.core.database import get_db_context
from src.services.buyer_entity_resolution import refresh_portfolio_aggregates, run_incremental
from src.services.whale_detection import refresh_whale_flags
from src.utils.logger import get_logger, setup_logging

setup_logging()
logger = get_logger(__name__)


def run_sweep(county_id: str = "hillsborough") -> dict:
    # Checked before any read/write -- an operator's "STOP Hunter"
    # (redis-cli SET kill_switch_override:hunter_global red EX 3600) must
    # halt this cron-triggered writer within one cycle, per Hunter's
    # constitution. Previously only the one-time backfill script checked
    # this; the nightly sweep (and whale_auction_fast_follow.py) ran
    # unconditionally regardless of an active halt.
    if hunter_halted():
        logger.warning("[HunterNightlySweep] Hunter kill switch active -- skipping sweep, no DB mutation.")
        return {"county_id": county_id, "halted": True}

    with get_db_context() as session:
        resolution_stats = run_incremental(session, county_id=county_id)
        changed_ids = resolution_stats["changed_entity_ids"]

        if changed_ids:
            portfolio_entities_refreshed = refresh_portfolio_aggregates(session, entity_ids=changed_ids)
            whale_entities_rescored = refresh_whale_flags(session, entity_ids=changed_ids)
            profiling_entities_refreshed = refresh_portfolio_profiling(session, entity_ids=changed_ids)
            buyer_types_classified = classify_buyer_types(session, entity_ids=changed_ids)
        else:
            logger.info("[HunterNightlySweep] no entities changed this run -- skipping downstream refreshes.")
            portfolio_entities_refreshed = whale_entities_rescored = 0
            profiling_entities_refreshed = buyer_types_classified = 0

    stats = {
        "county_id": county_id,
        "resolution": resolution_stats,
        "portfolio_entities_refreshed": portfolio_entities_refreshed,
        "whale_entities_rescored": whale_entities_rescored,
        "profiling_entities_refreshed": profiling_entities_refreshed,
        "buyer_types_classified": buyer_types_classified,
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
