"""
One-time backfill: populate portfolio profiling (HUNTER-04: cadence,
estimated acquisition capacity, financing signal, average hold-time) and
buyer-type classification (HUNTER-03: flipper/buy-and-hold/wholesaler/
institutional) for every existing buyer_entities row.

Follows scripts/backfill_buyer_entities.py's (HUNTER-01, H2.6) convention:
this is the ONLY caller that passes entity_ids=None to
src.agents.hunter.portfolio_profiling.refresh_portfolio_profiling and
src.agents.hunter.buyer_type_classification.classify_buyer_types. Every other
caller (src.tasks.hunter_nightly_sweep) scopes to the specific
changed_entity_ids its own run touched -- entity_ids=None means "recompute
every entity," which is only ever correct for this one-time initial
population, never for a routine incremental run.

No --dry-run flag, unlike backfill_buyer_entities.py: both functions here
compute AND write+commit in one call (the same self-committing convention
already used by refresh_whale_flags/refresh_portfolio_aggregates and relied
on by hunter_nightly_sweep.py, which has no dry-run option either) -- there's
no preview-only mode to defer to without changing that shared convention, and
adding a rollback-after-commit "dry run" here would be misleading, not safe.

Checks the Hunter kill switch before running (redis-cli SET
kill_switch_override:hunter_global red EX 3600 halts it), same as the
nightly sweep and the H1/H2 backfill.

Not county-scoped -- a buyer entity isn't bound to one county (see
hunter_nightly_sweep.py's own note on this), and both functions already
sweep the full buyer_entities table in one pass regardless of county.

Usage:
    PYTHONPATH=. python scripts/backfill_hunter_profiling.py
"""
from __future__ import annotations

import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def run_backfill() -> None:
    from src.agents.hunter.buyer_type_classification import classify_buyer_types
    from src.agents.hunter.kill_switch import hunter_halted
    from src.agents.hunter.portfolio_profiling import refresh_portfolio_profiling
    from src.core.database import get_db_context

    if hunter_halted():
        logger.warning("backfill_hunter_profiling: Hunter kill switch active -- aborting, no DB mutation.")
        return

    with get_db_context() as session:
        profiled = refresh_portfolio_profiling(session, entity_ids=None)
        logger.info("backfill_hunter_profiling: portfolio-profiled %d entit(y/ies).", profiled)

        classified = classify_buyer_types(session, entity_ids=None)
        logger.info("backfill_hunter_profiling: buyer-type-classified %d entit(y/ies).", classified)

    logger.info("Backfill complete: %d profiled, %d classified.", profiled, classified)


if __name__ == "__main__":
    run_backfill()
