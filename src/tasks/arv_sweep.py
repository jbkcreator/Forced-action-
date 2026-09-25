"""WP-8A ARV sweep — nightly compute+persist for dial-list candidates.

Computes and persists canonical ARV results for properties that are active
dial-list candidates (present in financing_intent_scores within the 36-month
lookback window) but either have no computed ARV row or have a stale one
(>7 days old). Stale rows are superseded by the idempotent persist contract
in arv_persistence — same inputs produce a no-op; changed inputs insert+mark-
old-superseded.

Runs at 10:00 UTC — after CDS (07:00) so distress scores are settled and
after the financing-intent sweep so scores are today's, and before the dial
list (10:45) so _batch_published_arv can read fresh rows.

    python -m src.tasks.arv_sweep
    python -m src.tasks.arv_sweep --county-id hillsborough
    python -m src.tasks.arv_sweep --dry-run
    python -m src.tasks.arv_sweep --stale-days 3
    python -m src.tasks.arv_sweep --as-of 2026-09-22
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, timedelta
from typing import Optional

from sqlalchemy import text

from src.core.database import get_db_context
from src.services.quote_ready.arv_persistence import persist_arv_result
from src.services.quote_ready.arv_repository import compute_arv_for_property

logger = logging.getLogger(__name__)

_FIS_LOOKBACK_DAYS = 36 * 30   # mirrors dial list's financing-intent window
_DEFAULT_STALE_DAYS = 7         # recompute rows older than this
_AFTER_REPAIR_CONDITION = 3     # Average — neutral when post-repair condition unknown


def _candidate_property_ids(
    session,
    *,
    county_id: Optional[str],
    since: date,
    stale_before: date,
    limit: Optional[int] = None,
) -> list[int]:
    """Return property IDs that are active financing-intent candidates with a
    missing or stale computed ARV row.

    Bounded to financing_intent_scores within the 36-month lookback window —
    the primary dial-list feed. Properties only on other detectors (cash buys,
    permits, probate) will be picked up in a future expansion; for now the FIS
    set covers the highest-value candidates.
    """
    county_filter = "AND fis.county_id = :county_id" if county_id else ""
    limit_clause = "LIMIT :limit" if limit else ""
    params: dict = {"since": since, "stale_before": stale_before}
    if county_id:
        params["county_id"] = county_id
    if limit:
        params["limit"] = limit
    rows = session.execute(
        text(f"""
            SELECT DISTINCT fis.property_id
            FROM financing_intent_scores fis
            LEFT JOIN (
                SELECT property_id, MAX(computed_at) AS latest_arv_at
                FROM fa_max_arv_results
                WHERE status = 'computed'
                GROUP BY property_id
            ) arv ON arv.property_id = fis.property_id
            WHERE fis.score_date >= :since
              {county_filter}
              AND (arv.latest_arv_at IS NULL OR arv.latest_arv_at < :stale_before)
            ORDER BY fis.property_id
            {limit_clause}
        """),
        params,
    ).fetchall()
    return [r[0] for r in rows]


def run(
    *,
    county_id: Optional[str] = None,
    dry_run: bool = False,
    stale_days: int = _DEFAULT_STALE_DAYS,
    as_of: Optional[date] = None,
    limit: Optional[int] = None,
) -> dict:
    """Compute and persist ARV for stale/missing dial-list candidates.

    Returns summary: candidates, computed, persisted, skipped_unknown, errors.
    """
    effective_as_of = as_of or date.today()
    since = effective_as_of - timedelta(days=_FIS_LOOKBACK_DAYS)
    stale_before = effective_as_of - timedelta(days=stale_days)

    computed = 0
    persisted = 0
    skipped_unknown = 0
    errors = 0

    with get_db_context() as db:
        property_ids = _candidate_property_ids(
            db,
            county_id=county_id,
            since=since,
            stale_before=stale_before,
            limit=limit,
        )
        logger.info(
            "[ARVSweep] %d candidates (county=%s stale_days=%d as_of=%s)",
            len(property_ids), county_id or "all", stale_days, effective_as_of,
        )

        for pid in property_ids:
            try:
                result = compute_arv_for_property(
                    db,
                    subject_property_id=pid,
                    as_of_yr=effective_as_of.year,
                    as_of_mo=effective_as_of.month,
                    after_repair_condition=_AFTER_REPAIR_CONDITION,
                )
                computed += 1

                if result.arv_unknown:
                    skipped_unknown += 1
                    logger.debug(
                        "[ARVSweep] property_id=%d: unknown (%s), skipping persist",
                        pid, result.unknown_reason,
                    )
                    continue

                if dry_run:
                    logger.info(
                        "[ARVSweep] DRY RUN property_id=%d: point=%s conf=%s",
                        pid, result.point, result.confidence,
                    )
                    persisted += 1
                    continue

                persist_arv_result(
                    db,
                    property_id=pid,
                    result=result,
                    computed_by="arv_sweep",
                )
                persisted += 1

                # WP-T3-8 event-driven hook: notify fundability agent that a
                # new ARV is available for this property so any waiting
                # opportunity can resolve its pending_enrichment gap the same
                # day rather than waiting for the next fundability sweep.
                # Guard: if T3-7 tables (fa_max_opportunity_facts) aren't
                # applied yet, the import fails silently so arv_sweep itself
                # is never broken by the T3-8 dependency.
                try:
                    from src.services.fa_max_fundability_agent import (
                        populate_arv_for_property,
                    )
                    with db.begin_nested():
                        populate_arv_for_property(session=db, property_id=pid)
                except ImportError:
                    pass
                except Exception:
                    logger.warning(
                        "[ARVSweep] fundability hook failed for property_id=%d"
                        " — sweep result unaffected",
                        pid,
                    )

            except Exception as exc:
                errors += 1
                logger.warning("[ARVSweep] property_id=%d failed: %s", pid, exc)

    summary = {
        "candidates": len(property_ids),
        "computed": computed,
        "persisted": persisted,
        "skipped_unknown": skipped_unknown,
        "errors": errors,
    }
    logger.info("[ARVSweep] complete: %s", summary)
    return summary


def main(argv: Optional[list[str]] = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="WP-8A ARV nightly sweep")
    parser.add_argument("--county-id", default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--stale-days", type=int, default=_DEFAULT_STALE_DAYS)
    parser.add_argument("--as-of", default=None)
    parser.add_argument("--limit", type=int, default=None,
                        help="max candidates to process (operational safety cap)")
    args = parser.parse_args(argv)

    as_of = date.fromisoformat(args.as_of) if args.as_of else None
    try:
        run(
            county_id=args.county_id,
            dry_run=args.dry_run,
            stale_days=args.stale_days,
            as_of=as_of,
            limit=args.limit,
        )
    except Exception:
        logger.error("[ARVSweep] run failed", exc_info=True)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
