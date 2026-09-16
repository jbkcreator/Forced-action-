"""
WP-5B — FA Max borrower profile nightly sweep.

Runs after Hunter's nightly sweep (hunter_nightly_sweep.py, 08:30–08:31 UTC)
so that BuyerEntity cadence/portfolio data is already fresh when we compute
buy-box profiles and next-need predictions.

For each active (non-merged) fa_max_persons row that has a buyer_entity_id
populated (provisional bridge from WP-5B migration, later formalised by
WP-3/WP-4), calls compute_person_profile() and upserts the result into
fa_max_person_profiles.

Also attempts to discover entity links for persons where buyer_entity_id is
NULL, by looking up buyer_entities rows that share the same source_reference
(e.g. a county parcel owner record).  This is a best-effort, conservative
lookup — no fuzzy matching; the result sets confidence_tier accordingly.

Persons already profiled within the last 20 hours are skipped unless
--force-all is passed, to avoid redundant recomputation on quiet days.

Usage (cron):
    PYTHONPATH=. python -m src.tasks.fa_max_profile_sweep
    PYTHONPATH=. python -m src.tasks.fa_max_profile_sweep --force-all

Wiring note (WP-3/WP-4):
    When WP-3/WP-4 ship, they will populate fa_max_persons.buyer_entity_id
    with a formal, reversible-merge-logged entity link.  Until then this sweep
    provides a provisional first-pass link and deliberately uses conservative
    matching to avoid the "a bad merge is worse than a duplicate" principle from
    SOT.md Part 4.
"""
from __future__ import annotations

import argparse
import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import text

from src.core.database import get_db_context
from src.services.borrower_profile_service import compute_person_profile
from src.utils.logger import get_logger, setup_logging

setup_logging()
logger = get_logger(__name__)

# Skip persons profiled more recently than this (unless --force-all)
_RECENCY_HOURS = 20


def run_sweep(*, force_all: bool = False) -> dict:
    """Compute and upsert profiles for all active fa_max_persons.

    Returns a summary dict for logging.
    """
    staleness_cutoff = datetime.now(timezone.utc) - timedelta(hours=_RECENCY_HOURS)

    with get_db_context() as session:
        # Step 1: best-effort entity discovery for unlinked persons
        linked_count = _link_unlinked_persons(session)

        # Step 2: collect persons to profile
        if force_all:
            where_clause = "merged_into_id IS NULL"
        else:
            where_clause = """
                merged_into_id IS NULL
                AND (
                    NOT EXISTS (
                        SELECT 1 FROM fa_max_person_profiles p
                        WHERE p.person_id = fp.person_id
                          AND p.computed_at > :cutoff
                    )
                )
            """

        result = session.execute(
            text(f"""
                SELECT fp.person_id::text
                FROM fa_max_persons fp
                WHERE {where_clause}
                ORDER BY fp.created_at DESC
            """),
            {"cutoff": staleness_cutoff},
        )

        total = 0
        computed = 0
        errors = 0

        # Stream rows one partition at a time to avoid loading all person_ids
        # into memory at once (CLAUDE.md: never .fetchall() on large tables).
        for partition in result.partitions(500):
            for row in partition:
                person_id = row[0]
                total += 1
                try:
                    compute_person_profile(session, person_id)
                    session.commit()
                    computed += 1
                except Exception:
                    session.rollback()
                    logger.exception("[FA Max ProfileSweep] error computing profile for %s", person_id)
                    errors += 1

    stats = {
        "total_persons": total,
        "computed": computed,
        "errors": errors,
        "newly_linked": linked_count,
        "force_all": force_all,
    }
    logger.info("[FA Max ProfileSweep] %s", stats)
    return stats


def _link_unlinked_persons(session) -> int:
    """Attempt a conservative entity link for persons where buyer_entity_id is NULL.

    Conservative rule: a BuyerEntity row is linked only when a single
    buyer_entity_links row with source_table='owners' references an owner row
    whose canonical_name exactly matches (case-insensitive) the
    fa_max_persons.source_reference value AND the match_confidence >= 70
    (Hunter's UNVERIFIED threshold from buyer_entities docstring).

    This is intentionally narrow — it links obvious cases and leaves ambiguous
    ones unlinked so confidence_tier reflects the data quality honestly.
    WP-3/WP-4 will replace this with a proper identity-graph join.
    """
    result = session.execute(
        text("""
            UPDATE fa_max_persons fp
            SET buyer_entity_id = be.id
            FROM buyer_entity_links bel
            JOIN buyer_entities be ON bel.buyer_entity_id = be.id
            JOIN owners o ON bel.source_id = o.id
            WHERE bel.source_table = 'owners'
              AND bel.match_confidence >= 70
              AND fp.buyer_entity_id IS NULL
              AND fp.merged_into_id IS NULL
              AND fp.source_reference IS NOT NULL
              AND LOWER(be.canonical_name) = LOWER(fp.source_reference)
            RETURNING fp.person_id
        """)
    )
    count = result.rowcount
    if count:
        session.commit()
        logger.info("[FA Max ProfileSweep] linked %d unlinked persons to buyer entities", count)
    return count


def main() -> None:
    parser = argparse.ArgumentParser(description="FA Max WP-5B borrower profile nightly sweep.")
    parser.add_argument(
        "--force-all",
        action="store_true",
        default=False,
        help="Recompute all profiles regardless of recency.",
    )
    args = parser.parse_args()
    stats = run_sweep(force_all=args.force_all)
    print(stats)


if __name__ == "__main__":
    main()
