"""
One-time backfill: build the initial buyer_entities/buyer_entity_links
roster from the full historical owners+deeds dataset (HUNTER-01, H2.6).

Runs the full resolution pipeline (H2.1-H2.5) once over every owners/deeds
row, materializes one BuyerEntity per cluster (plus one per unmatched
singleton), and bulk-inserts BuyerEntityLink rows tracing every raw record
back to its entity. Materialization helpers (canonical_name, entity_type,
confidence rollup, ...) live in src/services/buyer_entity_resolution.py,
shared with the nightly incremental sweep (H2.7) rather than duplicated here.

Checks the Hunter kill switch before running (redis-cli SET
kill_switch_override:hunter_global red EX 3600 halts it).

Resumable by construction: only_unresolved=True (via extract_candidates)
means every invocation -- the first, and any rerun after an interruption --
only pulls owners/deeds rows with no existing buyer_entity_links row yet.
Already-linked rows from prior committed batches are loaded instead as
existing-entity ANCHORS (load_existing_entity_candidates) and matched
against via the same cluster_against_anchors/attach_or_create_entities pair
run_incremental (H2.7) uses -- a cluster touching exactly one anchor
attaches to it, never creating a duplicate entity or re-inserting an
already-committed link. On a from-scratch run (empty buyer_entities table)
this is behaviorally identical to a full unscoped extraction, since nothing
is linked yet. Confirmed as a real bug in the prior always-full-rescan
version: rerunning after any committed batch hit a unique-constraint
violation on buyer_entity_links(source_table, source_id) for every row that
batch had already inserted.

Commits in batches of _ENTITY_COMMIT_BATCH clusters, not as one transaction
for the whole run -- there's no separate staging DB in this platform, so
this writes against the same shared DB live traffic uses. A crash mid-run
only loses the current uncommitted batch, not the whole run, and progress
is logged after every batch commit (grep "Committed:" in the log to watch
progress on a long unattended run).

Usage:
    PYTHONPATH=. python scripts/backfill_buyer_entities.py [--dry-run] [--county-id X]
"""
from __future__ import annotations

import argparse
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_ENTITY_COMMIT_BATCH = 2000  # commit every N clusters -- see run_backfill docstring on why this isn't one giant transaction


def run_backfill(county_id=None, dry_run: bool = False) -> None:
    from src.agents.hunter.kill_switch import hunter_halted
    from src.core.database import get_db_context
    from src.services.buyer_entity_resolution import (
        _ENTITY_ANCHOR_TABLE,
        attach_or_create_entities,
        cluster_against_anchors,
        extract_candidates,
        load_existing_entity_candidates,
        record_ambiguous_pair_exceptions,
    )

    if hunter_halted():
        logger.warning("Hunter kill switch active — aborting backfill.")
        return

    with get_db_context() as session:
        logger.info("Extracting unresolved candidates%s...", f" (county={county_id})" if county_id else "")
        candidates = list(extract_candidates(session, only_unresolved=True))
        if county_id:
            candidates = [c for c in candidates if c.county_id == county_id]
        logger.info("Extracted %d unresolved candidates.", len(candidates))

        if not candidates:
            logger.info("Nothing unresolved -- backfill already complete (or nothing to do).")
            return

        logger.info("Loading existing buyer_entities as anchors...")
        # Anchors are loaded across ALL counties, matching run_incremental's
        # policy (H2.7) -- a buyer entity isn't bound to one county, and on
        # a resumed run these anchors are exactly the entities prior batches
        # already committed.
        existing_entities = load_existing_entity_candidates(session, county_id=None)
        logger.info("Loaded %d existing entity anchors.", len(existing_entities))
        combined = candidates + existing_entities

        logger.info("Finding structural edges, blocking, scoring, clustering...")
        relevant_clusters, confidences, evidence_index, ambiguous_pairs = cluster_against_anchors(combined)

        would_create = sum(
            1 for c in relevant_clusters
            if not any(r.source_table == _ENTITY_ANCHOR_TABLE for r in c)
        )
        would_attach = len(relevant_clusters) - would_create
        total_links = sum(
            len([r for r in c if r.source_table != _ENTITY_ANCHOR_TABLE]) for c in relevant_clusters
        )
        logger.info(
            "Assembled %d clusters needing action (%d new entities, %d attaching to an "
            "existing entity, %d links total).",
            len(relevant_clusters), would_create, would_attach, total_links,
        )

        if dry_run:
            logger.info(
                "[DRY RUN] No writes performed. %d ambiguous pairs would route to EXCEPTIONS.",
                len(ambiguous_pairs),
            )
            return

        exceptions_recorded = record_ambiguous_pair_exceptions(session, ambiguous_pairs)
        session.commit()
        logger.info("Recorded %d ambiguous-pair exceptions for review.", exceptions_recorded)

        # Committed in batches of _ENTITY_COMMIT_BATCH clusters, NOT one
        # transaction for the whole run -- this is a one-time job against
        # the shared production DB (no separate staging DB in this
        # platform), and a single multi-hour transaction would hold back
        # Postgres vacuum for the whole database for that entire duration.
        # A crash mid-run only loses the current uncommitted batch (up to
        # _ENTITY_COMMIT_BATCH clusters); rerunning picks up exactly where
        # it left off via the only_unresolved/anchor-matching above.
        total_entities_inserted = 0
        total_links_inserted = 0
        total_conflicts = 0
        n_clusters = len(relevant_clusters)

        for batch_start in range(0, n_clusters, _ENTITY_COMMIT_BATCH):
            batch_clusters = relevant_clusters[batch_start:batch_start + _ENTITY_COMMIT_BATCH]
            batch_confidences = confidences[batch_start:batch_start + _ENTITY_COMMIT_BATCH]

            stats = attach_or_create_entities(session, batch_clusters, batch_confidences, evidence_index)
            session.commit()

            total_entities_inserted += stats["new_entities"]
            total_links_inserted += stats["new_links"]
            total_conflicts += stats["conflicts"]
            logger.info(
                "Committed: %d/%d clusters (%.1f%%) — %d entities, %d links, %d conflicts so far.",
                min(batch_start + _ENTITY_COMMIT_BATCH, n_clusters), n_clusters,
                100 * min(batch_start + _ENTITY_COMMIT_BATCH, n_clusters) / n_clusters,
                total_entities_inserted, total_links_inserted, total_conflicts,
            )

        logger.info(
            "Backfill complete: %d buyer_entities and %d buyer_entity_links inserted, %d conflicts left unresolved.",
            total_entities_inserted, total_links_inserted, total_conflicts,
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--county-id", default=None)
    args = parser.parse_args()
    run_backfill(county_id=args.county_id, dry_run=args.dry_run)
