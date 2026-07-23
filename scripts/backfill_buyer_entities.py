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

from sqlalchemy import insert

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_LINK_INSERT_BATCH = 1000
_ENTITY_COMMIT_BATCH = 2000  # commit every N clusters -- see run_backfill docstring on why this isn't one giant transaction


def run_backfill(county_id=None, dry_run: bool = False) -> None:
    from src.agents.hunter.kill_switch import hunter_halted
    from src.core.database import get_db_context
    from src.core.models import BuyerEntityLink
    from src.services.buyer_entity_resolution import (
        _new_entity_from_cluster,
        _record_key,
        block_candidates,
        build_clusters,
        build_evidence_index,
        compute_cluster_confidences,
        extract_candidates,
        find_singletons,
        find_structural_edges,
        score_blocked_pairs,
    )

    if hunter_halted():
        logger.warning("Hunter kill switch active — aborting backfill.")
        return

    with get_db_context() as session:
        logger.info("Extracting candidates%s...", f" (county={county_id})" if county_id else "")
        candidates = list(extract_candidates(session, only_unresolved=False))
        if county_id:
            candidates = [c for c in candidates if c.county_id == county_id]
        logger.info("Extracted %d candidates.", len(candidates))

        logger.info("Finding structural (Sunbiz) edges...")
        structural_edges = find_structural_edges(candidates)
        logger.info("Found %d structural edges.", len(structural_edges))

        logger.info("Blocking + scoring...")
        blocks = block_candidates(candidates)
        scored = score_blocked_pairs(blocks)
        n_ambiguous = sum(1 for _, _, v in scored if v.method == "ambiguous")
        logger.info(
            "Scored %d pairs (%d ambiguous — left UNMATCHED, no LLM call; "
            "never merge on uncertain evidence)...",
            len(scored), n_ambiguous,
        )

        # No LLM tie-break: ambiguous verdicts already carry is_match=False
        # (score_candidate_pair's own return value), so filtering scored
        # directly for is_match already excludes them -- nothing else needed.
        all_edges = structural_edges + [e for e in scored if e[2].is_match]
        logger.info("Total confirmed match edges: %d", len(all_edges))

        clusters = build_clusters(all_edges)
        singletons = find_singletons(candidates, clusters)
        all_clusters = clusters + singletons
        logger.info(
            "Assembled %d clusters (%d multi-record, %d singleton).",
            len(all_clusters), len(clusters), len(singletons),
        )

        confidences = compute_cluster_confidences(all_clusters, all_edges)
        evidence_index = build_evidence_index(all_edges)
        total_links = sum(len(c) for c in all_clusters)

        if dry_run:
            logger.info("[DRY RUN] Would insert %d buyer_entities.", len(all_clusters))
            logger.info("[DRY RUN] Would insert %d buyer_entity_links.", total_links)
            return

        # Committed in batches of _ENTITY_COMMIT_BATCH clusters, NOT one
        # transaction for the whole run -- this is a one-time job against
        # the shared production DB (no separate staging DB in this
        # platform), and a single multi-hour transaction would hold back
        # Postgres vacuum for the whole database for that entire duration.
        # A crash mid-run only loses the current uncommitted batch (up to
        # _ENTITY_COMMIT_BATCH clusters), not the whole run.
        total_entities_inserted = 0
        total_links_inserted = 0
        n_clusters = len(all_clusters)

        for batch_start in range(0, n_clusters, _ENTITY_COMMIT_BATCH):
            batch_clusters = all_clusters[batch_start:batch_start + _ENTITY_COMMIT_BATCH]
            batch_confidences = confidences[batch_start:batch_start + _ENTITY_COMMIT_BATCH]

            batch_entities = [
                _new_entity_from_cluster(cluster, conf) for cluster, conf in zip(batch_clusters, batch_confidences)
            ]
            session.add_all(batch_entities)
            session.flush()  # populates entity.id on every new row in this batch

            link_rows = []
            for cluster, entity in zip(batch_clusters, batch_entities):
                for rec in cluster:
                    method, confidence = evidence_index.get(_record_key(rec), ("manual", 100))
                    link_rows.append({
                        "buyer_entity_id": entity.id,
                        "source_table": rec.source_table,
                        "source_id": rec.source_id,
                        "match_confidence": confidence,
                        "match_method": method,
                    })

            for i in range(0, len(link_rows), _LINK_INSERT_BATCH):
                session.execute(insert(BuyerEntityLink).values(link_rows[i:i + _LINK_INSERT_BATCH]))

            session.commit()
            total_entities_inserted += len(batch_entities)
            total_links_inserted += len(link_rows)
            logger.info(
                "Committed: %d/%d clusters (%.1f%%) — %d entities, %d links so far.",
                min(batch_start + _ENTITY_COMMIT_BATCH, n_clusters), n_clusters,
                100 * min(batch_start + _ENTITY_COMMIT_BATCH, n_clusters) / n_clusters,
                total_entities_inserted, total_links_inserted,
            )

        logger.info(
            "Backfill complete: %d buyer_entities and %d buyer_entity_links inserted total.",
            total_entities_inserted, total_links_inserted,
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--county-id", default=None)
    args = parser.parse_args()
    run_backfill(county_id=args.county_id, dry_run=args.dry_run)
