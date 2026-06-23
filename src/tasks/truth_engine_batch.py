"""
M6 — Lead Quality Truth Engine daily verdict batch.

Run daily via cron, after CDS scoring, skip-trace, and cohort recompute:

    15 8 * * 1-6 python -m src.tasks.truth_engine_batch

Consumes unprocessed enrichment events from the bus and produces a verdict per
prospect, idempotently (deduped via processed_events, consumer 'truth_engine'):

    enrichment.completed → grade against CDS + contactability (Truth Engine)
    enrichment.failed    → sub_grade / recycle_suppress (unreachable, §5/§130)

A prospect with no CDS score yet is left held (not marked processed) and retried
on the next run. This job never mutates prospects.
"""
import argparse

from sqlalchemy import text as sa_text

from src.core.database import get_db_context
from src.services.event_bus import mark_processed
from src.services.truth_engine import grade_prospect, record_sub_grade
from src.utils.logger import get_logger

logger = get_logger(__name__)

CONSUMER = "truth_engine"

_UNPROCESSED_EVENTS = """
    SELECT e.event_id, e.prospect_id, e.event_type
    FROM events e
    WHERE e.event_type IN ('enrichment.completed', 'enrichment.failed')
      AND NOT EXISTS (
          SELECT 1 FROM processed_events pe
          WHERE pe.event_id = e.event_id AND pe.consumer = :consumer
      )
    ORDER BY e.occurred_at
"""


def run_truth_engine_batch(dry_run: bool = False) -> dict:
    """Grade every prospect with a pending enrichment event."""
    counts = {"scanned": 0, "graded": 0, "suppressed": 0, "held": 0, "missing": 0, "errors": 0, "dry_run": dry_run}

    with get_db_context() as db:
        events = db.execute(sa_text(_UNPROCESSED_EVENTS), {"consumer": CONSUMER}).mappings().all()
        counts["scanned"] = len(events)

        if dry_run:
            logger.info("[TruthEngine] dry-run: %d unprocessed enrichment event(s)", len(events))
            return counts

        for ev in events:
            event_id = ev["event_id"]
            prospect_id = str(ev["prospect_id"])
            event_type = ev["event_type"]
            try:
                if event_type == "enrichment.failed":
                    record_sub_grade(db, prospect_id, reason="enrichment_failed")
                    mark_processed(db, event_id, CONSUMER)
                    counts["suppressed"] += 1
                    db.commit()
                    continue

                result = grade_prospect(db, prospect_id)
                if result is None:
                    # Unknown prospect — anomalous; mark processed to avoid a retry loop.
                    mark_processed(db, event_id, CONSUMER)
                    counts["missing"] += 1
                    db.commit()
                elif result.get("held"):
                    # No CDS score yet — leave unprocessed so it retries next run.
                    counts["held"] += 1
                    db.rollback()
                else:
                    mark_processed(db, event_id, CONSUMER)
                    counts["graded"] += 1
                    db.commit()
            except Exception:
                db.rollback()
                counts["errors"] += 1
                logger.warning("[TruthEngine] failed event_id=%s prospect_id=%s", event_id, prospect_id, exc_info=True)

    logger.info(
        "[TruthEngine] batch complete scanned=%d graded=%d suppressed=%d held=%d missing=%d errors=%d",
        counts["scanned"], counts["graded"], counts["suppressed"], counts["held"], counts["missing"], counts["errors"],
    )
    return counts


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Truth Engine daily verdict batch")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    print(run_truth_engine_batch(dry_run=args.dry_run))
