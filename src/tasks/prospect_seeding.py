"""
Prospect seeding cron — create prospect stubs for properties entering Gold+ today.

Runs at 07:15 UTC daily (after CDS at 07:00, before skip-trace at 07:30).
Queries today's Gold+ distress_scores, left-joins prospects to find properties
without a stub, inserts stubs, and emits prospect.created for each new one.

Usage:
    python -m src.tasks.prospect_seeding [--dry-run]
"""
import logging
import sys
from datetime import datetime, timezone

from sqlalchemy import text as sa_text

from src.core.database import get_db_context
from src.services.event_bus import emit_event

logger = logging.getLogger(__name__)

_GOLD_PLUS_TIERS = ["Gold", "Platinum", "Ultra Platinum"]
_BATCH_SIZE = 200


def _fetch_unseeded(session, score_date) -> list:
    """Properties scoring Gold+ today that have no prospect stub yet."""
    return session.execute(sa_text("""
        SELECT
            ds.property_id,
            ds.lead_tier,
            ds.county_id,
            ds.scoring_run_id,
            pr.zip
        FROM distress_scores ds
        JOIN properties pr ON pr.id = ds.property_id
        LEFT JOIN prospects p ON p.property_id = ds.property_id
        WHERE ds.score_date::date = :today
          AND ds.lead_tier = ANY(:tiers)
          AND p.prospect_id IS NULL
        ORDER BY ds.property_id
    """), {"today": score_date, "tiers": _GOLD_PLUS_TIERS}).fetchall()


def _seed_batch(session, batch: list) -> list:
    """
    Insert prospect stubs for a batch of unseeded rows.

    Returns list of (prospect_id, property_id) for rows actually inserted
    (ON CONFLICT DO NOTHING silently skips any races).
    """
    created = []
    for row in batch:
        result = session.execute(sa_text("""
            INSERT INTO prospects (property_id)
            VALUES (:pid)
            ON CONFLICT (property_id) DO NOTHING
            RETURNING prospect_id, property_id
        """), {"pid": row.property_id}).fetchone()
        if result:
            created.append((result.prospect_id, result.property_id))
    return created


def run(dry_run: bool = False) -> dict:
    results = {
        "candidates": 0,
        "created": 0,
        "events_emitted": 0,
        "errors": 0,
    }
    today = datetime.now(timezone.utc).date()

    with get_db_context() as session:
        unseeded = _fetch_unseeded(session, today)
        results["candidates"] = len(unseeded)

        if not unseeded:
            logger.info("[ProspectSeeding] no new Gold+ properties to seed today")
            return results

        logger.info("[ProspectSeeding] found %d unseeded Gold+ properties", len(unseeded))

        meta = {r.property_id: r for r in unseeded}

        for batch_start in range(0, len(unseeded), _BATCH_SIZE):
            batch = unseeded[batch_start : batch_start + _BATCH_SIZE]

            if dry_run:
                logger.info(
                    "[ProspectSeeding] dry-run: would seed %d properties (batch %d)",
                    len(batch), batch_start // _BATCH_SIZE + 1,
                )
                results["created"] += len(batch)
                continue

            try:
                created_rows = _seed_batch(session, batch)
                results["created"] += len(created_rows)

                for prospect_id, property_id in created_rows:
                    m = meta[property_id]
                    try:
                        emit_event(
                            session,
                            event_type="prospect.created",
                            actor="prospect_seeding",
                            source_component="prospect_seeding",
                            prospect_id=prospect_id,
                            payload={
                                "property_id":    property_id,
                                "lead_tier":      m.lead_tier,
                                "county_id":      m.county_id,
                                "scoring_run_id": m.scoring_run_id,
                                "zip":            m.zip,
                            },
                        )
                        results["events_emitted"] += 1
                    except Exception as exc:
                        logger.error(
                            "[ProspectSeeding] emit_event failed property_id=%s: %s",
                            property_id, exc,
                        )

                session.commit()
                logger.info(
                    "[ProspectSeeding] batch committed — created=%d events=%d",
                    len(created_rows), results["events_emitted"],
                )

            except Exception as exc:
                session.rollback()
                logger.error(
                    "[ProspectSeeding] batch failed at offset %d: %s",
                    batch_start, exc, exc_info=True,
                )
                results["errors"] += 1

    logger.info(
        "[ProspectSeeding] complete — candidates=%d created=%d events=%d errors=%d dry_run=%s",
        results["candidates"], results["created"],
        results["events_emitted"], results["errors"], dry_run,
    )
    return results


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    dry = "--dry-run" in sys.argv
    print(run(dry_run=dry))
