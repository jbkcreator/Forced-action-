"""R-1 Routing sweep: route unrouted LendingFlow files and retry failed Slack posts.

Cron: */5 * * * *  (every 5 minutes, same cadence as the enrichment sweep)

Picks up files that:
  - Have a file_routing row with status='unrouted', or
  - Have no file_routing row yet (created before R-1 was deployed), or
  - Have a file_routing_events row with slack_posted_at IS NULL (Slack retry).
"""
import logging
import sys

from sqlalchemy import text

from src.lending.db import lending_session

logger = logging.getLogger(__name__)


def run_routing_sweep() -> None:
    from config.lending_routing import SWEEP_BATCH_SIZE, SWEEP_UNROUTED_AFTER_MINUTES
    from src.lending.routing.router import route_file
    from src.lending.routing.notify import retry_unposted_events

    with lending_session() as db:
        # Create file_routing rows for leads that don't have one yet.
        db.execute(
            text(
                "INSERT INTO lending.file_routing (lead_id, status)"
                " SELECT id, 'unrouted'"
                "   FROM lending.lendingflow_leads ll"
                "  WHERE NOT EXISTS ("
                "       SELECT 1 FROM lending.file_routing fr"
                "        WHERE fr.lead_id = ll.id"
                "  )"
                "    AND ll.ghl_status IN ('synced', 'contact_only')"
                "    AND ll.suppressed = FALSE"
                " ON CONFLICT (lead_id) DO NOTHING"
            )
        )
        db.commit()

        lead_ids = db.execute(
            text(
                "SELECT lead_id FROM lending.file_routing"
                " WHERE status = 'unrouted'"
                " ORDER BY created_at"
                " LIMIT :batch"
                " FOR UPDATE SKIP LOCKED"
            ),
            {"batch": SWEEP_BATCH_SIZE},
        ).scalars().all()

        if not lead_ids:
            logger.debug("lending_routing_sweep: no unrouted files")
        else:
            logger.info("lending_routing_sweep: routing %d files", len(lead_ids))
            for lead_id in lead_ids:
                try:
                    route_file(lead_id, db)
                except Exception:
                    logger.exception("lending_routing_sweep: route_file lead_id=%d failed", lead_id)

        # Retry unposted Slack events.
        posted = retry_unposted_events(db)
        if posted:
            logger.info("lending_routing_sweep: retried %d Slack posts", posted)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        run_routing_sweep()
    except Exception:
        logger.exception("lending_routing_sweep failed")
        sys.exit(1)
