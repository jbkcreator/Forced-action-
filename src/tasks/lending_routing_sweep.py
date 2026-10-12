"""R-1 Routing sweep: route unrouted LendingFlow files and retry failed Slack posts.

Cron: */5 * * * *  (every 5 minutes, same cadence as the enrichment sweep)

The spec routes a file once it has been scored by the lender box, which the T-12 enrichment does.
So the sweep picks up files that:
  - have finished enrichment (lendingflow_enrichment.status = 'ready') and are not suppressed,
    but have no file_routing row yet (the in-process call after enrichment missed them), or
  - have a file_routing row with status 'unrouted' (for example after a decline re-open), or
  - have a routing event with slack_posted_at IS NULL (Slack retry).

GoHighLevel sync state is deliberately not a condition: routing is internal and does not need the
CRM push to have happened.  Suppressed leads are never routed (Q8).
"""
import logging
import sys
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from src.lending.db import lending_session

logger = logging.getLogger(__name__)


def sweep(db: Session) -> None:
    """One sweep pass on an open session (the caller commits / closes it)."""
    from config.lending_routing import SWEEP_BATCH_SIZE
    from src.lending.routing.router import route_file
    from src.lending.routing.notify import retry_unposted_events

    db.execute(
        text(
            "INSERT INTO lending.file_routing (lead_id, status)"
            " SELECT ll.id, 'unrouted'"
            "   FROM lending.lendingflow_leads ll"
            "   JOIN lending.lendingflow_enrichment e ON e.lead_id = ll.id"
            "  WHERE e.status = 'ready'"
            "    AND ll.suppressed = FALSE"
            "    AND NOT EXISTS ("
            "         SELECT 1 FROM lending.file_routing fr WHERE fr.lead_id = ll.id)"
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
                db.rollback()
                logger.exception("lending_routing_sweep: route_file lead_id=%d failed", lead_id)

    posted = retry_unposted_events(db)
    if posted:
        logger.info("lending_routing_sweep: retried %d Slack posts", posted)


def run_routing_sweep(db: Optional[Session] = None) -> None:
    if db is not None:
        sweep(db)
        return
    with lending_session() as session:
        sweep(session)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        run_routing_sweep()
    except Exception:
        logger.exception("lending_routing_sweep failed")
        sys.exit(1)
