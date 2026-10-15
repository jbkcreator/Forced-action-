"""R-1 Routing sweep: route unrouted files (LendingFlow leads and partner files) and retry failed Slack posts.

Cron: */5 * * * *  (every 5 minutes, same cadence as the enrichment sweep)

The spec routes a file once it has been scored by the lender box.  The sweep picks up:
  - LendingFlow leads that have finished enrichment (lendingflow_enrichment.status = 'ready') and are not
    suppressed, but have no file_routing row yet (the in-process call after enrichment missed them);
  - partner files (lending.loan_files) that the Partner Door parked as "awaiting router" before the router
    was installed, and that have no file_routing row yet;
  - any file_routing row with status 'unrouted' (for example after a decline re-open), of either kind;
  - any routing event with slack_posted_at IS NULL (Slack retry).

GoHighLevel sync state is deliberately not a condition: routing is internal and does not need the
CRM push to have happened.  Suppressed leads are never routed (Q8).  A partner file that is still
'received' is the Partner Door's own sweep's job (its background run never finished).
"""
import logging
import sys
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from src.lending.db import lending_session

logger = logging.getLogger(__name__)


def _has_partner_files(db: Session) -> bool:
    """True when the Partner Door table exists (its migration may not have run in this database yet)."""
    return db.execute(text("SELECT to_regclass('lending.loan_files')")).scalar() is not None


def sweep(db: Session) -> None:
    """One sweep pass on an open session (the caller commits / closes it)."""
    from config.lending_partners import AWAITING_ROUTER_REASON, SOURCE_PARTNER
    from config.lending_routing import SWEEP_BATCH_SIZE
    from src.lending.routing.notify import retry_unposted_events
    from src.lending.routing.router import route_file, route_loan_file

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
    if _has_partner_files(db):
        db.execute(
            text(
                "INSERT INTO lending.file_routing (loan_file_id, status)"
                " SELECT lf.id, 'unrouted'"
                "   FROM lending.loan_files lf"
                "  WHERE lf.source = :source AND lf.status = 'held' AND lf.status_reason = :awaiting"
                "    AND NOT EXISTS ("
                "         SELECT 1 FROM lending.file_routing fr WHERE fr.loan_file_id = lf.id)"
                " ON CONFLICT (loan_file_id) WHERE loan_file_id IS NOT NULL DO NOTHING"
            ),
            {"source": SOURCE_PARTNER, "awaiting": AWAITING_ROUTER_REASON},
        )
    db.commit()

    files = db.execute(
        text(
            "SELECT lead_id, loan_file_id FROM lending.file_routing"
            " WHERE status = 'unrouted'"
            " ORDER BY created_at"
            " LIMIT :batch"
            " FOR UPDATE SKIP LOCKED"
        ),
        {"batch": SWEEP_BATCH_SIZE},
    ).mappings().all()

    if not files:
        logger.debug("lending_routing_sweep: no unrouted files")
    else:
        logger.info("lending_routing_sweep: routing %d files", len(files))
        for row in files:
            try:
                if row["lead_id"] is not None:
                    route_file(row["lead_id"], db)
                else:
                    route_loan_file(row["loan_file_id"], db)
            except Exception:
                db.rollback()
                logger.exception(
                    "lending_routing_sweep: routing failed lead_id=%s loan_file_id=%s",
                    row["lead_id"], row["loan_file_id"],
                )

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
