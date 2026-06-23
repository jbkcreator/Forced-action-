"""Daily lead-delivery sweep (M10 / B2).

Runs after CDS scoring. Picks qualified, not-yet-delivered leads and assigns each
to its best-matched paying account via the atomic claim. Unmatched leads stay in
the derived undelivered pool (no row) and are re-tried on the next run — so a lead
held today because nobody covers its ZIP is delivered automatically the day a
contractor locks that territory.

    python -m src.tasks.lead_delivery_sweep
"""

from __future__ import annotations

import logging
from typing import Optional

from sqlalchemy import text

from src.core.database import get_db_context
from src.services.lead_delivery import Lead, claim

logger = logging.getLogger(__name__)

# Cap per run so a backlog can't run unbounded; leftover leads are picked up next day.
_BATCH_LIMIT = 5000


def _pending_leads(db, limit: int) -> list[Lead]:
    """Latest-scored, qualified properties with no delivery yet → Lead objects."""
    rows = db.execute(text("""
        SELECT DISTINCT ON (ds.property_id)
               ds.property_id, p.zip, p.county_id, ds.lead_tier, ds.vertical_scores
        FROM distress_scores ds
        JOIN properties p ON p.id = ds.property_id
        LEFT JOIN deliveries d ON d.property_id = ds.property_id
        WHERE ds.qualified = TRUE
          AND ds.lead_tier IS NOT NULL
          AND p.zip IS NOT NULL
          AND p.county_id IS NOT NULL
          AND d.id IS NULL
        ORDER BY ds.property_id, ds.score_date DESC
        LIMIT :lim
    """), {"lim": limit}).fetchall()

    leads: list[Lead] = []
    for r in rows:
        verticals = list((r.vertical_scores or {}).keys())
        if not verticals:
            continue  # no scored trade → nothing to match against
        leads.append(Lead(
            property_id=r.property_id, zip_code=r.zip, county_id=r.county_id,
            grade=r.lead_tier, verticals=verticals,
        ))
    return leads


def run_lead_delivery_sweep(db=None, *, limit: int = _BATCH_LIMIT) -> dict:
    """Match + claim every pending lead. Each claim commits in its own short
    transaction so the property-row lock is held briefly and one failure never
    rolls back the batch. Returns counts {delivered, undelivered}."""
    own = db is None
    ctx = get_db_context() if own else None
    db = ctx.__enter__() if own else db
    try:
        leads = _pending_leads(db, limit)
        delivered = undelivered = 0
        for lead in leads:
            try:
                d = claim(db, lead)
                if d is not None:
                    db.commit()
                    delivered += 1
                else:
                    db.rollback()  # release the property lock; lead stays in pool
                    undelivered += 1
            except Exception:
                db.rollback()
                logger.error("lead_delivery_sweep: claim failed for property %s",
                             lead.property_id, exc_info=True)
                undelivered += 1
        logger.info("lead_delivery_sweep: %d delivered, %d undelivered (of %d pending)",
                    delivered, undelivered, len(leads))
        return {"delivered": delivered, "undelivered": undelivered, "pending": len(leads)}
    finally:
        if own:
            ctx.__exit__(None, None, None)


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
    run_lead_delivery_sweep()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
