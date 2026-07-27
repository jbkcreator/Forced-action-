"""Daily lead-delivery sweep (M10 / B2).

Runs after CDS scoring. Picks qualified, not-yet-delivered leads and assigns each
to its best-matched paying account via the atomic claim. Unmatched leads stay in
the derived undelivered pool (no row) and are re-tried on the next run — so a lead
held today because nobody covers its ZIP is delivered automatically the day a
contractor locks that territory.

Each successful claim also emails the receiving contractor (best-effort — a
notification failure never fails the delivery, which is already committed).

    python -m src.tasks.lead_delivery_sweep
"""

from __future__ import annotations

import logging
from typing import Optional

from sqlalchemy import text

from src.core.database import get_db_context
from src.services.lead_delivery import Lead, claim, is_deliverable_verdict

logger = logging.getLogger(__name__)

# Cap per run so a backlog can't run unbounded; leftover leads are picked up next day.
_BATCH_LIMIT = 5000


def _verdicts_available(db) -> bool:
    """True once M6 (Truth Engine) has shipped its `verdicts` table. Until then
    the sweep falls back to grading leads off the CDS tier directly."""
    return db.execute(text("SELECT to_regclass('verdicts')")).scalar() is not None


def _pending_leads_from_verdicts(db, limit: int) -> list[Lead]:
    """M6-driven source (Option B): the latest verdict per property that M6 routed
    to a contractor channel and hasn't been delivered yet. Grade comes from the
    verdict; the trade is still derived from the CDS vertical_scores (M6's
    routed_channel is a product lane, not a trade). Bridged via prospects.property_id.

    Requires a locked zip_territories match (zip+county+vertical) before a lead
    is even a candidate. Without this, properties with zero possible match (no
    territory ever locked in their ZIP) sort to the same position every run
    (ORDER BY property_id, no delivery row ever written for them) and can
    permanently fill the LIMIT window once the true candidate count exceeds
    it, starving reachable leads with a higher property_id. Entitlement/
    headroom exhaustion is deliberately not filtered here — that's cycle-bound
    and self-resolves each billing period, unlike a ZIP with no territory.
    """
    rows = db.execute(text("""
        SELECT DISTINCT ON (pr.property_id)
               pr.property_id, p.zip, p.county_id, v.grade, v.routed_channel, ds.vertical_scores
        FROM verdicts v
        JOIN prospects pr  ON pr.prospect_id = v.prospect_id
        JOIN properties p  ON p.id = pr.property_id
        LEFT JOIN deliveries d ON d.property_id = pr.property_id
        LEFT JOIN LATERAL (
            SELECT vertical_scores FROM distress_scores
            WHERE property_id = pr.property_id ORDER BY score_date DESC LIMIT 1
        ) ds ON TRUE
        WHERE d.id IS NULL
          AND p.zip IS NOT NULL AND p.county_id IS NOT NULL
          AND ds.vertical_scores IS NOT NULL
          AND EXISTS (
              SELECT 1 FROM zip_territories zt
              WHERE zt.zip_code = p.zip
                AND zt.county_id = p.county_id
                AND zt.status = 'locked'
                AND zt.vertical IN (SELECT jsonb_object_keys(ds.vertical_scores))
          )
        ORDER BY pr.property_id, v.created_at DESC
        LIMIT :lim
    """), {"lim": limit}).fetchall()

    leads: list[Lead] = []
    for r in rows:
        if not is_deliverable_verdict(r.grade, r.routed_channel):
            continue  # latest verdict is sub_grade or a non-contractor lane
        verticals = list((r.vertical_scores or {}).keys())
        if not verticals:
            continue
        leads.append(Lead(property_id=r.property_id, zip_code=r.zip, county_id=r.county_id,
                          grade=r.grade, verticals=verticals))
    return leads


def _select_pending(db, limit: int, source: str) -> tuple[list[Lead], str]:
    """Pick the lead source: M6 verdicts when available (or forced), else CDS."""
    use_verdicts = source == "verdict" or (source == "auto" and _verdicts_available(db))
    if use_verdicts:
        return _pending_leads_from_verdicts(db, limit), "verdict"
    return _pending_leads(db, limit), "cds"


def _pending_leads(db, limit: int) -> list[Lead]:
    """Latest-scored, qualified properties with no delivery yet → Lead objects.

    Requires a locked zip_territories match (zip+county+vertical) before a
    lead is even a candidate — see the docstring on _pending_leads_from_verdicts
    for why (backlog-starvation guard: an unmatchable property otherwise sorts
    to the same position on every run and can permanently occupy the LIMIT
    window once the candidate count exceeds it).
    """
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
          AND ds.vertical_scores IS NOT NULL
          AND EXISTS (
              SELECT 1 FROM zip_territories zt
              WHERE zt.zip_code = p.zip
                AND zt.county_id = p.county_id
                AND zt.status = 'locked'
                AND zt.vertical IN (SELECT jsonb_object_keys(ds.vertical_scores))
          )
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


def _notify(db, delivery_id: int) -> bool:
    """Tell the contractor a lead landed. Best-effort: the delivery is already
    committed and its entitlement spent, so a notification failure must never
    fail the sweep."""
    from src.services.delivery_notifier import notify_delivery

    try:
        return notify_delivery(db, delivery_id)
    except Exception:
        logger.error("lead_delivery_sweep: notify failed for delivery %s",
                     delivery_id, exc_info=True)
        return False


def run_lead_delivery_sweep(db=None, *, limit: int = _BATCH_LIMIT, source: str = "auto") -> dict:
    """Match + claim every pending lead. Each claim commits in its own short
    transaction so the property-row lock is held briefly and one failure never
    rolls back the batch. `source`: 'auto' (verdicts if M6 has shipped, else CDS),
    'verdict', or 'cds'. Returns counts {delivered, undelivered, source}."""
    own = db is None
    ctx = get_db_context() if own else None
    db = ctx.__enter__() if own else db
    try:
        leads, src = _select_pending(db, limit, source)
        delivered = undelivered = notified = 0
        for lead in leads:
            try:
                d = claim(db, lead)
                if d is not None:
                    # Read the id while the instance is still populated from the
                    # flush inside claim(); commit expires it otherwise.
                    delivery_id = d.id
                    db.commit()
                    delivered += 1
                    if _notify(db, delivery_id):
                        notified += 1
                else:
                    db.rollback()  # release the property lock; lead stays in pool
                    undelivered += 1
            except Exception:
                db.rollback()
                logger.error("lead_delivery_sweep: claim failed for property %s",
                             lead.property_id, exc_info=True)
                undelivered += 1
        logger.info("lead_delivery_sweep[%s]: %d delivered (%d notified), %d undelivered "
                    "(of %d pending)", src, delivered, notified, undelivered, len(leads))
        return {"delivered": delivered, "notified": notified, "undelivered": undelivered,
                "pending": len(leads), "source": src}
    finally:
        if own:
            ctx.__exit__(None, None, None)


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
    run_lead_delivery_sweep()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
