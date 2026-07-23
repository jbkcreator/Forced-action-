"""
Deal-of-the-day service — T-B12-07.

Daily pick: the top-CDS qualified lead not yet delivered, opened for a 24h
exclusive-unlock window at STANDARD price (scarcity mechanic — this is not a
discount, see ticket resolution). Reuses the same qualified/non-guess-lead
scoring filters as hero_deal.py (T-B12-01) and new_distress_digest.py
(T-B12-03), plus new_distress_digest.py's sent_leads "not yet delivered"
pattern.

"Not yet delivered" = no sent_leads row for the property from ANY subscriber
(a lead already fully sent/exhausted is excluded), and never previously
picked as a deal-of-the-day (across all dates, not just today).

    python -m src.services.deal_of_the_day --dry-run
"""
from __future__ import annotations

import argparse
import logging
from datetime import date, datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from src.core.database import get_db_context
from src.services.proof_moment import _blur_address

logger = logging.getLogger(__name__)

WINDOW_HOURS = 24


def _query_top_undelivered_lead(db: Session) -> Optional[dict]:
    """
    Top-CDS qualified, non-guess-lead property with no sent_leads row from any
    subscriber and never previously featured as a deal-of-the-day.
    """
    row = db.execute(text("""
        SELECT p.id AS property_id, p.address, p.city, p.state, p.zip, p.county_id,
               ds.final_cds_score, ds.lead_tier, ds.urgency_level, ds.distress_types
        FROM distress_scores ds
        JOIN properties p ON p.id = ds.property_id
        WHERE ds.qualified = TRUE
          AND ds.is_guess_lead = FALSE
          AND NOT EXISTS (
              SELECT 1 FROM sent_leads sl WHERE sl.property_id = p.id
          )
          AND NOT EXISTS (
              SELECT 1 FROM deal_of_the_day dotd WHERE dotd.lead_id = p.id
          )
        ORDER BY ds.final_cds_score DESC
        LIMIT 1
    """)).first()
    if not row:
        return None

    dt = row.distress_types
    distress = list(dt.keys()) if isinstance(dt, dict) else (dt or [])
    return {
        "property_id": row.property_id,
        "address": row.address,
        "city": row.city,
        "state": row.state,
        "zip": row.zip,
        "county_id": row.county_id,
        "score": float(row.final_cds_score) if row.final_cds_score else 0.0,
        "lead_tier": row.lead_tier or "Gold",
        "urgency": row.urgency_level or "",
        "distress_types": distress,
    }


def select_deal_of_the_day(
    db: Session,
    target_date: Optional[date] = None,
    dry_run: bool = False,
) -> Optional[dict]:
    """
    Picks and persists today's deal-of-the-day if one doesn't already exist
    for target_date. Idempotent — returns the existing row's lead if today's
    pick was already made (safe to run more than once a day).
    """
    target_date = target_date or date.today()

    existing = db.execute(text("""
        SELECT id, lead_id, window_start, window_end
        FROM deal_of_the_day WHERE date = :d
    """), {"d": target_date}).first()
    if existing:
        logger.info("deal_of_the_day: already picked for %s (lead_id=%s)", target_date, existing.lead_id)
        return {
            "lead_id": existing.lead_id,
            "window_start": existing.window_start,
            "window_end": existing.window_end,
            "already_picked": True,
        }

    lead = _query_top_undelivered_lead(db)
    if not lead:
        logger.warning("deal_of_the_day: no undelivered qualified lead available for %s", target_date)
        return None

    window_start = datetime.now(timezone.utc)
    window_end = window_start + timedelta(hours=WINDOW_HOURS)

    if dry_run:
        logger.info(
            "[DRY RUN] deal_of_the_day: would pick lead_id=%s score=%s for %s",
            lead["property_id"], lead["score"], target_date,
        )
        return {**lead, "window_start": window_start, "window_end": window_end, "already_picked": False}

    db.execute(text("""
        INSERT INTO deal_of_the_day (date, lead_id, window_start, window_end)
        VALUES (:d, :lead_id, :start, :end)
        ON CONFLICT (date) DO NOTHING
    """), {
        "d": target_date, "lead_id": lead["property_id"],
        "start": window_start, "end": window_end,
    })
    db.commit()
    logger.info(
        "deal_of_the_day: picked lead_id=%s score=%s for %s",
        lead["property_id"], lead["score"], target_date,
    )
    return {**lead, "window_start": window_start, "window_end": window_end, "already_picked": False}


def get_current_deal(db: Session) -> dict:
    """
    Returns today's deal-of-the-day payload for the public surface. Scarcity
    framing (countdown to window_end), never a discount — unlock is at
    standard price (Stripe hot_lead_unlock price_id, same as any hot lead).
    """
    row = db.execute(text("""
        SELECT dotd.date, dotd.window_start, dotd.window_end,
               p.id AS property_id, p.address, p.city, p.state, p.zip, p.county_id,
               ds.final_cds_score, ds.lead_tier, ds.urgency_level, ds.distress_types
        FROM deal_of_the_day dotd
        JOIN properties p ON p.id = dotd.lead_id
        LEFT JOIN distress_scores ds ON ds.property_id = p.id
        WHERE dotd.date = CURRENT_DATE
        ORDER BY ds.score_date DESC NULLS LAST
        LIMIT 1
    """)).first()

    if not row:
        return {"status": "empty", "deal": None, "message": "No exclusive deal live right now — check back soon."}

    now = datetime.now(timezone.utc)
    window_end = row.window_end if row.window_end.tzinfo else row.window_end.replace(tzinfo=timezone.utc)
    if now >= window_end:
        return {"status": "expired", "deal": None, "message": "Today's exclusive window has closed."}

    dt = row.distress_types
    distress = list(dt.keys()) if isinstance(dt, dict) else (dt or [])

    return {
        "status": "live",
        "deal": {
            "property_id": row.property_id,
            "address_masked": _blur_address(row.address),
            "city": row.city,
            "state": row.state,
            "zip": row.zip,
            "county_id": row.county_id,
            "score": float(row.final_cds_score) if row.final_cds_score else None,
            "lead_tier": row.lead_tier,
            "distress_types": distress,
            "pricing": "standard",
        },
        "window_start": row.window_start.isoformat(),
        "window_end": row.window_end.isoformat(),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Pick today's deal-of-the-day")
    parser.add_argument("--dry-run", action="store_true", help="Log the pick without writing")
    args = parser.parse_args()

    with get_db_context() as db:
        result = select_deal_of_the_day(db, dry_run=args.dry_run)
    print(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
