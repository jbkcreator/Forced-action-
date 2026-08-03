"""
Ranked whale list + Hunter->Lifecycle data contract (HUNTER-02, W3).

Cell #1 ("25 biggest whales") input, produced the moment an entity carries
is_whale=true (src/services/whale_detection.py) -- ranked by purchase count,
then cash volume, matching the plan's ordering. Per the client's Q2 answer
("there is no investor or buyer roster in the platform"), this function *is*
that roster.

Contract shape: RankedWhale below is the full Hunter->Lifecycle handoff --
every field a future Lifecycle graph can rely on existing, none it must derive
itself. Fields:
  - canonical_name / entity_type / county_id       -- who
  - total_purchase_count / total_cash_volume        -- portfolio summary
  - contact_channel / contact_confidence            -- resolved via the
    entity's linked `owners` rows (source_table='owners' buyer_entity_links),
    never invented here; confidence is the best per-phone `score` any linked
    owner record carries in Owner.phone_metadata (0 if no scored contact)
  - why_now                                         -- the qualifying
    catalyst in one line: which rule tripped (purchase-count vs cash-volume,
    or both) plus the most recent linked deed, so a drafted outreach message
    has a concrete, current reason to open with
  - opportunity_thread_id / whale_flagged_at         -- traceability back to
    the moment this became an opportunity (see whale_detection.py)
  - acquisition_velocity                            -- HUNTER-04: distinct
    properties/year, from the entity's own first-to-last purchase span.
    None when there isn't enough history for a rate to mean anything (a
    single purchase, or a burst of same-day purchases).

NOT written to the shared facts directory (`/shared/facts/...`) -- per the
dev-split plan §6b that schema doesn't exist yet and is being defined via a
Vera/Hunter sync; this function is the query Hunter's eventual facts-writer
will call, not the writer itself.

Usage:
    from src.services.whale_ranking import get_ranked_whales
    top_whales = get_ranked_whales(session, limit=25)
"""
from __future__ import annotations

import logging
from dataclasses import asdict, dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from src.services.whale_detection import NOMINAL_CONSIDERATION_FLOOR, WHALE_MIN_CASH_VOLUME, WHALE_MIN_PURCHASES

MIN_SPAN_DAYS_FOR_VELOCITY = 1  # a burst of same-day purchases isn't a sustained rate

logger = logging.getLogger(__name__)

DEFAULT_LIMIT = 25


@dataclass
class RankedWhale:
    rank: int
    entity_id: int
    opportunity_thread_id: Optional[str]
    canonical_name: str
    entity_type: str
    county_id: Optional[str]
    total_purchase_count: int
    total_cash_volume: Decimal
    contact_channel: str          # "phone" | "email" | "none"
    contact_confidence: int       # 0-100; 0 when contact_channel == "none"
    why_now: str
    whale_flagged_at: Optional[datetime]
    acquisition_velocity: Optional[float]  # distinct properties/year; None when not computable (HUNTER-04)


def _acquisition_velocity(purchase_count: int, first_date: Optional[date], last_date: Optional[date]) -> Optional[float]:
    """Distinct properties acquired per year, from the entity's own purchase
    history span. None when there isn't enough history to describe a rate
    (a single purchase, or a burst of same-day purchases) rather than a
    misleading divide-by-near-zero number."""
    if purchase_count < 2 or not first_date or not last_date:
        return None
    span_days = (last_date - first_date).days
    if span_days < MIN_SPAN_DAYS_FOR_VELOCITY:
        return None
    return round(purchase_count / (span_days / 365.25), 2)


def _why_now(recent_count: int, total_cash_volume: Decimal, last_deed_type: Optional[str], last_purchase_date: Optional[date]) -> str:
    reasons = []
    if recent_count >= WHALE_MIN_PURCHASES:
        reasons.append(f"{recent_count} purchases in the trailing 18 months")
    if total_cash_volume and total_cash_volume > WHALE_MIN_CASH_VOLUME:
        reasons.append(f"${total_cash_volume:,.0f} total cash volume")
    catalyst = " and ".join(reasons) if reasons else "qualifying activity"
    if last_deed_type and last_purchase_date:
        return f"{catalyst} -- most recent: {last_deed_type} on {last_purchase_date.isoformat()}"
    return catalyst


def get_ranked_whales(session: Session, limit: int = DEFAULT_LIMIT, county_id: Optional[str] = None) -> list[dict]:
    """
    The ranked "N biggest whales" list, in the Hunter->Lifecycle contract shape.
    Read-only, no writes -- safe to call as often as Lifecycle needs it.
    """
    county_filter = "AND be.county_id = :county_id" if county_id else ""
    rows = session.execute(
        text(f"""
            WITH owner_contacts AS (
                SELECT bel.buyer_entity_id,
                       o.phone_1 IS NOT NULL AS has_phone,
                       o.email_1 IS NOT NULL AS has_email,
                       COALESCE((o.phone_metadata->'phone_1'->>'score')::int, 0) AS phone_score
                FROM buyer_entity_links bel
                JOIN owners o ON o.id = bel.source_id AND bel.source_table = 'owners'
            ),
            best_contact AS (
                SELECT buyer_entity_id,
                       BOOL_OR(has_phone) AS has_phone,
                       BOOL_OR(has_email) AS has_email,
                       MAX(phone_score) AS phone_score
                FROM owner_contacts
                GROUP BY buyer_entity_id
            ),
            recent_purchases AS (
                SELECT bel.buyer_entity_id,
                       COUNT(DISTINCT d.property_id) FILTER (
                           WHERE d.record_date >= (CURRENT_DATE - INTERVAL '548 days')
                             AND (d.sale_price IS NULL OR d.sale_price >= :nominal_floor)
                       ) AS recent_count,
                       MIN(d.record_date) FILTER (WHERE d.sale_price IS NULL OR d.sale_price >= :nominal_floor) AS first_purchase_date,
                       MAX(d.record_date) AS last_purchase_date
                FROM buyer_entity_links bel
                JOIN deeds d ON d.id = bel.source_id AND bel.source_table = 'deeds'
                GROUP BY bel.buyer_entity_id
            ),
            last_deed AS (
                -- NULLS LAST is required here: Postgres sorts NULL first in a
                -- bare DESC order, so DISTINCT ON would otherwise pick a
                -- NULL-dated deed as "most recent" whenever an entity has any
                -- (found via testing: entity #16902, 2 of 34 linked deeds
                -- have no record_date, and the buggy version silently
                -- returned NULL for last_purchase_date, killing why_now's
                -- date text and HUNTER-04's acquisition_velocity for it).
                SELECT DISTINCT ON (bel.buyer_entity_id)
                       bel.buyer_entity_id, d.deed_type, d.record_date
                FROM buyer_entity_links bel
                JOIN deeds d ON d.id = bel.source_id AND bel.source_table = 'deeds'
                ORDER BY bel.buyer_entity_id, d.record_date DESC NULLS LAST
            )
            SELECT be.id, be.opportunity_thread_id, be.canonical_name, be.entity_type,
                   be.county_id, be.total_purchase_count, be.total_cash_volume,
                   be.whale_flagged_at,
                   COALESCE(bc.has_phone, false) AS has_phone,
                   COALESCE(bc.has_email, false) AS has_email,
                   COALESCE(bc.phone_score, 0) AS phone_score,
                   COALESCE(rp.recent_count, 0) AS recent_count,
                   rp.first_purchase_date,
                   ld.deed_type AS last_deed_type,
                   ld.record_date AS last_purchase_date
            FROM buyer_entities be
            LEFT JOIN best_contact bc ON bc.buyer_entity_id = be.id
            LEFT JOIN recent_purchases rp ON rp.buyer_entity_id = be.id
            LEFT JOIN last_deed ld ON ld.buyer_entity_id = be.id
            WHERE be.is_whale {county_filter}
            ORDER BY be.total_purchase_count DESC, be.total_cash_volume DESC
            LIMIT :limit
        """),
        {"limit": limit, "nominal_floor": NOMINAL_CONSIDERATION_FLOOR,
         **({"county_id": county_id} if county_id else {})},
    ).fetchall()

    ranked = []
    for i, r in enumerate(rows, start=1):
        if r.has_phone:
            channel, confidence = "phone", r.phone_score
        elif r.has_email:
            channel, confidence = "email", 0
        else:
            channel, confidence = "none", 0
        ranked.append(RankedWhale(
            rank=i,
            entity_id=r.id,
            opportunity_thread_id=r.opportunity_thread_id,
            canonical_name=r.canonical_name,
            entity_type=r.entity_type,
            county_id=r.county_id,
            total_purchase_count=r.total_purchase_count,
            total_cash_volume=r.total_cash_volume,
            contact_channel=channel,
            contact_confidence=confidence,
            why_now=_why_now(r.recent_count, r.total_cash_volume, r.last_deed_type, r.last_purchase_date),
            whale_flagged_at=r.whale_flagged_at,
            acquisition_velocity=_acquisition_velocity(r.total_purchase_count, r.first_purchase_date, r.last_purchase_date),
        ))

    logger.info("get_ranked_whales: returned %d of up to %d whales.", len(ranked), limit)
    return [asdict(w) for w in ranked]
