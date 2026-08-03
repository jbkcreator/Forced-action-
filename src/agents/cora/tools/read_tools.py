"""
Cora's read tools — thin, read-only wrappers. Raw SQL via session.execute
(text(...)), never the ORM query API, per this repo's SQL convention. No
write tools exist in this package at all — that's what makes "zero send
capability" statically checkable (no src.agents.tools.write_tools import
anywhere under src/agents/cora/).
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from src.agents.cora import store


def get_buyer_entity_by_opportunity_thread_id(session: Session, opportunity_thread_id: str) -> Optional[Dict[str, Any]]:
    row = session.execute(
        text(
            """
            SELECT id, canonical_name, entity_type, primary_mailing_address,
                   confidence_score, verification_status, total_purchase_count,
                   total_cash_volume, is_whale, whale_flagged_at,
                   opportunity_thread_id, county_id
            FROM buyer_entities
            WHERE opportunity_thread_id = :opportunity_thread_id
            """
        ),
        {"opportunity_thread_id": opportunity_thread_id},
    ).mappings().first()
    return dict(row) if row else None


def get_ranked_whales(session: Session, limit: int = 25, county_id: Optional[str] = None) -> List[Dict[str, Any]]:
    """Re-exports Hunter's own Hunter->Cora data contract, unmodified."""
    from src.services.whale_ranking import get_ranked_whales as _get_ranked_whales
    return _get_ranked_whales(session, limit=limit, county_id=county_id)


def get_prior_conversation(session: Session, opportunity_thread_id: str) -> List[Dict[str, Any]]:
    """Cora's own prior drafts + replies for a thread — never a subscriber-keyed lookup."""
    return store.read_conversation(session, opportunity_thread_id)


def get_recent_auction_fast_follow_whales(
    session: Session,
    lookback_days: int = 7,
    limit: int = 25,
    county_id: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Cell #2 population: whales already flagged (is_whale=true) whose flag is
    tied to a distressed-acquisition deed recorded in the last lookback_days.

    Deliberately read-only — reuses src.connectors.whale_auction_fast_follow's
    own DISTRESSED_KEYWORDS detection vocabulary and buyer_entity_links join,
    but never calls refresh_whale_flags() or otherwise mutates buyer_entities.
    That connector already runs on its own cron schedule (crontab.txt's
    08:30/08:35 stagger); this function only reads whatever it already wrote,
    it never re-triggers or duplicates that write path.
    """
    from src.connectors.deed_flip_outcomes import DISTRESSED_KEYWORDS

    keyword_clauses = " OR ".join(f"LOWER(d.deed_type) LIKE :kw{i}" for i in range(len(DISTRESSED_KEYWORDS)))
    params: Dict[str, Any] = {f"kw{i}": f"%{kw}%" for i, kw in enumerate(DISTRESSED_KEYWORDS)}
    params.update({"lookback_days": lookback_days, "limit": limit, "county_id": county_id})

    rows = session.execute(
        text(
            f"""
            SELECT be.id, be.canonical_name, be.entity_type, be.primary_mailing_address,
                   be.confidence_score, be.verification_status, be.total_purchase_count,
                   be.total_cash_volume, be.is_whale, be.whale_flagged_at,
                   be.opportunity_thread_id, be.county_id,
                   MAX(d.record_date) AS latest_auction_deed_date
            FROM buyer_entities be
            JOIN buyer_entity_links bel ON bel.buyer_entity_id = be.id AND bel.source_table = 'deeds'
            JOIN deeds d ON d.id = bel.source_id
            WHERE be.is_whale = true
              AND be.opportunity_thread_id IS NOT NULL
              AND d.record_date >= CURRENT_DATE - :lookback_days * INTERVAL '1 day'
              AND d.deed_type IS NOT NULL
              AND ({keyword_clauses})
              AND (:county_id IS NULL OR be.county_id = :county_id)
            GROUP BY be.id, be.canonical_name, be.entity_type, be.primary_mailing_address,
                     be.confidence_score, be.verification_status, be.total_purchase_count,
                     be.total_cash_volume, be.is_whale, be.whale_flagged_at,
                     be.opportunity_thread_id, be.county_id
            ORDER BY latest_auction_deed_date DESC
            LIMIT :limit
            """
        ),
        params,
    ).mappings().all()
    return [dict(row) for row in rows]


def get_contact_channel(session: Session, buyer_entity_id: int) -> Dict[str, Optional[Any]]:
    """
    Best-effort contact info for a buyer entity, via its linked owner rows.
    Returns {"email": ..., "phone": ..., "contact_confidence": ...} — any may
    be None. contact_confidence (QUALITY-v2.2 Q3 fix) is
    buyer_entity_links.match_confidence for whichever linked owner row was
    picked (already used to ORDER BY here, but previously dropped before
    reaching the caller — a real gap: the Hunter->Cora contract requires
    contact-channel confidence and this was the one place it was silently lost).
    """
    row = session.execute(
        text(
            """
            SELECT o.email_1, o.phone_1, bel.match_confidence AS contact_confidence
            FROM buyer_entity_links bel
            JOIN owners o ON bel.source_table = 'owners' AND bel.source_id = o.id
            WHERE bel.buyer_entity_id = :buyer_entity_id
            ORDER BY bel.match_confidence DESC
            LIMIT 1
            """
        ),
        {"buyer_entity_id": buyer_entity_id},
    ).mappings().first()
    if not row:
        return {"email": None, "phone": None, "contact_confidence": None}
    return {
        "email": row.get("email_1"),
        "phone": row.get("phone_1"),
        "contact_confidence": row.get("contact_confidence"),
    }
