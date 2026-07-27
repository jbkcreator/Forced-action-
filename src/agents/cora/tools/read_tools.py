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


def get_prior_conversation(opportunity_thread_id: str) -> List[Dict[str, Any]]:
    """Cora's own prior drafts + replies for a thread — never a subscriber-keyed lookup."""
    return store.read_conversation(opportunity_thread_id)


def get_contact_channel(session: Session, buyer_entity_id: int) -> Dict[str, Optional[str]]:
    """
    Best-effort contact info for a buyer entity, via its linked owner rows.
    Returns {"email": ..., "phone": ...} — either may be None.
    """
    row = session.execute(
        text(
            """
            SELECT o.email_1, o.phone_1
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
        return {"email": None, "phone": None}
    return {"email": row.get("email_1"), "phone": row.get("phone_1")}
