"""
Standalone offer recommendation service.
Wraps recommend_offer() from src.agents.cora.contracts for use outside the
CORA agent context (NBRA engine, pipeline entry points, admin tooling).
"""
from __future__ import annotations

import logging
from typing import Any, Dict

from src.agents.cora.contracts import OfferRecommendation, recommend_offer

logger = logging.getLogger(__name__)


def recommend_offer_for_entity(buyer_entity: Dict[str, Any]) -> OfferRecommendation:
    """
    Thin wrapper over recommend_offer() for non-CORA callers.

    Args:
        buyer_entity: Dict with prospect/entity signals. Expected keys (all
            optional — missing keys degrade gracefully to lower-priority rules):
            - is_whale (bool)
            - entity_links (list[dict]) — each dict must have a ``source_table`` key;
              auction-winner detection checks ``source_table == "auction_records"``
            - is_auction_winner (bool)
            - has_active_subscription (bool)
            - had_prior_subscription (bool)
            - total_purchase_count (int)
            - entity_type (str)
            - signals (list[str])

    Returns:
        OfferRecommendation TypedDict with offer, reason, confidence, and
        full rule-tracing metadata.
    """
    recommendation = recommend_offer(buyer_entity)
    logger.info(
        "offer_recommendation rule=%s offer=%s confidence=%.2f",
        recommendation["matched_rule_id"],
        recommendation["offer"],
        recommendation["confidence"],
    )
    return recommendation
