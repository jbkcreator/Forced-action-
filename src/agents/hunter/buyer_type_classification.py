"""
Hunter — behavioral buyer-type classification (HUNTER-03).

Distinct from BuyerEntity.entity_type (legal structure: Individual/LLC/Trust/
Corporate/Estate) -- this classifies flipper / buy-and-hold / wholesaler /
institutional behavior. Named in src.agents.hunter.gating's docstring as the
next expected capability module ("... buyer_entity_resolution.py,
whale_detection.py, buyer_type_classification.py, ...").

Reads src.agents.hunter.portfolio_profiling's persisted portfolio_evidence
JSONB directly -- this module never re-queries deeds itself. Classifying off
the FULL purchase history that module computes (not just deeds tied to a
distressed acquisition) is deliberate: a normal investor who never buys a
distressed property would otherwise never accumulate enough evidence to be
classified at all.

Institutional takes precedence over flipper/wholesaler/buy-and-hold when both
would otherwise apply -- portfolio scale is the stronger signal. Wholesaler
requires real volume (a minimum rapid-resale count AND ratio), not one
outlier fast flip on an otherwise ordinary flipper.

Open item: docs/plans/agent_lane_phase3_week3_dev_split.md references "Q3's
Hunter priority order" from an external client Q&A doc not checked into this
repo -- the precedence here (institutional > wholesaler > flipper >
buy-and-hold) is a reasonable default pending confirmation against Q3, not a
confirmed client answer.

Usage:
    from src.agents.hunter.buyer_type_classification import classify_buyer_types
    classify_buyer_types(session)                     # full-table (backfill only)
    classify_buyer_types(session, entity_ids=[1, 2])   # incremental
"""
from __future__ import annotations

import json
import logging
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

RULE_VERSION = 1

FLIPPER_RATIO_MIN = 0.5

WHOLESALER_MIN_COUNT = 2
WHOLESALER_MIN_RATIO = 0.4

INSTITUTIONAL_MIN_PURCHASES = 10
INSTITUTIONAL_MIN_LLC_DENSITY = 3  # distinct sunbiz_llc_piercing-linked owner records

CONFIDENCE_FLOOR = 50
CONFIDENCE_CEILING = 95
CONFIDENCE_STEP = 5  # per additional unit of evidence beyond the first


def _scaled_confidence(evidence_units: int) -> int:
    """First unit of evidence -> CONFIDENCE_FLOOR; climbs by CONFIDENCE_STEP
    per additional unit, capped at CONFIDENCE_CEILING. Tuned against the
    acceptance harness / founder spot-check, not asserted as final."""
    if evidence_units <= 0:
        return CONFIDENCE_FLOOR
    return min(CONFIDENCE_CEILING, CONFIDENCE_FLOOR + CONFIDENCE_STEP * (evidence_units - 1))


def _classify_one(
    portfolio_evidence: dict, total_purchase_count: int, llc_link_count: int,
) -> Optional[dict]:
    """
    Returns None if there's no evidence to classify from at all (buyer_type
    stays NULL) -- otherwise a dict with buyer_type/confidence/evidence ready
    to persist.
    """
    flip_count = portfolio_evidence.get("exit_within_730_days", 0)
    hold_count = portfolio_evidence.get("still_held_past_730_days", 0)
    rapid_resale_count = portfolio_evidence.get("exit_within_7_days", 0)
    eligible_exits = flip_count + hold_count

    base_type: Optional[str] = None
    base_confidence: Optional[int] = None
    if eligible_exits > 0:
        ratio = flip_count / eligible_exits
        base_type = "flipper" if ratio >= FLIPPER_RATIO_MIN else "buy-and-hold"
        if (
            rapid_resale_count >= WHOLESALER_MIN_COUNT
            and (rapid_resale_count / eligible_exits) >= WHOLESALER_MIN_RATIO
        ):
            base_type = "wholesaler"
        base_confidence = _scaled_confidence(eligible_exits)

    is_institutional = (
        total_purchase_count >= INSTITUTIONAL_MIN_PURCHASES
        and llc_link_count >= INSTITUTIONAL_MIN_LLC_DENSITY
    )

    if is_institutional:
        final_type = "institutional"
        final_confidence = _scaled_confidence(total_purchase_count - INSTITUTIONAL_MIN_PURCHASES + 1)
    elif base_type is not None:
        final_type = base_type
        final_confidence = base_confidence
    else:
        return None

    return {
        "buyer_type": final_type,
        "buyer_type_confidence": final_confidence,
        "buyer_type_evidence": {
            "acquisition_count": portfolio_evidence.get("acquisition_count", 0),
            "eligible_exits": eligible_exits,
            "flip_count": flip_count,
            "hold_count": hold_count,
            "rapid_resale_count": rapid_resale_count,
            "total_purchase_count": total_purchase_count,
            "llc_link_count": llc_link_count,
            "is_institutional": is_institutional,
        },
    }


def _fetch_llc_link_counts(session: Session, entity_ids: list[int]) -> dict[int, int]:
    if not entity_ids:
        return {}
    rows = session.execute(
        text("""
            SELECT buyer_entity_id, COUNT(*) AS llc_link_count
            FROM buyer_entity_links
            WHERE match_method = 'sunbiz_llc_piercing' AND buyer_entity_id = ANY(:entity_ids)
            GROUP BY buyer_entity_id
        """),
        {"entity_ids": entity_ids},
    ).fetchall()
    return {r.buyer_entity_id: r.llc_link_count for r in rows}


def classify_buyer_types(session: Session, entity_ids: Optional[list[int]] = None) -> int:
    """
    Classify every entity with persisted portfolio_evidence (or just
    entity_ids, if given -- the incremental nightly-sweep case). entity_ids=[]
    is explicitly a no-op, never "classify everything" -- same empty-vs-None
    safety as portfolio_profiling.compute_acquisition_evidence.

    One batched UPDATE (executemany-style), never a per-entity round trip.
    """
    if entity_ids is not None and len(entity_ids) == 0:
        return 0

    where_clause = "WHERE portfolio_evidence IS NOT NULL"
    params: dict = {}
    if entity_ids is not None:
        where_clause += " AND id = ANY(:entity_ids)"
        params["entity_ids"] = entity_ids

    rows = session.execute(
        text(f"SELECT id, total_purchase_count, portfolio_evidence FROM buyer_entities {where_clause}"),
        params,
    ).fetchall()
    if not rows:
        logger.info("classify_buyer_types: no profiled entities in scope -- nothing to do.")
        return 0

    llc_link_counts = _fetch_llc_link_counts(session, [r.id for r in rows])

    updates = []
    for row in rows:
        classification = _classify_one(
            row.portfolio_evidence, row.total_purchase_count, llc_link_counts.get(row.id, 0),
        )
        if classification is None:
            continue
        updates.append({
            "entity_id": row.id,
            "buyer_type": classification["buyer_type"],
            "buyer_type_confidence": classification["buyer_type_confidence"],
            "buyer_type_evidence": json.dumps(classification["buyer_type_evidence"]),
            "buyer_type_rule_version": RULE_VERSION,
        })

    if not updates:
        logger.info("classify_buyer_types: no entity in scope had enough evidence to classify.")
        return 0

    session.execute(
        text("""
            UPDATE buyer_entities
            SET buyer_type = :buyer_type,
                buyer_type_confidence = :buyer_type_confidence,
                buyer_type_evidence = CAST(:buyer_type_evidence AS JSONB),
                buyer_type_rule_version = :buyer_type_rule_version,
                buyer_type_classified_at = now()
            WHERE id = :entity_id
        """),
        updates,
    )
    session.commit()
    logger.info("classify_buyer_types: classified %d of %d entit(y/ies) in scope.", len(updates), len(rows))
    return len(updates)
