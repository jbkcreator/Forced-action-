"""WP-T2-8 Stage D — Builder loan-size feature.

Produces a loan-size estimate for each BuilderHit. This is a FEATURE, not a
score — WP-9 owns the expected-revenue scorer. The output is consumed by Stage
E (DialCandidate assembly) and stored for dial-list ranking.

Sizing logic (GRILL-DECISIONS.md Q8, amendment: 85% LTC):
  1. If the property has a live quote_ready scenario with an estimated_value /
     assessed_value_mkt / purchase_price, use compute_quote_ready(max_ltc=0.85).
  2. Else if the BuilderHit carries a total_job_value, use job_value * 0.85.
  3. Else return None (unknown — ranker treats as low-confidence).
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.orm import Session

from src.services.builder_patterns import BuilderHit
from src.services.quote_ready.compute import compute_quote_ready
from src.services.quote_ready.models import QuoteReadyInput

logger = logging.getLogger(__name__)

_CONSTRUCTION_LTC = Decimal("0.85")
_ZERO = Decimal("0")

# quote_ready.max_ltv for construction — generous LTV floor (no ARV yet; land + build)
_CONSTRUCTION_LTV = Decimal("0.90")


@dataclass
class BuilderSizingResult:
    buyer_entity_id: int
    pattern: str
    estimated_loan: Optional[Decimal]     # None = unknown
    confidence: str                        # "high" | "medium" | "low" | "unknown"
    sizing_source: str                     # "quote_ready" | "job_value" | "none"
    property_id: Optional[int] = None


def _arv_for_property(session: Session, property_id: int) -> Optional[Decimal]:
    """
    Pull the best available value basis for a property from the financials
    table. Falls back: arv → assessed_value_mkt → last_sale_price.
    """
    row = session.execute(
        text("""
            SELECT f.arv, f.assessed_value_mkt, f.last_sale_price
            FROM financials f
            WHERE f.property_id = :pid
            LIMIT 1
        """),
        {"pid": property_id},
    ).fetchone()
    if row is None:
        return None
    for v in (row.arv, row.assessed_value_mkt, row.last_sale_price):
        if v and Decimal(str(v)) > _ZERO:
            return Decimal(str(v))
    return None


def size_builder_hit(session: Session, hit: BuilderHit) -> BuilderSizingResult:
    """Compute a loan-size estimate for one BuilderHit."""

    # Path 1: property-backed value (quote_ready pipeline)
    if hit.property_id:
        arv = _arv_for_property(session, hit.property_id)
        if arv:
            # Construction project cost = land/ARV basis + build cost. The permit
            # job_value IS the build cost, so it maps to rehab_estimate; without
            # it compute_quote_ready cannot derive project_cost (SPEC Q8:
            # ARV/rehab → 85% LTC of project cost).
            rehab = hit.total_job_value if (hit.total_job_value and hit.total_job_value > _ZERO) else _ZERO
            inp = QuoteReadyInput(
                opportunity_id=UUID(int=0),  # sentinel — no real opportunity yet at detection time
                property_id=hit.property_id,
                max_ltc=_CONSTRUCTION_LTC,
                max_ltv=_CONSTRUCTION_LTV,
                estimated_value=arv,
                rehab_estimate=rehab,
            )
            result = compute_quote_ready(inp)
            proposed = result.proposed_loan
            if proposed and proposed.raw > _ZERO:
                return BuilderSizingResult(
                    buyer_entity_id=hit.buyer_entity_id,
                    pattern=hit.pattern,
                    estimated_loan=proposed.raw,
                    confidence=proposed.confidence,
                    sizing_source="quote_ready",
                    property_id=hit.property_id,
                )

    # Path 2: permit job_value * 85% LTC
    if hit.total_job_value and hit.total_job_value > _ZERO:
        estimated = (hit.total_job_value * _CONSTRUCTION_LTC).quantize(Decimal("1"))
        return BuilderSizingResult(
            buyer_entity_id=hit.buyer_entity_id,
            pattern=hit.pattern,
            estimated_loan=estimated,
            confidence="low",
            sizing_source="job_value",
            property_id=hit.property_id,
        )

    # Path 3: unknown
    return BuilderSizingResult(
        buyer_entity_id=hit.buyer_entity_id,
        pattern=hit.pattern,
        estimated_loan=None,
        confidence="unknown",
        sizing_source="none",
        property_id=hit.property_id,
    )


def size_builder_hits(session: Session, hits: list[BuilderHit]) -> list[BuilderSizingResult]:
    """Batch-size a list of BuilderHits."""
    results = []
    for hit in hits:
        try:
            results.append(size_builder_hit(session, hit))
        except Exception as exc:
            logger.warning(
                "size_builder_hit failed for entity_id=%s pattern=%s: %s",
                hit.buyer_entity_id, hit.pattern, exc,
            )
            results.append(BuilderSizingResult(
                buyer_entity_id=hit.buyer_entity_id,
                pattern=hit.pattern,
                estimated_loan=None,
                confidence="unknown",
                sizing_source="none",
                property_id=hit.property_id,
            ))
    return results
