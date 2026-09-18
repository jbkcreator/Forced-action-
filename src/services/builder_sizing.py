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

from sqlalchemy import bindparam, text
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


@dataclass
class _PropertyValues:
    arv: Optional[Decimal] = None                 # after-repair value → collateral / LTV cap
    assessed_value_mkt: Optional[Decimal] = None  # acquisition-cost basis
    last_sale_price: Optional[Decimal] = None     # acquisition-cost basis


def _pos(v):
    return Decimal(str(v)) if v and Decimal(str(v)) > _ZERO else None


def _property_values_batch(
    session: Session, property_ids: list[int],
) -> dict[int, _PropertyValues]:
    """Fetch value fields for many properties in ONE query (avoids an N+1
    financials SELECT per builder hit). Missing property_ids simply absent."""
    if not property_ids:
        return {}
    rows = session.execute(
        text("""
            SELECT f.property_id, f.arv, f.assessed_value_mkt, f.last_sale_price
            FROM financials f
            WHERE f.property_id IN :pids
        """).bindparams(bindparam("pids", expanding=True)),
        {"pids": list(set(property_ids))},
    )
    return {
        r.property_id: _PropertyValues(
            arv=_pos(r.arv),
            assessed_value_mkt=_pos(r.assessed_value_mkt),
            last_sale_price=_pos(r.last_sale_price),
        )
        for r in rows
    }


def _property_values(session: Session, property_id: int) -> _PropertyValues:
    """Single-property value fetch. ARV is collateral (LTV cap basis),
    assessed/sale are acquisition-cost bases. Conflating them lets the loan
    exceed the LTV cap (ARV must not be reused as the cost basis)."""
    return _property_values_batch(session, [property_id]).get(
        property_id, _PropertyValues()
    )


def size_builder_hit(
    session: Session,
    hit: BuilderHit,
    values: Optional[_PropertyValues] = None,
) -> BuilderSizingResult:
    """Compute a loan-size estimate for one BuilderHit.

    values: pre-fetched property values (from _property_values_batch) — pass
    it when sizing many hits to avoid a per-hit financials query.
    """

    # Path 1: property-backed value (quote_ready pipeline).
    # Requires BOTH an acquisition-cost basis (assessed/sale) AND a rehab (permit
    # job_value) to derive project cost. ARV is passed separately as collateral so
    # compute_quote_ready enforces min(LTC×cost, LTV×ARV) — the loan can never
    # exceed the LTV cap. If no cost basis exists we do NOT reuse ARV as cost
    # (that bypassed the cap); we fall through to job-value sizing.
    if hit.property_id:
        vals = values if values is not None else _property_values(session, hit.property_id)
        cost_basis = vals.assessed_value_mkt or vals.last_sale_price
        rehab = hit.total_job_value if (hit.total_job_value and hit.total_job_value > _ZERO) else None
        if cost_basis and rehab:
            inp = QuoteReadyInput(
                opportunity_id=UUID(int=0),  # sentinel — no real opportunity yet at detection time
                property_id=hit.property_id,
                max_ltc=_CONSTRUCTION_LTC,
                max_ltv=_CONSTRUCTION_LTV,
                assessed_value_mkt=vals.assessed_value_mkt,
                last_sale_price=vals.last_sale_price,
                rehab_estimate=rehab,
                arv=vals.arv,  # None → LTV cap simply not applied; never exceeded
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
    """Batch-size a list of BuilderHits. Property values for every hit are
    fetched in ONE query up front, then reused per hit (no N+1)."""
    values_by_property = _property_values_batch(
        session, [h.property_id for h in hits if h.property_id],
    )
    results = []
    for hit in hits:
        try:
            vals = values_by_property.get(hit.property_id) if hit.property_id else None
            results.append(size_builder_hit(session, hit, values=vals))
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
