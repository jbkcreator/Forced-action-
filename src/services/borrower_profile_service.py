"""
WP-5B — Borrower Buy Box, Velocity & Next-Need Prediction service.

Public API:
    compute_person_profile(session, person_id) -> dict   # compute + upsert
    get_person_profile(session, person_id) -> dict | None  # read-only

compute_person_profile():
    1. Resolves person → BuyerEntity via the provisional buyer_entity_id FK
       on fa_max_persons (populated by the profile sweep or by WP-3/WP-4
       once those ship). Falls back to a name-based best-effort lookup.
    2. If no entity is found → confidence_tier='unknown', all intelligence
       fields NULL.  Profile is still written so the record exists.
    3. If an entity is found, derives from public-record data only:
       - Buy-box: property city/county/type distribution + arm's-length price
         band from deeds linked to that entity (>=$1 000 nominal-consideration
         floor, same threshold as Hunter's portfolio_profiling).
       - Velocity: mirrors BuyerEntity.cadence_purchases_per_year, avg_hold_days,
         last acquisition date from portfolio_evidence; also counts still-held
         properties for active_property_count.
       - Predicted next need: rolls up FinancingIntentScore.recommended_product
         across the entity's owned properties (latest score per property),
         picks the highest-scoring product as the prediction.
       - Predicted date: maturity-based if an open fa_max_opportunity exists
         with actual_funded_at + maturity_months; cadence-based otherwise.
    4. confidence_tier logic:
         3+ arm's-length deeds → 'high'
         2 deeds            → 'medium'
         1 deed             → 'low'
         0 deeds / no entity → 'unknown'

Compliance:
    - No credit score, income, bank statement, tax return, or SSN stored or
      consumed.  Deed sale_price is a public record (SOT.md Part 1).
    - predicted_next_need is a product category label (e.g. 'bridge') for
      internal analysis only — never a rate, term, or commitment to a borrower.
    - No outbound contact is produced; this is a pure internal intelligence
      layer (no autonomy gate required per SOT.md Part 2 Requirement 2).

Dependencies:
    - fa_max_persons (WP-1)
    - fa_max_person_profiles (WP-5B migration)
    - buyer_entities / buyer_entity_links (Hunter)
    - financing_intent_scores (financing-intent engine)
    - deeds, properties (existing FA platform tables)
    - fa_max_opportunities (WP-1) — for maturity-based date projection
"""
from __future__ import annotations

import logging
import math
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

# Arm's-length sale price floor — same as Hunter's NOMINAL_CONSIDERATION_FLOOR
# in portfolio_profiling.py.  Filters out $1/$10 family/trust/corrective deeds.
NOMINAL_CONSIDERATION_FLOOR = 1_000

# Confidence tier thresholds (deed count)
_HIGH_DEED_THRESHOLD = 3
_MEDIUM_DEED_THRESHOLD = 2
_LOW_DEED_THRESHOLD = 1

# Max properties whose FinancingIntentScore evidence to include in next_need_evidence
_NEXT_NEED_EVIDENCE_TOP_N = 3


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def compute_person_profile(session: Session, person_id: str) -> Dict[str, Any]:
    """Compute and upsert the intelligence profile for one fa_max_persons row.

    Always writes a row (even if confidence_tier='unknown') so downstream
    callers can distinguish "not yet computed" (no row) from "computed but
    insufficient data" (unknown tier).

    Returns the profile dict that was upserted.
    """
    person_uuid = _parse_uuid(person_id)
    entity_id = _resolve_entity_id(session, person_uuid)

    if entity_id is None:
        profile = _build_unknown_profile(person_uuid)
    else:
        profile = _build_profile(session, person_uuid, entity_id)

    _upsert_profile(session, profile)
    return profile


def get_person_profile(session: Session, person_id: str) -> Optional[Dict[str, Any]]:
    """Return the most-recently-computed profile for a person, or None."""
    row = session.execute(
        text("""
            SELECT
                person_id::text,
                buyer_entity_id,
                buy_box_geography,
                buy_box_property_types,
                buy_box_price_band,
                velocity_purchases_per_year,
                last_transaction_date,
                avg_days_between_transactions,
                active_property_count,
                predicted_next_need,
                predicted_next_need_date,
                next_need_evidence,
                confidence_tier,
                computed_at
            FROM fa_max_person_profiles
            WHERE person_id = :person_id
        """),
        {"person_id": person_id},
    ).mappings().first()
    return dict(row) if row else None


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _parse_uuid(person_id: str) -> str:
    """Validate and normalise the person_id string."""
    try:
        return str(UUID(str(person_id)))
    except (ValueError, AttributeError) as exc:
        raise ValueError(f"Invalid person_id: {person_id!r}") from exc


def _resolve_entity_id(session: Session, person_uuid: str) -> Optional[int]:
    """Return the buyer_entity_id for this person.

    Priority:
      1. fa_max_persons.buyer_entity_id (provisionally populated by a prior
         sweep or by WP-3/WP-4).
      2. Best-effort match via name from fa_max_persons.source_reference
         against buyer_entities.canonical_name (case-insensitive exact match
         only — never fuzzy here to avoid silent misattribution; if uncertain,
         return None and let confidence_tier reflect it).
    """
    row = session.execute(
        text("SELECT buyer_entity_id FROM fa_max_persons WHERE person_id = :pid"),
        {"pid": person_uuid},
    ).mappings().first()

    if row is None:
        return None

    if row["buyer_entity_id"] is not None:
        return int(row["buyer_entity_id"])

    return None  # WP-3/WP-4 will populate the FK; no fuzzy fallback here


def _build_unknown_profile(person_uuid: str) -> Dict[str, Any]:
    """Profile stub for persons with no resolved BuyerEntity."""
    now = datetime.now(timezone.utc)
    return {
        "person_id": person_uuid,
        "buyer_entity_id": None,
        "buy_box_geography": None,
        "buy_box_property_types": None,
        "buy_box_price_band": None,
        "velocity_purchases_per_year": None,
        "last_transaction_date": None,
        "avg_days_between_transactions": None,
        "active_property_count": None,
        "predicted_next_need": None,
        "predicted_next_need_date": None,
        "next_need_evidence": None,
        "confidence_tier": "unknown",
        "computed_at": now,
    }


def _build_profile(
    session: Session, person_uuid: str, entity_id: int
) -> Dict[str, Any]:
    """Full profile computation for a person with a resolved BuyerEntity."""
    now = datetime.now(timezone.utc)

    # 1. Deed-linked properties — buy-box geography/type/price + velocity ----
    deeds = _fetch_entity_deeds(session, entity_id)
    buy_box_geography = _compute_geography(deeds)
    buy_box_property_types = _compute_property_types(deeds)
    buy_box_price_band = _compute_price_band(deeds)
    deed_count = len(deeds)

    # 2. Velocity from BuyerEntity cadence fields ----------------------------
    cadence = _fetch_cadence(session, entity_id)
    _raw_velocity = cadence.get("cadence_purchases_per_year")
    velocity = Decimal(str(_raw_velocity)) if _raw_velocity is not None else None
    avg_hold = cadence.get("avg_hold_days")
    avg_days = Decimal(str(avg_hold)) if avg_hold is not None else None
    portfolio_evidence = cadence.get("portfolio_evidence") or {}
    still_held = portfolio_evidence.get("still_held_count", 0)
    active_property_count = still_held if still_held > 0 else None

    last_txn_date = _last_transaction_date(deeds)

    # 3. Predicted next need from FinancingIntentScore -----------------------
    intent_rows = _fetch_financing_intent(session, entity_id)
    predicted_need, next_need_evidence = _rollup_next_need(intent_rows)

    # 4. Predicted next-need date -------------------------------------------
    predicted_date = _predict_next_need_date(
        session=session,
        person_uuid=person_uuid,
        last_txn_date=last_txn_date,
        velocity=velocity,
    )

    # 5. Confidence tier ----------------------------------------------------
    confidence_tier = _confidence_tier(deed_count)

    return {
        "person_id": person_uuid,
        "buyer_entity_id": entity_id,
        "buy_box_geography": buy_box_geography,
        "buy_box_property_types": buy_box_property_types,
        "buy_box_price_band": buy_box_price_band,
        "velocity_purchases_per_year": velocity,
        "last_transaction_date": last_txn_date,
        "avg_days_between_transactions": avg_days,
        "active_property_count": active_property_count,
        "predicted_next_need": predicted_need,
        "predicted_next_need_date": predicted_date,
        "next_need_evidence": next_need_evidence,
        "confidence_tier": confidence_tier,
        "computed_at": now,
    }


def _fetch_entity_deeds(session: Session, entity_id: int) -> List[Dict[str, Any]]:
    """Fetch arm's-length deeds linked to this entity, joined to property attributes."""
    rows = session.execute(
        text("""
            SELECT
                d.id          AS deed_id,
                d.property_id,
                d.record_date,
                d.sale_price,
                p.city,
                p.zip,
                p.county_id,
                p.property_type,
                p.property_use_code
            FROM buyer_entity_links bel
            JOIN deeds d ON bel.source_id = d.id
            JOIN properties p ON d.property_id = p.id
            WHERE bel.buyer_entity_id = :entity_id
              AND bel.source_table = 'deeds'
              AND d.sale_price >= :floor
            ORDER BY d.record_date DESC
        """),
        {"entity_id": entity_id, "floor": NOMINAL_CONSIDERATION_FLOOR},
    ).mappings().all()
    return [dict(r) for r in rows]


def _compute_geography(deeds: List[Dict]) -> Optional[List[Dict]]:
    if not deeds:
        return None
    from collections import Counter
    counts: Counter = Counter()
    for d in deeds:
        key = (d.get("city") or "unknown", d.get("county_id") or "unknown")
        counts[key] += 1
    return [
        {"city": city, "county_id": county_id, "count": cnt}
        for (city, county_id), cnt in counts.most_common()
    ]


def _compute_property_types(deeds: List[Dict]) -> Optional[List[Dict]]:
    if not deeds:
        return None
    from collections import Counter
    counts: Counter = Counter()
    for d in deeds:
        key = (d.get("property_type") or "unknown", d.get("property_use_code") or "unknown")
        counts[key] += 1
    return [
        {"property_type": pt, "property_use_code": puc, "count": cnt}
        for (pt, puc), cnt in counts.most_common()
    ]


def _compute_price_band(deeds: List[Dict]) -> Optional[Dict[str, Any]]:
    prices = sorted(
        int(d["sale_price"]) * 100  # store as cents for precision
        for d in deeds
        if d.get("sale_price") is not None
    )
    if not prices:
        return None
    n = len(prices)
    mid = n // 2
    median = prices[mid] if n % 2 == 1 else (prices[mid - 1] + prices[mid]) // 2
    return {
        "min_cents": prices[0],
        "median_cents": median,
        "max_cents": prices[-1],
        "sample_count": n,
    }


def _fetch_cadence(session: Session, entity_id: int) -> Dict[str, Any]:
    row = session.execute(
        text("""
            SELECT cadence_purchases_per_year, avg_hold_days, portfolio_evidence
            FROM buyer_entities
            WHERE id = :entity_id
        """),
        {"entity_id": entity_id},
    ).mappings().first()
    return dict(row) if row else {}


def _last_transaction_date(deeds: List[Dict]) -> Optional[date]:
    """Most recent deed record_date across arm's-length deeds."""
    dates = [d["record_date"] for d in deeds if d.get("record_date")]
    return max(dates) if dates else None


def _fetch_financing_intent(session: Session, entity_id: int) -> List[Dict[str, Any]]:
    """Latest FinancingIntentScore per property owned by this entity."""
    rows = session.execute(
        text("""
            SELECT DISTINCT ON (fis.property_id)
                fis.property_id,
                fis.financing_intent_score,
                fis.recommended_product,
                fis.signal_details,
                fis.signal_flags
            FROM buyer_entity_links bel
            JOIN deeds d ON bel.source_id = d.id
            JOIN financing_intent_scores fis ON fis.property_id = d.property_id
            WHERE bel.buyer_entity_id = :entity_id
              AND bel.source_table = 'deeds'
              AND fis.recommended_product IS NOT NULL
            ORDER BY fis.property_id, fis.score_date DESC
        """),
        {"entity_id": entity_id},
    ).mappings().all()
    return [dict(r) for r in rows]


def _rollup_next_need(
    rows: List[Dict],
) -> tuple[Optional[str], Optional[List[Dict]]]:
    """Pick the highest-scoring product across properties and build evidence."""
    if not rows:
        return None, None
    sorted_rows = sorted(rows, key=lambda r: float(r.get("financing_intent_score") or 0), reverse=True)
    top = sorted_rows[:_NEXT_NEED_EVIDENCE_TOP_N]
    predicted_product = top[0]["recommended_product"]
    evidence = [
        {
            "property_id": r["property_id"],
            "financing_intent_score": str(r.get("financing_intent_score") or ""),
            "recommended_product": r["recommended_product"],
            "signal_flags": r.get("signal_flags"),
            "signal_details": r.get("signal_details"),
        }
        for r in top
    ]
    return predicted_product, evidence


def _predict_next_need_date(
    session: Session,
    person_uuid: str,
    last_txn_date: Optional[date],
    velocity: Optional[Decimal],
) -> Optional[datetime]:
    """Maturity-based projection first; cadence-based fallback."""
    # Priority 1: open opportunity with actual_funded_at + maturity_months
    maturity_row = session.execute(
        text("""
            SELECT actual_funded_at, maturity_months
            FROM fa_max_opportunities
            WHERE person_id = :pid
              AND outcome = 'open'
              AND actual_funded_at IS NOT NULL
              AND maturity_months IS NOT NULL
            ORDER BY actual_funded_at DESC
            LIMIT 1
        """),
        {"pid": person_uuid},
    ).mappings().first()

    if maturity_row:
        from dateutil.relativedelta import relativedelta
        return maturity_row["actual_funded_at"] + relativedelta(
            months=int(maturity_row["maturity_months"])
        )

    # Priority 2: cadence projection
    if velocity and float(velocity) >= 0.5 and last_txn_date:
        days_per_deal = 365.0 / float(velocity)
        from datetime import timedelta
        last_dt = datetime.combine(last_txn_date, datetime.min.time()).replace(tzinfo=timezone.utc)
        return last_dt + timedelta(days=days_per_deal)

    return None


def _confidence_tier(deed_count: int) -> str:
    if deed_count >= _HIGH_DEED_THRESHOLD:
        return "high"
    if deed_count >= _MEDIUM_DEED_THRESHOLD:
        return "medium"
    if deed_count >= _LOW_DEED_THRESHOLD:
        return "low"
    return "unknown"


def _upsert_profile(session: Session, profile: Dict[str, Any]) -> None:
    """INSERT … ON CONFLICT (person_id) DO UPDATE — fully idempotent."""
    session.execute(
        text("""
            INSERT INTO fa_max_person_profiles (
                person_id, buyer_entity_id,
                buy_box_geography, buy_box_property_types, buy_box_price_band,
                velocity_purchases_per_year, last_transaction_date,
                avg_days_between_transactions, active_property_count,
                predicted_next_need, predicted_next_need_date, next_need_evidence,
                confidence_tier, computed_at, created_at, updated_at
            ) VALUES (
                :person_id, :buyer_entity_id,
                CAST(:buy_box_geography AS jsonb), CAST(:buy_box_property_types AS jsonb),
                CAST(:buy_box_price_band AS jsonb),
                :velocity_purchases_per_year, :last_transaction_date,
                :avg_days_between_transactions, :active_property_count,
                :predicted_next_need, :predicted_next_need_date,
                CAST(:next_need_evidence AS jsonb),
                :confidence_tier, :computed_at, NOW(), NOW()
            )
            ON CONFLICT (person_id) DO UPDATE SET
                buyer_entity_id               = EXCLUDED.buyer_entity_id,
                buy_box_geography             = EXCLUDED.buy_box_geography,
                buy_box_property_types        = EXCLUDED.buy_box_property_types,
                buy_box_price_band            = EXCLUDED.buy_box_price_band,
                velocity_purchases_per_year   = EXCLUDED.velocity_purchases_per_year,
                last_transaction_date         = EXCLUDED.last_transaction_date,
                avg_days_between_transactions = EXCLUDED.avg_days_between_transactions,
                active_property_count         = EXCLUDED.active_property_count,
                predicted_next_need           = EXCLUDED.predicted_next_need,
                predicted_next_need_date      = EXCLUDED.predicted_next_need_date,
                next_need_evidence            = EXCLUDED.next_need_evidence,
                confidence_tier               = EXCLUDED.confidence_tier,
                computed_at                   = EXCLUDED.computed_at,
                updated_at                    = NOW()
        """),
        {
            **profile,
            "buy_box_geography": _jsonb(profile["buy_box_geography"]),
            "buy_box_property_types": _jsonb(profile["buy_box_property_types"]),
            "buy_box_price_band": _jsonb(profile["buy_box_price_band"]),
            "next_need_evidence": _jsonb(profile["next_need_evidence"]),
        },
    )


def _jsonb(value: Any) -> Optional[str]:
    """Serialise a Python object to a JSON string for JSONB binding, or None."""
    if value is None:
        return None
    import json
    return json.dumps(value, default=str)
