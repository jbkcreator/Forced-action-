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
from uuid import UUID, uuid4

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
                buy_box_preferences,
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
        "buy_box_preferences": None,
        "velocity_purchases_per_year": None,
        "last_transaction_date": None,
        "avg_days_between_transactions": None,
        "active_property_count": 0,
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

    # 2. Velocity from deed date history (fix: purchase-to-purchase intervals,
    #    not avg_hold_days which measures acquisition-to-exit hold duration)
    cadence = _fetch_cadence(session, entity_id)
    _raw_velocity = cadence.get("cadence_purchases_per_year")
    velocity = Decimal(str(_raw_velocity)) if _raw_velocity is not None else None
    avg_days = _compute_avg_days_between_transactions(deeds)
    portfolio_evidence = cadence.get("portfolio_evidence") or {}
    still_held = portfolio_evidence.get("still_held_count", 0)
    active_property_count = still_held if still_held is not None else 0

    last_txn_date = _last_transaction_date(deeds)

    # 3. Buy-box condition/preferences from property attributes ---------------
    buy_box_preferences = _compute_preferences(deeds)

    # 4. Predicted next need from FinancingIntentScore -----------------------
    intent_rows = _fetch_financing_intent(session, entity_id)
    predicted_need, next_need_evidence = _rollup_next_need(intent_rows)

    # 5. Predicted next-need date with stored evidence -----------------------
    predicted_date, date_evidence = _predict_next_need_date(
        session=session,
        person_uuid=person_uuid,
        last_txn_date=last_txn_date,
        velocity=velocity,
    )

    # Merge date prediction basis into next_need_evidence
    if date_evidence:
        combined_evidence = {
            "financing_intent": next_need_evidence or [],
            "date_prediction": date_evidence,
        }
    else:
        combined_evidence = {"financing_intent": next_need_evidence or []} if next_need_evidence else None

    # 6. Confidence tier ----------------------------------------------------
    confidence_tier = _confidence_tier(deed_count)

    return {
        "person_id": person_uuid,
        "buyer_entity_id": entity_id,
        "buy_box_geography": buy_box_geography,
        "buy_box_property_types": buy_box_property_types,
        "buy_box_price_band": buy_box_price_band,
        "buy_box_preferences": buy_box_preferences,
        "velocity_purchases_per_year": velocity,
        "last_transaction_date": last_txn_date,
        "avg_days_between_transactions": avg_days,
        "active_property_count": active_property_count,
        "predicted_next_need": predicted_need,
        "predicted_next_need_date": predicted_date,
        "next_need_evidence": combined_evidence,
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
                p.property_use_code,
                p.year_built,
                p.beds,
                p.baths,
                p.lot_size,
                p.building_condition
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


def _compute_avg_days_between_transactions(deeds: List[Dict]) -> Optional[Decimal]:
    """Average gap in days between consecutive purchase dates (NOT hold time)."""
    dates = sorted(
        d["record_date"] for d in deeds if d.get("record_date") is not None
    )
    if len(dates) < 2:
        return None
    gaps = [(dates[i + 1] - dates[i]).days for i in range(len(dates) - 1)]
    return Decimal(str(round(sum(gaps) / len(gaps), 2)))


def _compute_preferences(deeds: List[Dict]) -> Optional[Dict[str, Any]]:
    """Aggregate property condition/size preferences from deed-joined property fields."""
    conditions: List[str] = []
    years: List[int] = []
    beds_list: List[float] = []
    baths_list: List[float] = []
    lot_sizes: List[float] = []

    for d in deeds:
        if d.get("building_condition"):
            conditions.append(d["building_condition"])
        if d.get("year_built") is not None:
            years.append(int(d["year_built"]))
        if d.get("beds") is not None:
            beds_list.append(float(d["beds"]))
        if d.get("baths") is not None:
            baths_list.append(float(d["baths"]))
        if d.get("lot_size") is not None:
            lot_sizes.append(float(d["lot_size"]))

    if not any([conditions, years, beds_list, baths_list, lot_sizes]):
        return None

    result: Dict[str, Any] = {}
    if conditions:
        from collections import Counter
        counts = Counter(conditions)
        result["condition_distribution"] = dict(counts.most_common())
        result["most_common_condition"] = counts.most_common(1)[0][0]
    if years:
        result["year_built_min"] = min(years)
        result["year_built_max"] = max(years)
        result["year_built_avg"] = round(sum(years) / len(years))
    if beds_list:
        result["beds_avg"] = round(sum(beds_list) / len(beds_list), 1)
    if baths_list:
        result["baths_avg"] = round(sum(baths_list) / len(baths_list), 1)
    if lot_sizes:
        result["lot_size_avg_sqft"] = round(sum(lot_sizes) / len(lot_sizes))
    result["sample_count"] = len(deeds)
    return result


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
) -> tuple[Optional[datetime], Optional[Dict[str, Any]]]:
    """Maturity-based projection first; cadence-based fallback.

    Returns (predicted_date, evidence_dict) where evidence_dict records the
    basis used so callers can store it alongside next_need_evidence.
    """
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
        predicted = maturity_row["actual_funded_at"] + relativedelta(
            months=int(maturity_row["maturity_months"])
        )
        evidence = {
            "basis": "maturity",
            "opportunity_funded_at": maturity_row["actual_funded_at"].isoformat()
            if hasattr(maturity_row["actual_funded_at"], "isoformat")
            else str(maturity_row["actual_funded_at"]),
            "maturity_months": int(maturity_row["maturity_months"]),
        }
        return predicted, evidence

    # Priority 2: cadence projection
    if velocity and float(velocity) >= 0.5 and last_txn_date:
        days_per_deal = 365.0 / float(velocity)
        from datetime import timedelta
        last_dt = datetime.combine(last_txn_date, datetime.min.time()).replace(tzinfo=timezone.utc)
        predicted = last_dt + timedelta(days=days_per_deal)
        evidence = {
            "basis": "cadence",
            "last_transaction_date": last_txn_date.isoformat()
            if hasattr(last_txn_date, "isoformat")
            else str(last_txn_date),
            "velocity_purchases_per_year": str(velocity),
            "days_projected": round(days_per_deal),
        }
        return predicted, evidence

    return None, None


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
                buy_box_preferences,
                velocity_purchases_per_year, last_transaction_date,
                avg_days_between_transactions, active_property_count,
                predicted_next_need, predicted_next_need_date, next_need_evidence,
                confidence_tier, computed_at, created_at, updated_at
            ) VALUES (
                :person_id, :buyer_entity_id,
                CAST(:buy_box_geography AS jsonb), CAST(:buy_box_property_types AS jsonb),
                CAST(:buy_box_price_band AS jsonb), CAST(:buy_box_preferences AS jsonb),
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
                buy_box_preferences           = EXCLUDED.buy_box_preferences,
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
            "buy_box_preferences": _jsonb(profile.get("buy_box_preferences")),
            "next_need_evidence": _jsonb(profile["next_need_evidence"]),
        },
    )


def _jsonb(value: Any) -> Optional[str]:
    """Serialise a Python object to a JSON string for JSONB binding, or None."""
    if value is None:
        return None
    import json
    return json.dumps(value, default=str)


def schedule_profile_recompute(session: Session, person_id: str, reason: str) -> None:
    """Enqueue a profile recomputation for person_id via fa_max_work_queue.

    Three cases handled atomically:

    1. No existing item → insert (available).
    2. Existing item is done/failed → reactivate it (available) so the new
       event triggers a fresh recompute; prevents permanent deduplication where
       the first drain blocks all future events for the same person.
    3. Existing item is available → leave it; pending work covers this event.
    4. Existing item is claimed (worker mid-compute) → the worker will complete
       before seeing this event.  Insert a NEW item with an event-specific key
       so the event is never silently lost.
    """
    idempotency_key = f"profile_recompute:{person_id}"
    payload_json = _jsonb({"reason": reason})
    params = {
        "ikey": idempotency_key,
        "person_id": person_id,
        "payload": payload_json,
    }

    # Statement 1: insert or reactivate a done/failed item.
    # Returns a row when an insert or a reactivation actually happened.
    row = session.execute(
        text("""
            INSERT INTO fa_max_work_queue (
                queue_name, idempotency_key, person_id, payload, status,
                created_at, updated_at
            ) VALUES (
                'profile_recompute', :ikey, :person_id ::uuid,
                CAST(:payload AS jsonb), 'available', NOW(), NOW()
            )
            ON CONFLICT (idempotency_key)
            WHERE idempotency_key IS NOT NULL
            DO UPDATE
               SET status     = 'available',
                   payload    = CAST(:payload AS jsonb),
                   done_at    = NULL,
                   updated_at = NOW()
             WHERE fa_max_work_queue.status IN ('done', 'failed')
            RETURNING work_item_id
        """),
        params,
    ).fetchone()

    if row is not None:
        return  # inserted or reactivated — done

    # Statement 2: the conflict row is available (fine) or claimed (must not
    # drop the event).  Insert a new item only when the conflict row is
    # currently claimed — a unique per-event key avoids a second conflict.
    session.execute(
        text("""
            INSERT INTO fa_max_work_queue (
                queue_name, idempotency_key, person_id, payload, status,
                created_at, updated_at
            )
            SELECT 'profile_recompute',
                   :fallback_key,
                   :person_id ::uuid,
                   CAST(:payload AS jsonb),
                   'available',
                   NOW(), NOW()
            WHERE EXISTS (
                SELECT 1 FROM fa_max_work_queue
                WHERE idempotency_key = :ikey
                  AND status = 'claimed'
            )
        """),
        {
            "fallback_key": f"{idempotency_key}:retry:{uuid4()}",
            "person_id": person_id,
            "payload": payload_json,
            "ikey": idempotency_key,
        },
    )


def schedule_profile_recompute_for_property(
    session: Session, property_id: int, reason: str
) -> None:
    """Enqueue profile recomputes for all fa_max_persons linked to property_id.

    Resolves persons via the deed-link path:
      buyer_entity_links(source_table='deeds') → deeds.property_id → fa_max_persons

    No-op when no persons are linked (pre-WP-3/WP-4 state). Each enqueue is
    idempotent via the partial unique index on idempotency_key.
    """
    rows = session.execute(
        text("""
            SELECT DISTINCT p.person_id
            FROM fa_max_persons p
            JOIN buyer_entity_links bel
              ON bel.buyer_entity_id = p.buyer_entity_id
             AND bel.source_table = 'deeds'
            JOIN deeds d ON d.id = bel.source_id
            WHERE d.property_id = :prop_id
              AND p.buyer_entity_id IS NOT NULL
              AND p.merged_into_id IS NULL
        """),
        {"prop_id": property_id},
    ).mappings().all()
    for row in rows:
        schedule_profile_recompute(session, str(row["person_id"]), reason)
