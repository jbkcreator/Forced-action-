"""
Attribution Service — Stage 8: Outcome Attribution + Revenue Signal Scoring.

`record_conversion_attribution()` is the single write path for every
billable conversion event.  It:

  1. Resolves all 8 attribution dimensions (lead, ZIP, trade, wallet tier,
     lock status, AutoPilot tier, bundle, deal size).
  2. Inserts one row into `conversion_attribution_events` inside a savepoint.
     On UniqueViolation (same source_table + source_event_id) → returns None.
  3. Reads the subscriber's current revenue signal score.
  4. Computes delta via `_compute_score_delta()`.
  5. Inserts one row into `revenue_signal_score_events` with attribution
     context in its metadata column.
  6. Updates `subscribers.revenue_signal_score/band/breakdown/updated_at`.

Steps 2–6 share the caller's transaction — no independent commit.
Attribution failure never breaks the surrounding billing/SMS flow: callers
wrap this in try/except.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

from psycopg2.errors import UniqueViolation
from sqlalchemy import text as sa_text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

# ── Canonical conversion types ────────────────────────────────────────────────

CONVERSION_TYPES: frozenset[str] = frozenset({
    "paid_unlock",
    "saved_card",
    "wallet_activation",
    "wallet_topup",
    "bundle_purchase",
    "territory_lock_purchase",
    "autopilot_lite_upgrade",
    "autopilot_pro_upgrade",
    "annual_upgrade",
    "data_only_save",
    "deal_win_reported",
    "failed_payment_recovered",
})

# ── Scoring weights ───────────────────────────────────────────────────────────
# Positive deltas from conversion events; negatives applied by callers.

SCORE_WEIGHTS: dict[str, int] = {
    "paid_unlock":               10,
    "saved_card":                15,
    "wallet_activation":         20,
    "wallet_topup":              10,
    "bundle_purchase":           10,
    "territory_lock_purchase":   30,
    "autopilot_lite_upgrade":    35,
    "autopilot_pro_upgrade":     40,
    "annual_upgrade":            30,
    "data_only_save":            15,
    "deal_win_reported":         25,
    "failed_payment_recovered":   5,
    # bonus applied on top of deal_win when deal_size_bucket in ('10_25k','25k_plus')
    "_deal_size_10k_plus_bonus": 20,
}

_DEAL_SIZE_BONUS_BUCKETS = frozenset({"10_25k", "25k_plus"})

_BAND_THRESHOLDS = [
    (81, 100, "very_high"),
    (61, 80,  "high"),
    (31, 60,  "medium"),
    (0,  30,  "low"),
]


def _score_to_band(score: int) -> str:
    for lo, hi, label in _BAND_THRESHOLDS:
        if lo <= score <= hi:
            return label
    return "low"


def _compute_score_delta(
    conversion_type: str,
    old_score: int,
    deal_size_bucket: Optional[str],
) -> tuple[int, int, str, dict]:
    """Return (delta, new_score, band, breakdown)."""
    base_delta = SCORE_WEIGHTS.get(conversion_type, 0)
    final_delta = base_delta

    reasons: list[str] = [conversion_type]
    if conversion_type == "deal_win_reported" and deal_size_bucket in _DEAL_SIZE_BONUS_BUCKETS:
        bonus = SCORE_WEIGHTS["_deal_size_10k_plus_bonus"]
        final_delta += bonus
        reasons.append(f"deal_size_bonus_{deal_size_bucket}")

    new_score = max(0, min(100, old_score + final_delta))
    band = _score_to_band(new_score)

    breakdown = {
        "conversion_type": conversion_type,
        "base_delta": base_delta,
        "final_delta": final_delta,
        "previous_score": old_score,
        "new_score": new_score,
        "reasons": reasons,
    }
    return final_delta, new_score, band, breakdown


# ── 8-Dimension resolution helpers ───────────────────────────────────────────

def _resolve_lead_id(
    db: Session,
    subscriber_id: int,
    occurred_at: datetime,
    lead_id: Optional[int],
) -> Optional[int]:
    if lead_id is not None:
        return lead_id
    row = db.execute(sa_text("""
        SELECT id FROM sent_leads
        WHERE subscriber_id = :sub_id
          AND sent_at <= :occurred_at
        ORDER BY sent_at DESC
        LIMIT 1
    """), {"sub_id": subscriber_id, "occurred_at": occurred_at}).mappings().first()
    return row["id"] if row else None


def _resolve_zip_code(
    db: Session,
    subscriber_id: int,
    property_id: Optional[int],
    zip_code: Optional[str],
) -> Optional[str]:
    if zip_code:
        return zip_code
    if property_id:
        row = db.execute(sa_text("""
            SELECT zip FROM properties WHERE id = :pid LIMIT 1
        """), {"pid": property_id}).mappings().first()
        if row and row["zip"]:
            return row["zip"]
    # fall back to locked territory
    row = db.execute(sa_text("""
        SELECT zip_code FROM zip_territories
        WHERE subscriber_id = :sub_id AND status = 'locked'
        ORDER BY locked_at DESC
        LIMIT 1
    """), {"sub_id": subscriber_id}).mappings().first()
    return row["zip_code"] if row else None


def _resolve_wallet_tier(
    db: Session,
    subscriber_id: int,
    wallet_tier: Optional[str],
) -> str:
    if wallet_tier:
        return wallet_tier
    row = db.execute(sa_text("""
        SELECT wallet_tier FROM wallet_balances
        WHERE subscriber_id = :sub_id
        LIMIT 1
    """), {"sub_id": subscriber_id}).mappings().first()
    return row["wallet_tier"] if row and row["wallet_tier"] else "unknown"


def _resolve_lock_info(
    db: Session,
    subscriber_id: int,
    lock_status: Optional[str],
    lock_zip: Optional[str],
) -> tuple[str, Optional[str]]:
    if lock_status and lock_zip:
        return lock_status, lock_zip
    row = db.execute(sa_text("""
        SELECT zip_code FROM zip_territories
        WHERE subscriber_id = :sub_id AND status = 'locked'
        ORDER BY locked_at DESC
        LIMIT 1
    """), {"sub_id": subscriber_id}).mappings().first()
    if row:
        return "locked", row["zip_code"]
    return lock_status or "unlocked", lock_zip


def _resolve_autopilot_tier(db: Session, subscriber_id: int) -> str:
    row = db.execute(sa_text("""
        SELECT tier FROM subscribers WHERE id = :sub_id LIMIT 1
    """), {"sub_id": subscriber_id}).mappings().first()
    if row and row["tier"] in ("autopilot_lite", "autopilot_pro"):
        return row["tier"]
    return "not_applicable"


def _resolve_bundle(
    db: Session,
    subscriber_id: int,
    occurred_at: datetime,
    bundle_id: Optional[int],
    bundle_type: Optional[str],
) -> tuple[Optional[int], str]:
    if bundle_id is not None:
        return bundle_id, bundle_type or "unknown"
    row = db.execute(sa_text("""
        SELECT id, bundle_type FROM bundle_purchases
        WHERE subscriber_id = :sub_id
          AND status = 'completed'
          AND purchased_at <= :occurred_at
        ORDER BY purchased_at DESC
        LIMIT 1
    """), {"sub_id": subscriber_id, "occurred_at": occurred_at}).mappings().first()
    if row:
        return row["id"], row["bundle_type"] or "unknown"
    return None, "not_applicable"


def _resolve_trade(db: Session, subscriber_id: int) -> Optional[str]:
    row = db.execute(sa_text("""
        SELECT vertical FROM subscribers WHERE id = :sub_id LIMIT 1
    """), {"sub_id": subscriber_id}).mappings().first()
    return row["vertical"] if row else None


def _attribution_status(resolved_dims: dict) -> tuple[str, str]:
    """Return (attribution_status, attribution_confidence)."""
    primary = [
        resolved_dims.get("zip_code"),
        resolved_dims.get("trade"),
        resolved_dims.get("wallet_tier"),
        resolved_dims.get("lock_status"),
        resolved_dims.get("autopilot_tier"),
    ]
    resolved_count = sum(
        1 for v in primary
        if v and v not in ("unknown", None)
    )
    if resolved_count == 5:
        status, confidence = "complete", "high"
    elif resolved_count >= 3:
        status, confidence = "partial", "medium"
    else:
        status, confidence = "unresolved", "low"
    return status, confidence


# ── Public API ────────────────────────────────────────────────────────────────

def record_conversion_attribution(
    *,
    conversion_type: str,
    source_table: str,
    source_event_id: str,
    subscriber_id: int,
    occurred_at: datetime,
    revenue_amount: Optional[float] = None,
    currency: str = "usd",
    lead_id: Optional[int] = None,
    property_id: Optional[int] = None,
    zip_code: Optional[str] = None,
    wallet_tier: Optional[str] = None,
    lock_status: Optional[str] = None,
    lock_zip: Optional[str] = None,
    autopilot_tier: Optional[str] = None,
    bundle_id: Optional[int] = None,
    bundle_type: Optional[str] = None,
    deal_size_bucket: Optional[str] = None,
    metadata: Optional[dict] = None,
    db: Session,
) -> Optional[int]:
    """Record one conversion attribution event and update the subscriber score.

    Returns the new `conversion_attribution_events.id`, or None when a row
    with the same (source_table, source_event_id) already exists.

    Shares the caller's transaction — never commits independently.
    """
    if conversion_type not in CONVERSION_TYPES:
        raise ValueError(f"Unknown conversion_type: {conversion_type!r}")

    # ── Resolve all 8 attribution dimensions ─────────────────────────────
    resolved_lead_id = _resolve_lead_id(db, subscriber_id, occurred_at, lead_id)
    resolved_zip = _resolve_zip_code(db, subscriber_id, property_id, zip_code)
    resolved_wallet_tier = _resolve_wallet_tier(db, subscriber_id, wallet_tier)
    resolved_lock_status, resolved_lock_zip = _resolve_lock_info(
        db, subscriber_id, lock_status, lock_zip
    )
    resolved_autopilot_tier = autopilot_tier or _resolve_autopilot_tier(db, subscriber_id)
    resolved_bundle_id, resolved_bundle_type = _resolve_bundle(
        db, subscriber_id, occurred_at, bundle_id, bundle_type
    )
    resolved_trade = _resolve_trade(db, subscriber_id)
    resolved_deal_size = deal_size_bucket or "not_applicable"

    dims = {
        "zip_code": resolved_zip,
        "trade": resolved_trade,
        "wallet_tier": resolved_wallet_tier,
        "lock_status": resolved_lock_status,
        "autopilot_tier": resolved_autopilot_tier,
    }
    attr_status, attr_confidence = _attribution_status(dims)

    # Build attribution_metadata — record missing dimensions + caller extras.
    missing_dims = [k for k, v in dims.items() if not v or v in ("unknown", "not_applicable")]
    attr_meta: dict = {
        "missing_dimensions": missing_dims,
        "resolution_notes": f"{len(missing_dims)} dimension(s) unresolved at record time",
    }
    if metadata:
        attr_meta.update(metadata)

    # ── INSERT attribution row inside a savepoint (duplicate-safe) ────────
    attribution_event_id: Optional[int] = None
    try:
        with db.begin_nested():
            row = db.execute(sa_text("""
                INSERT INTO conversion_attribution_events (
                    conversion_type, source_table, source_event_id,
                    subscriber_id, lead_id, property_id,
                    zip_code, trade, wallet_tier,
                    lock_status, lock_zip, autopilot_tier,
                    bundle_id, bundle_type, deal_size_bucket,
                    revenue_amount, currency,
                    occurred_at, attribution_status, attribution_confidence,
                    attribution_metadata,
                    created_at, updated_at
                ) VALUES (
                    :conversion_type, :source_table, :source_event_id,
                    :subscriber_id, :lead_id, :property_id,
                    :zip_code, :trade, :wallet_tier,
                    :lock_status, :lock_zip, :autopilot_tier,
                    :bundle_id, :bundle_type, :deal_size_bucket,
                    :revenue_amount, :currency,
                    :occurred_at, :attribution_status, :attribution_confidence,
                    CAST(:attribution_metadata AS jsonb),
                    NOW(), NOW()
                )
                RETURNING id
            """), {
                "conversion_type": conversion_type,
                "source_table": source_table,
                "source_event_id": source_event_id,
                "subscriber_id": subscriber_id,
                "lead_id": resolved_lead_id,
                "property_id": property_id,
                "zip_code": resolved_zip,
                "trade": resolved_trade,
                "wallet_tier": resolved_wallet_tier,
                "lock_status": resolved_lock_status,
                "lock_zip": resolved_lock_zip,
                "autopilot_tier": resolved_autopilot_tier,
                "bundle_id": resolved_bundle_id,
                "bundle_type": resolved_bundle_type,
                "deal_size_bucket": resolved_deal_size,
                "revenue_amount": revenue_amount,
                "currency": currency,
                "occurred_at": occurred_at,
                "attribution_status": attr_status,
                "attribution_confidence": attr_confidence,
                "attribution_metadata": _to_jsonb(attr_meta),
            }).mappings().first()
            attribution_event_id = row["id"]
    except IntegrityError as exc:
        if isinstance(exc.orig, UniqueViolation):
            logger.debug(
                "Attribution duplicate skipped source_table=%s source_event_id=%s",
                source_table, source_event_id,
            )
            return None
        raise

    # ── Read current score ────────────────────────────────────────────────
    score_row = db.execute(sa_text("""
        SELECT revenue_signal_score FROM subscribers WHERE id = :sub_id FOR UPDATE
    """), {"sub_id": subscriber_id}).mappings().first()
    old_score: int = score_row["revenue_signal_score"] if score_row else 0

    # ── Compute delta ─────────────────────────────────────────────────────
    delta, new_score, band, breakdown = _compute_score_delta(
        conversion_type, old_score, resolved_deal_size
    )

    # ── INSERT score event ────────────────────────────────────────────────
    score_meta = {
        "attribution_event_id": attribution_event_id,
        "source_table": source_table,
        "source_event_id": source_event_id,
        "zip_code": resolved_zip,
        "trade": resolved_trade,
        "wallet_tier": resolved_wallet_tier,
        "lock_status": resolved_lock_status,
        "autopilot_tier": resolved_autopilot_tier,
        "bundle_type": resolved_bundle_type,
        "deal_size_bucket": resolved_deal_size,
    }
    db.execute(sa_text("""
        INSERT INTO revenue_signal_score_events (
            subscriber_id, action_type, old_score, new_score,
            delta, band, breakdown, metadata, created_at
        ) VALUES (
            :sub_id, :action_type, :old_score, :new_score,
            :delta, :band, CAST(:breakdown AS jsonb), CAST(:metadata AS jsonb), NOW()
        )
    """), {
        "sub_id": subscriber_id,
        "action_type": conversion_type,
        "old_score": old_score,
        "new_score": new_score,
        "delta": delta,
        "band": band,
        "breakdown": _to_jsonb(breakdown),
        "metadata": _to_jsonb(score_meta),
    })

    # ── UPDATE subscriber latest score ─────────────────────────────────────
    db.execute(sa_text("""
        UPDATE subscribers
        SET revenue_signal_score     = :new_score,
            revenue_signal_band      = :band,
            revenue_signal_breakdown = CAST(:breakdown AS jsonb),
            revenue_signal_updated_at = NOW()
        WHERE id = :sub_id
    """), {
        "sub_id": subscriber_id,
        "new_score": new_score,
        "band": band,
        "breakdown": _to_jsonb(breakdown),
    })

    logger.debug(
        "Attribution recorded sub=%s type=%s score %d→%d band=%s event_id=%d",
        subscriber_id, conversion_type, old_score, new_score, band, attribution_event_id,
    )

    # Mark the subscriber's active cora_attribution_v1 assignment as converted
    # so the rollout monitor can compute per-arm conversion rates.
    try:
        from src.services.ab_engine import (
            ATTRIBUTION_ROLLOUT_TEST_NAME,
            record_outcome,
        )
        record_outcome(subscriber_id, ATTRIBUTION_ROLLOUT_TEST_NAME, "converted", db)
    except Exception:
        logger.debug("rollout outcome label failed sub=%s — non-fatal", subscriber_id, exc_info=True)

    return attribution_event_id


# ── Internal helper ───────────────────────────────────────────────────────────

def _to_jsonb(value: dict) -> str:
    """Serialize dict → JSON string for ::jsonb cast parameters."""
    import json
    return json.dumps(value)
