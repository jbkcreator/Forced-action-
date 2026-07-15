"""
Stage 10 — Trade + County Pricing Cohort Engine (fa055).

Enables per-trade-vertical and per-county pricing adjustments that activate
only after 6+ weeks of deal data. Every adjustment is constrained within
guardrail bounds (config/cora_guardrails.py) and logged to pricing_cohorts.

Activation gate:
  - >= 6 distinct weeks with deal_outcomes rows for (county_id, trade_vertical)
  - >= 10 total deals (statistical floor)
  - Adjustment within ±25% of base price (guardrail: bundle_pricing variance_pct)
  - Final price within guardrail hard bounds for the price_type

Rollback trigger:
  - Conversion rate drops > 2σ below the pre-cohort baseline
  - Price falls outside guardrail hard bounds (never happens if engine is correct)
  - Explicit operator rollback via rollback_cohort()

All DB I/O uses raw SQL via sa_text (repo convention).

Usage:
    from src.services.pricing_cohort_engine import (
        get_price_for_subscriber,
        evaluate_and_activate,
        rollback_cohort,
        check_cohort_rollback_trigger,
    )
"""

from __future__ import annotations

import logging
import math
from typing import Any, Optional

from sqlalchemy import text as sa_text
from sqlalchemy.orm import Session

from config.cora_guardrails import GUARDRAILS, get_guardrail
from config.stage10_config import PRICING_COHORT

logger = logging.getLogger(__name__)


# ── guardrail helpers ─────────────────────────────────────────────────────────

# Hard bounds per price_type (cents). Derived from cora_guardrails.py.
_PRICE_BOUNDS: dict[str, tuple[int, int]] = {
    "lock":          (14700, 24700),   # $147–$247/mo
    "wallet_starter": (3900, 9900),    # $39–$99/mo
    "wallet_growth":  (9900, 19900),   # $99–$199/mo
    "wallet_power":   (19900, 24900),  # $199–$249/mo
    "bundle":        (1500, 9900),     # $15–$99 (conservative range)
    # Block 1 storefront subscription tiers (src/api/deps.py VALID_TIERS).
    # Bounds mirror the founding→regular price range per tier (stripe_service.py).
    "starter":       (60000, 110000),  # $600–$1100/mo
    "pro":           (110000, 190000), # $1100–$1900/mo
    "dominator":     (200000, 350000), # $2000–$3500/mo
    "annual_lock":   (197000, 197000), # flat $1970/yr, no founding/regular split
}


def _clamp_to_guardrail(price_type: str, price_cents: int) -> int:
    bounds = _PRICE_BOUNDS.get(price_type)
    if not bounds:
        return price_cents
    lo, hi = bounds
    return max(lo, min(hi, price_cents))


def _within_adjustment_bounds(base: int, adjusted: int) -> bool:
    max_pct = PRICING_COHORT["max_price_adjustment_pct"]
    if base <= 0:
        return False
    pct = abs((adjusted - base) / base) * 100
    return pct <= max_pct


# ── deal data gate ────────────────────────────────────────────────────────────

def _count_deal_weeks(county_id: str, trade_vertical: str, db: Session) -> tuple[int, int]:
    """Return (distinct_weeks, total_deals) for county+vertical from deal_outcomes."""
    row = db.execute(sa_text("""
        SELECT
            COUNT(DISTINCT DATE_TRUNC('week', COALESCE(deal_date, created_at::date)))
                AS deal_weeks,
            COUNT(*) AS total_deals
        FROM deal_outcomes
        WHERE county_id = :county
          AND trade_vertical = :vertical
          AND pipeline_stage = 'closed_won'
    """), {"county": county_id, "vertical": trade_vertical}).first()

    if not row:
        return 0, 0
    return int(row.deal_weeks or 0), int(row.total_deals or 0)


def check_activation_gates(
    county_id: str,
    trade_vertical: str,
    db: Session,
) -> dict:
    """Return gate status: ready/not_ready with reasons."""
    min_weeks = PRICING_COHORT["activation_min_weeks"]
    min_deals = PRICING_COHORT["min_deal_count"]

    weeks, total = _count_deal_weeks(county_id, trade_vertical, db)
    gates = {
        "deal_weeks_met": weeks >= min_weeks,
        "deal_count_met": total >= min_deals,
        "weeks": weeks,
        "deals": total,
    }
    gates["ready"] = all([gates["deal_weeks_met"], gates["deal_count_met"]])
    return gates


# ── active cohort lookup ──────────────────────────────────────────────────────

def _get_active_cohort(
    county_id: str,
    trade_vertical: str,
    price_type: str,
    db: Session,
) -> Optional[Any]:
    return db.execute(sa_text("""
        SELECT * FROM pricing_cohorts
        WHERE county_id = :county
          AND trade_vertical = :vertical
          AND price_type = :ptype
          AND status = 'active'
        LIMIT 1
    """), {"county": county_id, "vertical": trade_vertical, "ptype": price_type}).first()


def get_price_for_subscriber(
    county_id: str,
    trade_vertical: str,
    price_type: str,
    base_price_cents: int,
    db: Session,
) -> tuple[int, str]:
    """Return (final_price_cents, source) for a subscriber.

    source: 'cohort_adjusted' | 'base_price'
    Applies the active cohort override when available; falls through to base price.
    """
    if (
        price_type not in PRICING_COHORT["allowed_price_types"]
        or trade_vertical not in PRICING_COHORT["allowed_trade_verticals"]
    ):
        return base_price_cents, "base_price"

    cohort = _get_active_cohort(county_id, trade_vertical, price_type, db)
    if not cohort:
        return base_price_cents, "base_price"

    adjusted = int(cohort.adjusted_price_cents)
    # Defensive re-clamp in case guardrails changed after activation.
    adjusted = _clamp_to_guardrail(price_type, adjusted)
    return adjusted, "cohort_adjusted"


# ── activation ────────────────────────────────────────────────────────────────

def evaluate_and_activate(
    county_id: str,
    trade_vertical: str,
    price_type: str,
    base_price_cents: int,
    adjustment_pct: float,
    db: Session,
    *,
    activation_reason: Optional[str] = None,
) -> dict:
    """Check activation gates; if met, create or update the pricing cohort.

    adjustment_pct: signed percent (e.g. +10 or -5). Clamped to ±25%.
    Returns a dict with 'status' in {'activated', 'already_active', 'gates_not_met',
    'guardrail_violation'}.
    Idempotent: calling again with the same params on an already-active cohort
    updates the adjusted price.
    """
    if price_type not in PRICING_COHORT["allowed_price_types"]:
        return {"status": "invalid_price_type", "price_type": price_type}
    if trade_vertical not in PRICING_COHORT["allowed_trade_verticals"]:
        return {"status": "invalid_trade_vertical", "trade_vertical": trade_vertical}

    gates = check_activation_gates(county_id, trade_vertical, db)
    if not gates["ready"]:
        return {"status": "gates_not_met", "gates": gates}

    # Compute adjusted price, clamped to guardrail.
    raw_adjusted = int(base_price_cents * (1 + adjustment_pct / 100))
    adjusted = _clamp_to_guardrail(price_type, raw_adjusted)
    actual_pct = round((adjusted - base_price_cents) / base_price_cents * 100, 2)

    if not _within_adjustment_bounds(base_price_cents, adjusted):
        return {
            "status": "guardrail_violation",
            "base": base_price_cents,
            "requested_adjusted": raw_adjusted,
            "max_pct": PRICING_COHORT["max_price_adjustment_pct"],
        }

    existing = _get_active_cohort(county_id, trade_vertical, price_type, db)
    if existing:
        if existing.adjusted_price_cents == adjusted:
            return {"status": "already_active", "cohort_id": existing.id}
        # Update price in place.
        db.execute(sa_text("""
            UPDATE pricing_cohorts
            SET adjusted_price_cents = :adj,
                adjustment_pct = :pct,
                activation_reason = :reason,
                deal_weeks = :weeks,
                deal_count = :deals,
                updated_at = NOW()
            WHERE id = :id
        """), {
            "adj": adjusted,
            "pct": actual_pct,
            "reason": activation_reason or "re-evaluated",
            "weeks": gates["weeks"],
            "deals": gates["deals"],
            "id": existing.id,
        })
        return {"status": "updated", "cohort_id": existing.id, "adjusted_cents": adjusted}

    result = db.execute(sa_text("""
        INSERT INTO pricing_cohorts
            (county_id, trade_vertical, price_type,
             base_price_cents, adjusted_price_cents, adjustment_pct,
             status, activation_reason, deal_weeks, deal_count,
             activated_at, created_at, updated_at)
        VALUES
            (:county, :vertical, :ptype,
             :base, :adj, :pct,
             'active', :reason, :weeks, :deals,
             NOW(), NOW(), NOW())
        RETURNING id
    """), {
        "county": county_id,
        "vertical": trade_vertical,
        "ptype": price_type,
        "base": base_price_cents,
        "adj": adjusted,
        "pct": actual_pct,
        "reason": activation_reason or "auto-activated after gate check",
        "weeks": gates["weeks"],
        "deals": gates["deals"],
    }).first()

    cohort_id = result.id if result else None
    logger.info(
        "[pricing-cohort] activated county=%s vertical=%s type=%s "
        "base=%d adjusted=%d pct=%.2f",
        county_id, trade_vertical, price_type, base_price_cents, adjusted, actual_pct,
    )
    return {
        "status": "activated",
        "cohort_id": cohort_id,
        "base_cents": base_price_cents,
        "adjusted_cents": adjusted,
        "adjustment_pct": actual_pct,
    }


# ── rollback ──────────────────────────────────────────────────────────────────

def rollback_cohort(
    county_id: str,
    trade_vertical: str,
    price_type: str,
    db: Session,
    *,
    reason: str = "manual_rollback",
) -> dict:
    """Roll back an active pricing cohort. Idempotent if already rolled back."""
    cohort = _get_active_cohort(county_id, trade_vertical, price_type, db)
    if not cohort:
        return {"status": "no_active_cohort"}

    db.execute(sa_text("""
        UPDATE pricing_cohorts
        SET status = 'rolled_back',
            rollback_reason = :reason,
            rolled_back_at = NOW(),
            updated_at = NOW()
        WHERE id = :id
    """), {"reason": reason, "id": cohort.id})

    logger.warning(
        "[pricing-cohort] rolled back county=%s vertical=%s type=%s reason=%s",
        county_id, trade_vertical, price_type, reason,
    )
    return {
        "status": "rolled_back",
        "cohort_id": cohort.id,
        "reason": reason,
    }


# ── auto-rollback trigger check ───────────────────────────────────────────────

def check_cohort_rollback_trigger(
    county_id: str,
    trade_vertical: str,
    price_type: str,
    db: Session,
) -> dict:
    """Check if an active cohort should roll back based on conversion rate signal.

    Compares post-activation conversion rate to the 7-day pre-activation baseline
    using z-test. Triggers rollback if drop > 2σ.

    Returns dict with 'action': 'rolled_back' | 'no_action' | 'no_cohort' | 'insufficient_data'.
    """
    cohort = _get_active_cohort(county_id, trade_vertical, price_type, db)
    if not cohort:
        return {"action": "no_cohort"}

    # Post-activation conversions from deal_outcomes since cohort activated.
    post = db.execute(sa_text("""
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN pipeline_stage = 'closed_won' THEN 1 ELSE 0 END) AS wins
        FROM deal_outcomes
        WHERE county_id = :county
          AND trade_vertical = :vertical
          AND created_at >= :since
    """), {
        "county": county_id,
        "vertical": trade_vertical,
        "since": cohort.activated_at,
    }).first()

    # Pre-activation 7-day baseline.
    pre = db.execute(sa_text("""
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN pipeline_stage = 'closed_won' THEN 1 ELSE 0 END) AS wins
        FROM deal_outcomes
        WHERE county_id = :county
          AND trade_vertical = :vertical
          AND created_at < :since
          AND created_at >= :since - INTERVAL '7 days'
    """), {
        "county": county_id,
        "vertical": trade_vertical,
        "since": cohort.activated_at,
    }).first()

    min_deals = PRICING_COHORT["min_deal_count"]
    n_post = int(post.total or 0)
    n_pre = int(pre.total or 0)

    if n_post < min_deals or n_pre < min_deals:
        return {"action": "insufficient_data", "post_n": n_post, "pre_n": n_pre}

    p_post = int(post.wins or 0) / n_post
    p_pre = int(pre.wins or 0) / n_pre

    sigma = PRICING_COHORT["rollback_trigger_sigma"]
    p_pool = (int(post.wins or 0) + int(pre.wins or 0)) / (n_post + n_pre)
    se = math.sqrt(p_pool * (1 - p_pool) * (1 / n_post + 1 / n_pre)) if p_pool not in (0, 1) else 0
    z = (p_post - p_pre) / se if se > 0 else 0.0

    if z < -sigma:
        result = rollback_cohort(
            county_id, trade_vertical, price_type, db,
            reason=f"auto-rollback: z={z:.3f} < -{sigma}σ (post={p_post:.3f} pre={p_pre:.3f})",
        )
        return {"action": "rolled_back", "z_score": round(z, 3), **result}

    return {
        "action": "no_action",
        "z_score": round(z, 3),
        "post_rate": round(p_post, 4),
        "pre_rate": round(p_pre, 4),
    }


# ── bulk evaluation (cron job entry point) ────────────────────────────────────

def evaluate_all_cohorts(db: Session) -> list[dict]:
    """Evaluate rollback triggers for all active pricing cohorts.

    Called by variant_mutation_job. Returns a list of per-cohort results.
    """
    active = db.execute(sa_text("""
        SELECT county_id, trade_vertical, price_type, id
        FROM pricing_cohorts
        WHERE status = 'active'
    """)).fetchall()

    results = []
    for row in active:
        try:
            result = check_cohort_rollback_trigger(
                row.county_id, row.trade_vertical, row.price_type, db
            )
            result["cohort_id"] = row.id
            results.append(result)
        except Exception:
            logger.exception(
                "[pricing-cohort] rollback check failed county=%s vertical=%s type=%s",
                row.county_id, row.trade_vertical, row.price_type,
            )
    return results
