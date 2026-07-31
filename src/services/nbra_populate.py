"""
NBRA population sweep (REVINT I1 — the missing populate loop).

REVINT-v2.2 (PR #185) built the scoring primitives — `recommend_offer`,
`get_or_create_score`, `nbra_engine.get_ranked_queue` — but nothing wires them
over the opportunity population, so `opportunity_scores` never fills and the
ranked queue stays empty. This is that wiring.

Per opportunity it: picks the offer (PR #185's `recommend_offer`), maps the
offer to a revenue type + price, computes the expected retained gross profit
over the retained-revenue horizon, and persists via PR #185's
`get_or_create_score`. The NBRA score (`rgp ÷ josh_minutes`) is computed inside
`get_or_create_score` — this sweep only supplies the money-value inputs.

The margin + horizon assumptions live HERE (PR #185 has no such concept): a
subscription is valued over a 12-month retained horizon, one-time offers at
face, both at a flat gross margin. That is deliberately the sweep's
responsibility — it is the money-value half of the objective the client asked
to protect (`expected retained gross profit ÷ founder-minutes`).

Day-1 scope: whales only (the sole population with an `opportunity_thread_id`).
Reads the `is_whale` flag — so it inherits the corrected flag split
(whale_detection.py) and scores the 156 real investors, not the pre-fix
homeowner-polluted list. The moment other segments mint thread IDs, extend
`_segment_for` + the whale source.

Called by: src/tasks/nbra_populate_sweep.py (nightly, after hunter_nightly_sweep).
"""
from __future__ import annotations

import logging
from typing import Any, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from src.core.models import RevenueType
from src.services.offer_recommendation import recommend_offer_for_entity
from src.services.opportunity_score import COLD_START_PRIORS, get_or_create_score
from src.services.whale_ranking import get_ranked_whales

logger = logging.getLogger(__name__)

# ── Money-value assumptions (owned here; PR #185 has no margin/horizon) ──────
DEFAULT_GROSS_MARGIN = 0.90
SUBSCRIPTION_HORIZON_MONTHS = 12

# ── Offer → revenue type ─────────────────────────────────────────────────────
# Only offers a day-1 whale can receive need a mapping; recommend_offer sends
# every whale to founder_tier (Priority 1). The rest are here so the sweep is
# correct if the offer rules widen. hard_money_intro → REFERRAL_FEE is
# intentionally omitted: its projection is RESPA-gated and get_or_create_score
# raises on it, so it is skipped before scoring.
REVENUE_TYPE_BY_OFFER: dict[str, RevenueType] = {
    "founder_tier": RevenueType.SUBSCRIPTION,
    "core_subscription": RevenueType.SUBSCRIPTION,
    "single_ZIP_pack": RevenueType.ONE_TIME,
    "lead_packs": RevenueType.ONE_TIME,
    "insurance_distress_pack": RevenueType.ONE_TIME,
    "bankruptcy_alert": RevenueType.ONE_TIME,
}

# ── Offer → the plan row whose price seeds the estimate ──────────────────────
# Price is authoritative in the `plans` table, never hardcoded. Only offers
# backed by a plan row appear here; others resolve to no price and are skipped.
PLAN_ID_BY_OFFER: dict[str, str] = {
    "founder_tier": "founder_monthly",
    "core_subscription": "starter",
}

# ── Offer → founder action type (drives josh_minutes in get_or_create_score) ─
# A whale founder-tier outreach is a personal call → PR #185's call_outreach
# default is 15 min, matching the grilled estimate.
ACTION_TYPE_BY_OFFER: dict[str, str] = {
    "founder_tier": "call_outreach",
    "core_subscription": "email_outreach",
    "single_ZIP_pack": "email_outreach",
    "lead_packs": "email_outreach",
    "insurance_distress_pack": "email_outreach",
    "bankruptcy_alert": "email_outreach",
}

# Offers whose projection is disabled (RESPA) — skipped before scoring.
RESPA_GATED_OFFERS = frozenset({"hard_money_intro"})
# Booking-only / no-price offers a cold whale can't be scored on yet.
NON_REVENUE_OFFERS = frozenset({"concierge_wedge"})


def _resolve_plan_price(db: Session, plan_id: str) -> tuple[int, Optional[str]]:
    """(price_cents, interval) for a plan; (0, None) if missing/inactive."""
    row = db.execute(
        text("SELECT price_cents, interval FROM plans WHERE plan_id = :pid AND is_active = true"),
        {"pid": plan_id},
    ).first()
    if not row:
        logger.warning("nbra_populate: plan_id %r not found/inactive — skipping", plan_id)
        return 0, None
    return int(row.price_cents or 0), row.interval


def _revenue_estimate(
    revenue_type: RevenueType, price_cents: int, interval: Optional[str]
) -> tuple[int, Optional[int], Optional[str]]:
    """(expected_revenue_cents, expected_mrr_cents, billing_interval).

    Subscriptions are valued over the 12-month retained horizon; one-time at
    face. MRR only meaningful for subscriptions.
    """
    if revenue_type == RevenueType.SUBSCRIPTION:
        if interval == "annual":
            return price_cents, round(price_cents / 12), "annual"
        return price_cents * SUBSCRIPTION_HORIZON_MONTHS, price_cents, (interval or "monthly")
    return price_cents, None, None


def _segment_for(whale: dict) -> str:
    """Day 1 every ranked whale is the `whale` segment. Extend when other
    opportunity pipelines start minting thread IDs."""
    return "whale"


def _buyer_entity_signals(whale: dict) -> dict[str, Any]:
    """Shape a get_ranked_whales row into the dict recommend_offer expects."""
    return {
        "is_whale": True,
        "entity_type": whale.get("entity_type"),
        "total_purchase_count": whale.get("total_purchase_count", 0),
        "entity_links": [],
        "signals": [],
    }


def populate_opportunity_scores(
    db: Session, county_id: Optional[str] = None, whale_limit: int = 1000
) -> dict:
    """Score every current whale through PR #185's primitives. Caller commits.

    Idempotent: get_or_create_score returns the existing row per
    (thread, action_type, revenue_type), so re-running a sweep does not
    duplicate. Returns a summary.
    """
    whales = [w for w in get_ranked_whales(db, limit=whale_limit, county_id=county_id)
              if w.get("opportunity_thread_id")]

    scored = skipped_no_price = skipped_respa = skipped_no_offer = 0
    for w in whales:
        rec = recommend_offer_for_entity(_buyer_entity_signals(w))
        offer = rec["offer"]

        if offer in RESPA_GATED_OFFERS:
            skipped_respa += 1
            continue
        if offer in NON_REVENUE_OFFERS or offer not in REVENUE_TYPE_BY_OFFER:
            skipped_no_offer += 1
            continue

        plan_id = PLAN_ID_BY_OFFER.get(offer)
        price_cents, interval = _resolve_plan_price(db, plan_id) if plan_id else (0, None)
        if price_cents <= 0:
            skipped_no_price += 1
            continue

        revenue_type = REVENUE_TYPE_BY_OFFER[offer]
        segment = _segment_for(w)
        expected_revenue, mrr, billing_interval = _revenue_estimate(revenue_type, price_cents, interval)

        # RGP uses the same segment prior get_or_create_score will store, so the
        # numerator is internally consistent with p_close.
        p_close = COLD_START_PRIORS.get(segment, COLD_START_PRIORS["default"])["p_close"]
        rgp = round(p_close * expected_revenue * DEFAULT_GROSS_MARGIN)

        get_or_create_score(
            db,
            buyer_entity_id=w["entity_id"],
            opportunity_thread_id=w["opportunity_thread_id"],
            segment=segment,
            revenue_type=revenue_type,
            expected_revenue_cents=expected_revenue,
            expected_retained_gross_profit_cents=rgp,
            expected_mrr_cents=mrr,
            billing_interval=billing_interval,
            source_action_type=ACTION_TYPE_BY_OFFER.get(offer, "email_outreach"),
            is_automated=False,
        )
        scored += 1

    summary = {
        "whales": len(whales),
        "scored": scored,
        "skipped_no_price": skipped_no_price,
        "skipped_respa": skipped_respa,
        "skipped_no_offer": skipped_no_offer,
    }
    logger.info("nbra_populate: %s", summary)
    return summary
