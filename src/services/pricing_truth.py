"""
Stripe pricing reconciliation service.

Fetches live Stripe price objects and compares their unit_amount against the
amounts displayed on the frontend. Returns a structured result:

    {"ok": bool, "mismatches": [{"surface": str, "tier": str,
                                  "displayed_cents": int, "stripe_cents": int}]}

Fails closed: any exception during a Stripe call → ok=False, mismatches=[].
No DB session required.

Displayed-amount sources
------------------------
- Subscription founding/regular tiers: src/config/pricing.js (values in USD,
  converted to cents by ×100).
- hold_deposit: $97 (DealRoomPage.jsx / CancelModal.jsx).
- annual_lock: $1,970/yr flat rate (pricing_cohort_engine.py comment).
- wallet tiers: base prices ($49/$99/$199) from AcceleratedWalletOfferModal
  defaults and pricing_cohort_engine.py lower bounds.
- ICP channel add-ons: $197/mo (issue spec, SettingsPage.jsx).
"""

from __future__ import annotations

import logging
from typing import Optional

import stripe

from config.settings import get_settings

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Price table: (surface, tier) → displayed_cents
# Values come from src/config/pricing.js (×100) and frontend components.
# ---------------------------------------------------------------------------

_PRICE_TABLE: list[tuple[str, str, str, int]] = [
    # (settings_attr, surface, tier, displayed_cents)
    # ── Subscription tiers (pricing.js values in USD × 100) ─────────────────
    ("stripe_price_starter_founding",  "subscription", "starter_founding",  60_000),
    ("stripe_price_starter_regular",   "subscription", "starter_regular",   80_000),
    ("stripe_price_pro_founding",      "subscription", "pro_founding",     110_000),
    ("stripe_price_pro_regular",       "subscription", "pro_regular",      150_000),
    # founder_monthly ≈ $1,100/mo (cohort engine founder floor, pricing.js pro.founding)
    ("stripe_price_founder_monthly",   "subscription", "founder_monthly",  110_000),
    # annual_lock = $1,970/yr flat (pricing_cohort_engine.py: "flat $1970/yr rate")
    ("stripe_price_annual_lock",       "subscription", "annual_lock",      197_000),
    # ── Hold deposit ────────────────────────────────────────────────────────
    ("hold_deposit_price_id",          "checkout",     "hold_deposit",       9_700),
    # ── Wallet subscription tiers ───────────────────────────────────────────
    ("stripe_price_wallet_starter",    "wallet",       "starter",            4_900),
    ("stripe_price_wallet_growth",     "wallet",       "growth",             9_900),
    ("stripe_price_wallet_power",      "wallet",       "power",             19_900),
    # ── ICP channel add-ons ($197/mo each) ─────────────────────────────────
    ("stripe_price_icp_rei_investor",       "icp", "rei_investor",       19_700),
    ("stripe_price_icp_insurance_adjuster", "icp", "insurance_adjuster", 19_700),
    ("stripe_price_icp_hard_money_lender",  "icp", "hard_money_lender",  19_700),
    ("stripe_price_icp_property_manager",   "icp", "property_manager",   19_700),
    ("stripe_price_icp_bankruptcy_attorney","icp", "bankruptcy_attorney",19_700),
    ("stripe_price_icp_title_company",      "icp", "title_company",      19_700),
]


def _active_key() -> Optional[str]:
    s = get_settings()
    sk = s.active_stripe_secret_key
    if sk is None:
        return None
    return sk.get_secret_value()


def check() -> dict:
    """
    Reconcile all configured Stripe prices against displayed frontend amounts.

    Returns:
        {"ok": True, "mismatches": []} when every configured price matches.
        {"ok": False, "mismatches": [...]} when one or more differ.
        {"ok": False, "mismatches": []} on any exception (fail closed).
    """
    try:
        api_key = _active_key()
        if not api_key:
            logger.warning("pricing_truth.check: no active Stripe secret key configured")
            return {"ok": False, "mismatches": []}

        settings = get_settings()
        mismatches: list[dict] = []

        for attr, surface, tier, displayed_cents in _PRICE_TABLE:
            price_id: Optional[str] = getattr(settings, attr, None)
            if not price_id:
                # Not configured in this environment — skip silently.
                continue

            try:
                price_obj = stripe.Price.retrieve(price_id, api_key=api_key)
            except stripe.StripeError as exc:
                logger.error(
                    "pricing_truth: Stripe error fetching %s (price_id=%s): %s",
                    attr, price_id, exc,
                )
                return {"ok": False, "mismatches": []}

            stripe_cents: Optional[int] = price_obj.get("unit_amount")
            if stripe_cents is None:
                # Metered or multi-currency price without a fixed unit_amount.
                logger.warning(
                    "pricing_truth: price %s (%s/%s) has no unit_amount — skipping",
                    price_id, surface, tier,
                )
                continue

            if stripe_cents != displayed_cents:
                mismatches.append(
                    {
                        "surface": surface,
                        "tier": tier,
                        "displayed_cents": displayed_cents,
                        "stripe_cents": stripe_cents,
                    }
                )

        return {"ok": len(mismatches) == 0, "mismatches": mismatches}

    except Exception:
        logger.exception("pricing_truth.check: unexpected error — failing closed")
        return {"ok": False, "mismatches": []}
