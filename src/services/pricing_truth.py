"""
Stripe pricing config diagnostic (advisory).

Verifies that each *configured* price the app can sell resolves to an **active**
Stripe price with a fixed ``unit_amount`` in the currently active Stripe mode.
Returns:

    {"ok": bool, "problems": [{"name": str, "surface": str, "tier": str,
                                "price_id": str, "reason": str}]}

``reason`` is one of ``not_found`` (Stripe 404 / StripeError), ``inactive``
(price archived), or ``no_amount`` (metered / no fixed unit_amount).

Why advisory, not a hard gate
-----------------------------
The landing/deal-room surfaces display Stripe's *own* amounts (fetched live via
``GET /api/pricing``), so there is no independent "displayed" figure to
reconcile against — an amount comparison against a hard-coded table could only
manufacture false mismatches and take the deal-room down over stale constants.
This check therefore only surfaces genuinely broken (unsellable) price config,
for logging/monitoring. Callers must NOT use it to hard-block a request; a bad
price ID reveals itself at charge time in ``/api/checkout``, not on page load.

No DB session required.
"""

from __future__ import annotations

import logging
from typing import Optional

import stripe

from config.settings import get_settings

logger = logging.getLogger(__name__)

# Price name (for the mode-aware settings.active_stripe_price(name) resolver),
# surface, tier. hold_deposit resolves via a dedicated mode-aware property (its
# live var is HOLD_DEPOSIT_PRICE_ID, not the stripe_price_* convention) —
# handled in _resolve_price_id below.
_PRICE_TABLE: list[tuple[str, str, str]] = [
    # (price_name, surface, tier)
    # ── Subscription tiers ──────────────────────────────────────────────────
    ("starter_founding",  "subscription", "starter_founding"),
    ("starter_regular",   "subscription", "starter_regular"),
    ("pro_founding",      "subscription", "pro_founding"),
    ("pro_regular",       "subscription", "pro_regular"),
    ("founder_monthly",   "subscription", "founder_monthly"),
    ("annual_lock",       "subscription", "annual_lock"),
    # ── Hold deposit ────────────────────────────────────────────────────────
    ("hold_deposit",      "checkout",     "hold_deposit"),
    # ── Wallet subscription tiers ───────────────────────────────────────────
    ("wallet_starter",    "wallet",       "starter"),
    ("wallet_growth",     "wallet",       "growth"),
    ("wallet_power",      "wallet",       "power"),
    # ── ICP channel add-ons ─────────────────────────────────────────────────
    ("icp_rei_investor",       "icp", "rei_investor"),
    ("icp_insurance_adjuster", "icp", "insurance_adjuster"),
    ("icp_hard_money_lender",  "icp", "hard_money_lender"),
    ("icp_property_manager",   "icp", "property_manager"),
    ("icp_bankruptcy_attorney","icp", "bankruptcy_attorney"),
    ("icp_title_company",      "icp", "title_company"),
]


def _resolve_price_id(settings, name: str) -> Optional[str]:
    """Mode-aware price-ID lookup — MUST match the mode of the active key, or a
    test key ends up querying live price IDs (and vice-versa), which 404s.
    hold_deposit uses its own property because its live env var breaks the
    stripe_price_* naming convention."""
    if name == "hold_deposit":
        return settings.active_hold_deposit_price_id
    return settings.active_stripe_price(name)


def _active_key() -> Optional[str]:
    s = get_settings()
    sk = s.active_stripe_secret_key
    if sk is None:
        return None
    return sk.get_secret_value()


def check() -> dict:
    """
    Diagnose the configured Stripe prices in the active mode.

    Returns:
        {"ok": True, "problems": []} when every configured price is sellable
            (or when Stripe is not configured — nothing to diagnose).
        {"ok": False, "problems": [...]} when one or more configured prices are
            missing, archived, or have no fixed amount. All problems are
            collected (no short-circuit).

    Advisory only — never raises for the caller to treat as a hard failure.
    """
    problems: list[dict] = []
    try:
        api_key = _active_key()
        if not api_key:
            # Nothing to diagnose without a key — don't cry wolf (advisory).
            logger.warning("pricing_truth.check: no active Stripe secret key configured")
            return {"ok": True, "problems": []}

        settings = get_settings()

        for name, surface, tier in _PRICE_TABLE:
            price_id: Optional[str] = _resolve_price_id(settings, name)
            if not price_id:
                # Not configured in this environment — not a problem.
                continue

            try:
                price_obj = stripe.Price.retrieve(price_id, api_key=api_key)
            except stripe.StripeError as exc:
                logger.warning(
                    "pricing_truth: %s (price_id=%s) not retrievable: %s",
                    name, price_id, exc,
                )
                problems.append({"name": name, "surface": surface, "tier": tier,
                                 "price_id": price_id, "reason": "not_found"})
                continue

            # Attribute access, not .get() — Stripe SDK >=15 objects are not
            # dicts and raise AttributeError on .get() (see stripe-sdk skew).
            if getattr(price_obj, "active", True) is False:
                problems.append({"name": name, "surface": surface, "tier": tier,
                                 "price_id": price_id, "reason": "inactive"})
            elif getattr(price_obj, "unit_amount", None) is None:
                problems.append({"name": name, "surface": surface, "tier": tier,
                                 "price_id": price_id, "reason": "no_amount"})

        return {"ok": len(problems) == 0, "problems": problems}

    except Exception:
        logger.exception("pricing_truth.check: unexpected error (advisory — not blocking)")
        return {"ok": False, "problems": problems}
