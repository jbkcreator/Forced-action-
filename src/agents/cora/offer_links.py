"""
Cora's offer-link resolver — the only module on Cora's side that imports
src.services.stripe_service. Isolated deliberately: drafting/LLM code
(subgraphs/outreach.py) calls resolve_offer_link(...) and gets back plain
strings; it never touches Stripe or settings directly.

hard_money_intro resolves to a calendar link ONLY — no fee mechanics, per
the client's Q6 answer (RESPA clearance not yet confirmed in writing).

Checkout-kind offers, one at a time, checked directly against the real
src.services.stripe_service function bodies before calling any of them:

  - founder_tier: resolves via a direct stripe.checkout.Session.create call
    in this module, NOT via src.services.stripe_service.create_subscription_
    checkout. That function's price resolution for tier=="founder" reads
    plans.stripe_price_id from the DB — confirmed live-broken in this
    environment (a stale/mismatched price id seeded before this build,
    unrelated to it) and, per direct instruction, Cora's own checkout
    should source its price id from settings (config.settings.get_settings()
    .active_stripe_price("founder_monthly"/"founder_annual")) instead of the
    DB either way. That helper is already mode-aware (live vs
    STRIPE_TEST_MODE) — both stripe_price_founder_monthly and
    stripe_test_price_founder_monthly are real, already-defined settings
    fields. vertical/county_id have no bearing on the founder tier's price
    at all (flat-rate, no FoundingSubscriberCount lock), so they only ever
    go into Stripe metadata here, same as the shared function would do.
  - core_subscription (tier="starter"), lead_packs, insurance_distress_pack,
    bankruptcy_alert: still resolve payment_link=None. tier="starter" (and
    every other real tier) DOES key a live FoundingSubscriberCount row by
    (tier, vertical, county_id) — a cold buyer entity has no real vertical
    of ITS OWN in that sense (unlike founder_tier's flat-rate path, this
    would decrement a real county's founding-slot count for a lead that
    hasn't converted). create_lead_pack_checkout / create_hot_lead_unlock_link
    both require an existing subscriber_stripe_customer_id, which cold
    prospects never have. Flagged as real, unbuilt scope (a dedicated
    cold-checkout entry point on the Stripe side for non-founder tiers),
    not a Cora-side gap to paper over.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, Optional

from config.cora_offer_links import get_offer_link_config
from config.settings import get_settings

logger = logging.getLogger(__name__)

_COLD_PROSPECT_VERTICAL = "investor"


@dataclass
class ResolvedOfferLink:
    booking_link: Optional[str] = None
    payment_link: Optional[str] = None


def _resolve_founder_tier_checkout(
    buyer_entity: Dict[str, Any],
    success_url: Optional[str],
    cancel_url: Optional[str],
    customer_email: Optional[str],
    interval: str = "monthly",
) -> Optional[str]:
    import stripe

    from src.services.stripe_service import _init_stripe

    settings = get_settings()

    if not success_url or not cancel_url:
        success_url = success_url or f"{settings.app_base_url}/success?session_id={{CHECKOUT_SESSION_ID}}"
        cancel_url = cancel_url or f"{settings.app_base_url}/"

    if not _init_stripe():
        logger.info("offer_links: Stripe not configured — founder_tier payment_link left None")
        return None

    price_name = "founder_annual" if interval == "annual" else "founder_monthly"
    price_id = settings.active_stripe_price(price_name)
    if not price_id:
        logger.warning(
            "offer_links: no Stripe price configured for %s (STRIPE_PRICE_FOUNDER_%s / "
            "STRIPE_TEST_PRICE_FOUNDER_%s, mode-aware via active_stripe_price) — payment_link left None",
            price_name, price_name.split("_")[1].upper(), price_name.split("_")[1].upper(),
        )
        return None

    try:
        session = stripe.checkout.Session.create(
            mode="subscription",
            payment_method_types=["card"],
            customer_email=customer_email,
            phone_number_collection={"enabled": True},
            line_items=[{"price": price_id, "quantity": 1}],
            success_url=success_url,
            cancel_url=cancel_url,
            metadata={
                "tier": "founder",
                "vertical": _COLD_PROSPECT_VERTICAL,
                "county_id": buyer_entity.get("county_id") or "unknown",
                "source": "cora_cold_outreach",
                "buyer_entity_id": str(buyer_entity.get("id") or ""),
            },
            subscription_data={
                "metadata": {"tier": "founder", "source": "cora_cold_outreach"},
            },
        )
        return session.url
    except Exception:
        logger.exception("offer_links: founder_tier checkout creation failed for buyer_entity_id=%s", buyer_entity.get("id"))
        return None


def resolve_offer_link(
    offer: str,
    *,
    buyer_entity: Optional[Dict[str, Any]] = None,
    db: Any = None,
    success_url: Optional[str] = None,
    cancel_url: Optional[str] = None,
    customer_email: Optional[str] = None,
) -> ResolvedOfferLink:
    config = get_offer_link_config(offer)
    if config is None:
        logger.warning("offer_links: no config for offer=%r", offer)
        return ResolvedOfferLink()

    settings = get_settings()

    if config["link_kind"] == "booking":
        attr = config.get("calendar_settings_attr")
        link = getattr(settings, attr, None) if attr else None
        if not link:
            logger.info("offer_links: no calendar link configured for offer=%r", offer)
        return ResolvedOfferLink(booking_link=link)

    if offer == "founder_tier" and buyer_entity is not None:
        return ResolvedOfferLink(payment_link=_resolve_founder_tier_checkout(
            buyer_entity, success_url, cancel_url, customer_email,
        ))

    # Every other checkout-kind offer — no existing stripe_service function
    # fits a cold buyer-entity prospect yet (see module docstring). Left as
    # None, deliberately, rather than calling a mismatched function.
    logger.info(
        "offer_links: offer=%r is checkout-kind but no cold-prospect checkout "
        "path exists yet in src.services.stripe_service — payment_link left None",
        offer,
    )
    return ResolvedOfferLink()
