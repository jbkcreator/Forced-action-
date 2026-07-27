"""
Cora's offer-link resolver — the only module on Cora's side that imports
src.services.stripe_service. Isolated deliberately: drafting/LLM code
(subgraphs/outreach.py) calls resolve_offer_link(...) and gets back plain
strings; it never touches Stripe or settings directly.

hard_money_intro resolves to a calendar link ONLY — no fee mechanics, per
the client's Q6 answer (RESPA clearance not yet confirmed in writing).

Every "checkout"-kind offer currently resolves payment_link to None,
deliberately. Checked directly: none of the existing
src.services.stripe_service checkout functions fit a cold buyer-entity
prospect —
  - create_subscription_checkout(db, tier, vertical, county_id, zip_codes,
    ...) requires a property vertical/county/ZIP context that doesn't exist
    for a buyer entity at all (it's a property-owner subscription-signup
    flow, not an investor cold-outreach flow).
  - create_lead_pack_checkout / create_hot_lead_unlock_link both require an
    existing subscriber_stripe_customer_id — cold prospects, by definition,
    never have one yet.
Calling any of them with placeholder/None arguments would either raise
immediately or silently misrepresent the offer, so this module doesn't call
Stripe at all yet rather than shipping a call that's structurally wrong. A
draft with payment_link=None is still valid (the field is optional) — this
is flagged as real, unbuilt scope (a new cold-checkout entry point on the
Stripe side), not a Cora-side gap to paper over.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

from config.cora_offer_links import get_offer_link_config
from config.settings import get_settings

logger = logging.getLogger(__name__)


@dataclass
class ResolvedOfferLink:
    booking_link: Optional[str] = None
    payment_link: Optional[str] = None


def resolve_offer_link(
    offer: str,
    *,
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

    # link_kind == "checkout" — no existing stripe_service function fits a
    # cold buyer-entity prospect (see module docstring). Left as None,
    # deliberately, rather than calling a mismatched function.
    logger.info(
        "offer_links: offer=%r is checkout-kind but no cold-prospect checkout "
        "path exists yet in src.services.stripe_service — payment_link left None",
        offer,
    )
    return ResolvedOfferLink()
