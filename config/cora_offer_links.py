"""
Cora's per-offer link-resolution config — the "BookingLink model" per the
build plan, kept as a static config module rather than a DB table (no table
was ever actually required for a fixed per-offer/per-rep mapping,
independent of the no-new-migrations constraint this branch is under).

Two link kinds only:
  - "booking": a calendar link, read from existing settings (no new Stripe
    object, no fee mechanics). Used for hard_money_intro per the client's
    Q6 answer — lead handoff only until RESPA clearance is confirmed in
    writing; do not add commission/fee logic here.
  - "checkout": a Stripe checkout session, created via the existing
    src.services.stripe_service functions — never called directly from
    LLM-facing drafting code (src/agents/cora/offer_links.py is the only
    module that imports stripe_service on Cora's side).

Reuses existing config/settings.py Stripe price fields (stripe_price_*) by
name — never introduces a new settings.py field. Offers with no configured
price env resolve to a checkout link of None (drafts still validate; the
draft simply carries payment_link=None until a real price is wired).
"""
from __future__ import annotations

from typing import Literal, Optional, TypedDict

LinkKind = Literal["booking", "checkout"]


class OfferLinkConfig(TypedDict):
    offer: str
    link_kind: LinkKind
    # For "checkout": the attribute name on AppSettings holding the Stripe price id.
    stripe_price_env: Optional[str]
    # For "booking": which rep's calendar this offer routes to (settings attr name).
    calendar_settings_attr: Optional[str]
    # For "checkout": whether the existing checkout-creation function needs an
    # existing subscriber_stripe_customer_id. Cold prospects (Cora's whole
    # population) never have one — src.services.stripe_service's pack-style
    # functions (create_lead_pack_checkout, create_hot_lead_unlock_link)
    # require this param; its subscription-signup function
    # (create_subscription_checkout) does not. True here means "cannot
    # resolve a real payment_link for a cold prospect yet" — resolves to
    # None rather than calling a mismatched function.
    requires_existing_subscriber: bool


OFFER_LINKS: dict[str, OfferLinkConfig] = {
    "core_subscription": {
        "offer": "core_subscription",
        "link_kind": "checkout",
        "stripe_price_env": "stripe_price_starter_founding",
        "calendar_settings_attr": None,
        "requires_existing_subscriber": False,
    },
    "lead_packs": {
        "offer": "lead_packs",
        "link_kind": "checkout",
        "stripe_price_env": "stripe_price_lead_pack",
        "calendar_settings_attr": None,
        # create_lead_pack_checkout requires subscriber_stripe_customer_id —
        # not available for a cold prospect. Resolves to None until a
        # cold-checkout path exists for pack-style offers.
        "requires_existing_subscriber": True,
    },
    "insurance_distress_pack": {
        "offer": "insurance_distress_pack",
        "link_kind": "checkout",
        "stripe_price_env": "stripe_price_insurance_distress_pack",
        "calendar_settings_attr": None,
        "requires_existing_subscriber": True,
    },
    "bankruptcy_alert": {
        "offer": "bankruptcy_alert",
        "link_kind": "checkout",
        # No stripe_price_bankruptcy_alert env exists yet in config/settings.py.
        # Resolves to payment_link=None until that's wired — not a Cora-side gap.
        "stripe_price_env": None,
        "calendar_settings_attr": None,
        "requires_existing_subscriber": True,
    },
    "hard_money_intro": {
        "offer": "hard_money_intro",
        "link_kind": "booking",
        "stripe_price_env": None,
        "calendar_settings_attr": "demo_calendly_url",
        "requires_existing_subscriber": False,
    },
    "founder_tier": {
        "offer": "founder_tier",
        "link_kind": "checkout",
        "stripe_price_env": "stripe_price_founder_monthly",
        "calendar_settings_attr": None,
        "requires_existing_subscriber": False,
    },
    # NOTE: "single_ZIP_pack" removed — single-ZIP is not a distinct offer, it
    # is core_subscription at Starter (zip_limit=1) resolution (section 5.4).
    # ZIP quantity / $197 territory_lock upsell live in the revenue ladder, not
    # the offer catalog. The recommender no longer emits single_ZIP_pack.
    "concierge_wedge": {
        "offer": "concierge_wedge",
        "link_kind": "booking",
        "stripe_price_env": None,
        "calendar_settings_attr": "demo_calendly_url",
        "requires_existing_subscriber": False,
    },
}


def get_offer_link_config(offer: str) -> Optional[OfferLinkConfig]:
    return OFFER_LINKS.get(offer)
