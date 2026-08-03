"""
One-off Stripe live Starter checkout link for manual E2E testing.

Replaces the removed GET /api/test/starter-checkout-link endpoint (commit
9d47c6ee / PR #170), which was unauthenticated, unrate-limited, and stayed
live at dev HEAD long after its own "remove once testing is done" comment.
A checkout link for manual testing does not need to be a permanent HTTP
route — this mints one live session on demand and prints the URL.

Run from repo root (always uses the LIVE secret key, regardless of the
local STRIPE_TEST_MODE toggle — this creates a REAL live Stripe Checkout
Session, not a fixture):
    python -m scripts.mint_starter_checkout_link --zip 33601 --county hillsborough

The printed URL expires in 24 hours — Stripe's hard maximum for hosted
Checkout Sessions (expires_at accepts 30 minutes to 24 hours out; it
cannot be extended further). Re-run this script for a fresh link.
"""
from __future__ import annotations

import argparse
import re
import sys
import time

import stripe

from config.settings import get_settings
from src.core.database import get_db_context
from src.services.stripe_service import get_price_id_for_checkout

_ZIP_RE = re.compile(r"^\d{5}$")
_MAX_SESSION_TTL_SECONDS = 24 * 60 * 60  # Stripe's hard max for expires_at


def mint(zip_code: str, county_id: str, vertical: str = "roofing") -> str:
    if not _ZIP_RE.match(zip_code):
        raise SystemExit(f"Invalid zip: {zip_code!r} — must be 5 digits")

    settings = get_settings()
    if not settings.stripe_secret_key:
        raise SystemExit("STRIPE_SECRET_KEY not set — required for a live checkout link")
    stripe.api_key = settings.stripe_secret_key.get_secret_value()

    with get_db_context() as db:
        price_id, _ = get_price_id_for_checkout(db, "starter", vertical, county_id, "monthly")

    session = stripe.checkout.Session.create(
        mode="subscription",
        ui_mode="hosted",
        line_items=[{"price": price_id, "quantity": 1}],
        allow_promotion_codes=True,
        phone_number_collection={"enabled": True},
        expires_at=int(time.time()) + _MAX_SESSION_TTL_SECONDS,
        success_url=f"{settings.app_base_url}/success?session_id={{CHECKOUT_SESSION_ID}}",
        cancel_url=f"{settings.app_base_url}/",
        metadata={
            "tier": "starter",
            "vertical": vertical,
            "county_id": county_id,
            "zip_codes": zip_code,
            "manual_test_link": "true",
        },
    )
    return session.url


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--zip", dest="zip_code", required=True, help="5-digit ZIP for the test subscription")
    parser.add_argument("--county", dest="county_id", default="hillsborough")
    parser.add_argument("--vertical", dest="vertical", default="roofing")
    args = parser.parse_args()

    try:
        url = mint(args.zip_code, args.county_id, args.vertical)
    except stripe.error.StripeError as e:
        print(f"Stripe error: {e}", file=sys.stderr)
        sys.exit(1)

    print(url)
