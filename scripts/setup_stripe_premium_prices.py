"""
One-shot helper: create the four Stage 5 premium SKU products + prices in
Stripe (test mode), then print the env-var lines you should add to .env.

Idempotent: looks for existing products by their well-known names and
reuses if found. Prints all four price IDs at the end.

Usage:
    python -m scripts.setup_stripe_premium_prices
"""
from __future__ import annotations

import sys

import stripe

from config.settings import settings


SKUS = [
    {
        "key": "premium_report",
        "env_var": "STRIPE_TEST_PRICE_PREMIUM_REPORT",
        "name": "Forced Action — Property Report",
        "description": "Full distress + ownership + financials dossier on a parcel.",
        "amount_cents": 700,    # $7
    },
    {
        "key": "premium_brief",
        "env_var": "STRIPE_TEST_PRICE_PREMIUM_BRIEF",
        "name": "Forced Action — Lead Brief",
        "description": "Investor-grade lead brief with talking points + comps.",
        "amount_cents": 1200,   # $12
    },
    {
        "key": "premium_transfer",
        "env_var": "STRIPE_TEST_PRICE_PREMIUM_TRANSFER",
        "name": "Forced Action — Skip-Trace Transfer",
        "description": "Full skip-trace transfer with mobile/landline/email + relatives.",
        "amount_cents": 6500,   # $65
    },
    {
        "key": "premium_byol",
        "env_var": "STRIPE_TEST_PRICE_PREMIUM_BYOL",
        "name": "Forced Action — BYOL Skip-Trace",
        "description": "Bring-your-own-lead skip-trace on any address you supply.",
        "amount_cents": 500,    # $5
    },
]


def _init_stripe() -> bool:
    if not settings.stripe_test_mode:
        print("[error] STRIPE_TEST_MODE must be true in your .env.")
        return False
    key = settings.active_stripe_secret_key
    if not key:
        print("[error] STRIPE_TEST_SECRET_KEY not set in .env.")
        return False
    stripe.api_key = key.get_secret_value()
    return True


def find_existing_product(name: str) -> str | None:
    """Search for a Product by exact name. Returns product id or None."""
    products = stripe.Product.search(query=f'name:"{name}" AND active:"true"')
    for p in products.data:
        if p.name == name:
            return p.id
    return None


def find_existing_price(product_id: str, amount_cents: int) -> str | None:
    """Look for a price on this product matching the desired amount."""
    prices = stripe.Price.list(product=product_id, active=True, limit=20)
    for p in prices.data:
        if p.unit_amount == amount_cents and p.currency == "usd" and p.recurring is None:
            return p.id
    return None


def setup_one(sku: dict) -> dict:
    name = sku["name"]
    amount_cents = sku["amount_cents"]

    product_id = find_existing_product(name)
    if not product_id:
        product = stripe.Product.create(
            name=name,
            description=sku["description"],
            metadata={"stage5_sku": sku["key"]},
        )
        product_id = product.id
        created_product = True
    else:
        created_product = False

    price_id = find_existing_price(product_id, amount_cents)
    if not price_id:
        price = stripe.Price.create(
            product=product_id,
            unit_amount=amount_cents,
            currency="usd",
            metadata={"stage5_sku": sku["key"]},
        )
        price_id = price.id
        created_price = True
    else:
        created_price = False

    return {
        "key": sku["key"],
        "env_var": sku["env_var"],
        "product_id": product_id,
        "price_id": price_id,
        "created_product": created_product,
        "created_price": created_price,
    }


def main() -> int:
    if not _init_stripe():
        return 2

    print("Setting up Stage 5 premium SKUs in Stripe (test mode)...")
    print()
    results = []
    for sku in SKUS:
        try:
            r = setup_one(sku)
            results.append(r)
            tag = ("created" if r["created_product"] else "reused") + " product, " + ("created" if r["created_price"] else "reused") + " price"
            print(f"  [{tag:42}] {r['key']:20} -> {r['price_id']}")
        except stripe.error.StripeError as exc:
            print(f"  [FAILED] {sku['key']}: {exc}")
            return 3
    print()
    print("=" * 78)
    print("ADD THESE LINES TO .env (Forced-action- backend):")
    print("=" * 78)
    print()
    for r in results:
        print(f"{r['env_var']}={r['price_id']}")
    print()
    print("=" * 78)
    print("Then restart the backend (uvicorn) so the new env vars are picked up.")
    print("=" * 78)
    return 0


if __name__ == "__main__":
    sys.exit(main())
