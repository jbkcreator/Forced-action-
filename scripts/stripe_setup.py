"""
Stripe Product & Price Setup — one-time idempotent script.

Creates Forced Action subscription products and prices in Stripe if they
don't already exist. Safe to re-run — skips anything already present.

Usage:
    python scripts/stripe_setup.py              # dry-run (preview only)
    python scripts/stripe_setup.py --apply      # create in Stripe

After running with --apply, copy the printed env block into your .env file.
"""

import sys
import argparse
from pathlib import Path

# Allow running from project root
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import get_settings

# ---------------------------------------------------------------------------
# Pricing definition — single source of truth
# ---------------------------------------------------------------------------

VERTICALS = [
    "roofing", "remediation", "public_adjusters",
    "wholesalers", "fix_flip", "attorneys",
]

TIERS = {
    "starter": {
        "name":        "Forced Action Starter",
        "description": "1 ZIP territory — daily lead feed, CDS scoring, all event types",
        "founding":    60_000,   # $600 in cents
        "regular":     80_000,   # $800
    },
    "pro": {
        "name":        "Forced Action Pro",
        "description": "3 ZIP territories — priority delivery, skip-traced phones",
        "founding":    110_000,  # $1,100
        "regular":     150_000,  # $1,500
    },
    "dominator": {
        "name":        "Forced Action Dominator",
        "description": "Unlimited ZIPs — first-access delivery, dedicated account manager",
        "founding":    200_000,  # $2,000
        "regular":     280_000,  # $2,800
    },
}

ONE_TIME = {
    "lead_pack": {
        "name":        "Lead Pack",
        "description": "5 leads, 72hr exclusivity, 15min delivery",
        "amount":      9_900,    # $99
    },
    "hot_lead_unlock": {
        "name":        "Hot Lead Unlock",
        "description": "Unlock full contact details for a single hot lead",
        "amount":      15_000,   # $150
    },
}


def _find_existing_product(stripe, name: str):
    """Return first active product matching name, or None."""
    products = stripe.Product.list(limit=100, active=True)
    for p in products.auto_paging_iter():
        if p.name == name:
            return p
    return None


def _find_existing_price(stripe, product_id: str, amount: int, recurring: bool):
    """Return first active price matching product/amount/type, or None."""
    prices = stripe.Price.list(product=product_id, limit=20, active=True)
    for p in prices.data:
        if p.unit_amount != amount:
            continue
        if recurring and p.recurring and p.recurring.interval == "month":
            return p
        if not recurring and not p.recurring:
            return p
    return None


def run(apply: bool):
    import stripe as stripe_lib

    s = get_settings()
    stripe_lib.api_key = s.stripe_secret_key.get_secret_value()

    results = {}  # env_key -> price_id

    print(f"\n{'DRY RUN' if not apply else 'APPLYING'} — Stripe product/price setup\n")
    print("=" * 60)

    # ── Subscription tiers ────────────────────────────────────────
    for tier_key, tier in TIERS.items():
        print(f"\n[{tier_key.upper()}]")

        product = _find_existing_product(stripe_lib, tier["name"])
        if product:
            print(f"  Product exists: {product.id}")
        elif apply:
            product = stripe_lib.Product.create(
                name=tier["name"],
                description=tier["description"],
            )
            print(f"  Product created: {product.id}")
        else:
            print(f"  Would create product: {tier['name']}")
            product = type("P", (), {"id": "prod_DRY_RUN"})()

        for price_type in ("founding", "regular"):
            amount = tier[price_type]
            env_key = f"STRIPE_PRICE_{tier_key.upper()}_{price_type.upper()}"

            price = _find_existing_price(stripe_lib, product.id, amount, recurring=True)
            if price:
                print(f"  {price_type} price exists: {price.id}  (${amount // 100}/mo)")
                results[env_key] = price.id
            elif apply:
                price = stripe_lib.Price.create(
                    product=product.id,
                    unit_amount=amount,
                    currency="usd",
                    recurring={"interval": "month"},
                    nickname=f"{tier['name']} — {price_type.capitalize()}",
                )
                print(f"  {price_type} price created: {price.id}  (${amount // 100}/mo)")
                results[env_key] = price.id
            else:
                print(f"  Would create {price_type} price: ${amount // 100}/mo")
                results[env_key] = f"price_DRY_{tier_key.upper()}_{price_type.upper()}"

    # ── One-time products ─────────────────────────────────────────
    print(f"\n[ONE-TIME PRODUCTS]")
    for key, item in ONE_TIME.items():
        env_key = f"STRIPE_PRICE_{key.upper()}"
        product = _find_existing_product(stripe_lib, item["name"])

        if product:
            print(f"  Product exists: {product.id}  ({item['name']})")
        elif apply:
            product = stripe_lib.Product.create(
                name=item["name"],
                description=item["description"],
            )
            print(f"  Product created: {product.id}  ({item['name']})")
        else:
            print(f"  Would create product: {item['name']}")
            product = type("P", (), {"id": "prod_DRY_RUN"})()

        price = _find_existing_price(stripe_lib, product.id, item["amount"], recurring=False)
        if price:
            print(f"  Price exists: {price.id}  (${item['amount'] // 100})")
            results[env_key] = price.id
        elif apply:
            price = stripe_lib.Price.create(
                product=product.id,
                unit_amount=item["amount"],
                currency="usd",
            )
            print(f"  Price created: {price.id}  (${item['amount'] // 100})")
            results[env_key] = price.id
        else:
            print(f"  Would create price: ${item['amount'] // 100}")
            results[env_key] = f"price_DRY_{key.upper()}"

    # ── Print env block ───────────────────────────────────────────
    print("\n" + "=" * 60)
    print("Add to .env:")
    print("=" * 60)
    for k, v in results.items():
        print(f"{k}={v}")

    if not apply:
        print("\nRun with --apply to create missing products/prices in Stripe.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Stripe product/price setup")
    parser.add_argument("--apply", action="store_true", help="Actually create in Stripe (default: dry-run)")
    args = parser.parse_args()
    run(apply=args.apply)
